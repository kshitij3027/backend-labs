"""False positive management: anomaly grouping, feedback tracking, and accuracy reporting."""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class AnomalyGroup:
    """A cluster of related anomalies sharing common features."""

    group_id: str
    anomalies: list[dict]
    common_features: dict
    first_seen: datetime
    last_seen: datetime
    count: int


class FalsePositiveManager:
    """Tracks anomaly feedback, groups similar anomalies, and reports accuracy.

    Anomalies within a configurable time window that share an IP subnet or
    status code are grouped together so operators can triage them as a batch.

    Thread-safe via an internal lock.

    Args:
        time_window: Maximum seconds between anomalies to be grouped together.
        feature_similarity_threshold: Reserved for future use (currently unused).
    """

    # Anomalies older than this are pruned from the recent buffer.
    _PRUNE_AGE_SECONDS: float = 300.0  # 5 minutes

    def __init__(
        self,
        time_window: float = 60.0,
        feature_similarity_threshold: float = 0.8,
    ) -> None:
        self._time_window = time_window
        self._feature_similarity_threshold = feature_similarity_threshold

        self._recent_anomalies: list[dict] = []
        self._feedback: dict[str, bool] = {}
        self._type_accuracy: dict[str, dict] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add_anomaly(self, anomaly_dict: dict) -> None:
        """Record a new anomaly for grouping.

        Entries older than 5 minutes are pruned automatically.

        Args:
            anomaly_dict: Serialisable dict describing the anomaly (must
                include ``_added_ts`` or one will be set automatically).
        """
        with self._lock:
            anomaly_dict.setdefault("_added_ts", time.time())
            self._recent_anomalies.append(anomaly_dict)
            self._prune_old()

    def group_anomalies(self) -> list[AnomalyGroup]:
        """Group recent anomalies by similar features.

        Grouping criteria:
        - Same IP subnet (first three octets match).
        - Same status code.
        - Within ``time_window`` seconds of each other.

        Returns:
            A list of :class:`AnomalyGroup` instances.
        """
        with self._lock:
            self._prune_old()
            anomalies = list(self._recent_anomalies)

        if not anomalies:
            return []

        groups: list[AnomalyGroup] = []
        used: set[int] = set()

        for i, a in enumerate(anomalies):
            if i in used:
                continue

            cluster = [a]
            used.add(i)

            for j, b in enumerate(anomalies):
                if j in used:
                    continue
                if self._are_similar(a, b):
                    cluster.append(b)
                    used.add(j)

            # Build the group
            common = self._extract_common_features(cluster)
            timestamps = [
                datetime.fromtimestamp(c.get("_added_ts", 0), tz=timezone.utc)
                for c in cluster
            ]

            groups.append(
                AnomalyGroup(
                    group_id=str(uuid.uuid4()),
                    anomalies=cluster,
                    common_features=common,
                    first_seen=min(timestamps),
                    last_seen=max(timestamps),
                    count=len(cluster),
                )
            )

        return groups

    def record_feedback(self, anomaly_id: str, confirmed: bool) -> None:
        """Record operator feedback for an anomaly.

        Updates the per-type accuracy tracking so callers can query
        historical accuracy of each anomaly type.

        Args:
            anomaly_id: Unique identifier of the anomaly.
            confirmed: ``True`` if the operator confirms it is a real anomaly.
        """
        with self._lock:
            self._feedback[anomaly_id] = confirmed

            # Infer type from stored anomalies (best-effort lookup)
            anomaly_type = self._infer_type(anomaly_id)

            if anomaly_type not in self._type_accuracy:
                self._type_accuracy[anomaly_type] = {"confirmed": 0, "dismissed": 0}

            if confirmed:
                self._type_accuracy[anomaly_type]["confirmed"] += 1
            else:
                self._type_accuracy[anomaly_type]["dismissed"] += 1

    def get_historical_accuracy(self) -> dict:
        """Return per-type accuracy counts.

        Returns:
            Dict mapping anomaly type strings to ``{confirmed: int, dismissed: int}``.
        """
        with self._lock:
            return {k: dict(v) for k, v in self._type_accuracy.items()}

    def get_stats(self) -> dict:
        """Return a summary of feedback and grouping state.

        Returns:
            Dict with total_feedback, confirmed_count, dismissed_count, groups_count.
        """
        with self._lock:
            confirmed = sum(1 for v in self._feedback.values() if v)
            dismissed = sum(1 for v in self._feedback.values() if not v)
            self._prune_old()
            anomalies = list(self._recent_anomalies)

        groups = self.group_anomalies()

        return {
            "total_feedback": confirmed + dismissed,
            "confirmed_count": confirmed,
            "dismissed_count": dismissed,
            "groups_count": len(groups),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _prune_old(self) -> None:
        """Remove entries older than ``_PRUNE_AGE_SECONDS`` (caller holds lock)."""
        cutoff = time.time() - self._PRUNE_AGE_SECONDS
        self._recent_anomalies = [
            a for a in self._recent_anomalies if a.get("_added_ts", 0) >= cutoff
        ]

    def _get_subnet(self, ip: str) -> str:
        """Return the first three octets of an IPv4 address."""
        parts = ip.split(".")
        if len(parts) >= 3:
            return ".".join(parts[:3])
        return ip

    def _are_similar(self, a: dict, b: dict) -> bool:
        """Check whether two anomaly dicts should be grouped together."""
        # Time proximity
        ts_a = a.get("_added_ts", 0)
        ts_b = b.get("_added_ts", 0)
        if abs(ts_a - ts_b) > self._time_window:
            return False

        # IP subnet match
        ip_a = a.get("ip", "")
        ip_b = b.get("ip", "")
        if ip_a and ip_b and self._get_subnet(ip_a) == self._get_subnet(ip_b):
            return True

        # Status code match
        sc_a = a.get("status_code")
        sc_b = b.get("status_code")
        if sc_a is not None and sc_b is not None and sc_a == sc_b:
            return True

        return False

    @staticmethod
    def _extract_common_features(cluster: list[dict]) -> dict:
        """Find features that all entries in a cluster share."""
        if not cluster:
            return {}

        common: dict = {}
        keys = ["ip", "status_code", "method", "path"]

        for key in keys:
            values = {a.get(key) for a in cluster if key in a}
            if len(values) == 1:
                common[key] = values.pop()

        return common

    def _infer_type(self, anomaly_id: str) -> str:
        """Best-effort lookup of anomaly type from recent anomalies (caller holds lock)."""
        for a in self._recent_anomalies:
            if a.get("anomaly_id") == anomaly_id:
                return a.get("anomaly_type", "unknown")
        return "unknown"
