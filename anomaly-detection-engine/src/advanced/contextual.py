"""Contextual anomaly detection that adjusts confidence based on environmental context."""
from __future__ import annotations

import threading

from src.models import LogEntry


class ContextualDetector:
    """Adjusts anomaly confidence using contextual signals like IP frequency and maintenance mode.

    This is a post-ensemble multiplier: it does not detect anomalies on its own,
    but scales the confidence produced by the ensemble based on environmental
    context (e.g., whether the source IP is known, whether a maintenance window
    is active).

    Thread-safe via an internal lock.
    """

    def __init__(self) -> None:
        self._ip_frequencies: dict[str, int] = {}
        self._maintenance_mode: bool = False
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def update(self, log_entry: LogEntry) -> None:
        """Record an observation for the given log entry's IP address.

        Args:
            log_entry: The log entry whose IP should be tracked.
        """
        with self._lock:
            ip = log_entry.ip
            self._ip_frequencies[ip] = self._ip_frequencies.get(ip, 0) + 1

    def get_context_adjustment(self, log_entry: LogEntry) -> float:
        """Return a confidence multiplier in [0.5, 1.5] based on context.

        Adjustment factors:
        - New/unknown IP (seen < 3 times): boost by 1.3 (more suspicious).
        - Frequent IP (seen > 100 times): reduce by 0.7 (trusted user).
        - Normal IP: neutral 1.0.
        - Maintenance mode active: reduce by 0.6 (expected anomalies).

        Factors are multiplied together, then clamped to [0.5, 1.5].

        Args:
            log_entry: The log entry to evaluate context for.

        Returns:
            A float multiplier in [0.5, 1.5].
        """
        with self._lock:
            ip_count = self._ip_frequencies.get(log_entry.ip, 0)
            in_maintenance = self._maintenance_mode

        # IP frequency factor
        if ip_count < 3:
            ip_factor = 1.3
        elif ip_count > 100:
            ip_factor = 0.7
        else:
            ip_factor = 1.0

        # Maintenance factor
        maintenance_factor = 0.6 if in_maintenance else 1.0

        # Combine and clamp
        combined = ip_factor * maintenance_factor
        return max(0.5, min(1.5, combined))

    def set_maintenance_mode(self, enabled: bool) -> None:
        """Enable or disable maintenance mode.

        During maintenance, anomalies are expected and confidence is reduced.

        Args:
            enabled: ``True`` to enter maintenance mode, ``False`` to leave it.
        """
        with self._lock:
            self._maintenance_mode = enabled

    def get_stats(self) -> dict:
        """Return a snapshot of contextual detector state.

        Returns:
            Dict with unique_ips, maintenance_mode, and total_observations.
        """
        with self._lock:
            return {
                "unique_ips": len(self._ip_frequencies),
                "maintenance_mode": self._maintenance_mode,
                "total_observations": sum(self._ip_frequencies.values()),
            }
