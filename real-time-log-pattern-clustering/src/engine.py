"""The :class:`ClusteringEngine` — the brain that ties the whole system together (C8).

This is the single stateful object the API (C11), the metrics WebSocket (C12) and the
``demo`` mode (C10) all drive. It owns the *fit-once* :class:`~src.features.FeatureExtractor`
and the three concurrent clusterers (K-means / DBSCAN / HDBSCAN) and turns a stream of
:class:`~src.schemas.LogEntry` records into per-log cluster assignments, discovered
patterns, anomaly alerts and aggregate statistics.

Lifecycle
---------
1. :meth:`warm_up` — fit the feature pipeline + every clusterer **once** on a historical
   batch (project requirements §2: "fit an initial model on historical/batch data before
   streaming begins"). The feature pipeline is frozen after this; only the clusterers ever
   re-fit, and they re-fit on the same frozen feature space.
2. :meth:`process` / :meth:`process_batch` — the hot / throughput paths. Each log is
   transformed to a feature vector and fanned out to **all three** algorithms; their answers
   are combined into a :class:`~src.schemas.ClusterAssignment` (consensus anomaly, new-pattern
   flag, categorized pattern type) and folded into the running statistics.
3. :meth:`refit` — periodic, sliding-window re-fit so the clusterers track drift. The API's
   background task polls :meth:`should_refit` and calls this off the hot path.

Concurrency
-----------
All mutable state lives behind a single :class:`threading.RLock`. ``process`` /
``process_batch`` (request threads), the background ``refit`` task and the snapshot/getter
calls (the WebSocket broadcaster) run from *different* threads, so every read and write of
the counters, registries, buffers and quality metrics happens under the lock. The lock is
re-entrant because the public methods compose private ones that also take it.

Anomaly & new-pattern semantics
-------------------------------
* **new pattern** — any algorithm returns ``cluster_id == -1`` (the density algorithms'
  "fits no known cluster" signal). The first time a given ``(algorithm, cluster_id)`` pair is
  seen — *including* the ``-1`` buckets — a :class:`~src.schemas.PatternRecord` is created and
  ``patterns_discovered`` is incremented.
* **anomaly** — a *consensus* vote: a log is anomalous iff **>= 2 of the 3** algorithms flag
  it. Voting across independent algorithms keeps the false-positive rate down (success
  criteria §5) versus trusting any single one.

Categorization
--------------
:func:`categorize` maps each log to one of ``security_pattern`` / ``performance_pattern`` /
``error_pattern`` / ``generic`` with precedence **security > performance > error > generic**
(the first matching rule wins). The rules are tuned so the synthetic generator's families
(C4) land on the matching type.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Deque

import numpy as np
from sklearn.metrics import davies_bouldin_score, silhouette_score

from src.clustering.base import Clusterer
from src.clustering.dbscan import DBSCANClusterer
from src.clustering.hdbscan_clusterer import HDBSCANClusterer
from src.clustering.kmeans import KMeansClusterer
from src.config import AppConfig, load_config
from src.features import FeatureExtractor
from src.preprocessing import mask_log, parse_log
from src.schemas import (
    AlgoResult,
    AnomalyAlert,
    ClusterAssignment,
    PatternRecord,
    StatsSnapshot,
)


# --------------------------------------------------------------------------- #
# Module-level tuning constants
# --------------------------------------------------------------------------- #

#: Max masked example lines retained per (algorithm, cluster) for drill-down / summaries.
_EXAMPLES_PER_CLUSTER: int = 20

#: Max examples surfaced by :meth:`ClusteringEngine.get_clusters` per cluster.
_EXAMPLES_IN_SUMMARY: int = 5

#: Hard cap on the recent feature buffer (a ring of recent rows used for refit / quality /
#: scatter). Bounds memory regardless of how long the stream runs.
_FEATURE_BUFFER_CAP: int = 5000

#: Max anomalies retained for the dashboard's recent-alerts list.
_ANOMALY_BUFFER_CAP: int = 200

#: Max rows sampled (randomly) from the buffer when computing quality metrics — keeps the
#: O(n^2) silhouette computation bounded on the hot-ish path.
_QUALITY_SAMPLE_CAP: int = 1000

#: Consensus threshold: a log is an anomaly iff at least this many algorithms flag it.
_ANOMALY_VOTES: int = 2

# Keyword sets driving :func:`categorize`. Matched case-insensitively against the masked
# message (which has had IPs / numbers / ids masked out, so only the stable words remain).
_SECURITY_KEYWORDS: tuple[str, ...] = (
    "login",
    "unauthorized",
    "forbidden",
    "denied",
    "token",
    "brute",
    "credential",
    "auth",
)
_PERFORMANCE_KEYWORDS: tuple[str, ...] = (
    "slow",
    "latency",
    "timeout",
    "pool",
    "gc pause",
    "throttle",
    "degraded",
)
_ERROR_KEYWORDS: tuple[str, ...] = (
    "exception",
    "traceback",
    "refused",
    "null",
    "disk full",
)

#: Log levels that read as "this is at least a warning" for the security rule.
_SECURITY_LEVELS: frozenset[str] = frozenset({"WARN", "WARNING", "ERROR", "CRITICAL", "FATAL"})
#: Log levels that read as a hard error for the error rule.
_ERROR_LEVELS: frozenset[str] = frozenset({"ERROR", "CRITICAL", "FATAL"})

#: Response-time (ms) above which a log is considered a performance signal.
_PERF_RESPONSE_MS: float = 1000.0

# Pattern type tags (kept here so the schema's free-form ``pattern_type`` stays consistent).
PATTERN_SECURITY = "security_pattern"
PATTERN_PERFORMANCE = "performance_pattern"
PATTERN_ERROR = "error_pattern"
PATTERN_GENERIC = "generic"


def categorize(parsed: dict[str, Any], masked: str) -> str:
    """Classify one log into a pattern type from its parsed fields + masked message.

    Precedence is **security > performance > error > generic** — the first rule that matches
    wins, so e.g. an ``auth`` ``ERROR`` about a failed login is ``security_pattern`` rather
    than ``error_pattern`` even though it is also an error-level log.

    Within that ordering the message *keywords* are the strongest, least-ambiguous signal and
    are checked before the broad status/level fallbacks. This matters because some HTTP codes
    are shared across families — e.g. the generator emits ``Connection refused ... 503`` as an
    *error*, not a performance event — so an unambiguous error keyword (``refused``) must beat
    the bare ``status == 503`` performance hint.

    Rules (first match wins):

    1. **security** — ``service == "auth"`` with a warn-or-worse level, OR any security
       keyword (``login`` / ``unauthorized`` / ``forbidden`` / ``denied`` / ``token`` /
       ``brute`` / ``credential`` / ``auth``).
    2. **performance keywords** — ``slow`` / ``latency`` / ``timeout`` / ``pool`` /
       ``gc pause`` / ``throttle`` / ``degraded``.
    3. **error keywords** — ``exception`` / ``traceback`` / ``refused`` / ``null`` /
       ``disk full`` (these strong error markers beat the weak status/latency hints below).
    4. **performance fallback** — ``response_time_ms`` above :data:`_PERF_RESPONSE_MS`, OR an
       HTTP 503.
    5. **error fallback** — an error-or-worse level, OR ``status_code >= 500``.
    6. otherwise **generic**.

    Args:
        parsed: A :func:`src.preprocessing.parse_log` result (``service`` lowercased,
            ``level`` UPPERCASED, ``status_code`` int|None, ``response_time_ms`` float|None).
        masked: The masked message (output of :func:`src.preprocessing.mask_log`).

    Returns:
        One of :data:`PATTERN_SECURITY`, :data:`PATTERN_PERFORMANCE`, :data:`PATTERN_ERROR`,
        :data:`PATTERN_GENERIC`.
    """
    service = parsed.get("service") or ""
    level = (parsed.get("level") or "").upper()
    status = parsed.get("status_code")
    rt = parsed.get("response_time_ms")
    text = (masked or "").lower()

    # 1) security (highest precedence) -------------------------------------------
    if service == "auth" and level in _SECURITY_LEVELS:
        return PATTERN_SECURITY
    if any(kw in text for kw in _SECURITY_KEYWORDS):
        return PATTERN_SECURITY

    # 2) performance keywords ----------------------------------------------------
    if any(kw in text for kw in _PERFORMANCE_KEYWORDS):
        return PATTERN_PERFORMANCE

    # 3) strong error keywords (beat the weak status/latency hints below) --------
    if any(kw in text for kw in _ERROR_KEYWORDS):
        return PATTERN_ERROR

    # 4) performance fallbacks (high latency / 503) ------------------------------
    if rt is not None and rt > _PERF_RESPONSE_MS:
        return PATTERN_PERFORMANCE
    if status == 503:
        return PATTERN_PERFORMANCE

    # 5) error fallbacks (error level / 5xx) -------------------------------------
    if level in _ERROR_LEVELS:
        return PATTERN_ERROR
    if status is not None and status >= 500:
        return PATTERN_ERROR

    return PATTERN_GENERIC


class _ClusterBucket:
    """Mutable per-(algorithm, cluster_id) aggregate backing patterns + drill-down.

    One bucket per distinct ``(algorithm, cluster_id)`` pair the engine has ever seen
    (``cluster_id == -1`` gets its own bucket too). Tracks how many logs landed here, a
    representative masked line, a small ring of recent example lines, running confidence
    stats and first/last-seen timestamps — everything :meth:`ClusteringEngine.get_patterns`
    / :meth:`get_clusters` / :meth:`get_cluster_detail` surface.
    """

    __slots__ = (
        "algorithm",
        "cluster_id",
        "pattern_id",
        "count",
        "examples",
        "representative",
        "pattern_type",
        "_type_counts",
        "conf_sum",
        "conf_min",
        "conf_max",
        "first_seen",
        "last_seen",
    )

    def __init__(self, algorithm: str, cluster_id: int) -> None:
        self.algorithm = algorithm
        self.cluster_id = cluster_id
        self.pattern_id = f"{algorithm}:{cluster_id}"
        self.count = 0
        self.examples: Deque[str] = deque(maxlen=_EXAMPLES_PER_CLUSTER)
        self.representative = ""
        # Most common pattern type seen in this bucket (majority wins via a tiny counter).
        self.pattern_type = PATTERN_GENERIC
        self._type_counts: dict[str, int] = {}
        self.conf_sum = 0.0
        self.conf_min = 1.0
        self.conf_max = 0.0
        self.first_seen: datetime | None = None
        self.last_seen: datetime | None = None

    def update(
        self,
        masked: str,
        confidence: float,
        pattern_type: str,
        timestamp: datetime | None,
    ) -> None:
        """Fold one assigned log into the bucket (count, examples, confidence, type, times)."""
        self.count += 1
        if masked:
            self.examples.append(masked)
            if not self.representative:
                self.representative = masked

        self.conf_sum += confidence
        self.conf_min = min(self.conf_min, confidence)
        self.conf_max = max(self.conf_max, confidence)

        self._type_counts[pattern_type] = self._type_counts.get(pattern_type, 0) + 1
        # Representative pattern type = the majority vote so far.
        self.pattern_type = max(self._type_counts.items(), key=lambda kv: kv[1])[0]

        ts = timestamp if isinstance(timestamp, datetime) else datetime.now()
        if self.first_seen is None:
            self.first_seen = ts
        self.last_seen = ts

    @property
    def mean_confidence(self) -> float:
        """Mean assignment confidence over every log folded into this bucket."""
        return self.conf_sum / self.count if self.count else 0.0


class ClusteringEngine:
    """Concurrent multi-algorithm streaming clustering engine (the system's brain).

    Build one (optionally with an :class:`~src.config.AppConfig`), :meth:`warm_up` it on a
    historical batch, then drive it with :meth:`process` (single log) / :meth:`process_batch`
    (micro-batch). Periodically call :meth:`refit` (or poll :meth:`should_refit`). All the
    read accessors (:meth:`stats_snapshot`, :meth:`get_patterns`, :meth:`get_clusters`,
    :meth:`get_anomalies`, :meth:`scatter_points`) are safe to call concurrently from a
    different thread than the one processing logs.

    Public API is **pinned** — the API / WebSocket / demo layers import these signatures.
    """

    #: The clustering algorithms run concurrently, in canonical order.
    ALGORITHMS: tuple[str, ...] = ("kmeans", "dbscan", "hdbscan")

    def __init__(self, config: AppConfig | None = None) -> None:
        """Construct the (un-warmed) engine: feature pipeline + the three clusterers + state.

        Args:
            config: Application config. When ``None``, :func:`src.config.load_config` is used.
        """
        self.config: AppConfig = config if config is not None else load_config()

        # Fit-once feature pipeline (frozen after warm_up) shared by all three clusterers.
        self.features = FeatureExtractor(self.config)

        # The three concurrent clusterers, keyed by name. Built from the same config so they
        # share hyperparameters and the live-cluster ceiling.
        self.clusterers: dict[str, Clusterer] = {
            "kmeans": KMeansClusterer(self.config),
            "dbscan": DBSCANClusterer(self.config),
            "hdbscan": HDBSCANClusterer(self.config),
        }

        # Re-entrant lock guarding ALL mutable state below (process / refit / snapshots run
        # from different threads).
        self._lock = threading.RLock()

        # ---- counters ----------------------------------------------------------
        self.warmed: bool = False
        self.total_processed: int = 0
        self.anomalies_detected: int = 0
        self.patterns_discovered: int = 0

        # ---- per-(algorithm, cluster_id) buckets (patterns + drill-down) -------
        # Nested: algorithm -> {cluster_id -> _ClusterBucket}.
        self._buckets: dict[str, dict[int, _ClusterBucket]] = {
            name: {} for name in self.ALGORITHMS
        }

        # ---- recent feature buffer (ring) for refit / quality / scatter --------
        self._feature_buf: Deque[np.ndarray] = deque(maxlen=_FEATURE_BUFFER_CAP)
        # Count of rows the buffer held at the last refit, so should_refit() can require it
        # to have grown before re-fitting again.
        self._buf_rows_at_refit: int = 0

        # ---- recent anomalies (dashboard alerts) -------------------------------
        self._anomalies: Deque[AnomalyAlert] = deque(maxlen=_ANOMALY_BUFFER_CAP)

        # ---- latest quality metrics (recomputed on warm_up / refit) ------------
        self._quality: dict[str, float | None] = {
            "silhouette": None,
            "davies_bouldin": None,
            "coherence": None,
        }

        # ---- cached 2-D scatter projection of the buffer -----------------------
        # Invalidated (set None) whenever the buffer changes; recomputed lazily by
        # scatter_points so repeated dashboard polls between writes are cheap.
        self._scatter_cache: list[dict[str, float | int]] | None = None

        # ---- timing ------------------------------------------------------------
        # Real wall clock is fine in runtime code (only WORKFLOW scripts forbid perf_counter).
        self._start_time: float = time.perf_counter()
        self._last_refit: float = self._start_time

    # ------------------------------------------------------------------ #
    # Warm-up
    # ------------------------------------------------------------------ #

    def warm_up(self, logs: "list[Any]") -> None:
        """Fit the feature pipeline + every clusterer on the historical ``logs`` batch.

        Fits :class:`~src.features.FeatureExtractor` once (freezing it), transforms the batch
        and :meth:`~src.clustering.base.Clusterer.warm_fit`s each clusterer on the resulting
        matrix. Then seeds the pattern registry + per-cluster example buffers from the warm-up
        labels, seeds the recent-feature buffer, records the start time and computes the
        initial cluster-quality metrics. Sets :pyattr:`is_warmed`.

        Args:
            logs: A non-empty list of :class:`~src.schemas.LogEntry` or dicts.

        Raises:
            ValueError: If ``logs`` is empty (there is nothing to fit on).
        """
        if not logs:
            raise ValueError("ClusteringEngine.warm_up requires a non-empty batch of logs")

        # Fit + transform OUTSIDE the lock (no shared state touched; this is the slow part).
        self.features.fit(logs)
        X = self.features.transform(logs)
        for clusterer in self.clusterers.values():
            clusterer.warm_fit(X)

        parsed = [parse_log(entry) for entry in logs]
        masked = [mask_log(p["message"]) for p in parsed]

        with self._lock:
            # Seed buckets from each clusterer's training labels. We re-assign X (cheap,
            # predict-only) rather than read the private _train_labels so the seeded
            # confidences match what the hot path would emit.
            for name, clusterer in self.clusterers.items():
                res = clusterer.assign(X)
                for i in range(len(parsed)):
                    self._touch_bucket_locked(
                        name,
                        int(res.labels[i]),
                        masked[i],
                        float(res.confidences[i]),
                        categorize(parsed[i], masked[i]),
                        parsed[i].get("timestamp"),
                    )

            # Seed the recent-feature buffer with (up to cap) warm-up rows so the first
            # refit / quality / scatter call has data to work with.
            for row in X[-self._feature_buf.maxlen:]:
                self._feature_buf.append(np.asarray(row, dtype=np.float32))
            self._scatter_cache = None
            self._buf_rows_at_refit = len(self._feature_buf)

            self._start_time = time.perf_counter()
            self._last_refit = self._start_time
            self.warmed = True

            # Initial quality metrics from the warm-up buffer.
            self._quality = self._compute_quality_locked()

    @property
    def is_warmed(self) -> bool:
        """``True`` once :meth:`warm_up` has fit the pipeline + clusterers."""
        return self.warmed

    # ------------------------------------------------------------------ #
    # Processing — single log (hot path)
    # ------------------------------------------------------------------ #

    def process(self, log: "Any") -> ClusterAssignment:
        """Process a single ``log`` (hot path) and return its combined cluster assignment.

        Transforms the log via :meth:`FeatureExtractor.transform_stream` (which maintains the
        per-service / per-template streaming state), fans the feature vector out to all three
        algorithms, then combines their answers: new-pattern if any algorithm returns ``-1``,
        anomaly if **>= 2** algorithms flag it (consensus), and a categorized pattern type.
        Updates the running statistics / registry / buffers under the lock.

        Args:
            log: A single :class:`~src.schemas.LogEntry` or dict.

        Returns:
            A :class:`~src.schemas.ClusterAssignment` carrying the per-algorithm results, the
            new-pattern / anomaly verdicts, the pattern type and the masked message.

        Raises:
            RuntimeError: If called before :meth:`warm_up`.
        """
        self._require_warmed()

        x = self.features.transform_stream(log)  # (1, feature_dim)
        parsed = parse_log(log)
        masked = mask_log(parsed["message"])

        results = self._assign_all(x, row=0)
        assignment = self._combine(results, parsed, masked)

        with self._lock:
            self._record_locked(parsed, masked, assignment, x[0])

        return assignment

    # ------------------------------------------------------------------ #
    # Processing — batch (throughput path)
    # ------------------------------------------------------------------ #

    def process_batch(self, logs: "list[Any]") -> list[ClusterAssignment]:
        """Process a batch of ``logs`` (throughput path) into per-log assignments.

        The load test hammers this, so it is fully vectorized: the batch is transformed in one
        :meth:`FeatureExtractor.transform` call and each algorithm's :meth:`assign` runs
        **once** over the whole matrix; only the per-row assembly + bookkeeping loops in
        Python. There is **no** per-row ``transform`` / ``transform_stream``.

        Note:
            Because :meth:`FeatureExtractor.transform` replays the batch from a fresh local
            state, the behavioral / "time since last similar" features are computed
            **within-batch** (independent of prior batches) — the deterministic, throughput
            trade-off versus the stateful single-log :meth:`process` path.

        Args:
            logs: A list of :class:`~src.schemas.LogEntry` or dicts. An empty list yields
                ``[]``.

        Returns:
            A list of :class:`~src.schemas.ClusterAssignment`, one per input log. NOTE: the
            order matches :meth:`FeatureExtractor.transform`'s timestamp-sorted order, which
            for an already-sorted batch (the generator's output) equals input order.

        Raises:
            RuntimeError: If called before :meth:`warm_up`.
        """
        self._require_warmed()
        if not logs:
            return []

        X = self.features.transform(logs)  # (n, feature_dim), batch-fast
        n = X.shape[0]

        # transform() sorts by timestamp; re-derive that ordering so parsed/masked line up
        # with the matrix rows. parse once, sort indices by the same key transform uses.
        parsed_all = [parse_log(entry) for entry in logs]
        order = sorted(range(len(parsed_all)), key=lambda i: _ts_key(parsed_all[i]))
        parsed = [parsed_all[i] for i in order]
        masked = [mask_log(p["message"]) for p in parsed]

        # One assign() per algorithm over the WHOLE batch (vectorized).
        algo_results = {
            name: clusterer.assign(X) for name, clusterer in self.clusterers.items()
        }

        assignments: list[ClusterAssignment] = []
        with self._lock:
            for i in range(n):
                results = [
                    AlgoResult(
                        algorithm=name,
                        cluster_id=int(algo_results[name].labels[i]),
                        confidence=float(algo_results[name].confidences[i]),
                        is_anomaly=bool(algo_results[name].anomalies[i]),
                    )
                    for name in self.ALGORITHMS
                ]
                assignment = self._combine(results, parsed[i], masked[i])
                self._record_locked(parsed[i], masked[i], assignment, X[i])
                assignments.append(assignment)

        return assignments

    # ------------------------------------------------------------------ #
    # Refit (periodic, sliding-window)
    # ------------------------------------------------------------------ #

    def refit(self) -> None:
        """Re-fit every clusterer on the recent sliding-window feature buffer.

        Snapshots (under the lock) the last ``max(2000, batch_size * 20)`` buffered rows,
        re-fits each clusterer on them **outside** the lock (the expensive part), then
        recomputes quality metrics and stamps :pyattr:`_last_refit`. A no-op (returns quietly)
        if the engine is not warmed or the buffer is too small for a meaningful fit. Safe to
        call repeatedly from the background task.
        """
        if not self.warmed:
            return

        window = max(2000, int(self.config.realtime.batch_size) * 20)
        with self._lock:
            buf_len = len(self._feature_buf)
            if buf_len < 2:
                return
            take = min(buf_len, window)
            # Most-recent ``take`` rows -> a contiguous matrix snapshot.
            Xbuf = np.asarray(list(self._feature_buf)[-take:], dtype=np.float32)
            self._buf_rows_at_refit = buf_len

        # Re-fit each clusterer off the lock; tolerate a single algorithm failing (e.g. a
        # degenerate window) without taking down the others or the stream.
        for clusterer in self.clusterers.values():
            try:
                clusterer.refit(Xbuf)
            except Exception:  # noqa: BLE001 - a bad window must not crash the engine
                continue

        with self._lock:
            self._quality = self._compute_quality_locked()
            self._last_refit = time.perf_counter()

    def seconds_since_refit(self) -> float:
        """Wall-clock seconds elapsed since the last :meth:`refit` (or :meth:`warm_up`)."""
        with self._lock:
            return max(0.0, time.perf_counter() - self._last_refit)

    def should_refit(self) -> bool:
        """Whether a periodic :meth:`refit` is due.

        ``True`` when the engine is warmed, at least ``realtime.update_interval`` seconds have
        elapsed since the last refit, AND the feature buffer has grown since then (no point
        re-fitting on identical data). The API's background task polls this.
        """
        with self._lock:
            if not self.warmed:
                return False
            elapsed = time.perf_counter() - self._last_refit
            grew = len(self._feature_buf) > self._buf_rows_at_refit
            return elapsed >= float(self.config.realtime.update_interval) and grew

    # ------------------------------------------------------------------ #
    # Stats / quality
    # ------------------------------------------------------------------ #

    def stats_snapshot(self) -> StatsSnapshot:
        """Return the current aggregate statistics for the dashboard stat cards.

        ``total_clusters`` is the **sum** of each algorithm's live cluster count
        (:meth:`Clusterer.n_clusters`, which excludes the ``-1`` noise bucket) — a combined
        "how many distinct patterns across all three views" figure. ``throughput_per_sec`` is
        ``total_processed / elapsed_since_warm_up`` (a cumulative average rate). Quality
        metrics are the latest values computed at warm-up / refit time.
        """
        with self._lock:
            total_clusters = sum(c.n_clusters() for c in self.clusterers.values())
            elapsed = max(time.perf_counter() - self._start_time, 1e-6)
            throughput = self.total_processed / elapsed
            return StatsSnapshot(
                total_processed=self.total_processed,
                throughput_per_sec=float(throughput),
                total_clusters=int(total_clusters),
                patterns_discovered=self.patterns_discovered,
                anomalies_detected=self.anomalies_detected,
                algorithms=list(self.ALGORITHMS),
                silhouette=self._quality.get("silhouette"),
                davies_bouldin=self._quality.get("davies_bouldin"),
                coherence=self._quality.get("coherence"),
            )

    def quality_metrics(self) -> dict[str, float | None]:
        """Compute (and cache) cluster-quality metrics on a sample of the feature buffer.

        On a random sample of up to :data:`_QUALITY_SAMPLE_CAP` buffered rows, re-assigns the
        sample with K-means and computes, over the non-noise points (needs >= 2 clusters and
        >= 2 points per cluster):

        * ``silhouette`` — sklearn :func:`~sklearn.metrics.silhouette_score` (in ``[-1, 1]``).
        * ``davies_bouldin`` — sklearn :func:`~sklearn.metrics.davies_bouldin_score`
          (>= 0, lower is better).
        * ``coherence`` — mean intra-cluster cosine similarity to the cluster centroid, mapped
          into ``[0, 1]`` (the spec's "intra-cluster coherence", success criteria §5).

        Returns:
            ``{"silhouette": .., "davies_bouldin": .., "coherence": ..}`` with ``None`` for any
            metric that is not computable on the current buffer. The result is also cached into
            the stats snapshot.
        """
        with self._lock:
            self._quality = self._compute_quality_locked()
            return dict(self._quality)

    # ------------------------------------------------------------------ #
    # Read accessors (dashboard / API)
    # ------------------------------------------------------------------ #

    def get_clusters(self, algorithm: str) -> list[dict[str, Any]]:
        """Return per-cluster summaries for ``algorithm`` (excluding the ``-1`` noise bucket).

        Each entry: ``{cluster_id, size, representative, pattern_type, examples}`` where
        ``examples`` is up to :data:`_EXAMPLES_IN_SUMMARY` masked lines. Sorted by size desc.

        Args:
            algorithm: One of :pyattr:`ALGORITHMS`.

        Returns:
            A list of cluster summary dicts (empty if the algorithm is unknown / unseen).
        """
        with self._lock:
            buckets = self._buckets.get(algorithm, {})
            out: list[dict[str, Any]] = []
            for cid, bucket in buckets.items():
                if cid == -1:
                    continue
                out.append(
                    {
                        "cluster_id": cid,
                        "size": bucket.count,
                        "representative": bucket.representative,
                        "pattern_type": bucket.pattern_type,
                        "examples": list(bucket.examples)[-_EXAMPLES_IN_SUMMARY:],
                    }
                )
            out.sort(key=lambda d: d["size"], reverse=True)
            return out

    def get_cluster_detail(self, algorithm: str, cluster_id: int) -> dict[str, Any]:
        """Return drill-down detail for one cluster (members, representative, examples, conf).

        Args:
            algorithm: One of :pyattr:`ALGORITHMS`.
            cluster_id: The cluster id within that algorithm (``-1`` is allowed — the noise
                bucket).

        Returns:
            ``{algorithm, cluster_id, size, representative, pattern_type, examples,
            confidence: {mean, min, max}}``. ``size == 0`` with empty fields if not found.
        """
        with self._lock:
            bucket = self._buckets.get(algorithm, {}).get(cluster_id)
            if bucket is None:
                return {
                    "algorithm": algorithm,
                    "cluster_id": cluster_id,
                    "size": 0,
                    "representative": "",
                    "pattern_type": PATTERN_GENERIC,
                    "examples": [],
                    "confidence": {"mean": 0.0, "min": 0.0, "max": 0.0},
                }
            return {
                "algorithm": algorithm,
                "cluster_id": cluster_id,
                "size": bucket.count,
                "representative": bucket.representative,
                "pattern_type": bucket.pattern_type,
                "examples": list(bucket.examples),
                "confidence": {
                    "mean": bucket.mean_confidence,
                    "min": bucket.conf_min if bucket.count else 0.0,
                    "max": bucket.conf_max,
                },
            }

    def get_patterns(self) -> list[PatternRecord]:
        """Return every discovered pattern as a :class:`~src.schemas.PatternRecord`, count desc.

        One record per ``(algorithm, cluster_id)`` bucket the engine has ever populated,
        including the ``-1`` (new-pattern / noise) buckets. Sorted by ``count`` descending.
        """
        with self._lock:
            records: list[PatternRecord] = []
            now = datetime.now()
            for buckets in self._buckets.values():
                for bucket in buckets.values():
                    if bucket.count == 0:
                        continue
                    records.append(
                        PatternRecord(
                            pattern_id=bucket.pattern_id,
                            pattern_type=bucket.pattern_type,
                            algorithm=bucket.algorithm,
                            representative=bucket.representative,
                            count=bucket.count,
                            confidence=bucket.mean_confidence,
                            first_seen=bucket.first_seen or now,
                            last_seen=bucket.last_seen or now,
                        )
                    )
            records.sort(key=lambda r: r.count, reverse=True)
            return records

    def get_anomalies(self, limit: int = 50) -> list[AnomalyAlert]:
        """Return the most recent anomaly alerts (newest first), capped at ``limit``.

        Args:
            limit: Maximum number of alerts to return (default 50).

        Returns:
            Up to ``limit`` :class:`~src.schemas.AnomalyAlert` objects, most recent first.
        """
        with self._lock:
            # _anomalies appends in arrival order; newest is at the right end.
            recent = list(self._anomalies)[-max(0, limit):]
            recent.reverse()
            return recent

    def scatter_points(self, algorithm: str, limit: int = 500) -> list[dict[str, Any]]:
        """Return recent buffered points projected to 2-D for the dashboard scatter (C15).

        The most recent ``limit`` rows of the feature buffer are projected via
        :meth:`FeatureExtractor.project_2d` and each is assigned the cluster id that
        ``algorithm`` gives it. The projection of the buffer is cached and reused between
        polls (invalidated whenever the buffer changes), so repeated dashboard refreshes are
        cheap.

        Args:
            algorithm: One of :pyattr:`ALGORITHMS` (chooses which labels colour the points).
            limit: Maximum number of points (default 500).

        Returns:
            A list of ``{"x": float, "y": float, "cluster_id": int}`` dicts (empty before
            warm-up or with an empty buffer / unknown algorithm).
        """
        with self._lock:
            if not self.warmed or algorithm not in self.clusterers:
                return []
            if not self._feature_buf:
                return []

            take = min(len(self._feature_buf), max(0, limit))
            if take == 0:
                return []
            rows = np.asarray(list(self._feature_buf)[-take:], dtype=np.float32)

            # Use the cached projection when it already covers >= take rows; else recompute
            # the projection of exactly these rows.
            if self._scatter_cache is not None and len(self._scatter_cache) >= take:
                base = self._scatter_cache[-take:]
                coords = [(p["x"], p["y"]) for p in base]
            else:
                proj = self.features.project_2d(rows)
                coords = [(float(px), float(py)) for px, py in proj]
                # Cache the full projection of the current window for subsequent polls.
                self._scatter_cache = [
                    {"x": cx, "y": cy} for cx, cy in coords  # type: ignore[misc]
                ]

            labels = self.clusterers[algorithm].assign(rows).labels
            return [
                {"x": coords[i][0], "y": coords[i][1], "cluster_id": int(labels[i])}
                for i in range(take)
            ]

    # ------------------------------------------------------------------ #
    # Internal: assignment / combination
    # ------------------------------------------------------------------ #

    def _assign_all(self, X: np.ndarray, row: int) -> list[AlgoResult]:
        """Fan ``X`` out to every algorithm and pull out ``row`` as a list of AlgoResults."""
        results: list[AlgoResult] = []
        for name in self.ALGORITHMS:
            res = self.clusterers[name].assign(X)
            results.append(
                AlgoResult(
                    algorithm=name,
                    cluster_id=int(res.labels[row]),
                    confidence=float(res.confidences[row]),
                    is_anomaly=bool(res.anomalies[row]),
                )
            )
        return results

    @staticmethod
    def _combine(
        results: list[AlgoResult],
        parsed: dict[str, Any],
        masked: str,
    ) -> ClusterAssignment:
        """Combine per-algorithm results into the per-log verdict (new-pattern + consensus)."""
        is_new_pattern = any(r.cluster_id == -1 for r in results)
        votes = sum(1 for r in results if r.is_anomaly)
        is_anomaly = votes >= _ANOMALY_VOTES
        pattern_type = categorize(parsed, masked)
        return ClusterAssignment(
            results=results,
            is_new_pattern=is_new_pattern,
            is_anomaly=is_anomaly,
            pattern_type=pattern_type,
            masked_message=masked,
        )

    # ------------------------------------------------------------------ #
    # Internal: recording state (always under the lock)
    # ------------------------------------------------------------------ #

    def _record_locked(
        self,
        parsed: dict[str, Any],
        masked: str,
        assignment: ClusterAssignment,
        feature_row: np.ndarray,
    ) -> None:
        """Fold one processed log into all mutable state. Caller MUST hold ``self._lock``."""
        self.total_processed += 1

        # Per-algorithm bucket updates (creates buckets + bumps patterns_discovered on first
        # sight, including -1 buckets).
        for r in assignment.results:
            self._touch_bucket_locked(
                r.algorithm,
                r.cluster_id,
                masked,
                r.confidence,
                assignment.pattern_type,
                parsed.get("timestamp"),
            )

        # Recent-feature buffer (ring) + invalidate the scatter projection cache.
        self._feature_buf.append(np.asarray(feature_row, dtype=np.float32))
        self._scatter_cache = None

        # Consensus anomaly -> counter + dashboard alert.
        if assignment.is_anomaly:
            self.anomalies_detected += 1
            flagged = [r.algorithm for r in assignment.results if r.is_anomaly]
            score = max(
                (1.0 - r.confidence for r in assignment.results if r.is_anomaly),
                default=0.0,
            )
            ts = parsed.get("timestamp")
            self._anomalies.append(
                AnomalyAlert(
                    timestamp=ts if isinstance(ts, datetime) else datetime.now(),
                    message=parsed.get("message", "") or masked,
                    service=parsed.get("service") or None,
                    algorithms=flagged,
                    score=float(score),
                )
            )

    def _touch_bucket_locked(
        self,
        algorithm: str,
        cluster_id: int,
        masked: str,
        confidence: float,
        pattern_type: str,
        timestamp: datetime | None,
    ) -> None:
        """Get-or-create the (algorithm, cluster_id) bucket and fold one log in.

        Creates a fresh :class:`_ClusterBucket` (and bumps ``patterns_discovered``) the first
        time a pair is seen — *including* ``cluster_id == -1`` buckets. Caller holds the lock.
        """
        buckets = self._buckets.setdefault(algorithm, {})
        bucket = buckets.get(cluster_id)
        if bucket is None:
            bucket = _ClusterBucket(algorithm, cluster_id)
            buckets[cluster_id] = bucket
            self.patterns_discovered += 1
        bucket.update(masked, confidence, pattern_type, timestamp)

    # ------------------------------------------------------------------ #
    # Internal: quality computation (always under the lock)
    # ------------------------------------------------------------------ #

    def _compute_quality_locked(self) -> dict[str, float | None]:
        """Compute quality metrics on a sample of the buffer. Caller holds ``self._lock``.

        Returns the ``{silhouette, davies_bouldin, coherence}`` dict with ``None`` where a
        metric is not computable (too few points / clusters).
        """
        empty = {"silhouette": None, "davies_bouldin": None, "coherence": None}
        buf_len = len(self._feature_buf)
        if buf_len < 4 or "kmeans" not in self.clusterers:
            return empty

        rows = np.asarray(list(self._feature_buf), dtype=np.float32)
        # Random subsample (without replacement) to bound the O(n^2) silhouette cost.
        if rows.shape[0] > _QUALITY_SAMPLE_CAP:
            rng = np.random.default_rng(self.config.kmeans.random_state)
            idx = rng.choice(rows.shape[0], size=_QUALITY_SAMPLE_CAP, replace=False)
            sample = rows[idx]
        else:
            sample = rows

        km = self.clusterers["kmeans"]
        if not km.is_fitted:
            return empty
        labels = np.asarray(km.assign(sample).labels, dtype=int)

        # Keep only non-noise points (K-means never emits -1, but the filter keeps this
        # generic / robust if the buffer were ever scored by a density model).
        keep = labels != -1
        sample = sample[keep]
        labels = labels[keep]
        if sample.shape[0] < 2:
            return empty

        # Need >= 2 distinct clusters AND >= 2 points in each for silhouette to be defined.
        uniq, counts = np.unique(labels, return_counts=True)
        if uniq.size < 2 or np.any(counts < 2):
            # Drop singleton clusters and retry the validity check once.
            valid = {int(c) for c, n in zip(uniq, counts) if n >= 2}
            keep2 = np.array([int(lbl) in valid for lbl in labels])
            sample = sample[keep2]
            labels = labels[keep2]
            uniq = np.unique(labels)
            if uniq.size < 2 or sample.shape[0] < 2:
                return {
                    "silhouette": None,
                    "davies_bouldin": None,
                    "coherence": self._coherence(sample, labels)
                    if sample.shape[0] >= 1 and uniq.size >= 1
                    else None,
                }

        try:
            sil = float(silhouette_score(sample, labels))
        except Exception:  # noqa: BLE001 - degenerate geometry -> not computable
            sil = None  # type: ignore[assignment]
        try:
            db = float(davies_bouldin_score(sample, labels))
        except Exception:  # noqa: BLE001
            db = None  # type: ignore[assignment]

        coh = self._coherence(sample, labels)
        return {"silhouette": sil, "davies_bouldin": db, "coherence": coh}

    @staticmethod
    def _coherence(sample: np.ndarray, labels: np.ndarray) -> float | None:
        """Mean intra-cluster cosine similarity to the cluster centroid, mapped to ``[0, 1]``.

        For each cluster, computes the mean cosine similarity of its members to that cluster's
        mean vector, averages across clusters (size-weighted), and maps cosine ``[-1, 1]`` ->
        ``[0, 1]`` via ``(c + 1) / 2``. Returns ``None`` if there is nothing to measure.
        """
        if sample.shape[0] == 0:
            return None
        total_sim = 0.0
        total_n = 0
        for cid in np.unique(labels):
            members = sample[labels == cid]
            if members.shape[0] == 0:
                continue
            centroid = members.mean(axis=0)
            cnorm = float(np.linalg.norm(centroid))
            if cnorm < 1e-12:
                # Degenerate (all-zero) centroid: treat as perfectly coherent (identical pts).
                total_sim += members.shape[0] * 1.0
                total_n += members.shape[0]
                continue
            mnorm = np.linalg.norm(members, axis=1)
            denom = mnorm * cnorm
            sims = np.zeros(members.shape[0], dtype=np.float64)
            nonzero = denom > 1e-12
            sims[nonzero] = (members[nonzero] @ centroid) / denom[nonzero]
            total_sim += float(np.sum(sims))
            total_n += members.shape[0]
        if total_n == 0:
            return None
        mean_cos = total_sim / total_n
        coherence = (mean_cos + 1.0) / 2.0
        return float(min(max(coherence, 0.0), 1.0))

    # ------------------------------------------------------------------ #
    # Internal: misc
    # ------------------------------------------------------------------ #

    def _require_warmed(self) -> None:
        if not self.warmed:
            raise RuntimeError(
                "ClusteringEngine must be warm_up() on a historical batch before "
                "process/process_batch can be called"
            )


def _ts_key(parsed: dict[str, Any]) -> datetime:
    """Sort key mirroring :func:`src.features._timestamp_key` (None timestamps first)."""
    ts = parsed.get("timestamp")
    return ts if isinstance(ts, datetime) else datetime.min


__all__ = ["ClusteringEngine", "categorize"]
