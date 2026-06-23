"""Feature-extraction pipeline: logs -> one dense numerical matrix for clustering.

This module turns parsed log entries into a single ``float32`` feature matrix that the
streaming engine (C8) and the three clusterers (C5-C7) consume. It combines the five
feature groups the spec calls out (project_requirements §2):

* **content**    — TF-IDF over the *masked* message (the big, sparse block). Reuses
  :func:`src.preprocessing.mask_log` / :func:`src.preprocessing.tokenize` as the
  vectorizer's ``preprocessor`` / ``tokenizer`` so two logs that differ only in IP or
  numeric value normalize to the *same* tokens and land in the same content sub-space.
* **temporal**   — hour-of-day (+ sin/cos), day-of-week, weekend / business-hours flags,
  and ``time_since_last_similar_sec`` (seconds since the previous log with the *same*
  masked message, capped).
* **structural** — one-hot of ``service`` / ``level`` (via :class:`OneHotEncoder`) plus an
  ordinal ``level_severity``.
* **network**    — ``source_ip`` shape (private / missing / hash-bucket one-hot) and the
  HTTP ``status_class`` / ``has_error_status``.
* **behavioral** — per-service running request frequency, error rate, current response
  time and a per-service EWMA of response time.

Architecture contract (the engine depends on this)
---------------------------------------------------
The :class:`TfidfVectorizer`, :class:`StandardScaler`, :class:`OneHotEncoder` and
:class:`PCA` are **fit exactly once** on the warm-up batch in :meth:`FeatureExtractor.fit`
and then **frozen**. Streaming and any later refit never re-fit them, so the feature
dimension (:pyattr:`FeatureExtractor.feature_dim`) is stable forever — only the clusterers
re-fit, and they re-fit on this frozen feature space. The output is always a *dense*
``(n_logs, feature_dim)`` ``float32`` array with no ``NaN``/``Inf`` (everything is imputed
and clipped).

Two transform entry points exist with different state semantics:

* :meth:`transform` is a **pure batch** transform. It replays the given batch in timestamp
  order from a *fresh, local* state, so it is deterministic and independent of any prior
  call (calling it twice yields identical arrays).
* :meth:`transform_stream` is the **hot path**: a single log that *updates* the persistent
  streaming state (per-service counts / error-rate / EWMA response time and a per-template
  last-seen clock) and uses that state for its behavioral + "time since last similar"
  features.

The module-level ``mask_log`` / ``tokenize`` references make the fitted vectorizer
picklable, so a fitted :class:`FeatureExtractor` can be saved with the models later.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import TYPE_CHECKING, Any

import numpy as np
from sklearn.decomposition import PCA
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.config import AppConfig, load_config
from src.preprocessing import mask_log, parse_log, tokenize

if TYPE_CHECKING:  # pragma: no cover - typing-only import, avoids runtime coupling
    from src.schemas import LogEntry


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

#: Cap (seconds) for ``time_since_last_similar_sec``. The first sighting of a masked
#: template, or any gap larger than this, reads as this value — one hour. Keeps the
#: feature finite and bounded so :class:`StandardScaler` is not dominated by huge gaps.
_SINCE_CAP_SEC: float = 3600.0

#: Number of hash buckets for the ``source_ip`` one-hot. Small so repeated offenders (the
#: brute-force "bad IP" pool) collide into a handful of stable columns the clusterers can
#: latch onto, without exploding the dimension.
_IP_HASH_BUCKETS: int = 8

#: Ordinal severity per log level (DEBUG=0 … CRITICAL=4). Unknown levels -> 0.
_LEVEL_SEVERITY: dict[str, int] = {
    "DEBUG": 0,
    "INFO": 1,
    "WARN": 2,
    "WARNING": 2,
    "ERROR": 3,
    "CRITICAL": 4,
    "FATAL": 4,
}

#: EWMA smoothing factor for the per-service response-time signal. Higher => more weight
#: on the most recent observation.
_EWMA_ALPHA: float = 0.3

#: Max rows used to fit PCA when the warm-up batch is large (PCA is O(n · d); subsampling
#: keeps :meth:`fit` fast and deterministic via a fixed-seed RNG).
_PCA_FIT_MAX_ROWS: int = 5000


# --------------------------------------------------------------------------- #
# Per-service running state (behavioral features)
# --------------------------------------------------------------------------- #


class _ServiceState:
    """Mutable per-service running aggregates used for behavioral features.

    Tracks how many logs a service has emitted, how many were errors, the timestamp of
    its first/last log (to derive a frequency), and an EWMA of its response time. One
    instance per service lives inside a :class:`_StreamState`.
    """

    __slots__ = ("count", "error_count", "first_ts", "last_ts", "ewma_rt")

    def __init__(self) -> None:
        self.count: int = 0
        self.error_count: int = 0
        self.first_ts: datetime | None = None
        self.last_ts: datetime | None = None
        self.ewma_rt: float = 0.0


class _StreamState:
    """Replayable streaming state: per-service aggregates + per-template last-seen clock.

    Used both by the persistent hot-path state (:meth:`FeatureExtractor.transform_stream`)
    and by the throwaway local state that :meth:`FeatureExtractor.transform` builds per
    call. ``update`` mutates the state for one log *and returns* the behavioral / temporal
    "since-last" scalars computed against the state **as it was just before** this log —
    i.e. the values are causal (they never peek at the current log's own contribution in a
    way that would leak), matching how the engine sees a live stream.
    """

    __slots__ = ("services", "template_last_ts")

    def __init__(self) -> None:
        self.services: dict[str, _ServiceState] = {}
        self.template_last_ts: dict[str, datetime] = {}

    def update(
        self,
        service: str,
        level: str,
        masked_message: str,
        timestamp: datetime | None,
        response_time_ms: float | None,
        freq_threshold: float,
    ) -> dict[str, float]:
        """Fold one log into the state and return its behavioral/temporal scalars.

        Returns a dict with keys ``frequency`` (logs/sec for this service so far),
        ``error_rate`` (running error fraction for this service), ``resp_ewma`` (EWMA of
        the service's response time), and ``time_since_last_similar_sec`` (gap to the
        previous log carrying the *same* masked message, capped at :data:`_SINCE_CAP_SEC`).
        """
        # --- time since last *similar* (same masked template) ------------------
        since = _SINCE_CAP_SEC
        if timestamp is not None:
            prev = self.template_last_ts.get(masked_message)
            if prev is not None:
                delta = (timestamp - prev).total_seconds()
                # Clamp negatives (out-of-order arrivals) to 0 and large gaps to the cap.
                since = float(min(max(delta, 0.0), _SINCE_CAP_SEC))
            self.template_last_ts[masked_message] = timestamp

        # --- per-service running aggregates ------------------------------------
        st = self.services.get(service)
        if st is None:
            st = _ServiceState()
            self.services[service] = st

        is_error = _LEVEL_SEVERITY.get(level, 0) >= 3
        st.count += 1
        if is_error:
            st.error_count += 1
        if timestamp is not None:
            if st.first_ts is None:
                st.first_ts = timestamp
            st.last_ts = timestamp

        # Frequency = count / elapsed-seconds for the service (logs per second). With a
        # single observation (or zero elapsed time) elapsed is unknown, so fall back to
        # the configured frequency_threshold as a neutral floor rather than dividing by 0.
        elapsed = 0.0
        if st.first_ts is not None and st.last_ts is not None:
            elapsed = (st.last_ts - st.first_ts).total_seconds()
        frequency = (st.count / elapsed) if elapsed > 0 else max(freq_threshold, 0.0)

        error_rate = st.error_count / st.count if st.count else 0.0

        rt = response_time_ms if response_time_ms is not None else 0.0
        if st.count <= 1:
            st.ewma_rt = rt
        else:
            st.ewma_rt = _EWMA_ALPHA * rt + (1.0 - _EWMA_ALPHA) * st.ewma_rt

        return {
            "frequency": float(frequency),
            "error_rate": float(error_rate),
            "resp_ewma": float(st.ewma_rt),
            "time_since_last_similar_sec": float(since),
        }


# --------------------------------------------------------------------------- #
# Numeric feature names (the scaled block), in column order
# --------------------------------------------------------------------------- #
# Order here MUST match the column order assembled in ``_numeric_row``.
_NUMERIC_FEATURE_NAMES: tuple[str, ...] = (
    # temporal
    "hour",
    "hour_sin",
    "hour_cos",
    "day_of_week",
    "is_weekend",
    "is_business_hours",
    "time_since_last_similar_sec",
    # structural (ordinal; the one-hot of service/level is appended separately)
    "level_severity",
    # network
    "ip_is_private",
    "ip_is_missing",
    "status_class",
    "has_error_status",
    # behavioral
    "freq",
    "error_rate",
    "resp_time_ms",
    "resp_ewma",
    "resp_missing",
)


def _ip_is_private(ip: str) -> bool:
    """Return ``True`` if ``ip`` is an RFC1918 / loopback / link-local IPv4 address.

    A lightweight, dependency-free check on the dotted-quad prefix (10/8, 172.16/12,
    192.168/16, 127/8, 169.254/16). Anything that does not parse as four octets is treated
    as non-private (public/unknown). IPv6 is treated as non-private here — the security
    signal we care about (the repeated bad-IP pool) is IPv4.
    """
    head = ip.split(":", 1)[0]  # drop an optional :port
    parts = head.split(".")
    if len(parts) != 4:
        return False
    try:
        a, b, *_ = (int(p) for p in parts)
    except ValueError:
        return False
    if a == 10 or a == 127:
        return True
    if a == 172 and 16 <= b <= 31:
        return True
    if a == 192 and b == 168:
        return True
    if a == 169 and b == 254:
        return True
    return False


def _ip_hash_bucket(ip: str) -> int:
    """Stable bucket in ``[0, _IP_HASH_BUCKETS)`` for ``ip`` (port stripped).

    Uses a small FNV-1a hash rather than the builtin ``hash`` so bucketing is identical
    across processes (``PYTHONHASHSEED`` independence) — important for reproducible feature
    vectors across restarts.
    """
    head = ip.split(":", 1)[0]
    h = 2166136261
    for ch in head.encode("utf-8", "ignore"):
        h = ((h ^ ch) * 16777619) & 0xFFFFFFFF
    return h % _IP_HASH_BUCKETS


def _status_class(status_code: int | None) -> int:
    """Map an HTTP status code to an ordinal class: 0 unknown, 2/3/4/5 for 2xx..5xx."""
    if status_code is None:
        return 0
    bucket = status_code // 100
    return bucket if bucket in (2, 3, 4, 5) else 0


class FeatureExtractor:
    """Fit-once / frozen feature pipeline turning logs into a dense ``float32`` matrix.

    Build one, call :meth:`fit` on a warm-up batch (which freezes the TF-IDF vectorizer,
    scaler, one-hot encoder and PCA and fixes :pyattr:`feature_dim`), then use
    :meth:`transform` for batch scoring or :meth:`transform_stream` for the per-log hot
    path. :meth:`project_2d` gives a 2-D PCA projection for the dashboard scatter plot.

    All ``transform*`` methods return a finite dense ``float32`` array with exactly
    :pyattr:`feature_dim` columns; calling them before :meth:`fit` raises
    :class:`RuntimeError`.
    """

    def __init__(self, config: AppConfig | None = None) -> None:
        """Build the (unfitted) pipeline components from ``config`` (default loaded).

        Args:
            config: Application config. When ``None``, :func:`src.config.load_config` is
                used (defaults + YAML + env). The relevant knobs are
                ``text_features.max_features`` / ``text_features.ngram_tuple`` and
                ``behavioral_features.frequency_threshold``.
        """
        self.config: AppConfig = config if config is not None else load_config()

        tf = self.config.text_features
        # token_pattern=None + a custom tokenizer => sklearn uses our tokenizer verbatim.
        # lowercase=False because mask_log/tokenize already control casing (placeholders
        # such as <IP> must stay upper-case to survive as single tokens).
        self.vectorizer = TfidfVectorizer(
            preprocessor=mask_log,
            tokenizer=tokenize,
            token_pattern=None,
            lowercase=False,
            max_features=tf.max_features,
            ngram_range=tf.ngram_tuple,
        )
        self.scaler = StandardScaler()
        self.encoder = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
        self.pca = PCA(n_components=2)

        self.fitted: bool = False
        self.feature_dim: int = 0
        # Cached column counts (set on fit) so transform can validate / name columns.
        self._n_numeric: int = len(_NUMERIC_FEATURE_NAMES)
        self._n_onehot: int = 0
        self._n_tfidf: int = 0
        self._onehot_names: list[str] = []
        # Persistent hot-path streaming state (mutated only by transform_stream).
        self._stream_state = _StreamState()

    # ------------------------------------------------------------------ #
    # Fitting
    # ------------------------------------------------------------------ #

    def fit(self, logs: "list[LogEntry | dict[str, Any]]") -> "FeatureExtractor":
        """Fit (and freeze) every component on the warm-up ``logs`` batch.

        Processes the logs in **timestamp order**, builds the numeric / one-hot / TF-IDF
        blocks over the batch, fits each transformer once, assembles the full matrix and
        fits :class:`PCA` on it (subsampling to :data:`_PCA_FIT_MAX_ROWS` rows when large).
        Sets :pyattr:`fitted` and :pyattr:`feature_dim` and resets the streaming state.

        Args:
            logs: Warm-up batch of :class:`~src.schemas.LogEntry` or plain dicts. An empty
                batch raises :class:`ValueError` (there is nothing to fit a vocabulary on).

        Returns:
            ``self`` (for chaining).
        """
        if not logs:
            raise ValueError("FeatureExtractor.fit requires a non-empty batch of logs")

        parsed = [parse_log(entry) for entry in logs]
        parsed.sort(key=_timestamp_key)

        masked = [mask_log(p["message"]) for p in parsed]

        # --- content: fit TF-IDF on raw messages -------------------------------
        # The vectorizer applies mask_log itself (preprocessor), so we feed raw messages
        # to keep the fitted object self-contained / picklable.
        messages = [p["message"] for p in parsed]
        tfidf = self.vectorizer.fit_transform(messages)  # sparse (n, n_tfidf)
        self._n_tfidf = tfidf.shape[1]

        # --- structural: fit one-hot on (service, level) -----------------------
        cat = np.array(
            [[p["service"], p["level"]] for p in parsed], dtype=object
        )
        onehot = self.encoder.fit_transform(cat)  # dense (n, n_onehot)
        self._n_onehot = onehot.shape[1]
        self._onehot_names = self._build_onehot_names()

        # --- numeric block (temporal/structural-ordinal/network/behavioral) ----
        numeric = self._build_numeric_block(parsed, masked)
        scaled = self.scaler.fit_transform(numeric)  # (n, n_numeric)

        # --- assemble full matrix & fit PCA ------------------------------------
        full = self._assemble(scaled, onehot, tfidf)
        self.feature_dim = full.shape[1]

        self._fit_pca(full)

        self.fitted = True
        # A fresh fit invalidates any prior streaming state (dimensions/vocab changed).
        self._stream_state = _StreamState()
        return self

    def _fit_pca(self, full: np.ndarray) -> None:
        """Fit the 2-component PCA, subsampling large matrices for speed/determinism."""
        n = full.shape[0]
        if n > _PCA_FIT_MAX_ROWS:
            rng = np.random.default_rng(self.config.kmeans.random_state)
            idx = rng.choice(n, size=_PCA_FIT_MAX_ROWS, replace=False)
            self.pca.fit(full[idx])
        else:
            self.pca.fit(full)

    # ------------------------------------------------------------------ #
    # Transforming
    # ------------------------------------------------------------------ #

    def transform(self, logs: "list[LogEntry | dict[str, Any]]") -> np.ndarray:
        """Pure batch transform of ``logs`` into a ``(n, feature_dim)`` ``float32`` array.

        Deterministic and side-effect-free: the behavioral / "time since last similar"
        features are computed by replaying the batch in timestamp order from a **fresh,
        local** :class:`_StreamState`, so two calls with the same input return identical
        arrays and the persistent hot-path state is never touched.

        Args:
            logs: Logs to transform. An empty list yields a ``(0, feature_dim)`` array.

        Returns:
            A finite dense ``float32`` matrix with :pyattr:`feature_dim` columns.

        Raises:
            RuntimeError: If called before :meth:`fit`.
        """
        self._require_fitted()
        if not logs:
            return np.empty((0, self.feature_dim), dtype=np.float32)

        parsed = [parse_log(entry) for entry in logs]
        parsed.sort(key=_timestamp_key)
        masked = [mask_log(p["message"]) for p in parsed]

        local_state = _StreamState()
        numeric = self._build_numeric_block(parsed, masked, state=local_state)
        scaled = self.scaler.transform(numeric)

        cat = np.array([[p["service"], p["level"]] for p in parsed], dtype=object)
        onehot = self.encoder.transform(cat)
        tfidf = self.vectorizer.transform([p["message"] for p in parsed])

        return self._assemble(scaled, onehot, tfidf)

    def transform_stream(self, log: "LogEntry | dict[str, Any]") -> np.ndarray:
        """Transform a single ``log`` (hot path) into a ``(1, feature_dim)`` array.

        Unlike :meth:`transform`, this **mutates** the persistent streaming state: the
        per-service running counts / error-rate / EWMA response time and the per-template
        last-seen clock are updated by this log and used to compute its behavioral and
        "time since last similar event" features. This is the entry point the streaming
        engine calls once per arriving log.

        Args:
            log: A single :class:`~src.schemas.LogEntry` or dict.

        Returns:
            A finite dense ``float32`` matrix of shape ``(1, feature_dim)``.

        Raises:
            RuntimeError: If called before :meth:`fit`.
        """
        self._require_fitted()
        p = parse_log(log)
        masked = mask_log(p["message"])

        numeric = self._build_numeric_block([p], [masked], state=self._stream_state)
        scaled = self.scaler.transform(numeric)

        cat = np.array([[p["service"], p["level"]]], dtype=object)
        onehot = self.encoder.transform(cat)
        tfidf = self.vectorizer.transform([p["message"]])

        return self._assemble(scaled, onehot, tfidf)

    def project_2d(self, X: np.ndarray) -> np.ndarray:
        """Project a feature matrix to 2-D via the fitted PCA (for dashboard scatter).

        Args:
            X: A ``(n, feature_dim)`` matrix (typically a :meth:`transform` output).

        Returns:
            A finite dense ``(n, 2)`` ``float32`` array. An empty input yields ``(0, 2)``.

        Raises:
            RuntimeError: If called before :meth:`fit`.
            ValueError: If ``X`` does not have :pyattr:`feature_dim` columns.
        """
        self._require_fitted()
        X = np.asarray(X, dtype=np.float32)
        if X.ndim != 2 or X.shape[1] != self.feature_dim:
            raise ValueError(
                f"project_2d expected (n, {self.feature_dim}); got {X.shape}"
            )
        if X.shape[0] == 0:
            return np.empty((0, 2), dtype=np.float32)
        proj = self.pca.transform(X)
        return np.nan_to_num(proj, copy=False).astype(np.float32, copy=False)

    def feature_names(self) -> list[str]:
        """Return human-readable names for every column, in matrix order.

        The numeric block and the one-hot block are named individually; the (potentially
        large) TF-IDF block is summarized as ``tfidf_0 .. tfidf_{n-1}``. Useful for
        debugging and cluster drill-down. Available only after :meth:`fit`.

        Raises:
            RuntimeError: If called before :meth:`fit`.
        """
        self._require_fitted()
        names = list(_NUMERIC_FEATURE_NAMES)
        names.extend(self._onehot_names)
        names.extend(f"tfidf_{i}" for i in range(self._n_tfidf))
        return names

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _require_fitted(self) -> None:
        if not self.fitted:
            raise RuntimeError(
                "FeatureExtractor must be fit() on a warm-up batch before "
                "transform/transform_stream/project_2d can be called"
            )

    def _build_onehot_names(self) -> list[str]:
        """Build column names for the fitted one-hot encoder ([service, level] inputs)."""
        prefixes = ("service", "level")
        names: list[str] = []
        for prefix, cats in zip(prefixes, self.encoder.categories_):
            names.extend(f"{prefix}={c}" for c in cats)
        return names

    def _build_numeric_block(
        self,
        parsed: list[dict[str, Any]],
        masked: list[str],
        state: _StreamState | None = None,
    ) -> np.ndarray:
        """Build the (unscaled) numeric feature matrix for already-parsed logs.

        Args:
            parsed: Parsed log dicts (output of :func:`parse_log`), in the order to encode.
            masked: The masked message per row (parallel to ``parsed``).
            state: A :class:`_StreamState` to fold each row into for the behavioral /
                "since-last" features. When ``None`` a throwaway state is used (so the
                block is order-dependent only within this call). Callers that need
                determinism across calls pass a *fresh* state; the hot path passes the
                persistent one.

        Returns:
            A ``(len(parsed), _n_numeric)`` finite ``float32`` matrix.
        """
        if state is None:
            state = _StreamState()
        threshold = self.config.behavioral_features.frequency_threshold

        rows = np.empty((len(parsed), self._n_numeric), dtype=np.float32)
        for i, (p, m) in enumerate(zip(parsed, masked)):
            beh = state.update(
                service=p["service"],
                level=p["level"],
                masked_message=m,
                timestamp=p["timestamp"],
                response_time_ms=p["response_time_ms"],
                freq_threshold=threshold,
            )
            rows[i] = self._numeric_row(p, beh)
        # Defensive: guarantee finiteness regardless of upstream surprises.
        return np.nan_to_num(rows, copy=False, posinf=_SINCE_CAP_SEC, neginf=0.0)

    def _numeric_row(self, p: dict[str, Any], beh: dict[str, float]) -> np.ndarray:
        """Assemble one numeric feature row (order matches ``_NUMERIC_FEATURE_NAMES``)."""
        ts = p["timestamp"]
        if isinstance(ts, datetime):
            hour = ts.hour
            dow = ts.weekday()  # Mon=0 .. Sun=6
        else:
            hour = 0
            dow = 0
        is_weekend = 1.0 if dow >= 5 else 0.0
        is_business = 1.0 if (dow < 5 and 9 <= hour < 17) else 0.0
        hour_sin = math.sin(2.0 * math.pi * hour / 24.0)
        hour_cos = math.cos(2.0 * math.pi * hour / 24.0)

        level = p["level"]
        severity = float(_LEVEL_SEVERITY.get(level, 0))

        ip = p["source_ip"]
        ip_missing = 1.0 if not ip else 0.0
        ip_private = 1.0 if (ip and _ip_is_private(ip)) else 0.0

        status = p["status_code"]
        status_class = float(_status_class(status))
        has_error_status = 1.0 if (status is not None and status >= 400) else 0.0

        rt = p["response_time_ms"]
        resp_missing = 1.0 if rt is None else 0.0
        resp_time = float(rt) if rt is not None else 0.0

        return np.array(
            [
                float(hour),
                hour_sin,
                hour_cos,
                float(dow),
                is_weekend,
                is_business,
                beh["time_since_last_similar_sec"],
                severity,
                ip_private,
                ip_missing,
                status_class,
                has_error_status,
                beh["frequency"],
                beh["error_rate"],
                resp_time,
                beh["resp_ewma"],
                resp_missing,
            ],
            dtype=np.float32,
        )

    def _assemble(
        self,
        scaled: np.ndarray,
        onehot: np.ndarray,
        tfidf: Any,
    ) -> np.ndarray:
        """Concatenate ``[scaled_numeric | onehot | tfidf_dense]`` into finite float32."""
        # tfidf is a scipy sparse matrix; densify only at the very end (RETURN dense).
        tfidf_dense = (
            tfidf.toarray() if hasattr(tfidf, "toarray") else np.asarray(tfidf)
        )
        parts = [
            np.asarray(scaled, dtype=np.float32),
            np.asarray(onehot, dtype=np.float32),
            np.asarray(tfidf_dense, dtype=np.float32),
        ]
        out = np.hstack(parts).astype(np.float32, copy=False)
        # Belt-and-suspenders: no NaN/Inf ever reaches the clusterers.
        return np.nan_to_num(out, copy=False, posinf=0.0, neginf=0.0)


def _timestamp_key(parsed: dict[str, Any]) -> datetime:
    """Sort key that puts logs in ascending timestamp order, ``None`` timestamps first.

    A missing timestamp sorts before all real ones (``datetime.min``) so a partially
    populated batch still has a stable, deterministic ordering.
    """
    ts = parsed.get("timestamp")
    return ts if isinstance(ts, datetime) else datetime.min


__all__ = ["FeatureExtractor"]
