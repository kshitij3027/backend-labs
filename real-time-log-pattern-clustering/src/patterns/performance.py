"""Batch performance pattern mining (Feature Area C).

This module mines the warm-up corpus for *performance* structure that lives in the
``response_time_ms`` field and in per-service / per-endpoint latency profiles. The synthetic
corpus plants a performance family with elevated latencies (~800-5000ms) concentrated on the
``database`` and ``api-gateway`` services; :func:`mine_performance_patterns` rediscovers both
the *distribution* of response times and the *bottleneck signatures* (which services /
endpoints are slowest) without any prior knowledge of how they were planted.

Two views are produced:

* **Response-time bands.** Latencies are clustered into ``n_buckets`` bands. We cluster on
  ``log1p(response_time_ms)`` — latency is heavy-tailed, and the log transform keeps a handful
  of multi-second outliers from swallowing all the resolution at the fast end. K-means
  (``sklearn.cluster.KMeans``, already a project dependency) does the clustering; the bands are
  then **relabelled by ascending centre** to ``fast`` / ``normal`` / ``slow`` / ``critical`` (or
  ``band 0..k`` when ``n_buckets`` != 4) so the labels are stable regardless of K-means' internal
  cluster numbering. Each band reports ``count`` / ``min_ms`` / ``mean_ms`` / ``p95_ms`` /
  ``max_ms``. If sklearn is unavailable for any reason, a numpy-quantile fallback produces
  equivalent ascending bands so the function still works.

* **Bottleneck signatures.** Logs are grouped by ``service`` (and additionally by
  ``(service, endpoint)`` when an endpoint is present); for each group we compute the p95
  latency, count, and error rate. The groups with the highest p95 are flagged as bottleneck
  predictors with a ``severity`` score, so the dashboard can point at *what* is slow, not just
  *that* things are slow.

The function is defensive: input with no numeric ``response_time_ms`` yields empty ``bands`` /
``signatures`` and ``total_with_latency == 0`` rather than raising.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

import numpy as np

from src.preprocessing import parse_log

if TYPE_CHECKING:  # pragma: no cover - typing only
    from src.schemas import LogEntry

_ERROR_LEVELS: frozenset[str] = frozenset({"ERROR", "CRITICAL"})

# Ascending-severity labels for the 4-band default. Other ``n_buckets`` fall back to
# ``band {i}`` labels (still ordered by ascending latency centre).
_BAND_LABELS_4 = ("fast", "normal", "slow", "critical")

# A (service[, endpoint]) group needs at least this many latency samples before its p95 is
# trusted as a bottleneck signature (a 1-sample "p95" is meaningless).
_MIN_GROUP_SAMPLES: int = 5
# How many bottleneck signatures (per grouping) to surface, ranked by p95 descending.
_TOP_SIGNATURES: int = 6


def _latencies(parsed_logs: list[dict[str, Any]]) -> "np.ndarray":
    """Extract the numeric ``response_time_ms`` values as a float array (NaNs dropped)."""
    vals = [
        float(p["response_time_ms"])
        for p in parsed_logs
        if isinstance(p.get("response_time_ms"), (int, float))
    ]
    return np.asarray(vals, dtype=float)


def _band_label(rank: int, n_buckets: int) -> str:
    """Return the ascending-severity label for the ``rank``-th (0-based) slowest band."""
    if n_buckets == 4 and 0 <= rank < 4:
        return _BAND_LABELS_4[rank]
    return f"band {rank}"


def _band_stats(label: str, values: "np.ndarray") -> dict[str, Any]:
    """Summarize one band's latency values into the public per-band dict."""
    return {
        "band": label,
        "count": int(values.size),
        "min_ms": round(float(values.min()), 1),
        "mean_ms": round(float(values.mean()), 1),
        "p95_ms": round(float(np.percentile(values, 95)), 1),
        "max_ms": round(float(values.max()), 1),
    }


def _kmeans_band_labels(latencies: "np.ndarray", n_buckets: int) -> "np.ndarray | None":
    """Cluster ``log1p(latencies)`` into ``n_buckets`` bands via K-means.

    Returns a per-sample band index in ``0..n_buckets-1`` **ordered by ascending cluster
    centre** (so band 0 is the fastest), or ``None`` if sklearn is unavailable / the fit fails
    (the caller then uses the quantile fallback). Deterministic via a fixed ``random_state``.
    """
    try:
        from sklearn.cluster import KMeans
    except Exception:  # noqa: BLE001 - sklearn missing -> signal caller to use fallback
        return None

    try:
        x = np.log1p(latencies).reshape(-1, 1)
        km = KMeans(n_clusters=n_buckets, n_init=10, random_state=42)
        raw = km.fit_predict(x)
        # Relabel clusters by ascending centre so band index encodes severity, not K-means'
        # arbitrary label order.
        centers = km.cluster_centers_.ravel()
        order = np.argsort(centers)  # cluster ids fastest -> slowest
        remap = np.empty(n_buckets, dtype=int)
        for new_idx, old_id in enumerate(order):
            remap[old_id] = new_idx
        return remap[raw]
    except Exception:  # noqa: BLE001 - degenerate fit (e.g. fewer unique points than K)
        return None


def _quantile_band_labels(latencies: "np.ndarray", n_buckets: int) -> "np.ndarray":
    """Assign ascending-severity band indices via equal-frequency quantile cuts (fallback).

    Used when K-means is unavailable or fails. ``np.digitize`` against the interior quantile
    edges yields band indices ``0..n_buckets-1`` ordered by ascending latency.
    """
    if n_buckets <= 1:
        return np.zeros(latencies.size, dtype=int)
    qs = np.linspace(0, 1, n_buckets + 1)[1:-1]  # interior edges only
    edges = np.quantile(latencies, qs)
    return np.digitize(latencies, edges)


def _build_bands(latencies: "np.ndarray", n_buckets: int) -> list[dict[str, Any]]:
    """Cluster latencies into ascending bands and summarize each non-empty band.

    Prefers K-means on ``log1p`` latency; falls back to quantile cuts. Empty bands (possible
    when K-means leaves a cluster unused) are skipped. Bands are returned fastest-first.
    """
    if latencies.size == 0:
        return []

    # Cap K at the number of distinct latencies so K-means never gets fewer points than
    # clusters (which would error / produce empty bands).
    effective_k = int(min(n_buckets, np.unique(latencies).size))
    if effective_k <= 1:
        return [_band_stats(_band_label(0, n_buckets), latencies)]

    labels = _kmeans_band_labels(latencies, effective_k)
    if labels is None:
        labels = _quantile_band_labels(latencies, effective_k)

    bands: list[dict[str, Any]] = []
    for rank in range(effective_k):
        mask = labels == rank
        if not np.any(mask):
            continue
        # Label by global rank against the requested n_buckets so 4-bucket runs get the
        # fast/normal/slow/critical names even if a cluster collapsed.
        bands.append(_band_stats(_band_label(rank, n_buckets), latencies[mask]))
    # Guarantee ascending mean_ms ordering for consumers/tests (relabelling already orders by
    # centre, but a tie in log-space could in principle reorder means slightly).
    bands.sort(key=lambda b: b["mean_ms"])
    return bands


def _signatures_for_grouping(
    groups: dict[Any, dict[str, Any]], key_kind: str, top_n: int
) -> list[dict[str, Any]]:
    """Build bottleneck-signature dicts for one grouping (by service or service+endpoint).

    ``groups`` maps a key to ``{"lat": [...], "count": int, "errors": int, "service": str,
    "endpoint": str|None}``. For each group with enough latency samples we compute p95 / count /
    error rate and a ``severity`` (p95 scaled to seconds), then return the ``top_n`` groups by
    p95 descending.
    """
    rows: list[dict[str, Any]] = []
    for _key, g in groups.items():
        lats = g["lat"]
        if len(lats) < _MIN_GROUP_SAMPLES:
            continue
        arr = np.asarray(lats, dtype=float)
        p95 = float(np.percentile(arr, 95))
        count = g["count"]
        error_rate = (g["errors"] / count) if count else 0.0
        rows.append(
            {
                "service": g["service"],
                "endpoint": g["endpoint"],
                "p95_ms": round(p95, 1),
                "count": int(count),
                "error_rate": round(error_rate, 3),
                # Severity blends raw slowness (p95 in seconds) with how often it errors, so a
                # slow-and-failing group outranks a merely-slow one.
                "severity": round((p95 / 1000.0) * (1.0 + error_rate), 3),
                "grouping": key_kind,
            }
        )
    rows.sort(key=lambda r: (-r["p95_ms"], -r["count"], r["service"]))
    return rows[:top_n]


def mine_performance_patterns(
    logs: "list[LogEntry | dict]", n_buckets: int = 4
) -> dict[str, Any]:
    """Mine a batch of logs for performance patterns: latency bands + bottleneck signatures.

    Steps:

    1. Parse every log and keep those with a numeric ``response_time_ms``.
    2. Cluster the latencies into ``n_buckets`` ascending bands (K-means on ``log1p`` latency,
       quantile fallback) labelled ``fast``/``normal``/``slow``/``critical`` for the 4-band
       default.
    3. Group by ``service`` and by ``(service, endpoint)``; compute p95 latency, count, and
       error rate per group and surface the slowest groups as bottleneck signatures.

    Args:
        logs: A list of :class:`~src.schemas.LogEntry` or parsed/plain dicts. Logs without a
            numeric ``response_time_ms`` are ignored. Empty / latency-free input yields empty
            ``bands`` / ``signatures`` and ``total_with_latency == 0`` (never raises).
        n_buckets: Number of response-time bands to cluster into (default ``4`` ->
            fast/normal/slow/critical).

    Returns:
        ``{"bands": [...], "signatures": [...], "total_with_latency": int}`` where:

        * ``bands`` — ascending-by-``mean_ms`` band summaries, each
          ``{band, count, min_ms, mean_ms, p95_ms, max_ms}``.
        * ``signatures`` — bottleneck predictors, each
          ``{service, endpoint, p95_ms, count, error_rate, severity, grouping}``, ranked by p95
          descending (service-level groups first, then service+endpoint groups).
        * ``total_with_latency`` — number of logs that contributed a latency sample.
    """
    parsed = [parse_log(log) for log in (logs or [])]

    latencies = _latencies(parsed)
    if latencies.size == 0:
        return {"bands": [], "signatures": [], "total_with_latency": 0}

    bands = _build_bands(latencies, n_buckets)

    # Accumulate per-service and per-(service, endpoint) latency / error stats in one pass.
    svc_groups: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"lat": [], "count": 0, "errors": 0, "service": "", "endpoint": None}
    )
    ep_groups: dict[tuple[str, str], dict[str, Any]] = defaultdict(
        lambda: {"lat": [], "count": 0, "errors": 0, "service": "", "endpoint": None}
    )
    for p in parsed:
        service = p.get("service") or "unknown"
        rt = p.get("response_time_ms")
        is_err = p.get("level") in _ERROR_LEVELS

        sg = svc_groups[service]
        sg["service"] = service
        sg["count"] += 1
        if is_err:
            sg["errors"] += 1
        if isinstance(rt, (int, float)):
            sg["lat"].append(float(rt))

        endpoint = p.get("endpoint")
        if endpoint:
            eg = ep_groups[(service, endpoint)]
            eg["service"] = service
            eg["endpoint"] = endpoint
            eg["count"] += 1
            if is_err:
                eg["errors"] += 1
            if isinstance(rt, (int, float)):
                eg["lat"].append(float(rt))

    signatures = _signatures_for_grouping(svc_groups, "service", _TOP_SIGNATURES)
    signatures += _signatures_for_grouping(ep_groups, "endpoint", _TOP_SIGNATURES)

    return {
        "bands": bands,
        "signatures": signatures,
        "total_with_latency": int(latencies.size),
    }


__all__ = ["mine_performance_patterns"]
