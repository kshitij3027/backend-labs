"""Behavioral clustering of source/service interaction patterns (Feature Area D).

Where :mod:`src.patterns.temporal` looks at *when* events happen and
:mod:`src.patterns.performance` at *how slow* they are, this module asks *who behaves
how*: it builds a per-**entity** behavioral profile and clusters those profiles into a
handful of **behavior cohorts**. An entity is the ``source_ip`` when present (so the
brute-force "bad IP" pool from :func:`src.log_generator.generate_logs` is a first-class
actor), falling back to ``service`` when no IP is attached.

Each entity is summarized by a fixed numeric profile vector:

* ``requests``          — how many logs the entity produced.
* ``error_rate``        — share of its logs at level ERROR/CRITICAL.
* ``distinct_endpoints``— how many distinct endpoints it touched.
* ``distinct_services`` — how many distinct services it touched.
* ``mean_response_ms``  — mean ``response_time_ms`` over logs that carry one.
* ``security_share``    — share of its logs that look security-related (auth service,
  4xx status, or 401/403/429 codes) — this is what makes the brute-force IPs pop.
* ``off_hours_share``   — share of its logs outside business hours (Mon-Fri 09:00-17:00).

The profiles are **standardized** (zero mean / unit variance per feature) and clustered
with :class:`sklearn.cluster.KMeans` (``n_groups`` capped to the number of entities). Each
resulting group is then labelled by its dominant trait via simple thresholds on the
*cluster-mean* profile — ``security-suspect`` / ``error-heavy`` / ``high-volume`` /
``normal`` — so the output reads as named cohorts rather than opaque cluster numbers.

Everything is defensive: empty input yields zero groups, a single entity yields a single
(trivially-labelled) group, and a missing sklearn / degenerate fit falls back to a
single-cohort summary rather than raising.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING, Any

import numpy as np

from src.preprocessing import parse_log

if TYPE_CHECKING:  # pragma: no cover - typing only
    from src.schemas import LogEntry

_ERROR_LEVELS: frozenset[str] = frozenset({"ERROR", "CRITICAL"})

# Status codes treated as "security relevant" (auth failures / rate-limit / forbidden).
_SECURITY_STATUS: frozenset[int] = frozenset({401, 403, 429})

# The ordered numeric profile features fed to the clusterer (kept as a constant so the
# label heuristics below index a stable vector layout).
_FEATURES: tuple[str, ...] = (
    "requests",
    "error_rate",
    "distinct_endpoints",
    "distinct_services",
    "mean_response_ms",
    "security_share",
    "off_hours_share",
)

# Business-hours window used for the off-hours share (weekday 09:00-16:59).
_BIZ_HOURS = range(9, 17)

# Label thresholds, applied to a cluster's *mean* profile in priority order. Tuned so the
# brute-force IP cohort (high security_share + high error_rate) is flagged security-suspect,
# a noisy-but-not-secure cohort is error-heavy, a chatty cohort is high-volume, else normal.
_SECURITY_SUSPECT_SHARE: float = 0.4
_ERROR_HEAVY_RATE: float = 0.35
_HIGH_VOLUME_FACTOR: float = 1.5  # requests >= this x the overall mean requests/entity

# Up to this many example entity ids are surfaced per group (largest-volume first).
_MAX_EXAMPLES: int = 5


def _entity_key(parsed: dict[str, Any]) -> str | None:
    """Return the entity id for a parsed log: ``source_ip`` if present, else ``service``.

    Returns ``None`` when neither is usable so the log is skipped (it carries no actor).
    """
    ip = parsed.get("source_ip")
    if ip:
        return str(ip)
    svc = parsed.get("service")
    if svc:
        return str(svc)
    return None


def _is_security(parsed: dict[str, Any]) -> bool:
    """Heuristic: does this log look security-related (auth svc / 4xx-ish auth status)?"""
    if parsed.get("service") == "auth":
        return True
    status = parsed.get("status_code")
    return isinstance(status, int) and status in _SECURITY_STATUS


def _build_profiles(logs: "list[LogEntry | dict]") -> dict[str, dict[str, Any]]:
    """Accumulate a raw per-entity behavioral profile from the logs (single pass).

    Returns ``{entity: profile}`` where each profile holds the public numeric features plus
    the bookkeeping (sets / running sums) needed to derive them. Entities with no usable id
    are dropped.
    """
    acc: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "requests": 0,
            "errors": 0,
            "endpoints": set(),
            "services": set(),
            "lat_sum": 0.0,
            "lat_n": 0,
            "security": 0,
            "off_hours": 0,
        }
    )

    for log in logs or []:
        parsed = parse_log(log)
        entity = _entity_key(parsed)
        if entity is None:
            continue
        a = acc[entity]
        a["requests"] += 1
        if parsed.get("level") in _ERROR_LEVELS:
            a["errors"] += 1
        ep = parsed.get("endpoint")
        if ep:
            a["endpoints"].add(ep)
        svc = parsed.get("service")
        if svc:
            a["services"].add(svc)
        rt = parsed.get("response_time_ms")
        if isinstance(rt, (int, float)):
            a["lat_sum"] += float(rt)
            a["lat_n"] += 1
        if _is_security(parsed):
            a["security"] += 1
        ts = parsed.get("timestamp")
        if isinstance(ts, datetime):
            off = not (ts.weekday() < 5 and ts.hour in _BIZ_HOURS)
            if off:
                a["off_hours"] += 1

    # Reduce the accumulators to the public numeric profile.
    profiles: dict[str, dict[str, Any]] = {}
    for entity, a in acc.items():
        req = a["requests"]
        profiles[entity] = {
            "requests": float(req),
            "error_rate": (a["errors"] / req) if req else 0.0,
            "distinct_endpoints": float(len(a["endpoints"])),
            "distinct_services": float(len(a["services"])),
            "mean_response_ms": (a["lat_sum"] / a["lat_n"]) if a["lat_n"] else 0.0,
            "security_share": (a["security"] / req) if req else 0.0,
            "off_hours_share": (a["off_hours"] / req) if req else 0.0,
        }
    return profiles


def _standardize(matrix: "np.ndarray") -> "np.ndarray":
    """Zero-mean / unit-variance each column; constant columns are left at zero.

    Standardizing keeps a large-magnitude feature (``requests``, ``mean_response_ms``) from
    dominating the Euclidean distance KMeans uses, so every behavioral dimension contributes.
    """
    mean = matrix.mean(axis=0)
    std = matrix.std(axis=0)
    std_safe = np.where(std > 0, std, 1.0)
    return (matrix - mean) / std_safe


def _label_for(mean_profile: dict[str, float], overall_mean_requests: float) -> str:
    """Name a cohort from its mean profile via priority-ordered threshold heuristics.

    Priority: a high security share (the brute-force IPs) -> ``security-suspect``; else a high
    error rate -> ``error-heavy``; else conspicuously high volume -> ``high-volume``; otherwise
    ``normal``.
    """
    if mean_profile["security_share"] >= _SECURITY_SUSPECT_SHARE:
        return "security-suspect"
    if mean_profile["error_rate"] >= _ERROR_HEAVY_RATE:
        return "error-heavy"
    if (
        overall_mean_requests > 0
        and mean_profile["requests"] >= _HIGH_VOLUME_FACTOR * overall_mean_requests
    ):
        return "high-volume"
    return "normal"


def _cluster_labels(matrix: "np.ndarray", n_groups: int) -> "np.ndarray":
    """Cluster the standardized profile rows into ``n_groups`` via KMeans (deterministic).

    Falls back to a single all-zeros label vector if sklearn is unavailable or the fit fails,
    so the caller still produces one (well-formed) cohort rather than raising.
    """
    try:
        from sklearn.cluster import KMeans

        km = KMeans(n_clusters=n_groups, n_init=10, random_state=42)
        return km.fit_predict(matrix)
    except Exception:  # noqa: BLE001 - sklearn missing / degenerate fit -> single cohort
        return np.zeros(matrix.shape[0], dtype=int)


def mine_behavioral_patterns(
    logs: "list[LogEntry | dict]", n_groups: int = 4
) -> dict[str, Any]:
    """Cluster per-entity behavioral profiles into named behavior cohorts.

    Builds a profile per entity (``source_ip`` when present, else ``service``), standardizes
    the numeric profile vectors, clusters them with KMeans (``n_groups`` capped to the number
    of entities), and labels each group by its dominant trait. On the seeded corpus this
    surfaces the brute-force "bad IP" pool as a high-error / ``security-suspect`` cohort.

    Args:
        logs: A list of :class:`~src.schemas.LogEntry` or parsed/plain dicts. Empty / tiny /
            malformed input is handled gracefully (never raises).
        n_groups: Desired number of behavior cohorts (capped to the entity count).

    Returns:
        ``{"groups": [...], "entities": int}`` where ``entities`` is the number of profiled
        entities and each group is
        ``{group, label, count, mean_requests, mean_error_rate, mean_response_ms,
        example_entities}``. Groups are sorted by ``count`` descending.
    """
    profiles = _build_profiles(logs)
    entities = sorted(profiles)  # stable order for determinism
    n_entities = len(entities)
    if n_entities == 0:
        return {"groups": [], "entities": 0}

    # Build the standardized feature matrix in a fixed feature/entity order.
    raw = np.array(
        [[profiles[e][f] for f in _FEATURES] for e in entities], dtype=float
    )
    overall_mean_requests = float(raw[:, 0].mean())  # column 0 == "requests"

    effective_k = max(1, min(n_groups, n_entities))
    if effective_k == 1:
        labels = np.zeros(n_entities, dtype=int)
    else:
        labels = _cluster_labels(_standardize(raw), effective_k)

    # Group entity indices by cluster label.
    members: dict[int, list[int]] = defaultdict(list)
    for idx, lab in enumerate(labels):
        members[int(lab)].append(idx)

    groups: list[dict[str, Any]] = []
    for cluster_id, idxs in members.items():
        sub = raw[idxs]
        mean_vec = sub.mean(axis=0)
        mean_profile = {f: float(mean_vec[i]) for i, f in enumerate(_FEATURES)}
        # Example entities: largest-volume members first.
        ranked = sorted(idxs, key=lambda i: (-raw[i, 0], entities[i]))
        examples = [entities[i] for i in ranked[:_MAX_EXAMPLES]]
        groups.append(
            {
                "group": cluster_id,
                "label": _label_for(mean_profile, overall_mean_requests),
                "count": len(idxs),
                "mean_requests": round(mean_profile["requests"], 1),
                "mean_error_rate": round(mean_profile["error_rate"], 3),
                "mean_response_ms": round(mean_profile["mean_response_ms"], 1),
                "example_entities": examples,
            }
        )

    groups.sort(key=lambda g: (-g["count"], g["group"]))
    return {"groups": groups, "entities": n_entities}


__all__ = ["mine_behavioral_patterns"]
