"""Prometheus metric singletons for the HTTP layer.

``prometheus_client`` enforces a process-wide singleton registry: trying to
register a counter / histogram with a name that already exists raises
``ValueError``. Defining the metrics at module scope here gives us a single
canonical registration site — every importer reuses the same object.

The default registry is shared with
:mod:`prometheus_fastapi_instrumentator`, so the auto-instrumented
``http_requests_total`` family and our custom ``encryptions_total`` /
``decryptions_total`` / ``pii_detections_total`` counters all surface on
the same ``/metrics`` text page.

Label cardinality
-----------------
``encryptions_total`` and ``decryptions_total`` are labelled by
``(result, key_id)``. ``result`` is bounded ("success"/"failure"); ``key_id``
grows by exactly one per rotation, i.e. at most one new label value every
``settings.key_rotation_days`` days. That bounds the total label cardinality
to a handful per year, well within Prometheus' best-practice limits.

``pii_detections_total`` is labelled by ``field_type`` which is sourced from
:data:`src.detection.patterns.Detection.field_type` — a closed set (email,
phone, ssn, credit_card, jwt, ipv4, ipv6, plus the field-name-match types).

Idempotent registration
-----------------------
The ``_safe_*`` helpers swallow the ``ValueError`` that
``prometheus_client`` raises if the same metric name is already registered
(typically only happens when this module is reloaded during testing).
"""
from __future__ import annotations

from prometheus_client import REGISTRY, Counter, Histogram


def _safe_counter(name: str, documentation: str, labelnames: list[str]) -> Counter:
    """Register a :class:`Counter` once per process, idempotently.

    If a Counter under ``name`` has already been registered (e.g. the
    module is reloaded in a test), return the existing instance instead
    of raising. We look it up via the private ``REGISTRY._names_to_collectors``
    map — Prometheus' public surface for "find existing collector by name"
    is sparse, so private access is the cleanest path here.
    """
    try:
        return Counter(name, documentation, labelnames)
    except ValueError:
        existing = REGISTRY._names_to_collectors.get(name)  # type: ignore[attr-defined]
        if existing is None:  # pragma: no cover - extremely defensive
            raise
        return existing  # type: ignore[return-value]


def _safe_histogram(
    name: str,
    documentation: str,
    buckets: tuple[float, ...],
) -> Histogram:
    """Register a :class:`Histogram` once per process, idempotently.

    Same rationale as :func:`_safe_counter`.
    """
    try:
        return Histogram(name, documentation, buckets=buckets)
    except ValueError:
        existing = REGISTRY._names_to_collectors.get(name)  # type: ignore[attr-defined]
        if existing is None:  # pragma: no cover - extremely defensive
            raise
        return existing  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------

encryptions_total: Counter = _safe_counter(
    "encryptions_total",
    "Total field encryptions performed",
    ["result", "key_id"],
)
"""Bumped once per ``POST /v1/logs/encrypt`` (and per log in batch).

Labels:

* ``result``  — ``success`` or ``failure``.
* ``key_id``  — DEK version under which the encrypt ran. ``unknown``
  when the failure prevented us from resolving the active key.
"""


decryptions_total: Counter = _safe_counter(
    "decryptions_total",
    "Total field decryptions performed",
    ["result", "key_id"],
)
"""Bumped once per ``POST /v1/logs/decrypt`` call. Labels parallel
``encryptions_total``.
"""


pii_detections_total: Counter = _safe_counter(
    "pii_detections_total",
    "Total PII detections by type",
    ["field_type"],
)
"""Bumped per detection, labelled by detector ``field_type``."""


# ---------------------------------------------------------------------------
# Histograms
# ---------------------------------------------------------------------------

encrypt_duration_seconds: Histogram = _safe_histogram(
    "encrypt_duration_seconds",
    "Wall-clock time for a single log encrypt call (seconds)",
    (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)
"""Wall-clock distribution of a single ``LogProcessor.encrypt`` call.

The buckets span 1 ms (a typical small log with a couple of fields)
through 1 s (pathologically large or saturated parallel pool) — plenty
of resolution for the p95/p99 alerts a deployment is likely to set.
"""
