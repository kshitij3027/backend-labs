"""Prometheus counters surfaced via the ``/metrics`` endpoint.

These counters live alongside the default HTTP metrics installed by
:class:`prometheus_fastapi_instrumentator.Instrumentator` (request
duration, response status, etc.) and are registered against the same
default ``REGISTRY`` so they appear on the same scrape page.

Counters (not gauges or histograms) because every event we record is a
monotonic increment — "this redaction fired", "this detection happened" —
and Prometheus does the per-second rate computation downstream via PromQL.

The label cardinality is deliberately tiny:

* ``pattern`` ∈ {ssn, credit_card, email, us_phone, mrn, person, org}.
* ``strategy`` ∈ {mask, partial, hash, tokenize}.

Both axes are closed sets fixed by the schema, so the cross-product is
at most 28 series — well within the "low-cardinality" guideline that
keeps Prometheus storage cheap.
"""
from __future__ import annotations

from prometheus_client import Counter


# ``redactions_total`` is bumped once per applied redaction in
# :func:`src.api.routes.redact_endpoint`. The ``strategy`` label lets
# operators graph "how often did we mask vs partial vs hash vs tokenize"
# without re-deriving the mix from the audit log.
REDACTIONS_TOTAL: Counter = Counter(
    "redactions_total",
    "Total redactions applied",
    ["pattern", "strategy"],
)

# ``detections_total`` is bumped once per detection emitted by the
# dry-run ``/v1/detect`` endpoint. No ``strategy`` label here because no
# strategy is applied on the detect path — this counter measures
# probing volume, not redaction work.
DETECTIONS_TOTAL: Counter = Counter(
    "detections_total",
    "Total detections (no redaction)",
    ["pattern"],
)
