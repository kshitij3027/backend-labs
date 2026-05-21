"""Pydantic v2 request/response schemas for the HTTP API.

These models bracket every route in :mod:`src.api.routes`:

* Request models (:class:`RedactRequest`, :class:`DetectRequest`)
  constrain the inbound shape; FastAPI raises a structured 422 when the
  client sends a malformed payload.
* Response models (:class:`RedactResponse`, :class:`DetectResponse`,
  :class:`StatsResponse`) pin the wire format so an internal refactor
  cannot silently change what clients observe.

Privacy
-------
:class:`DetectionItem` carries a deliberately-masked ``value_preview``
field rather than the raw matched substring. The preview is produced
by ``_value_preview()`` in :mod:`src.api.routes` and never exposes more
than the first two and last two characters of the original value —
keeping the dry-run detect endpoint safe to enable in production
without re-introducing the leak the redaction engine exists to prevent.

``model_config = ConfigDict(extra="allow")`` on :class:`LogEntry` so
callers can attach arbitrary additional fields (``request_id``,
``trace_id``, custom labels) and have them round-trip through the
service unchanged — the redaction processor preserves them via the
same ``extra="allow"`` policy on :class:`RedactedEntry`.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Shared input model: one log entry
# ---------------------------------------------------------------------------


class LogEntry(BaseModel):
    """Minimal log-entry shape accepted by ``/api/redact`` and ``/v1/detect``.

    The three named fields (``message`` / ``timestamp`` / ``level``) are
    the canonical trio called out in the project spec, but every other
    top-level key is preserved verbatim because ``model_config`` sets
    ``extra="allow"``. The downstream processor reads ``message`` (and
    the configured ``fields_to_redact``) and passes everything else
    through unchanged.

    Defaults
    --------
    ``timestamp`` and ``level`` default to empty strings so a caller that
    only cares about redacting a single string can post ``{"message": "..."}``
    without manually filling the optional fields. The processor preserves
    that emptiness in its output.
    """

    message: str
    timestamp: str = ""
    level: str = ""

    # ``extra="allow"``: arbitrary additional fields (request_id, trace_id,
    # custom labels) round-trip through the API unchanged. The downstream
    # ``RedactedEntry`` carries the same policy so a request_id posted in
    # the body comes back in the response without modification.
    model_config = ConfigDict(extra="allow")


# ---------------------------------------------------------------------------
# Redact: request + response
# ---------------------------------------------------------------------------


class RedactRequest(BaseModel):
    """Body for ``POST /api/redact``.

    A batch of :class:`LogEntry` objects. The processor iterates serially
    in C5; the parallel-batch path lands in C11 keyed off
    ``BATCH_PARALLEL_THRESHOLD``.
    """

    log_entries: list[LogEntry]


class RedactResponse(BaseModel):
    """Response body for ``POST /api/redact``.

    Each entry in ``processed_entries`` is a free-form dict (the result
    of :meth:`RedactedEntry.model_dump`) so caller-supplied extras like
    ``request_id`` survive the round-trip. The ``redactions`` key inside
    each entry is the per-entry list of :class:`RedactionMetadata`
    dumps emitted by the processor.
    """

    processed_entries: list[dict[str, Any]]


# ---------------------------------------------------------------------------
# Detect: request + response (dry-run, no redaction applied)
# ---------------------------------------------------------------------------


class DetectRequest(BaseModel):
    """Body for ``POST /v1/detect`` — dry-run detection only."""

    log_entries: list[LogEntry]


class DetectionItem(BaseModel):
    """One detection hit projected for the HTTP wire.

    Attributes
    ----------
    entry_index : int
        Index of the input log entry the hit belongs to. Lets clients
        correlate a flat detection list back to the original batch
        without rebuilding the per-entry grouping.
    pattern : str
        The ``Detection.pattern_name`` of the hit (``"ssn"``,
        ``"credit_card"``, ...).
    value_preview : str
        A masked preview of the matched substring. NEVER the full
        plaintext — see :func:`src.api.routes._value_preview` for the
        masking rule (first 2 chars + ``***`` + last 2 chars for values
        of length >= 5; full-asterisk mask otherwise).
    start : int
        Inclusive start offset into the original field text.
    end : int
        Exclusive end offset paired with ``start``.
    confidence : float
        ``1.0`` for regex hits, ``0.85`` for NER hits.
    """

    entry_index: int
    pattern: str
    value_preview: str
    start: int
    end: int
    confidence: float


class DetectResponse(BaseModel):
    """Response body for ``POST /v1/detect``.

    Crucially, there is no ``processed_entries`` key — the dry-run path
    intentionally returns ONLY detection metadata. Callers that want
    redacted output must call ``/api/redact``.
    """

    detections: list[DetectionItem]


# ---------------------------------------------------------------------------
# Stats: response only
# ---------------------------------------------------------------------------


class StatsResponse(BaseModel):
    """Response body for ``GET /api/stats``.

    Surface the four operationally-interesting numbers plus the per-
    pattern hit map. The dashboard (C8) polls this endpoint; the
    Prometheus instrumentator (mounted in :mod:`src.main`) exposes the
    same data via the ``/metrics`` endpoint in scrape format.
    """

    logs_processed: int
    ops_per_second: float
    avg_latency_ms: float
    p95_latency_ms: float
    pattern_hits: dict[str, int] = Field(default_factory=dict)
