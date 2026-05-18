"""Pydantic v2 request/response schemas for the HTTP API.

These models bracket the FastAPI routes in :mod:`src.api.routes`:

* Request models (``EncryptRequest``, ``EncryptBatchRequest``,
  ``DecryptRequest``, ``DetectRequest``) constrain the shape of inbound
  bodies and let FastAPI return a 422 with a structured ``detail`` field
  when the client sends a malformed payload.
* Response models (``DetectResponse``, ``KeysResponse``, ``StatsResponse``,
  ``HealthResponse``) keep the wire format stable across implementation
  refactors — anything not in the schema simply isn't visible to clients.

Privacy
-------
The HTTP-facing :class:`DetectionView` deliberately **omits** the
``value_preview`` attribute carried internally by
:class:`src.detection.patterns.Detection`. A short preview is useful for
in-process audit correlation but it would be a small information leak
over the wire — we strip it at the API boundary.

Encrypt/decrypt requests carry a free-form ``log: dict`` payload because
the service deliberately makes no assumptions about the *application*'s
log schema. The downstream :class:`~src.processor.log_processor.LogProcessor`
walks the structure dynamically using the C2 detector.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------


class EncryptRequest(BaseModel):
    """Body for ``POST /v1/logs/encrypt``.

    Attributes
    ----------
    log : dict[str, Any]
        The structured log entry to be processed. Any JSON object; the
        processor walks it and encrypts only detected PII leaves.
    request_id : str | None
        Optional caller-supplied correlator. Threaded through to AAD via
        :meth:`LogProcessor.encrypt`'s ``record_id``. Auto-generated when
        omitted so clients that don't care never have to think about it.
    """

    log: dict[str, Any]
    request_id: str | None = None


class EncryptBatchRequest(BaseModel):
    """Body for ``POST /v1/logs/encrypt/batch``.

    A list of independent log entries. Each is processed by a separate
    call to :meth:`LogProcessor.encrypt` so a malformed log can't poison
    the rest of the batch — the route iterates serially in C7 and lets
    the per-log parallel path (C5) handle field-level parallelism inside
    each entry.
    """

    logs: list[dict[str, Any]]
    request_id: str | None = None


class DecryptRequest(BaseModel):
    """Body for ``POST /v1/logs/decrypt``.

    Accepts an already-encrypted log dict (i.e. the kind ``encrypt``
    produces, with a ``_processing`` envelope embedded). The optional
    ``record_id`` overrides the value carried in ``_processing.record_id``
    — useful when the caller has a canonical id from their request
    context that should win over what the log itself claims.
    """

    log: dict[str, Any]
    record_id: str | None = None


class DetectRequest(BaseModel):
    """Body for ``POST /v1/detect`` — dry-run detection."""

    log: dict[str, Any]


# ---------------------------------------------------------------------------
# Response bodies
# ---------------------------------------------------------------------------


class DetectionView(BaseModel):
    """HTTP-facing projection of a single detection.

    Note
    ----
    No ``value_preview`` — we never return any portion of the original
    value over the wire (privacy). The in-process
    :class:`~src.detection.patterns.Detection` carries a short preview
    for audit correlation only.
    """

    field_path: str
    field_type: str
    confidence: float
    reason: str


class DetectResponse(BaseModel):
    """Response body for ``POST /v1/detect``."""

    detections: list[DetectionView]


class KeyInfo(BaseModel):
    """One row in the ``GET /v1/keys`` listing.

    Mirrors :meth:`src.keystore.store.KeyStore.list_keys` — no DEK
    bytes and no encryptor reference, only lifecycle metadata.

    ``usage`` (added in C9) is a small ``{"encrypts": N, "decrypts": M}``
    map populated from the C9 :class:`~src.cache.CacheProvider`
    per-key counters. It defaults to an empty dict so existing tests
    that build :class:`KeyInfo` straight from ``KeyStore.list_keys``
    rows (no cache lookup) continue to work without modification.
    """

    key_id: str
    status: str
    created_at: datetime
    retired_at: datetime | None = None
    destroyed_at: datetime | None = None
    kek_id: str
    # Per-key usage frequency surfaced from the cache. Always a
    # ``dict[str, int]`` so downstream consumers can rely on the shape;
    # when no counter has been recorded yet, individual keys may be
    # absent (the cache returns 0 in that case).
    usage: dict[str, int] = Field(default_factory=dict)


class KeysResponse(BaseModel):
    """Response body for ``GET /v1/keys``."""

    keys: list[KeyInfo]
    active_key_id: str | None = None


class StatsResponse(BaseModel):
    """Response body for ``GET /api/stats``.

    The counter map is ``dict[str, int]`` — pre-populated well-known
    names from :class:`StatsCounters` plus anything callers might
    register dynamically in the future. ``extra="allow"`` is harmless
    here (the field is a single ``dict``, so extras don't apply at the
    model level), but we still set it explicitly to flag the intent.
    """

    counters: dict[str, int]

    model_config = ConfigDict(extra="allow")


class HealthResponse(BaseModel):
    """Response body for ``GET /api/health``."""

    status: str = Field(..., examples=["healthy"])
    service: str = Field(..., examples=["field-encryption-service"])
