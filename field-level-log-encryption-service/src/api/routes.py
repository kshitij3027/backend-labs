"""HTTP route handlers for the field-level log encryption service.

The router exposes the entire public surface of the service:

* ``GET  /api/health``        — liveness probe.
* ``POST /v1/logs/encrypt``   — single-log encrypt.
* ``POST /v1/logs/encrypt/batch`` — multi-log encrypt.
* ``POST /v1/logs/decrypt``   — single-log decrypt.
* ``POST /v1/detect``         — dry-run detection.
* ``GET  /v1/keys``           — DEK lifecycle listing.
* ``GET  /api/stats``         — counter snapshot.

Composition with ``app.state``
------------------------------
Every collaborator (processor, keystore, detector, audit logger, stats
counters) is built once at startup in :mod:`src.main`'s ``lifespan``
handler and stashed onto ``app.state``. The ``_proc`` / ``_keystore``
/ etc. helpers below pull them off the request — that's the FastAPI
idiom for "shared singletons whose lifetime matches the app".

Error mapping
-------------
* :class:`KeyNotFoundError` / :class:`KeyDestroyedError`  →  ``404``
  (operator-visible: someone asked us to decrypt under a key we don't
  hold or actively crypto-shredded).
* :class:`cryptography.exceptions.InvalidTag`  →  ``422``
  (ciphertext tampering / wrong AAD — the request payload was structurally
  valid but cryptographically unverifiable).
* :class:`ProcessorError` / :class:`pydantic.ValidationError`  →  ``422``
  (malformed encrypted-field record inside an otherwise-valid payload).
* anything else  →  ``500`` (uncaught) but routes wrap their critical
  sections so we can still bump ``stats.errors`` and emit a failure
  audit event before the response goes out.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from cryptography.exceptions import InvalidTag
from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from src.audit import AuditLogger
from src.detection.detector import Detector
from src.keystore.store import (
    KeyDestroyedError,
    KeyNotFoundError,
    KeyStore,
)
from src.processor.log_processor import LogProcessor, ProcessorError
from src.stats import StatsCounters

from .metrics import (
    decryptions_total,
    encrypt_duration_seconds,
    encryptions_total,
    pii_detections_total,
)
from .models import (
    DecryptRequest,
    DetectRequest,
    DetectResponse,
    DetectionView,
    EncryptBatchRequest,
    EncryptRequest,
    HealthResponse,
    KeyInfo,
    KeysResponse,
    StatsResponse,
)

logger = logging.getLogger(__name__)

# ``router`` is the single export. The application wires it via
# ``app.include_router(router)`` in :mod:`src.main`.
router = APIRouter()


# ---------------------------------------------------------------------------
# app.state accessors
# ---------------------------------------------------------------------------


def _proc(request: Request) -> LogProcessor:
    """Resolve the shared :class:`LogProcessor` instance.

    The instance is constructed once in :mod:`src.main`'s lifespan
    handler. Routes never construct their own processor (it would
    fork the keystore and lose all state).
    """
    return request.app.state.processor


def _keystore(request: Request) -> KeyStore:
    return request.app.state.keystore


def _detector(request: Request) -> Detector:
    return request.app.state.detector


def _stats(request: Request) -> StatsCounters:
    return request.app.state.stats


def _audit(request: Request) -> AuditLogger:
    return request.app.state.audit_logger


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get(
    "/api/health",
    response_model=HealthResponse,
    tags=["health"],
)
async def health() -> HealthResponse:
    """Liveness probe used by Docker healthcheck + the test suite.

    Always returns 200 with the canonical service-id payload — there is
    no readiness check yet because every collaborator is constructed
    synchronously at startup (lifespan handler) and there is no external
    dependency to ping.
    """
    return HealthResponse(status="healthy", service="field-encryption-service")


# ---------------------------------------------------------------------------
# Encrypt
# ---------------------------------------------------------------------------


@router.post("/v1/logs/encrypt", tags=["encrypt"])
async def encrypt_log(req: EncryptRequest, request: Request) -> JSONResponse:
    """Encrypt detected PII fields in a single log entry.

    The response is the transformed log dict (free-form JSON) — every
    detected leaf is replaced by its :class:`EncryptedField` dump and a
    ``_processing`` envelope listing the encrypted paths is stamped on
    the top level.
    """
    proc = _proc(request)
    stats = _stats(request)
    audit = _audit(request)
    keystore = _keystore(request)

    # Resolve the active key id once for metrics labelling. If startup
    # never bootstrapped a key the lookup raises — return 500 with a
    # diagnostic message.
    try:
        active_key_id = keystore.get_active().key_id
    except KeyNotFoundError as exc:
        stats.incr("errors")
        audit.record(
            event_type="encrypt",
            outcome="failure",
            request_id=req.request_id,
            failure_reason=str(exc),
        )
        encryptions_total.labels(result="failure", key_id="unknown").inc()
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="no active key — service is not ready",
        ) from exc

    t0 = time.perf_counter()
    try:
        result = proc.encrypt(req.log, record_id=req.request_id)
    except Exception as exc:
        # The LogProcessor already bumped its internal stats.errors
        # counter and emitted a failure audit event. We surface the
        # operator-readable failure here and bump the Prometheus
        # counter with the ``failure`` label so dashboards can chart it.
        encryptions_total.labels(result="failure", key_id=active_key_id).inc()
        logger.warning("encrypt failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"encrypt failed: {exc}",
        ) from exc
    finally:
        # Record latency regardless of outcome — failures are part of
        # what the histogram should show. ``time.perf_counter`` returns
        # seconds, matching the histogram's bucket units.
        encrypt_duration_seconds.observe(time.perf_counter() - t0)

    encryptions_total.labels(result="success", key_id=active_key_id).inc()
    return JSONResponse(content=result)


@router.post("/v1/logs/encrypt/batch", tags=["encrypt"])
async def encrypt_batch(
    req: EncryptBatchRequest, request: Request
) -> JSONResponse:
    """Encrypt a list of log entries serially.

    Per-LOG parallelism is a future commit; for now we iterate in input
    order. Inside each entry, ``LogProcessor.encrypt`` still takes the
    parallel-encrypt path when the threshold is met (≥4 detected fields
    AND ≥4 KB total plaintext, see :class:`ParallelEncryptor`).

    On a per-entry failure the entire batch fails — partial results
    would be ambiguous (which subset succeeded?) and the client almost
    certainly wants the all-or-nothing semantics for replay safety.
    """
    proc = _proc(request)
    stats = _stats(request)
    keystore = _keystore(request)

    try:
        active_key_id = keystore.get_active().key_id
    except KeyNotFoundError as exc:
        stats.incr("errors")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="no active key — service is not ready",
        ) from exc

    encrypted: list[dict[str, Any]] = []
    t0 = time.perf_counter()
    try:
        for log_dict in req.logs:
            encrypted.append(
                proc.encrypt(log_dict, record_id=req.request_id)
            )
    except Exception as exc:
        encryptions_total.labels(result="failure", key_id=active_key_id).inc()
        logger.warning("batch encrypt failed at idx %d: %s", len(encrypted), exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"batch encrypt failed: {exc}",
        ) from exc
    finally:
        encrypt_duration_seconds.observe(time.perf_counter() - t0)

    # Increment the success counter once per entry so the metric counts
    # logs, not batch calls (parallels the per-call increment in the
    # single-log handler).
    encryptions_total.labels(result="success", key_id=active_key_id).inc(
        len(encrypted)
    )
    return JSONResponse(content={"encrypted_logs": encrypted})


# ---------------------------------------------------------------------------
# Decrypt
# ---------------------------------------------------------------------------


@router.post("/v1/logs/decrypt", tags=["decrypt"])
async def decrypt_log(req: DecryptRequest, request: Request) -> JSONResponse:
    """Decrypt every encrypted-field record in the input log.

    Error mapping is the load-bearing piece here: clients need to
    distinguish "I asked for a key you don't have" (404) from "the
    payload you sent is structurally broken" (422). We make those
    distinctions explicit rather than collapsing everything to 500.
    """
    proc = _proc(request)
    stats = _stats(request)
    # Note: audit events for decrypt are emitted by the LogProcessor
    # itself (per-field success, batch-level failure), so the route
    # does not need to call ``audit_logger`` directly. We still resolve
    # ``stats`` for the catch-all branch that bumps the error counter on
    # an unexpected exception.

    # ``key_id`` for metrics: read it off the envelope if possible.
    # Failures may leave us without it, in which case we label "unknown".
    envelope = req.log.get("_processing") if isinstance(req.log, dict) else None
    metric_key_id = (
        envelope.get("key_id", "unknown") if isinstance(envelope, dict) else "unknown"
    )

    try:
        result = proc.decrypt(req.log, record_id=req.record_id)
    except KeyNotFoundError as exc:
        decryptions_total.labels(result="failure", key_id=metric_key_id).inc()
        # 404: the client asked for a key the service doesn't have.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"key not found: {exc}",
        ) from exc
    except KeyDestroyedError as exc:
        decryptions_total.labels(result="failure", key_id=metric_key_id).inc()
        # 404: the key was deliberately crypto-shredded — semantically
        # "the resource you're asking for no longer exists". 410 Gone
        # would also be defensible; we pick 404 for consistency with
        # KeyNotFound.
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"key destroyed: {exc}",
        ) from exc
    except InvalidTag as exc:
        decryptions_total.labels(result="failure", key_id=metric_key_id).inc()
        # 422: the request was syntactically valid but the ciphertext
        # auth tag did not verify — bad data, not a server fault.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="invalid ciphertext: authentication tag mismatch",
        ) from exc
    except (ProcessorError, ValidationError) as exc:
        decryptions_total.labels(result="failure", key_id=metric_key_id).inc()
        # 422: malformed envelope or encrypted-field record.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"malformed request: {exc}",
        ) from exc
    except Exception as exc:
        # Anything else is a genuine server fault.
        decryptions_total.labels(result="failure", key_id=metric_key_id).inc()
        stats.incr("errors")
        logger.warning("decrypt failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"decrypt failed: {exc}",
        ) from exc

    decryptions_total.labels(result="success", key_id=metric_key_id).inc()
    return JSONResponse(content=result)


# ---------------------------------------------------------------------------
# Detect (dry-run)
# ---------------------------------------------------------------------------


@router.post(
    "/v1/detect",
    response_model=DetectResponse,
    tags=["detect"],
)
async def detect_log(req: DetectRequest, request: Request) -> DetectResponse:
    """Return every detection without encrypting anything.

    Useful for ops: a customer can confirm what the service WOULD touch
    before flipping on encryption. Emits ONE audit event for the whole
    batch (not per-field) to keep the audit volume low for what is
    typically a high-traffic dry-run endpoint.
    """
    detector = _detector(request)
    stats = _stats(request)
    audit = _audit(request)

    try:
        detections = detector.detect(req.log)
    except Exception as exc:
        # Detection is pure-Python — a failure here would be a code bug,
        # not a user-input issue. Still, never 500 silently: bump the
        # error counter and emit a failure audit event before
        # propagating.
        stats.incr("errors")
        audit.record(
            event_type="detect",
            outcome="failure",
            failure_reason=str(exc),
        )
        logger.warning("detect failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"detect failed: {exc}",
        ) from exc

    # Per-type metric. This is the only counter that gives operators a
    # cheap "what kind of PII is flowing through this service" pie chart
    # without having to mine the audit log.
    for d in detections:
        pii_detections_total.labels(field_type=d.field_type).inc()

    # One audit event for the whole call: high-traffic dry-run shouldn't
    # 10x the audit log volume.
    audit.record(
        event_type="detect",
        outcome="success",
        field_path=None,
        field_type=None,
        byte_count=len(detections),
    )

    return DetectResponse(
        detections=[
            DetectionView(
                field_path=d.field_path,
                field_type=d.field_type,
                confidence=d.confidence,
                reason=d.reason,
            )
            for d in detections
        ]
    )


# ---------------------------------------------------------------------------
# Keys
# ---------------------------------------------------------------------------


@router.get(
    "/v1/keys",
    response_model=KeysResponse,
    tags=["keys"],
)
async def list_keys(request: Request) -> KeysResponse:
    """Return DEK lifecycle metadata for every key the store knows about.

    The shape mirrors :meth:`KeyStore.list_keys` — id, status, timestamps,
    KEK id — never the DEK material itself. ``active_key_id`` is a
    convenience field for clients that only want the current key without
    iterating.
    """
    keystore = _keystore(request)

    rows = keystore.list_keys()
    keys = [KeyInfo(**row) for row in rows]

    # Try to identify the active key; if no active key has been minted
    # yet (impossible after startup, but defensive), leave None.
    try:
        active_key_id: str | None = keystore.get_active().key_id
    except KeyNotFoundError:
        active_key_id = None

    return KeysResponse(keys=keys, active_key_id=active_key_id)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


@router.get(
    "/api/stats",
    response_model=StatsResponse,
    tags=["stats"],
)
async def get_stats(request: Request) -> StatsResponse:
    """Return a snapshot of every in-process counter.

    The snapshot is a value copy: callers see a stable map for the
    duration of their request even if a concurrent encrypt is bumping
    the live counters.
    """
    stats = _stats(request)
    return StatsResponse(counters=stats.snapshot())
