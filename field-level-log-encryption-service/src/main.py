"""FastAPI application entry point for the Field-Level Log Encryption Service.

C7 scope: wire every collaborator from C2-C6 into a live HTTP service.

Wiring lives in the :func:`lifespan` async context manager:

1. Construct each singleton (KeyProvider, KeyStore, Detector,
   ParallelEncryptor, AuditLogger, StatsCounters, LogProcessor,
   RotationManager).
2. Mint the initial active DEK so ``/v1/logs/encrypt`` is usable from
   request #1 — the keystore is lazy by design so this is the explicit
   bootstrap call.
3. Spin up a background ``asyncio`` task that polls
   :meth:`RotationManager.maybe_rotate` every 60s. The task is
   cancelled cleanly on shutdown.
4. Stash everything on ``app.state`` so the routes in
   :mod:`src.api.routes` can resolve them via dependency-style helpers.

Prometheus instrumentation
--------------------------
:func:`prometheus_fastapi_instrumentator.Instrumentator().instrument(app).expose(app)`
adds two things:

* Default HTTP metrics (``http_requests_total``,
  ``http_request_duration_seconds``, etc.) bracketing every route.
* A ``GET /metrics`` endpoint serving the standard Prometheus text format.

Our custom counters (``encryptions_total``, ``decryptions_total``,
``pii_detections_total``) and the ``encrypt_duration_seconds`` histogram
live in :mod:`src.api.metrics` and are registered against the same default
``REGISTRY``, so they surface on the same ``/metrics`` page.

Lifespan vs ``@app.on_event``
-----------------------------
``@app.on_event("startup"|"shutdown")`` is deprecated in modern FastAPI.
The :func:`asynccontextmanager` pattern lets us own both the setup and
teardown in a single function, including the ``finally`` block that
cancels the rotation task and drains the parallel encryption pool.
"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from src.api import router as api_router
from src.audit import AuditLogger, RingBuffer
from src.crypto.key_provider import EnvKeyProvider
from src.detection.detector import Detector
from src.keystore.rotator import RotationManager
from src.keystore.store import KeyStore
from src.processor.log_processor import LogProcessor
from src.processor.parallel import ParallelEncryptor
from src.settings import settings
from src.stats import StatsCounters


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def _configure_logging(level: str) -> None:
    """Configure stdlib logging once at module import.

    Uvicorn wires its own loggers; this just makes sure any direct
    ``logging.getLogger(...)`` calls in our code respect the configured
    level set in :data:`settings.log_level`.
    """
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


_configure_logging(settings.log_level)
logger = logging.getLogger(__name__)


# Background rotation poll interval (seconds). Kept short so a manual
# operator-triggered rotation (POST /v1/keys/rotate, future commit) takes
# effect quickly; the per-poll cost is a single timestamp comparison.
_ROTATION_POLL_INTERVAL_SECONDS: float = 60.0


# ---------------------------------------------------------------------------
# Lifespan handler
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build every singleton at startup, tear it down at shutdown.

    The lifespan handler is the **only** place in the application where
    these objects are constructed. Routes and the background rotation
    task pull them off ``app.state``.

    Startup sequence
    ----------------
    1. KEK / KeyProvider — required dependency; failure here means the
       service refuses to boot (caught at the ``settings.master_key_b64``
       validator long before this point).
    2. KeyStore + initial active DEK — bootstrap is explicit so we never
       leak a "no active key" 503 to the first request.
    3. Detector — loads YAML config on construction.
    4. ParallelEncryptor — owns its own ThreadPoolExecutor.
    5. AuditLogger + StatsCounters — observers.
    6. LogProcessor — composes the above five.
    7. RotationManager + background poll task.

    Shutdown sequence (under ``finally``)
    -------------------------------------
    1. Cancel the rotation task and await its exit. We swallow
       ``CancelledError`` plus any other exception the task happens to
       have raised — shutdown is best-effort.
    2. Shut down the parallel pool (``parallel.close()``). ``wait=False``
       inside that helper means in-flight encrypts finish on their
       own thread without blocking the response cycle.
    """
    logger.info("startup: building service singletons")

    # 1) KEK / provider.
    provider = EnvKeyProvider()

    # 2) Keystore + initial DEK.
    keystore = KeyStore(provider)
    initial = keystore.create_initial_active()
    logger.info("startup: initial active DEK minted, key_id=%s", initial.key_id)

    # 3) Detection (loads YAML once).
    detector = Detector()

    # 4) Parallel encryptor (one shared ThreadPoolExecutor).
    parallel = ParallelEncryptor(
        thread_pool_size=settings.thread_pool_size,
        threshold_fields=settings.batch_parallel_threshold_fields,
        threshold_bytes=settings.batch_parallel_threshold_bytes,
    )

    # 5) Observers.
    audit_logger = AuditLogger(RingBuffer(maxlen=1000))
    stats = StatsCounters()

    # 6) Processor (composes 1-5).
    processor = LogProcessor(
        detector=detector,
        keystore=keystore,
        parallel=parallel,
        audit_logger=audit_logger,
        stats=stats,
    )

    # 7) Rotation policy.
    rotation = RotationManager(keystore, interval_days=settings.key_rotation_days)

    # Publish.
    app.state.provider = provider
    app.state.keystore = keystore
    app.state.detector = detector
    app.state.parallel = parallel
    app.state.audit_logger = audit_logger
    app.state.stats = stats
    app.state.processor = processor
    app.state.rotation = rotation

    # 8) Background rotation poll. Wrapped in a try/finally so we can
    #    cancel it cleanly on shutdown without leaving the asyncio
    #    loop with a dangling task.
    async def _rotation_loop() -> None:
        """Sleep ``_ROTATION_POLL_INTERVAL_SECONDS`` then call ``maybe_rotate``.

        We sleep FIRST so the first poll happens after the interval — there
        is no point checking rotation in the first millisecond after
        startup (the active key was minted seconds ago). On each iteration:

        * ``maybe_rotate()`` returns ``True`` iff a rotation actually
          occurred. We bump ``keys_rotated`` and emit an audit event.
        * On any exception, we record a ``key_rotate`` failure event but
          keep the loop going — a transient failure (e.g. clock skew
          glitch) shouldn't kill the background task forever.
        """
        while True:
            try:
                await asyncio.sleep(_ROTATION_POLL_INTERVAL_SECONDS)
                if rotation.maybe_rotate():
                    new_active = keystore.get_active().key_id
                    stats.incr("keys_rotated")
                    audit_logger.record(
                        event_type="key_rotate",
                        outcome="success",
                        key_id=new_active,
                    )
                    logger.info(
                        "rotation: minted new active key_id=%s", new_active
                    )
            except asyncio.CancelledError:
                # Re-raise so the outer ``await rotation_task`` sees it
                # and the standard cancellation semantics apply.
                raise
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("rotation poll failed: %s", exc)
                try:
                    audit_logger.record(
                        event_type="key_rotate",
                        outcome="failure",
                        failure_reason=str(exc),
                    )
                except Exception:
                    # If even the audit channel is broken, swallow — we
                    # don't want a logging failure to take down the task.
                    pass

    rotation_task = asyncio.create_task(_rotation_loop(), name="rotation-poll")
    logger.info("startup: rotation poll task created")

    try:
        # Yield control to the application. FastAPI keeps the lifespan
        # context open for the lifetime of the process.
        yield
    finally:
        # Shutdown path. Run the cancellation + pool close even on
        # exceptions so a noisy startup doesn't leak threads.
        logger.info("shutdown: cancelling rotation task")
        rotation_task.cancel()
        try:
            await rotation_task
        except (asyncio.CancelledError, Exception):
            # Cancellation raises CancelledError; any other exception
            # was already logged inside the task. Either way we want
            # shutdown to continue.
            pass

        logger.info("shutdown: closing parallel encryptor pool")
        parallel.close()


# ---------------------------------------------------------------------------
# FastAPI app + Prometheus instrumentation
# ---------------------------------------------------------------------------


app = FastAPI(
    title="Field-Level Log Encryption Service",
    description=(
        "Middleware that detects PII in structured log entries and selectively "
        "encrypts the sensitive fields using AES-256-GCM while leaving "
        "operational fields readable."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# Default HTTP instrumentation plus the ``/metrics`` endpoint. ``Instrumentator``
# is idempotent across reloads — multiple ``instrument(app)`` calls would
# attach the middleware twice, so we only do it at module scope here.
Instrumentator().instrument(app).expose(app)

# Mount the application router (every business endpoint). Order matters
# only in that the router is mounted after the instrumentator has already
# registered the ``/metrics`` route — they don't collide.
app.include_router(api_router)
