"""FastAPI application entry point for the Intelligent Log Redaction Engine.

C7 scope: wire every collaborator from C2-C6 into a live HTTP service.

Wiring lives in the :func:`lifespan` async context manager — the
``@app.on_event`` decorators are deprecated in modern FastAPI and the
context-manager pattern lets us own both setup and teardown in a single
function.

Startup sequence
----------------
1. ``Settings`` — process-wide config, salt, ports, NER toggles.
2. ``load_salt()`` + ``NERDetector`` (eager-loaded if ``NER_ENABLED``)
   so the first /api/redact doesn't eat the ~200 ms model warm-up.
3. ``Detector`` composing regex + (optional) NER.
4. ``TokenStore`` + ``StrategyRegistry`` (cheap, eager).
5. ``ConfigurationManager`` seeded from the configured preset on disk.
6. ``RingBuffer`` + ``AuditLogger`` (bounded audit channel).
7. ``ThroughputCounter`` + ``LatencyHistogram`` + ``PatternCounters``,
   bundled into the ``Stats`` namespace facade.
8. ``RedactionProcessor`` composing everything above.

Everything is stashed onto ``app.state`` so the routes in
:mod:`src.api.routes` can resolve them via ``request.app.state.*``.

Prometheus instrumentation
--------------------------
:class:`prometheus_fastapi_instrumentator.Instrumentator` is wired at
module scope, AFTER the lifespan declaration. It installs:

* Default HTTP metrics (``http_requests_total``,
  ``http_request_duration_seconds``, etc.) bracketing every route.
* A ``GET /metrics`` endpoint serving Prometheus text format.

Our custom counters (``redactions_total``, ``detections_total``) in
:mod:`src.api.metrics` are registered against the same default
``REGISTRY`` so they surface on the same scrape page.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from src.api.routes import router as api_router
from src.audit.audit_logger import AuditLogger
from src.audit.ring_buffer import RingBuffer
from src.config.loader import load_preset
from src.config.manager import ConfigurationManager
from src.detection.detector import Detector
from src.detection.ner import NERDetector
from src.processor.redaction_processor import RedactionProcessor
from src.redaction.salt import load_salt
from src.redaction.strategies import StrategyRegistry
from src.redaction.token_store import TokenStore
from src.settings import get_settings
from src.stats import Stats
from src.stats.counters import PatternCounters
from src.stats.latency import LatencyHistogram
from src.stats.throughput import ThroughputCounter


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
#
# Configure stdlib logging once at module import. Uvicorn installs its own
# access-log + error-log loggers; this call exists so any direct
# ``logging.getLogger(...)`` call in our code respects ``LOG_LEVEL`` from
# the env. Resolved at module scope (not lazily) because we need the log
# level before the first request lands.

_settings = get_settings()

logging.basicConfig(
    level=_settings.LOG_LEVEL.upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)
logger.info(
    "startup: log-redaction-engine booting (log_level=%s, preset=%s)",
    _settings.LOG_LEVEL,
    _settings.REDACTION_PRESET,
)


# ---------------------------------------------------------------------------
# Lifespan: build every singleton at startup, teardown at shutdown
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Construct every collaborator at startup; clean up at shutdown.

    The lifespan handler is the **only** place these singletons exist —
    routes pull them off ``app.state``. Construction order matches the
    docstring at the top of this module (settings → salt → NER → detector
    → strategies → config → audit → stats → processor).

    Shutdown is currently a single log line; future commits (C10 Redis,
    C11 thread pool) will add the corresponding teardown calls.
    """
    logger.info("lifespan: starting up")

    settings = get_settings()

    # ---- Salt + NER detector -------------------------------------------
    # Salt is loaded eagerly so a misconfigured REDACTION_HASH_SALT fails
    # at startup rather than on the first request that picks the hash
    # strategy. The error message in load_salt() is operator-friendly.
    salt = load_salt()

    # Build the NER detector unconditionally so the Detector composition
    # below has a real object to compose with. When NER is disabled we
    # pass None to the Detector so its detect() never invokes spaCy.
    ner_detector = NERDetector()

    if settings.NER_ENABLED:
        # Eager spaCy load — the ~200 ms warm-up happens here (during
        # FastAPI startup) rather than on the first /api/redact request.
        # The docker image bakes en_core_web_sm into the venv during the
        # builder stage, so this is an in-process model load, not a
        # network download.
        logger.info("lifespan: eagerly loading spaCy NER model")
        ner_detector._load()

    # ---- Detection orchestrator ----------------------------------------
    # Pass ner_detector=None when NER is disabled so the Detector's
    # detect() skips spaCy entirely — matches the unit-test path.
    detector = Detector(
        ner_detector=ner_detector if settings.NER_ENABLED else None,
        ner_min_length=settings.NER_MIN_LENGTH,
        regex_timeout=settings.REGEX_TIMEOUT_SEC,
    )

    # ---- Tokenization + strategy registry ------------------------------
    # TokenStore is process-local (in-memory dict + RLock); replaced with
    # a Redis-backed implementation in C10 for cross-process consistency.
    token_store = TokenStore(max_size=settings.MAX_TOKEN_COUNT)
    strategy_registry = StrategyRegistry(salt=salt, token_store=token_store)

    # ---- Initial config + manager --------------------------------------
    # The config dir resolves relative to this file: in Docker that is
    # /app/config/ (after the new ``COPY config/`` line in the
    # Dockerfile); in a dev checkout it is <repo>/log-redaction-engine/config/.
    config_dir = Path(__file__).parent.parent / "config"
    initial_config = load_preset(settings.REDACTION_PRESET, config_dir)
    config_manager = ConfigurationManager(initial=initial_config)

    # ---- Audit ring buffer + recorder ----------------------------------
    # AUDIT_BUFFER_SIZE is the high-water mark for the in-memory ring;
    # once full, the oldest event is silently dropped. The default 10k
    # gives the compliance report (C8) ~10 min of headroom at 100 rps.
    ring_buffer = RingBuffer(maxlen=settings.AUDIT_BUFFER_SIZE)
    audit_logger = AuditLogger(ring_buffer=ring_buffer)

    # ---- Stats facade (throughput / latency / counters) ----------------
    throughput = ThroughputCounter(window_seconds=settings.STATS_WINDOW_SECONDS)
    latency = LatencyHistogram()
    counters = PatternCounters()
    stats = Stats(throughput=throughput, latency=latency, counters=counters)

    # ---- Processor: ties everything together ---------------------------
    processor = RedactionProcessor(
        detector=detector,
        strategy_registry=strategy_registry,
        config_manager=config_manager,
        audit_logger=audit_logger,
        stats=stats,
    )

    # ---- Publish onto app.state for the routes -------------------------
    # The route layer reaches in via ``request.app.state.*``. Every
    # attribute exposed here matches the field-encryption-service shape
    # so engineers familiar with that project find the same layout.
    app.state.processor = processor
    app.state.config_manager = config_manager
    app.state.audit_logger = audit_logger
    app.state.ring_buffer = ring_buffer
    app.state.stats = stats
    app.state.token_store = token_store
    app.state.detector = detector
    app.state.strategy_registry = strategy_registry

    logger.info("lifespan: ready")

    try:
        # Yield control to the application. FastAPI keeps the context
        # open for the lifetime of the process; everything above stays
        # reachable through app.state.
        yield
    finally:
        logger.info("lifespan: shutting down")
        # No explicit teardown yet — every singleton is in-memory and
        # the GC reclaims them when the process exits. C10 (Redis) and
        # C11 (thread pool) will add their close() calls here.


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


app = FastAPI(
    title="Log Redaction Engine",
    description=(
        "Real-time log processing service that detects and redacts sensitive "
        "data (PII, PHI, payment info) from log entries using configurable "
        "strategies. Exposes a REST API plus a live dashboard."
    ),
    version="0.2.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Health endpoint (C1 behaviour preserved)
# ---------------------------------------------------------------------------


@app.get("/api/health")
async def health() -> dict[str, str]:
    """Liveness probe used by Docker HEALTHCHECK and orchestrators.

    Returns a deterministic JSON body the C1 smoke test asserts against:

        {"status": "healthy", "service": "log-redaction-engine"}

    No dependencies are checked here — this endpoint must remain
    dependency-free so a transient external outage cannot flip the
    container unhealthy and cause a restart loop.
    """
    return {"status": "healthy", "service": "log-redaction-engine"}


# ---------------------------------------------------------------------------
# Mount the application router + Prometheus instrumentation
# ---------------------------------------------------------------------------

# Mount the business endpoints before the instrumentator so the
# instrumentator's middleware sees them (it wraps every existing route).
# include_router is idempotent here — we call it exactly once.
app.include_router(api_router)

# Default HTTP metrics + a GET /metrics endpoint serving Prometheus text.
# instrument() registers the middleware; expose() adds the /metrics route.
# Both calls are idempotent across reloads, but we only invoke once at
# module scope.
Instrumentator().instrument(app).expose(app)
