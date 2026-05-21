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

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from prometheus_fastapi_instrumentator import Instrumentator

from src.api.routes import router as api_router
from src.audit.audit_logger import AuditLogger
from src.audit.ring_buffer import RingBuffer
from src.cache.in_memory import InMemoryBackend
from src.cache.redis_backend import RedisBackend
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

    # ---- Cache backend (C10): try Redis, fall back to in-memory --------
    # The backend is the shared point of cross-process consistency for
    # the token store and pattern counters. We try Redis first because
    # that's the production path; if the ping fails (Redis down, DNS
    # failure, auth mismatch, anything) we log a warning and continue
    # with an in-memory backend so the service stays up. The catch is
    # intentionally broad — every failure mode triggers the same
    # fallback, and we don't want to depend on a redis-py exception
    # hierarchy that may shift between minor versions.
    backend = None
    try:
        backend = RedisBackend(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            socket_connect_timeout=1.0,
        )
        logger.info("lifespan: using Redis backend (%s:%s)", settings.REDIS_HOST, settings.REDIS_PORT)
    except Exception as exc:
        logger.warning(
            "lifespan: Redis unreachable (%s), falling back to InMemoryBackend",
            exc,
        )
        backend = InMemoryBackend()

    # ---- Tokenization + strategy registry ------------------------------
    # TokenStore is process-local (in-memory dict + RLock); the optional
    # ``backend`` argument introduced in C10 lets the store mirror to a
    # shared backend for cross-process consistency. Actual mirror writes
    # ship in a follow-up commit — C10 wires the abstraction end-to-end.
    token_store = TokenStore(max_size=settings.MAX_TOKEN_COUNT, backend=backend)
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
    # PatternCounters also accepts the C10 backend so per-pattern hit
    # counts can be aggregated across processes in a follow-up commit.
    throughput = ThroughputCounter(window_seconds=settings.STATS_WINDOW_SECONDS)
    latency = LatencyHistogram()
    counters = PatternCounters(backend=backend)
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
    # Backend is exposed so tests + future endpoints (e.g. a /debug/backend
    # introspection probe) can identify which implementation is live
    # without re-trying the Redis ping.
    app.state.backend = backend

    logger.info("lifespan: ready (backend=%s)", backend.name)

    try:
        # Yield control to the application. FastAPI keeps the context
        # open for the lifetime of the process; everything above stays
        # reachable through app.state.
        yield
    finally:
        logger.info("lifespan: shutting down")
        # Release the Redis connection pool (no-op for InMemoryBackend).
        # Wrapped in try/except so a network blip on shutdown can't
        # cascade into a noisy traceback — the process is going away
        # anyway. C11 (thread pool) will add its own close() here.
        try:
            backend.close()
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("lifespan: backend close failed (ignored): %s", exc)


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
# Static assets + Jinja2 templates (C9 dashboard)
# ---------------------------------------------------------------------------
#
# Both directories live at the project root (alongside ``src/``). In
# Docker they're copied to ``/app/templates`` and ``/app/static`` by the
# new ``COPY`` lines in the Dockerfile; in a dev checkout they resolve
# relative to this file.

templates_dir = Path(__file__).parent.parent / "templates"
static_dir = Path(__file__).parent.parent / "static"

app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Module-level templates singleton. The three dashboard routes below
# reach in directly; we don't bother re-publishing onto ``app.state``
# because the routes live in this same module.
templates = Jinja2Templates(directory=str(templates_dir))


# ---------------------------------------------------------------------------
# Dashboard routes (C9)
# ---------------------------------------------------------------------------
#
# Three routes:
#   GET /                       full HTML page
#   GET /api/stats/html         HTMX partial (live stats card, 5 s poll)
#   GET /api/pattern_hits/html  HTMX partial (pattern-hits table, 10 s poll)
#
# All three live in main.py (not src/api/routes.py) so the
# module-local ``templates`` singleton is reachable without an extra
# import dance.


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    """Serve the live HTML dashboard at the project root.

    Renders ``templates/dashboard.html`` with the active preset name and
    rule count baked into the header. The two HTMX-driven partials
    inside the page (``/api/stats/html`` and ``/api/pattern_hits/html``)
    refresh on their own polling cadence; this route only renders the
    chrome.
    """
    settings = get_settings()
    cfg = request.app.state.config_manager.get()
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "preset_name": settings.REDACTION_PRESET,
            "rule_count": len(cfg.rules),
        },
    )


@app.get("/api/stats/html", response_class=HTMLResponse)
async def stats_html(request: Request) -> HTMLResponse:
    """Render the ``_stats_card.html`` partial for HTMX polling.

    Same numbers as ``GET /api/stats`` (the JSON sibling), shaped for
    direct HTML insertion rather than client-side rendering. The
    ``data-logs-processed`` attribute on the stats-grid lets the
    Chrome MCP test scrape the value without parsing the formatted
    cell text.
    """
    stats = request.app.state.stats
    lat = stats.latency.snapshot()
    return templates.TemplateResponse(
        "_stats_card.html",
        {
            "request": request,
            "logs_processed": stats.throughput.total_count(),
            "ops_per_second": stats.throughput.ops_per_second(),
            "avg_latency_ms": lat["mean_ms"],
            "p95_latency_ms": lat["p95_ms"],
        },
    )


@app.get("/api/pattern_hits/html", response_class=HTMLResponse)
async def pattern_hits_html(request: Request) -> HTMLResponse:
    """Render the ``_pattern_hits.html`` partial for HTMX polling.

    Returns the same per-pattern hit counts surfaced by
    ``GET /api/stats``, projected into a small ``<table>`` (or the
    "No redactions yet." paragraph when the counter map is empty).
    """
    stats = request.app.state.stats
    return templates.TemplateResponse(
        "_pattern_hits.html",
        {
            "request": request,
            "pattern_hits": stats.counters.snapshot(),
        },
    )


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
