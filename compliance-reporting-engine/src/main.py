"""FastAPI app entry point.

The lifespan handles persistence wiring (engine + session factory +
``init_db``) and report-generation wiring (Fernet key, HMAC signing
keys, the concurrency-bounding semaphore, and the
:class:`ReportCoordinator` instance). Everything is stashed on
``app.state`` so request handlers can grab a session via dependency
injection and dispatch work via ``app.state.coordinator``.

On shutdown the engine pool is disposed cleanly.

The ``/health`` endpoint still returns the minimal C1 payload so
docker-compose healthchecks and the Test Agent's curl probe keep
working untouched. Routers and exporters land in subsequent commits;
the coordinator is wired with an empty exporter registry for now and
populated in commits 10-12.
"""
import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from .api import routes_frameworks, routes_reports, routes_stats
from .logging_config import configure_logging, get_logger
from .persistence.db import init_db, make_engine, make_session_factory
from .reporting.coordinator import ReportCoordinator
from .reporting.exporters import EXPORTERS
from .scheduling.scheduler import ReportScheduler
from .settings import get_settings
from .signing.fernet_store import load_or_create_fernet
from .signing.hmac_signer import load_secondary_signing_key, load_signing_key


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger("main")
    logger.info("app_starting", host=settings.api_host, port=settings.api_port)

    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    await init_db(engine)
    app.state.settings = settings
    app.state.engine = engine
    app.state.session_factory = session_factory
    # Strip the credentials half of the URL before logging — host/db only.
    logger.info("db_ready", url=str(settings.database_url).split("@")[-1])

    # --- Report generation wiring ---
    # Build the Fernet instance + HMAC signing keys + concurrency-bounding
    # semaphore once at startup so every request shares them. The secondary
    # HMAC key is optional (used only for FinHealth dual-sign); if it's
    # unset / too short, fall back to a single-signature flow rather than
    # crashing the whole app.
    fernet = load_or_create_fernet(settings)
    signing_key = load_signing_key(settings)
    try:
        secondary_signing_key = load_secondary_signing_key(settings)
    except ValueError:
        logger.warning(
            "secondary_signing_key_unavailable",
            message="HMAC_SIGNING_KEY_SECONDARY missing or too short — "
                    "FinHealth dual-signature will be disabled.",
        )
        secondary_signing_key = None

    semaphore = asyncio.Semaphore(settings.max_concurrent_reports)
    coordinator = ReportCoordinator(
        session_factory=session_factory,
        signing_key=signing_key,
        secondary_signing_key=secondary_signing_key,
        fernet=fernet,
        storage_path=Path(settings.storage_path),
        semaphore=semaphore,
        # Wire in every exporter that has self-registered into the
        # EXPORTERS dict at import time (JSON / XML / CSV / PDF in
        # commits 10-12). Copy via dict(...) so a runtime mutation of
        # the live registry can't surprise an in-flight coordinator.
        exporters=dict(EXPORTERS),
    )
    app.state.coordinator = coordinator
    app.state.semaphore = semaphore
    # Stash the signing keys + Fernet on app.state so the request
    # handlers can pull them via Depends(...) without re-loading from
    # settings on every call.
    app.state.signing_key = signing_key
    app.state.secondary_signing_key = secondary_signing_key
    app.state.fernet = fernet
    logger.info(
        "coordinator_ready",
        storage_path=str(settings.storage_path),
        max_concurrent=settings.max_concurrent_reports,
        secondary_key_loaded=secondary_signing_key is not None,
    )

    # --- APScheduler wiring (commit 14) ---
    # Optional: gated by ``SCHEDULER_ENABLED``. We always set
    # ``app.state.scheduler`` (to either the wrapper or ``None``) so
    # the shutdown branch below can branch on a single attribute
    # without an ``hasattr`` dance.
    if settings.scheduler_enabled:
        scheduler_wrapper = ReportScheduler(coordinator, settings)
        scheduler_wrapper.install_jobs()
        scheduler_wrapper.start()
        app.state.scheduler = scheduler_wrapper
        logger.info(
            "scheduler_enabled",
            job_count=len(scheduler_wrapper.installed_jobs),
        )
    else:
        app.state.scheduler = None
        logger.info("scheduler_disabled")

    try:
        yield
    finally:
        logger.info("app_shutting_down")
        if app.state.scheduler is not None:
            # ``wait=False`` so a slow in-flight generate doesn't block
            # the lifespan teardown. The coordinator is fire-and-forget
            # so any in-flight tasks will drain (or fail cleanly) as
            # the event loop closes.
            app.state.scheduler.shutdown(wait=False)
        await engine.dispose()


app = FastAPI(title="Compliance Reporting Engine", lifespan=lifespan)

# Mount the resource routers. Each one owns a clean URL prefix; the
# dashboard partials + Jinja templates land in commits 15-17.
app.include_router(routes_reports.router)
app.include_router(routes_frameworks.router)
app.include_router(routes_stats.router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
