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

from .logging_config import configure_logging, get_logger
from .persistence.db import init_db, make_engine, make_session_factory
from .reporting.coordinator import ReportCoordinator
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
        # Exporters land in commits 10-12; coordinator can still be
        # instantiated with an empty registry — generate() will fail
        # any incoming request with "no exporter registered" until then.
        exporters={},
    )
    app.state.coordinator = coordinator
    app.state.semaphore = semaphore
    logger.info(
        "coordinator_ready",
        storage_path=str(settings.storage_path),
        max_concurrent=settings.max_concurrent_reports,
        secondary_key_loaded=secondary_signing_key is not None,
    )

    try:
        yield
    finally:
        logger.info("app_shutting_down")
        await engine.dispose()


app = FastAPI(title="Compliance Reporting Engine", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
