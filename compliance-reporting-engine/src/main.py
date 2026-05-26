"""FastAPI app entry point.

This commit adds persistence wiring on top of the C1 skeleton: the
lifespan now builds an async engine + session factory from
``settings.database_url``, runs ``init_db`` so all tables exist, and
stashes everything on ``app.state`` so downstream routers (landing in
later commits) can pull a session via dependency injection. On
shutdown the engine pool is disposed cleanly.

The ``/health`` endpoint still returns the minimal C1 payload so
docker-compose healthchecks and the Test Agent's curl probe keep
working untouched. Routers, exporters, and business logic land in
subsequent commits.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .logging_config import configure_logging, get_logger
from .persistence.db import init_db, make_engine, make_session_factory
from .settings import get_settings


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
    try:
        yield
    finally:
        logger.info("app_shutting_down")
        await engine.dispose()


app = FastAPI(title="Compliance Reporting Engine", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
