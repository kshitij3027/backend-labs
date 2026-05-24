"""FastAPI app entry point with DB engine + tracking router."""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.api.routes_tracking import router as tracking_router
from src.logging_config import configure_logging, get_logger
from src.persistence.db import init_db, make_engine, make_session_factory
from src.settings import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    log = get_logger(__name__)
    log.info("startup", host=settings.api_host, port=settings.api_port)

    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)
    await init_db(engine)

    app.state.settings = settings
    app.state.engine = engine
    app.state.session_factory = session_factory

    try:
        yield
    finally:
        await engine.dispose()
        log.info("shutdown")


app = FastAPI(title="GDPR Log Erasure System", version="0.1.0", lifespan=lifespan)
app.include_router(tracking_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
