"""FastAPI app entry point — API + HTMX dashboard."""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from src.api.routes_dashboard import router as dashboard_router
from src.api.routes_erasure import router as erasure_router
from src.api.routes_stats import router as stats_router
from src.api.routes_tracking import router as tracking_router
from src.erasure.coordinator import ErasureCoordinator
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

    coordinator = ErasureCoordinator(session_factory, settings)

    app.state.settings = settings
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.coordinator = coordinator

    try:
        yield
    finally:
        await engine.dispose()
        log.info("shutdown")


app = FastAPI(title="GDPR Log Erasure System", version="0.1.0", lifespan=lifespan)

# CORS for the API (lets an external frontend hit /api/* if needed)
_settings_for_cors = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=_settings_for_cors.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")

app.include_router(tracking_router)
app.include_router(stats_router)
app.include_router(erasure_router)
app.include_router(dashboard_router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
