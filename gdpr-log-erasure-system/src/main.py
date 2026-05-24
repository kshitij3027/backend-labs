"""FastAPI app entry point — API + HTMX dashboard + healthcheck with DB/Redis probes."""
from contextlib import asynccontextmanager

import redis.asyncio as redis_async
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from sqlalchemy import text

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

    redis_client: redis_async.Redis | None = None
    try:
        redis_client = redis_async.from_url(settings.redis_url, decode_responses=True)
    except Exception as e:
        log.warning("redis.connect_failed", error=repr(e))

    coordinator = ErasureCoordinator(session_factory, settings)

    app.state.settings = settings
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.coordinator = coordinator
    app.state.redis = redis_client

    try:
        yield
    finally:
        if redis_client is not None:
            try:
                await redis_client.aclose()
            except Exception:
                pass
        await engine.dispose()
        log.info("shutdown")


app = FastAPI(title="GDPR Log Erasure System", version="0.1.0", lifespan=lifespan)

# CORS for the API (so a separate frontend can call /api/* if needed).
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
async def health(request: Request) -> dict[str, object]:
    """Liveness + dependency-readiness probe."""
    out: dict[str, object] = {"status": "ok"}

    # DB probe
    try:
        async with request.app.state.session_factory() as session:
            await session.execute(text("SELECT 1"))
        out["db_ok"] = True
    except Exception as e:
        out["status"] = "degraded"
        out["db_ok"] = False
        out["db_error"] = repr(e)

    # Redis probe (optional — degraded but not failed if unavailable)
    redis_client = getattr(request.app.state, "redis", None)
    if redis_client is None:
        out["redis_ok"] = False
        out["redis_error"] = "redis client not initialised"
    else:
        try:
            pong = await redis_client.ping()
            out["redis_ok"] = bool(pong)
        except Exception as e:
            out["redis_ok"] = False
            out["redis_error"] = repr(e)
            if out["status"] != "degraded":
                out["status"] = "degraded"

    return out
