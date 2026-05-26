"""FastAPI app entry point.

This commit ships only the skeleton: lifespan wires up settings + JSON
logging, and the single ``/health`` endpoint returns ``{"status":"ok"}``
so docker-compose healthchecks and the Test Agent's curl probe can
confirm the container is alive. Routers, persistence, and business
logic land in subsequent commits.
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .logging_config import configure_logging, get_logger
from .settings import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = get_logger("main")
    logger.info("app_starting", host=settings.api_host, port=settings.api_port)
    yield
    logger.info("app_shutting_down")


app = FastAPI(title="Compliance Reporting Engine", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
