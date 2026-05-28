from contextlib import asynccontextmanager

from fastapi import FastAPI

from src.logging_config import configure_logging, get_logger
from src.settings import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    configure_logging(settings.log_level)
    app.state.settings = settings
    get_logger("main").info("profiler_starting", port=settings.profiler_port)
    yield
    get_logger("main").info("profiler_shutdown")


app = FastAPI(title="Log Pipeline Performance Profiler", lifespan=lifespan)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
