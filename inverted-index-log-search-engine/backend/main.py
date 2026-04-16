"""FastAPI application for the inverted index log search engine."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from backend.config import settings  # noqa: F401
from backend.models import HealthResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup/shutdown."""
    # Startup: load index from disk, initialize resources
    yield
    # Shutdown: flush index to disk, cleanup resources


app = FastAPI(
    title="Inverted Index Log Search Engine",
    description="High-performance full-text search for log entries",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Return service health status and basic index stats."""
    return HealthResponse(
        status="healthy",
        documents=0,
        terms=0,
    )
