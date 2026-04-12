"""FastAPI application entry point for the Distributed User Sessionization Engine."""
from __future__ import annotations
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from src.config import get_config
from src.session_engine import SessionEngine

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    config = get_config()
    app.state.config = config
    engine = SessionEngine(config)
    app.state.session_engine = engine
    logger.info("Sessionization engine started (port=%s)", config.port)
    yield
    logger.info("Sessionization engine shutting down")


app = FastAPI(title="Distributed User Sessionization Engine", lifespan=lifespan)


@app.get("/health")
async def health():
    return JSONResponse({"status": "healthy"})


@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(content="<h1>Distributed User Sessionization Engine</h1><p>Dashboard coming soon.</p>")
