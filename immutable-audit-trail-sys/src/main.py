"""FastAPI app with lifespan that wires the audit chain components.

C4 brings up: engine (WAL) -> init_db (schema + triggers + genesis) ->
Ed25519Signer attached to app.state. Later commits attach ChainAppender,
ChainVerifier, stats counters, etc.
"""
from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from src.chain.appender import ChainAppender
from src.crypto.signer import Ed25519Signer
from src.persistence.db import init_db, make_engine, make_session_factory
from src.settings import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level.upper())

    signer = Ed25519Signer(settings.signing_key_b64)
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)

    await init_db(engine, signer, settings.chain_genesis_note)

    appender = ChainAppender(session_factory, signer)

    # Stash on app.state so route handlers (added in later commits) can pull
    # the dependencies without re-reading settings or rebuilding objects.
    app.state.settings = settings
    app.state.signer = signer
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.appender = appender

    try:
        yield
    finally:
        await engine.dispose()


app = FastAPI(
    title="Immutable Audit Trail System",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/api/health")
async def health() -> dict[str, int | str]:
    return {"status": "healthy", "timestamp": int(time.time())}
