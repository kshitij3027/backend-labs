"""FastAPI app with lifespan that wires the audit chain components."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from prometheus_fastapi_instrumentator import Instrumentator

from src.api.routes import router as api_router
from src.chain.appender import ChainAppender
from src.chain.verifier import ChainVerifier
from src.crypto.signer import Ed25519Signer, Ed25519Verifier
from src.interceptor.decorator import set_appender
from src.persistence.db import init_db, make_engine, make_session_factory
from src.settings import get_settings


# Module-level constants: StaticFiles mount and Jinja2Templates factory are
# stateless and safe to instantiate at import time. The mount itself happens
# below at app construction; the templates factory is attached to app.state
# during lifespan so route handlers can pull it off ``request.app.state``.
_TEMPLATES = Jinja2Templates(directory="templates")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level.upper())

    signer = Ed25519Signer(settings.signing_key_b64)
    engine = make_engine(settings.database_url)
    session_factory = make_session_factory(engine)

    await init_db(engine, signer, settings.chain_genesis_note)

    appender = ChainAppender(session_factory, signer)
    chain_verifier = ChainVerifier(
        session_factory,
        Ed25519Verifier(signer.public_key_b64()),
    )

    # Register the appender so @audit_access decorators can find it.
    set_appender(appender)

    app.state.settings = settings
    app.state.signer = signer
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.appender = appender
    app.state.chain_verifier = chain_verifier
    app.state.templates = _TEMPLATES

    try:
        yield
    finally:
        await engine.dispose()


app = FastAPI(
    title="Immutable Audit Trail System",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(api_router)

# Mount /static at app construction (no per-request state — safe outside lifespan).
app.mount("/static", StaticFiles(directory="static"), name="static")

Instrumentator().instrument(app).expose(app, endpoint="/metrics", tags=["observability"])
