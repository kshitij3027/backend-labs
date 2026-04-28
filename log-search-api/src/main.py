from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse

from src.api.v1.router import router as v1_router
from src.clients.elasticsearch import make_es_client
from src.clients.redis import make_redis_client, make_redis_pool
from src.config import get_settings

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    logging.basicConfig(level=settings.LOG_LEVEL.upper())
    logger.info("starting %s version %s", settings.PROJECT_NAME, app.version)

    app.state.es = make_es_client(settings)
    app.state.redis_cache_pool = make_redis_pool(settings.REDIS_URL, settings.CACHE_REDIS_DB)
    app.state.redis_cache = make_redis_client(app.state.redis_cache_pool)

    try:
        yield
    finally:
        logger.info("stopping %s", settings.PROJECT_NAME)
        try:
            await app.state.es.close()
        except Exception as exc:
            logger.warning("error closing elasticsearch client: %s", exc)
        try:
            await app.state.redis_cache.aclose()
        except Exception as exc:
            logger.warning("error closing redis client: %s", exc)
        try:
            await app.state.redis_cache_pool.aclose()
        except Exception as exc:
            logger.warning("error closing redis pool: %s", exc)


def build_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.PROJECT_NAME,
        version="0.1.0",
        default_response_class=ORJSONResponse,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(v1_router)
    return app


app = build_app()
