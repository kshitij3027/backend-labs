from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from fastapi.staticfiles import StaticFiles
from slowapi.middleware import SlowAPIMiddleware

from src.api import dashboard as dashboard_router
from src.api.v1.router import router as v1_router
from src.clients.elasticsearch import bootstrap_index, make_es_client
from src.clients.redis import make_redis_client, make_redis_pool
from src.config import get_settings
from src.middleware.errors import register_error_handlers
from src.middleware.rate_limit import limiter
from src.middleware.request_id import RequestIDMiddleware
from src.services.cache import CacheCounters, SearchCache

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    logging.basicConfig(level=settings.LOG_LEVEL.upper())
    logger.info("starting %s version %s", settings.PROJECT_NAME, app.version)

    app.state.es = make_es_client(settings)
    app.state.redis_cache_pool = make_redis_pool(settings.REDIS_URL, settings.CACHE_REDIS_DB)
    app.state.redis_cache = make_redis_client(app.state.redis_cache_pool)
    app.state.cache_counters = CacheCounters()
    app.state.search_cache = SearchCache(
        app.state.redis_cache,
        settings.SEARCH_CACHE_TTL_SECONDS,
        app.state.cache_counters,
    )

    try:
        await bootstrap_index(app.state.es, settings.ELASTICSEARCH_INDEX)
    except Exception as exc:
        logger.warning(
            "elasticsearch index bootstrap failed for %s: %s",
            settings.ELASTICSEARCH_INDEX,
            exc,
        )

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
        version="1.0.0",
        description=(
            "JWT-protected, rate-limited, Redis-cached log search API backed by "
            "Elasticsearch. Exposes single + bulk ingest, ranked full-text "
            "search with filters, aggregations, pagination, and a built-in "
            "dashboard at `GET /`."
        ),
        default_response_class=ORJSONResponse,
        lifespan=lifespan,
        docs_url="/api/docs",
        redoc_url="/api/redoc",
        openapi_url="/openapi.json",
    )

    app.state.limiter = limiter

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=[
            "X-Request-ID",
            "X-RateLimit-Limit",
            "X-RateLimit-Remaining",
            "X-RateLimit-Reset",
            "Retry-After",
        ],
    )
    app.add_middleware(SlowAPIMiddleware)
    app.add_middleware(RequestIDMiddleware)

    register_error_handlers(app)

    # Static assets + Jinja dashboard (mounted at app root, unversioned).
    app.mount("/static", StaticFiles(directory="src/static"), name="static")

    app.include_router(v1_router)
    app.include_router(dashboard_router.router)
    return app


app = build_app()
