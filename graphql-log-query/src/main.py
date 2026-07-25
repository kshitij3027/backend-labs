"""Application entrypoint and the FastAPI factory for the GraphQL Log Query Platform.

Three things live here, and they are the seams the rest of the project hangs off:

* :func:`lifespan` — the production startup/shutdown path. At C1 it only configures the process
  and logs; C2 hangs the SQLAlchemy engine on it, C6 the Redis client and the subscription
  broker. Every one of those is a resource whose *shutdown* matters as much as its startup, which
  is why they belong in a lifespan and not in a module-level ``__init__``.
* :func:`create_app` — the construction site. Passing a pre-built ``settings`` skips
  :func:`~src.config.get_settings` entirely, which is the hermetic test seam: no ``.env``, no
  environment dependency, no shared LRU cache between tests.
* the module-level ``app`` — what uvicorn serves (``python -m uvicorn src.main:app``, the image's
  CMD).

.. rubric:: Route registration order is a correctness property here, not a style choice

Starlette matches routes in **registration order**, and C13 adds a catch-all SPA fallback so that
a browser deep-link like ``/orders/42`` returns ``index.html`` instead of a 404. A catch-all
registered before ``/graphql``, ``/health`` or ``/metrics`` would swallow all three and serve HTML
to an API client. The ordering below is annotated accordingly, and the SPA block is marked as the
**last** thing that may ever be registered.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.health import router as health_router
from src.config import Settings, get_settings

logger = logging.getLogger(__name__)

#: OpenAPI metadata. FastAPI's own docs surface is incidental here — the schema that matters is
#: the GraphQL one, exported to ``schema.graphql`` from C3 — but the title/version still show up
#: in ``/docs`` and in the app object, so they are set deliberately rather than defaulted.
API_TITLE = "GraphQL Log Query Platform"
API_VERSION = "1.0.0"
API_DESCRIPTION = """\
A GraphQL API over a persisted log store.

One endpoint (`/graphql`) serves flexible filtered queries, SQL-computed aggregate stats, log
creation, and real-time streaming over WebSocket — with DataLoader batching, Redis result
caching, automatic persisted queries, and a pre-execution depth/complexity gate in front of it.

`/health` is the only other unversioned HTTP route that is part of the contract; `/metrics`
carries Prometheus text exposition from C9.
"""


def configure_logging(settings: Settings) -> None:
    """Configure the root logger once, from configuration, before anything logs.

    Called from :func:`create_app` rather than from :func:`lifespan` so that a test client which
    never enters the lifespan still gets configured logging — otherwise a failing test's only
    diagnostic output would be whatever ``logging``'s "last resort" handler decides to emit.

    An unrecognised ``LOG_LEVEL`` degrades to ``INFO`` instead of raising: a typo in an env var
    should not be the reason a container will not start.
    """
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Own every process-scoped resource: build it on startup, tear it down on exit.

    At C1 this only logs — there is nothing to build yet — but the shape is already the one the
    later commits need, and each of them is marked below. Resources are attached to
    ``app.state`` so handlers reach them through the request (``request.app.state``) rather than
    through module globals, which is what keeps two apps in one test process independent.
    """
    settings: Settings = getattr(app.state, "settings", None) or get_settings()

    # === C2 ===  engine = create_async_engine(settings.database_url, pool_pre_ping=True, …)
    #             await init_db(engine)   # retry-on-boot create_all, db_init_retries x delay
    #             app.state.engine, app.state.session_factory = engine, async_sessionmaker(engine)
    # === C6 ===  app.state.redis = redis.asyncio.from_url(settings.redis_url)
    #             app.state.broker = Broker(...); await broker.start()   # psubscribe reader task
    # Each lands here rather than at import time because each also needs the matching teardown
    # in the `finally` below — an engine that is never disposed leaks connections, and a
    # psubscribe reader that is never cancelled keeps uvicorn from exiting.

    logger.info(
        "starting %s v%s (log_level=%s, seed_entries=%d, default_query_limit=%d, "
        "max_query_limit=%d, cache_enabled=%s, cache_ttl_seconds=%d, max_query_depth=%d, "
        "max_query_complexity=%d, subscription_queue_maxsize=%d, playground=%s, metrics=%s)",
        API_TITLE,
        API_VERSION,
        settings.log_level,
        settings.seed_entries,
        settings.default_query_limit,
        settings.max_query_limit,
        settings.cache_enabled,
        settings.cache_ttl_seconds,
        settings.max_query_depth,
        settings.max_query_complexity,
        settings.subscription_queue_maxsize,
        settings.graphql_playground_enabled,
        settings.metrics_enabled,
    )

    try:
        yield
    finally:
        # === C6 ===  await broker.close_all_subscribers(); await broker.stop()
        # === C6 ===  await app.state.redis.aclose()
        # === C2 ===  await app.state.engine.dispose()
        # Teardown is best-effort by construction: it runs on the way out, and a failure here
        # must not replace whatever actually caused the shutdown with a traceback from cleanup.
        logger.info("shutdown complete")


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build and return the FastAPI application.

    Args:
        settings: Tests inject a directly-constructed :class:`~src.config.Settings` here, so the
            app is built without consulting the environment, the ``.env`` file, or the
            :func:`~src.config.get_settings` LRU cache that another test may have populated.
            When omitted (production: the module-level ``app``), settings are read from the
            environment in the usual precedence order.
    """
    resolved = settings if settings is not None else get_settings()
    configure_logging(resolved)

    app = FastAPI(
        title=API_TITLE,
        version=API_VERSION,
        description=API_DESCRIPTION,
        lifespan=lifespan,
    )
    # Attached BEFORE the lifespan can run (startup happens on the first request / on entering
    # the TestClient context manager), so an injected Settings is what the lifespan sees too.
    app.state.settings = resolved

    # CORS is registered first and is therefore the OUTERMOST middleware once more are added:
    # Starlette applies middleware in reverse registration order, and CORS must be outside
    # everything so it can answer a preflight OPTIONS and decorate errors raised further in.
    #
    # The dashboard does not need any of this — C13 serves the SPA from this very process, so its
    # GraphQL calls are same-origin. This exists for third-party clients and for running the Vite
    # dev server on another port during frontend work.
    origins = list(resolved.cors_origin_list)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        # The CORS spec forbids pairing the wildcard origin with credentialed requests, so
        # credentials are enabled only when the operator has named real origins.
        allow_credentials="*" not in origins,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # --- Route registration. Order matters; see this module's docstring. ---

    # 1. Liveness. Unversioned and dependency-free — the container HEALTHCHECK and compose's
    #    `condition: service_healthy` both target it.
    app.include_router(health_router)

    # === C3 ===  The GraphQL surface, mounted at /graphql, serving BOTH transports on one path:
    #             HTTP POST for queries/mutations and graphql-transport-ws (+ the legacy
    #             graphql-ws) for subscriptions.
    #
    #                 graphql_app = GraphQLRouter(
    #                     schema,
    #                     graphiql=resolved.graphql_playground_enabled,
    #                     subscription_protocols=[
    #                         GRAPHQL_TRANSPORT_WS_PROTOCOL, GRAPHQL_WS_PROTOCOL,
    #                     ],
    #                     context_getter=get_context,
    #                 )
    #                 app.include_router(graphql_app, prefix="/graphql")
    #
    #             NOTE for C3: `context_getter` resolves ONCE PER WEBSOCKET CONNECTION, not per
    #             operation. Anything session- or loader-shaped created in it would live for the
    #             life of the socket and serve stale rows; those belong in a SchemaExtension's
    #             `on_operation` hook instead.

    # === C9 ===  GET /metrics — Prometheus text exposition from an explicit CollectorRegistry,
    #             gated on `resolved.metrics_enabled`.

    # === C13 ==  The React SPA, and it MUST BE REGISTERED LAST — after /graphql, /health and
    #             /metrics — because the SPA fallback is a catch-all:
    #
    #                 if SPA_DIR.is_dir():
    #                     app.mount("/assets", StaticFiles(directory=SPA_DIR / "assets"), ...)
    #                     app.include_router(spa_router)   # GET / and the {path:path} fallback
    #
    #             A catch-all registered earlier matches `/graphql` first and serves index.html
    #             to an API client, which surfaces as "the playground returns HTML" and takes an
    #             hour to diagnose. Guarded on `is_dir()` so a build without the SPA still runs
    #             the API: StaticFiles raises at CONSTRUCTION time when its directory is absent,
    #             which would be an import-time crash of the whole API over an optional UI.

    return app


#: Served by uvicorn (see the Dockerfile CMD). Built without injected settings, so configuration
#: is read from the environment in the documented precedence order.
app = create_app()
