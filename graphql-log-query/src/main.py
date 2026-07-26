"""Application entrypoint and the FastAPI factory for the GraphQL Log Query Platform.

Three things live here, and they are the seams the rest of the project hangs off:

* :func:`lifespan` — the production startup/shutdown path. It owns the
  :class:`~src.db.session.Database` (engine, session factory, schema creation, seeding) and will
  own the Redis client and the subscription broker from C6. Every one of those is a resource
  whose *shutdown* matters as much as its startup, which is why they belong in a lifespan and not
  in a module-level ``__init__``: an engine that is never disposed leaks connections, and a
  psubscribe reader that is never cancelled keeps uvicorn from exiting.
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
from strawberry.fastapi import GraphQLRouter
from strawberry.subscriptions import GRAPHQL_TRANSPORT_WS_PROTOCOL, GRAPHQL_WS_PROTOCOL

from src.api.health import router as health_router
from src.config import Settings, get_settings
from src.db.session import Database
from src.graphql.context import get_context
from src.graphql.schema import schema as graphql_schema

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

    Currently that is the :class:`~src.db.session.Database`; C6 adds the Redis client and the
    subscription broker at the marked seams. Resources are attached to ``app.state`` so handlers
    reach them through the request (``request.app.state.db``) rather than through module globals,
    which is what keeps two apps in one test process independent.
    """
    settings: Settings = getattr(app.state, "settings", None) or get_settings()

    # === C6 ===  app.state.redis = redis.asyncio.from_url(settings.redis_url)
    #             app.state.broker = Broker(...); await broker.start()   # psubscribe reader task
    # Lands here rather than at import time because it also needs the matching teardown in the
    # `finally` below — a psubscribe reader that is never cancelled keeps uvicorn from exiting.

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

    # The store. Built and attached BEFORE `init_db` is awaited, so the `finally` below owns the
    # engine even if startup fails partway through — an engine that raised during schema creation
    # still holds whatever connections it opened, and leaving them to the garbage collector is
    # how a restart loop turns into "too many clients already" on the database.
    db = Database.create(settings)
    app.state.db = db

    try:
        # Ordered: schema first (it retries while Postgres finishes coming up), then seeding,
        # which is a no-op unless the store is empty. Both are awaited here rather than lazily on
        # first request so a broken database is a container that fails to start — visible — rather
        # than a container that reports healthy and 500s on its first query.
        await db.init_db()
        seeded = await db.seed_if_empty(settings.seed_entries, settings.random_seed)
        logger.info(
            "store ready (rows_written=%d, seed_entries_configured=%d, random_seed=%d)",
            seeded,
            settings.seed_entries,
            settings.random_seed,
        )

        # NOTE: /health is deliberately NOT wired to any of this. The container HEALTHCHECK and
        # compose's `condition: service_healthy` both target it, so a probe that queried Postgres
        # would report the API unhealthy for as long as its dependency was reconnecting — and
        # Docker would restart a process that is working perfectly, turning a transient database
        # blip into a restart loop and flapping the gate the e2e/loadtest services wait on. Worse,
        # a probe that runs a query inherits that query's latency: one slow plan and the
        # healthcheck times out. Data-layer readiness is a startup concern, and it is handled
        # above, by failing startup.

        yield
    finally:
        # === C6 ===  await broker.close_all_subscribers(); await broker.stop()
        # === C6 ===  await app.state.redis.aclose()
        # Teardown is best-effort by construction: it runs on the way out, and a failure here
        # must not replace whatever actually caused the shutdown with a traceback from cleanup —
        # hence the swallow-and-log rather than letting `dispose` propagate out of a `finally`.
        try:
            await db.dispose()
        except Exception:  # pragma: no cover - defensive; disposal has no expected failure mode
            logger.exception("failed to dispose the database engine during shutdown")
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

    # 2. The GraphQL surface, on ONE path serving BOTH transports: HTTP POST for queries and
    #    mutations, and a WebSocket upgrade for subscriptions. Registered after /health so a
    #    dependency-free liveness probe can never be shadowed, and before the C13 SPA catch-all.
    #
    #    `context_getter` resolves ONCE PER WEBSOCKET CONNECTION, not once per operation
    #    (strawberry-graphql#1754). `get_context` therefore creates nothing that has a lifetime —
    #    it hands the resolvers `app.state.db.session_factory`, the FACTORY, never a session. See
    #    the module docstrings of src/graphql/context.py and src/db/session.py for what a
    #    per-socket session would actually do (pinned connections, permanently stale reads).
    #
    #    `graphql_ide` (not the deprecated `graphiql=`) takes the name of an IDE or None: GET
    #    /graphql serves GraphiQL when GRAPHQL_PLAYGROUND_ENABLED is set, and 404s when it is not.
    #    POST is unaffected either way, so disabling the IDE in a hostile environment does not
    #    disable the API.
    #
    #    `subscription_protocols` is declared NOW even though `Subscription` does not exist until
    #    C6. The list is a property of the transport, not of the schema — it decides which
    #    `Sec-WebSocket-Protocol` values the server will negotiate — so declaring it here means C6
    #    adds a schema field and changes nothing about the mount. Both protocols are offered:
    #    `graphql-transport-ws` is the current one (what graphql-ws@5 speaks, which is what the
    #    C13 Apollo client uses), and the legacy `graphql-ws` is kept because older clients and
    #    some tooling still only speak that one.
    graphql_router = GraphQLRouter(
        graphql_schema,
        context_getter=get_context,
        graphql_ide="graphiql" if resolved.graphql_playground_enabled else None,
        subscription_protocols=[GRAPHQL_TRANSPORT_WS_PROTOCOL, GRAPHQL_WS_PROTOCOL],
    )
    app.include_router(graphql_router, prefix="/graphql")

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
