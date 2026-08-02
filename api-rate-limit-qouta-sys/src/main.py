"""Application entrypoint, runtime wiring and the FastAPI factory.

Three things live here, and they are the three seams the rest of the project hangs off:

* :class:`Runtime` — the single container for per-process collaborators (settings, the Redis
  gateway and the tier registry now; C4's limiter and C9's analytics collector as they land).
  Handlers read it defensively off ``request.app.state.runtime`` and degrade to a safe fallback
  rather than raising, so a half-wired runtime is never a 500.
* :func:`lifespan` — the production startup path. It builds a Runtime, **starts** it (opening the
  Redis client) and attaches it to ``app.state`` before the app serves a single request, then
  stops it on the way out.
* :func:`create_app` — the construction site. Passing a pre-built ``runtime`` skips the lifespan
  entirely, which is the hermetic test seam: no env, no Redis connection, no I/O.

That last seam matters more here than in a typical service. C12's decisive test builds **two**
apps in one process, each with its own Redis client and its own pool, and drives both through
``httpx.ASGITransport`` against one real Redis — which is only possible because ``create_app``
takes a Runtime instead of constructing one from global state.

.. rubric:: Construction, start and stop are three separate steps

:meth:`Runtime.build` is synchronous and performs **no I/O** — it constructs the
:class:`~src.redis_client.RedisGateway` without connecting it. Opening the client is
:meth:`Runtime.start`, closing it is :meth:`Runtime.stop`, and :func:`lifespan` is what calls them
in production.

The split exists because ``create_app(runtime=...)`` skips the lifespan by design, so nothing would
call them on the injected path. **A test that injects a Runtime and needs Redis must therefore
await ``runtime.start()`` itself, and await ``runtime.stop()`` in teardown.** A test that does not
need Redis simply skips both: the gateway is inert until connected, and every read of it degrades
to a documented fallback (``/health`` reports ``redis: "unreachable"``), so the hermetic unit tests
that make up most of the suite still need no server at all.


The module-level ``app`` is what uvicorn serves (``python -m uvicorn src.main:app``). Building it
calls :func:`~src.config.get_settings`, so a missing or placeholder ``JWT_SECRET`` /
``API_KEY_PEPPER`` / ``ADMIN_TOKEN`` fails the process at import time — loudly, before the port is
bound.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse

from src.api.health import router as health_router
from src.config import Settings, get_settings
from src.redis_client import RedisGateway, redact_redis_url
from src.tiers import TierRegistry

logger = logging.getLogger(__name__)

#: OpenAPI metadata. ``API_VERSION`` is also what ``GET /health`` reports back.
API_TITLE = "API Rate Limiter & Quota Manager"
API_VERSION = "1.0.0"

#: The hard prefix every metered route carries. ``/health`` and the docs paths stay unversioned —
#: a liveness probe that moves with the API version is a probe that breaks your rollout.
API_V1_PREFIX = "/api/v1"

API_DESCRIPTION = """\
A distributed enforcement layer that sits in front of an HTTP API.

Every request passes two independent gates before it reaches a handler: a **token bucket**
(are you calling too fast *right now*?) and a **cumulative quota** (have you used up your
allowance for the period?). A caller can be inside its rate limit and out of quota, or the
reverse, so both are evaluated on every request and both report their state in headers.

All counter state lives in **Redis**, updated by a single atomic Lua script. That is the point
of the project rather than an implementation detail: a token bucket held in Python memory is not
a rate limit once there are two replicas behind a load balancer — it is two rate limits, and the
caller gets double.

Limits are advertised on **every** response, not only on rejection, so a well-behaved client can
pace itself instead of discovering the ceiling by hitting it. Note the deliberate unit asymmetry:
`X-RateLimit-Reset` is **delay seconds** (a duration no client needs a synced clock to use) while
`X-Quota-Reset` is a **unix timestamp** (a period rollover is a wall-clock fact).
"""

#: Response headers browser JavaScript is allowed to read cross-origin. Without an explicit
#: ``expose_headers``, the CORS spec restricts JS to a handful of safelisted headers — so the
#: dashboard would *receive* ``X-RateLimit-Remaining`` on every poll and still be unable to read
#: it. Every header this API uses to communicate out-of-band state has to be listed here or it may
#: as well not exist for the browser client, and the dashboard's entire job is displaying exactly
#: these numbers.
#:
#: Declared in full at C1 even though the middleware that emits them arrives in C6/C8: the list is
#: the contract, and a header added to the contract later but forgotten here fails silently in the
#: browser and nowhere else.
EXPOSE_HEADERS = [
    "X-RateLimit-Limit",
    "X-RateLimit-Remaining",
    "X-RateLimit-Reset",
    "X-Quota-Limit",
    "X-Quota-Remaining",
    "X-Quota-Reset",
    "Retry-After",
    "X-RateLimit-Degraded",
    "X-Served-By",
]


@dataclass(frozen=True, slots=True)
class Runtime:
    """Per-process runtime state shared by every handler.

    Frozen: a Runtime is a container of already-constructed collaborators, not a scratchpad. The
    limiter, the tier registry and the Redis gateway all own mutable state internally; what must
    never happen is one request rebinding *which* limiter the next request uses. Freezing the
    container makes that structural rather than conventional.

    ``started_at`` is captured from :func:`time.monotonic` despite the name, not from the wall
    clock, so reported uptime cannot go backwards when NTP steps the system clock — the same
    reasoning that puts the limiter's clock inside Redis rather than on each replica.

    C4 adds ``limiter``, C9 ``analytics``. Read sites use ``getattr(runtime, "...", None)`` so a
    half-wired runtime degrades to a documented fallback rather than a 500.
    """

    settings: Settings
    redis: RedisGateway
    tiers: TierRegistry
    started_at: float = field(default_factory=time.monotonic)

    @property
    def uptime_sec(self) -> float:
        """Seconds since this Runtime was constructed (never negative)."""
        return max(0.0, time.monotonic() - self.started_at)

    @classmethod
    def build(cls, settings: Settings) -> Runtime:
        """Construct a Runtime from configuration. **Synchronous, and performs no I/O.**

        One constructor for both paths — the lifespan and the injected-runtime test seam — on
        purpose. The moment production and test wiring diverge, the suite starts proving things
        about a system that is not the one being shipped, and for an enforcement layer that means
        a green test run over limits nobody is actually applying.

        The gateway is *constructed* here and *connected* in :meth:`start`. Keeping construction
        I/O-free is what lets ``create_app(runtime=Runtime.build(settings))`` stay a hermetic test
        seam: building a Runtime never dials anything, so a unit test does not need a Redis and an
        unreachable one is not an import-time failure.

        The tier registry is handed the **same** gateway object rather than building its own. One
        ``Redis`` client per process, sharing one pool, is a rule redis-py itself states: two
        clients over one pool means closing either leaves the other holding dead connections, and
        two clients with two pools doubles this process's connection footprint against a
        single-threaded server for no gain. Constructing the registry is likewise pure — it starts
        life serving ``settings.tier_limits`` and does not touch Redis until :meth:`start`.
        """
        gateway = RedisGateway(settings)
        return cls(settings=settings, redis=gateway, tiers=TierRegistry(settings, gateway))

    async def start(self) -> None:
        """Open every connection this Runtime owns. Called by :func:`lifespan`, never by ``build``.

        Idempotent, because the gateway's ``connect`` is. Note that ``redis.asyncio.from_url`` is
        lazy — no socket is opened until the first command — so an unreachable Redis does not fail
        startup here. That is the intended behaviour: the service is designed to serve (degraded)
        while Redis is down, so refusing to boot without it would trade a documented fail-open for
        an outage.

        **Order matters:** the gateway is connected before the registry is started, because
        :meth:`~src.tiers.TierRegistry.start` seeds ``config:tiers`` and takes its first snapshot —
        i.e. it is the first thing in the process to issue a command. Starting it against an
        unconnected gateway would classify every seed write as an outage and boot the replica on
        the fallback table for no reason. The registry's own start never raises, so an unreachable
        Redis still leaves a serving process.
        """
        await self.redis.connect()
        await self.tiers.start()

    async def stop(self) -> None:
        """Release every connection this Runtime owns. Never raises.

        **Order matters here too, and it is the reverse.** The registry is stopped *first*: it may
        have a background refresh in flight holding a pooled connection, and closing the pool
        underneath that task turns an orderly shutdown into a traceback (and, under compose, into
        a restart loop that looks like a crash). Cancel the borrower, then close the pool.
        """
        await self.tiers.stop()
        await self.redis.aclose()


def _configure_logging(settings: Settings) -> None:
    """Configure the root logger from configuration, before anything logs.

    Called from :func:`create_app` rather than from :func:`lifespan` so that the injected-runtime
    path gets it too. An unrecognised level name degrades to INFO rather than raising: a typo in
    ``LOG_LEVEL`` should not be the reason a rate limiter fails to start.
    """
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build a Runtime on startup, attach it to ``app.state``, tear it down on exit.

    The production entry point. :func:`~src.config.get_settings` is called here, so a missing or
    placeholder secret kills the process at startup rather than at the first authenticated
    request. Tests never enter this path — they inject a pre-built Runtime via
    ``create_app(runtime=...)`` — with one deliberate exception: a single test drives an app
    through this function so the path that actually ships is not the only one nothing exercises.
    """
    settings = get_settings()
    runtime = Runtime.build(settings)
    await runtime.start()
    app.state.runtime = runtime

    # `redact_redis_url` and NOT `settings.redis_url`: a Redis URL may legitimately carry
    # `user:password@`, and a startup line is the single most-copied piece of text in an incident —
    # it gets pasted into issues, chat and whatever aggregator ingests container stdout. Host, port
    # and database index are what an operator needs; the credentials are not.
    logger.info(
        "runtime initialised (log_level=%s, redis=%s, fail_mode=%s, "
        "rate_limit_enabled=%s, default_tier=%s, tiers=%s)",
        settings.log_level,
        redact_redis_url(settings.redis_url),
        settings.fail_mode,
        settings.rate_limit_enabled,
        settings.default_tier,
        ",".join(sorted(settings.tier_limits)),
    )

    try:
        yield
    finally:
        # try/finally rather than a bare `yield`: the pool must be released even when shutdown is
        # triggered by an exception, or a restart loop leaks a connection per cycle against a
        # server whose connection count is a finite, shared resource.
        await runtime.stop()
        logger.info("runtime shutdown complete (uptime %.1fs)", runtime.uptime_sec)


def create_app(runtime: Runtime | None = None) -> FastAPI:
    """Build and return the FastAPI application.

    Args:
        runtime: Tests inject a pre-built :class:`Runtime` here; the app is then constructed
            **without** a lifespan and the runtime is attached to ``app.state`` directly, so there
            is no startup work, no Redis connection and no environment dependency. When omitted
            (production: the module-level ``app``), :func:`lifespan` builds, starts and attaches
            one on startup and stops it on shutdown.

    .. warning::
       Injecting a ``runtime`` skips the lifespan, and therefore skips :meth:`Runtime.start` and
       :meth:`Runtime.stop` too. A test that needs the Redis gateway connected must ``await
       runtime.start()`` before driving the app and ``await runtime.stop()`` afterwards; one that
       does not need Redis skips both and gets the documented degraded behaviour instead.
    """
    common = {
        "title": API_TITLE,
        "version": API_VERSION,
        "description": API_DESCRIPTION,
        # orjson, and not only for speed: it serialises the integer-heavy counter payloads the
        # stats endpoint returns without the stdlib encoder's float round-tripping, and every
        # quantity in this project is deliberately an integer.
        "default_response_class": ORJSONResponse,
    }

    if runtime is not None:
        app = FastAPI(**common)  # type: ignore[arg-type]
        app.state.runtime = runtime
        settings = runtime.settings
    else:
        app = FastAPI(lifespan=lifespan, **common)  # type: ignore[arg-type]
        settings = get_settings()

    _configure_logging(settings)

    origins = list(settings.cors_origins)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        # The CORS spec forbids pairing the wildcard origin with credentialed requests, so
        # credentials are enabled only when the operator has named real origins.
        allow_credentials="*" not in origins,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=EXPOSE_HEADERS,
    )

    # C6 registers the pure-ASGI RateLimitMiddleware here, deliberately INSIDE CORS: Starlette
    # applies middleware in reverse registration order, so CORS (added above, hence outermost)
    # can still decorate a 429 the limiter short-circuits, and a preflight OPTIONS is answered
    # without ever reaching — or being charged by — the limiter.

    # Unversioned liveness. A future v2 adds a router beside v1 here; /health never moves.
    app.include_router(health_router)

    return app


#: Served by uvicorn (see the Dockerfile CMD). Built without an explicit Runtime, so
#: :func:`lifespan` constructs and attaches one on startup.
app = create_app()
