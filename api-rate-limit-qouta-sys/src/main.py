"""Application entrypoint, runtime wiring and the FastAPI factory.

Three things live here, and they are the three seams the rest of the project hangs off:

* :class:`Runtime` — the single container for per-process collaborators (settings, the Redis
  gateway, the tier registry, the limiter and the identity resolver now; C9's analytics collector
  as it lands).
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
from src.api.protected import router as protected_router
from src.api.protected import verify_route_pricing
from src.config import Settings, get_settings
from src.identity import IdentityResolver
from src.limiter import Limiter
from src.middleware import RateLimitMiddleware
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
#:
#: ``WWW-Authenticate`` joined the list at C6, when something finally emitted it. It is the one
#: header :class:`~src.middleware.RateLimitMiddleware` produces that does not come from
#: :meth:`~src.models.LimitDecision.headers`, and it is not CORS-safelisted, so without it a
#: browser client that got a 401 could see the status and not the challenge — i.e. could not
#: discover that this API accepts an ``ApiKey`` scheme at all. ``tests/unit/test_middleware.py``
#: pins the correspondence in the same way ``tests/unit/test_models.py`` pins the other eight.
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
    "WWW-Authenticate",
]


@dataclass(frozen=True, slots=True)
class Runtime:
    """Per-process runtime state shared by every handler.

    Frozen: a Runtime is a container of already-constructed collaborators, not a scratchpad. The
    limiter, the tier registry, the identity resolver and the Redis gateway all own mutable state
    internally; what must never happen is one request rebinding *which* limiter — or *which
    identity resolver* — the next request uses. Freezing the container makes that structural rather
    than conventional.

    ``started_at`` is captured from :func:`time.monotonic` despite the name, not from the wall
    clock, so reported uptime cannot go backwards when NTP steps the system clock — the same
    reasoning that puts the limiter's clock inside Redis rather than on each replica.

    C9 adds ``analytics``. Read sites use ``getattr(runtime, "...", None)`` so a half-wired runtime
    degrades to a documented fallback rather than a 500.
    """

    settings: Settings
    redis: RedisGateway
    tiers: TierRegistry
    limiter: Limiter
    identity: IdentityResolver
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

        The limiter is built **after** the registry because it holds a reference to it: the tier
        table it sends to the decision script is the registry's pre-rendered snapshot, read
        synchronously per request. Its constructor is pure too — it tries to register the decision
        script and tolerates the gateway not being connected yet, which is always the case here.

        The identity resolver is built last, and it depends on neither of them. That is the design
        rather than an accident of ordering: a principal is resolved from headers and one
        ``apikey:v1:*`` lookup, and **what tier that principal is on is never read here** — it is
        read from ``user:{uid}`` inside the decision script, on every request. Wiring the resolver
        to the registry would create exactly the per-user tier cache the whole design avoids.
        """
        gateway = RedisGateway(settings)
        registry = TierRegistry(settings, gateway)
        return cls(
            settings=settings,
            redis=gateway,
            tiers=registry,
            limiter=Limiter(gateway, registry, settings),
            identity=IdentityResolver(gateway, settings),
        )

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

        The identity seed runs alongside the tier seed and under the identical rule: it writes the
        demo ``apikey:v1:*`` and ``user:{id}`` records with ``HSETNX``, and
        :meth:`~src.identity.IdentityResolver.start` never raises. A replica that cannot seed still
        authenticates against whatever another replica (or an operator) already wrote — so failing
        the boot here would trade "the demo keys 401 until Redis returns" for "nothing serves at
        all", which is the worse half of that trade in every deployment.

        It runs **after** the tier seed on purpose: the demo ``user:{id}`` records name tiers, and
        seeding a principal onto a tier before ``config:tiers`` exists would leave a window in which
        that principal resolves to a tier the decision script cannot find and falls back to
        ``DEFAULT_TIER`` for. Both are ``HSETNX`` and both are idempotent, so the ordering costs
        nothing and removes the window.
        """
        await self.redis.connect()
        await self.tiers.start()
        await self.identity.start()

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

    # =========================================================================================
    # MIDDLEWARE ORDER. Read this before adding one.
    #
    # Starlette's `add_middleware` does `user_middleware.insert(0, ...)`, and the stack is built
    # by wrapping the router in that list REVERSED. The consequence is the opposite of what the
    # reading order suggests: **the LAST middleware registered is the OUTERMOST one.**
    #
    # So the limiter is registered FIRST and CORS SECOND, which puts the limiter *inside* CORS and
    # *outside* the router. Both halves of that sandwich are load-bearing:
    #
    #   * INSIDE CORS, because the limiter short-circuits its 429 without calling anything below
    #     it. Outside CORS, that 429 would carry no `Access-Control-Allow-Origin` and no
    #     `Access-Control-Expose-Headers` — so a browser client would be unable to read the
    #     rejection *or* the `Retry-After` telling it when to come back, and the fetch would
    #     surface as an opaque network error. The rate limiter would be invisible to precisely the
    #     client the dashboard is written in. It also means a preflight `OPTIONS` is answered by
    #     CORS and never reaches the limiter, so a browser's own protocol overhead is not charged
    #     to the caller's quota.
    #   * OUTSIDE the router, because an unrouted path must still be metered. A limiter that only
    #     sees requests the router recognised is one an attacker bypasses by sending `GET /x`.
    #
    # `tests/integration/test_middleware_flow.py` asserts the CORS half against a real 429 rather
    # than trusting this comment, because the failure mode is invisible from Python.
    #
    # ---------------------------------------------------------------------------------------
    # Registered UNCONDITIONALLY, including when RATE_LIMIT_ENABLED is false. The middleware
    # self-disables at step 3 of its own flow instead.
    #
    # The alternative — `if settings.rate_limit_enabled: app.add_middleware(...)` — reads tidier
    # and is a trap. A config toggle that changes the middleware *stack* changes the shape of the
    # request path itself: which callable owns `send`, whether `scope["state"]` exists, how many
    # frames deep an exception is raised. So a bug reproduces in one mode and not the other, and
    # the switch that was added to isolate the limiter's cost becomes a second code path nobody
    # tested. Worse for C14 specifically: the overhead measurement is supposed to compare
    # "limiter on" against "limiter off" through *the same stack*, and a conditional registration
    # would have it comparing two different applications and attributing the difference to the
    # limiter.
    #
    # With the middleware always installed, "off" costs a type check, a set lookup and a memoised
    # classify — which is the honest baseline, and the one the disabled path actually pays.
    # ---------------------------------------------------------------------------------------
    app.add_middleware(RateLimitMiddleware, settings=settings)

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

    # Unversioned liveness. A future v2 adds a router beside v1 here; /health never moves.
    app.include_router(health_router)

    # The metered stub downstream — the four routes `src.keys.ROUTE_TABLE` prices. Included
    # AFTER /health so the exempt liveness probe is matched first; the two paths cannot collide,
    # but a reader should not have to prove that to themselves.
    app.include_router(protected_router)

    # =========================================================================================
    # THE PRICING CROSS-CHECK. Read this before deleting the line below.
    #
    # `src.keys.classify` decides what a request COSTS, from the raw path, above the router.
    # Starlette decides what a request DOES, from the mounted routes. Two pieces of code reading
    # the same string to answer different questions — and any input they disagree about is a
    # pricing bypass: the caller is served endpoint X and charged for endpoint Y. `src/keys.py`
    # carries two rubrics on exactly this, because the difference between the categories is 5x.
    #
    # The failure mode this call closes is mundane and therefore likely: someone renames
    # `/api/v1/logs/query` in `src/api/protected.py` and does not touch `ROUTE_TABLE`. Nothing
    # about routing breaks — the endpoint serves perfectly — and the classifier silently drops it
    # to ("other", "default"), so the project's most expensive endpoint is charged 1 token
    # instead of 5, on a different bucket key, invisibly. No routing test notices, because
    # routing is fine.
    #
    # So the correspondence is asserted at construction time and fails LOUDLY: a mismatch kills
    # the process at startup with a message naming both sides, rather than showing up as a
    # billing discrepancy nobody reads. It walks the app's real routes, so it cannot be satisfied
    # by a declaration that has drifted from what is actually mounted.
    #
    # Cost is four regex matches against an lru_cached classifier, once per app construction.
    # Deliberately NOT an `assert`: assertions are stripped under `python -O`, and a safety check
    # that a runtime flag can remove is not a safety check.
    # =========================================================================================
    logger.debug("route pricing verified:\n%s", "\n".join(verify_route_pricing(app)))

    return app


#: Served by uvicorn (see the Dockerfile CMD). Built without an explicit Runtime, so
#: :func:`lifespan` constructs and attaches one on startup.
app = create_app()
