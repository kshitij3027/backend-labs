"""Application entrypoint, runtime wiring and the FastAPI factory for the Log Query API.

Three things live here, and they are the three seams the rest of the project hangs off:

* :class:`Runtime` — the single container for per-process state (settings now; the log store
  and rate limiter from C4/C8). Handlers read it defensively off ``request.app.state.runtime``
  and degrade to a safe fallback rather than raising, so a half-wired runtime is never a 500.
* :func:`lifespan` — the production startup path. It builds a **seeded** Runtime and attaches
  it to ``app.state`` before the app serves a single request.
* :func:`create_app` — the construction site. Passing a pre-built ``runtime`` skips the
  lifespan entirely, which is the hermetic test seam: no env, no corpus seeding, no I/O.

The module-level ``app`` is what uvicorn serves (``python -m uvicorn src.main:app``). Note that
building it calls :func:`~src.config.get_settings`, so an invalid or placeholder ``JWT_SECRET``
fails the process at import time — loudly, before the port is bound, which is exactly the
README's "refuses to start" contract.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from uuid import uuid4

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from src.api.health import router as health_router
from src.config import Settings, get_settings
from src.store import LogStore

logger = logging.getLogger(__name__)

#: OpenAPI metadata. ``API_VERSION`` is also what ``GET /health`` reports back.
API_TITLE = "Log Query API (REST)"
API_VERSION = "1.0.0"

#: The hard prefix every data route carries. Unversioned paths (``/health``, ``/docs``,
#: ``/redoc``, ``/openapi.json``) are the only exceptions — a future v2 mounts a second router
#: beside v1 rather than mutating it.
API_V1_PREFIX = "/api/v1"

API_DESCRIPTION = """\
A versioned REST service exposing an in-memory log store over HTTP.

Four read paths share one corpus — paginated retrieval, filtered/structured search, a
Server-Sent Events live tail, and aggregate statistics — behind three cross-cutting gates:
JWT authentication, a strictly-ordered role ladder (viewer < analyst < writer < admin), and
a per-principal token bucket sized by the caller's tier.

`401` means "I don't know who you are"; `403` means "I know, and no". Rate-limit headers are
advertised on **every** response, not just on rejection, so a well-behaved client can pace
itself instead of discovering the ceiling by hitting it.
"""

#: Response headers browser JavaScript is allowed to read cross-origin. Without an explicit
#: ``expose_headers``, the CORS spec restricts JS to a handful of safelisted headers — so a
#: dashboard could receive X-RateLimit-Remaining and still be unable to *see* it. Every header
#: this API uses to communicate out-of-band state has to be listed here or it may as well not
#: exist for the browser client.
EXPOSE_HEADERS = [
    "X-Request-ID",
    "X-RateLimit-Limit",
    "X-RateLimit-Remaining",
    "X-RateLimit-Reset",
    "Retry-After",
    "X-Page-Limit-Clamped",
    "X-Cursor-Truncated",
]


@dataclass
class Runtime:
    """Per-process runtime state shared by every handler.

    ``store`` is C4's :class:`~src.store.LogStore` and is built by **both** constructors, so no
    handler ever has to cope with a store-less runtime in practice. It stays ``Optional``
    anyway, and read sites still use ``getattr(runtime, "store", None)``, because the whole
    point of the defensive-read convention is that a half-wired runtime degrades to a documented
    fallback rather than a 500 — a guarantee that would evaporate the moment one field made it
    unconditional. ``limiter`` (C8's ``RateLimiter``) is still typed loosely because its class
    does not exist yet; each commit narrows the annotation as it lands.

    ``started_monotonic`` is captured from :func:`time.monotonic`, not the wall clock, so
    reported uptime cannot go backwards when NTP steps the system clock.
    """

    settings: Settings
    store: LogStore | None = None
    limiter: object | None = None
    started_monotonic: float = field(default_factory=time.monotonic)

    @property
    def uptime_sec(self) -> float:
        """Seconds since this Runtime was constructed (never negative)."""
        return max(0.0, time.monotonic() - self.started_monotonic)

    @classmethod
    def build(cls, settings: Settings) -> Runtime:
        """Construct a Runtime cheaply — **no corpus seeding, no I/O**.

        The unit-test path. Injected via ``create_app(runtime=Runtime.build(settings))``, it
        skips the lifespan entirely so the HTTP surface is exercised hermetically and a test
        never pays for 10,000 generated entries it does not use.

        The store is real but **empty**: constructing a :class:`~src.store.LogStore` allocates an
        empty ``deque`` and three empty dicts regardless of ``store_capacity``, so this stays as
        cheap as it was before C4 while removing the ``store is None`` branch from every test.
        A test that wants a corpus appends one explicitly.
        """
        return cls(settings=settings, store=LogStore(capacity=settings.store_capacity))

    @classmethod
    def build_seeded(cls, settings: Settings) -> Runtime:
        """Construct the production Runtime, with the store seeded to ``settings.seed_entries``.

        Kept as a separate, already-wired entry point so the production and test paths are
        distinguishable from C1 rather than being retrofitted later.
        """
        # C5 seeds the corpus here: fill the store with generate_entries(settings.seed_entries)
        # via `store.append_many(...)`. Deliberately not wired in C4 — `src/generators.py` lands
        # in the sibling commit, and importing a module that may not exist yet would make the
        # process fail to start rather than merely start empty.
        return cls(settings=settings, store=LogStore(capacity=settings.store_capacity))


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Mint or echo ``X-Request-ID`` on every request, and stamp it on every response.

    Correlation ids are only useful if they survive the whole request — including the error
    paths — so this is middleware rather than a dependency: it wraps 200s, 401s, 403s, 429s and
    unhandled 500s alike. An id supplied by the client is echoed (so a caller can correlate
    across services); otherwise a fresh hex uuid4 is minted.
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        request_id = request.headers.get("x-request-id") or uuid4().hex
        request.state.request_id = request_id

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id

        # --- C8 HOOK -------------------------------------------------------------------
        # C8's `rate_limit` dependency stashes its Decision on `request.state` and attaches
        # X-RateLimit-Limit / -Remaining / -Reset HERE, not via a `response: Response`
        # parameter. Reason: headers set on a dependency's injected Response do not survive
        # the exception path — and the exception path (429, 403, 401) is exactly the one
        # where a client most needs to be told the ceiling. Middleware sees whatever response
        # actually leaves the app, so this is the only placement that covers all of them.
        # -------------------------------------------------------------------------------

        return response


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build a **seeded** Runtime on startup, attach it to ``app.state``, tear it down on exit.

    The production entry point. :func:`~src.config.get_settings` is called here (and in
    :func:`create_app`), so a missing or placeholder ``JWT_SECRET`` kills the process at startup
    rather than at the first token request. Tests never enter this path — they inject a
    pre-built Runtime via ``create_app(runtime=...)``.
    """
    settings = get_settings()

    # Configure the root logger once, from config, before anything logs. An unrecognised level
    # name degrades to INFO rather than exploding — a typo in LOG_LEVEL should not be fatal.
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    runtime = Runtime.build_seeded(settings)
    app.state.runtime = runtime
    logger.info(
        "runtime initialised (log_level=%s, store_capacity=%d, seed_entries=%d, "
        "rate_limit_enabled=%s, tiers=%s)",
        settings.log_level,
        settings.store_capacity,
        settings.seed_entries,
        settings.rate_limit_enabled,
        ",".join(sorted(settings.tier_limits)),
    )

    try:
        yield
    finally:
        # C10 closes SSE subscribers here: drain and unregister every live subscriber so a
        # shutdown cannot leave generators parked forever on `await queue.get()`.
        pass


def create_app(runtime: Runtime | None = None) -> FastAPI:
    """Build and return the FastAPI application.

    Args:
        runtime: Tests inject a pre-built :class:`Runtime` here; the app is then constructed
            **without** a lifespan and the runtime is attached to ``app.state`` directly, so
            there is no startup work, no seeding and no environment dependency. When omitted
            (production: the module-level ``app``), :func:`lifespan` builds and attaches a
            seeded Runtime on startup.
    """
    common = {
        "title": API_TITLE,
        "version": API_VERSION,
        "description": API_DESCRIPTION,
        # orjson is measurably faster than the stdlib encoder on the list-heavy payloads this
        # API returns (a 500-entry page is the common case), and it serialises datetimes
        # natively.
        "default_response_class": ORJSONResponse,
    }

    if runtime is not None:
        app = FastAPI(**common)  # type: ignore[arg-type]
        app.state.runtime = runtime
        settings = runtime.settings
    else:
        app = FastAPI(lifespan=lifespan, **common)  # type: ignore[arg-type]
        settings = get_settings()

    # Middleware order matters. Starlette applies middleware in reverse registration order, so
    # the LAST one added is the OUTERMOST. Registering request-context first and CORS second
    # puts CORS on the outside, where it must be: it has to be able to answer a preflight
    # OPTIONS and to decorate error responses produced further in.
    app.add_middleware(RequestContextMiddleware)

    origins = list(settings.cors_origins)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        # The CORS spec forbids pairing the wildcard origin with credentialed requests — which
        # is exactly what the README's config table says ("credentials disabled with `*`"). So
        # credentials are enabled only when the operator has named real origins.
        allow_credentials="*" not in origins,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=EXPOSE_HEADERS,
    )

    # Unversioned liveness. The /api/v1 router joins it from C5 onwards.
    app.include_router(health_router)

    return app


#: Served by uvicorn (see the Dockerfile CMD). Built without an explicit Runtime, so
#: :func:`lifespan` constructs and seeds one on startup.
app = create_app()
