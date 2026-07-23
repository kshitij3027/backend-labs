"""Application entrypoint, runtime wiring and the FastAPI factory for the Log Query API.

Three things live here, and they are the three seams the rest of the project hangs off:

* :class:`Runtime` — the single container for per-process state (settings now; the log store
  and rate limiter from C4/C8). Handlers read it defensively off ``request.app.state.runtime``
  and degrade to a safe fallback rather than raising, so a half-wired runtime is never a 500.
* :func:`lifespan` — the production startup path. It builds a **seeded** Runtime and attaches
  it to ``app.state`` before the app serves a single request.
* :func:`create_app` — the construction site. Passing a pre-built ``runtime`` skips the
  lifespan entirely, which is the hermetic test seam: no env, no corpus seeding, no I/O.

.. rubric:: Rejected bodies

:func:`validation_exception_handler` renders every ``RequestValidationError`` the body routes
raise. It is registered inside :func:`create_app`, so both construction paths above carry it, and
it reports ``loc``/``msg`` while never encoding the rejected input — see its docstring for why
that is a stack-safety fix rather than a formatting preference.

The module-level ``app`` is what uvicorn serves (``python -m uvicorn src.main:app``). Note that
building it calls :func:`~src.config.get_settings`, so an invalid or placeholder ``JWT_SECRET``
fails the process at import time — loudly, before the port is bound, which is exactly the
README's "refuses to start" contract.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, status
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from src.api.health import router as health_router
from src.api.v1 import router as v1_router
from src.config import Settings, get_settings
from src.deps import rate_limit_headers
from src.generators import generate_entries
from src.ratelimit import RateLimiter
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


#: A clock. Named so the two constructors below can advertise the seam without repeating the
#: signature; :func:`time.monotonic` is what production uses.
TimeFunc = Callable[[], float]


def _build_limiter(settings: Settings, clock: TimeFunc | None = None) -> RateLimiter:
    """Size a :class:`~src.ratelimit.RateLimiter` from configuration.

    One construction site for both :meth:`Runtime.build` and :meth:`Runtime.build_seeded`, so the
    test path and the production path cannot be limited differently — which is precisely the class
    of bug that makes a limiter look correct in CI and behave differently in the container.

    ``clock`` is a **test seam**, and it exists because the alternative is a flaky suite. The
    limiter is defined in terms of elapsed time, so an integration test that fires 21 requests at
    a burst-20 bucket is racing its own runtime: at the free tier's 10 tokens/s, the ~50 ms those
    requests take is worth half a token, and whether the 21st is refused depends on how loaded the
    machine is. Freezing the clock makes the assertion exact. Production passes ``None`` and gets
    :func:`time.monotonic` — see :class:`~src.ratelimit.TokenBucket` for why never the wall clock.
    """
    if clock is None:
        return RateLimiter(settings.tier_limits, enabled=settings.rate_limit_enabled)
    return RateLimiter(
        settings.tier_limits, enabled=settings.rate_limit_enabled, time_func=clock
    )


@dataclass
class Runtime:
    """Per-process runtime state shared by every handler.

    ``store`` is C4's :class:`~src.store.LogStore` and ``limiter`` is C8's
    :class:`~src.ratelimit.RateLimiter`; both are built by **both** constructors, so no handler
    ever has to cope with a runtime missing either one in practice. They stay ``Optional``
    anyway, and read sites still use ``getattr(runtime, "store", None)`` /
    :func:`~src.deps.limiter_from_request`, because the whole point of the defensive-read
    convention is that a half-wired runtime degrades to a documented fallback rather than a 500 —
    a guarantee that would evaporate the moment one field made it unconditional.

    ``started_monotonic`` is captured from :func:`time.monotonic`, not the wall clock, so
    reported uptime cannot go backwards when NTP steps the system clock.
    """

    settings: Settings
    store: LogStore | None = None
    limiter: RateLimiter | None = None
    started_monotonic: float = field(default_factory=time.monotonic)

    @property
    def uptime_sec(self) -> float:
        """Seconds since this Runtime was constructed (never negative)."""
        return max(0.0, time.monotonic() - self.started_monotonic)

    @classmethod
    def build(cls, settings: Settings, *, limiter_clock: TimeFunc | None = None) -> Runtime:
        """Construct a Runtime cheaply — **no corpus seeding, no I/O**.

        The unit-test path. Injected via ``create_app(runtime=Runtime.build(settings))``, it
        skips the lifespan entirely so the HTTP surface is exercised hermetically and a test
        never pays for 10,000 generated entries it does not use.

        The store is real but **empty**: constructing a :class:`~src.store.LogStore` allocates an
        empty ``deque`` and three empty dicts regardless of ``store_capacity``, so this stays as
        cheap as it was before C4 while removing the ``store is None`` branch from every test.
        A test that wants a corpus appends one explicitly.

        Args:
            limiter_clock: See :func:`_build_limiter`. Test seam only.
        """
        return cls(
            settings=settings,
            store=LogStore(capacity=settings.store_capacity),
            limiter=_build_limiter(settings, limiter_clock),
        )

    @classmethod
    def build_seeded(
        cls, settings: Settings, *, limiter_clock: TimeFunc | None = None
    ) -> Runtime:
        """Construct the production Runtime, with the store seeded to ``settings.seed_entries``.

        Kept as a separate, already-wired entry point so the production and test paths are
        distinguishable from C1 rather than being retrofitted later.

        The corpus comes from :func:`~src.generators.generate_entries`, which returns entries
        **oldest first** — so the store's monotonic ``seq`` order agrees with time order, which is
        the assumption every newest-first scan and every cursor anchor in ``src/store.py`` rests
        on. Seeding newest-first would leave the store internally consistent and sorted backwards.

        Args:
            limiter_clock: See :func:`_build_limiter`. Test seam only.
        """
        store = LogStore(capacity=settings.store_capacity)
        # `seed_entries=0` is a normal configuration rather than an edge case — the compose
        # `test` service pins it so the suite starts from an empty, fully deterministic store —
        # so the guard skips a pointless generate-and-append round trip rather than defending
        # against something invalid.
        if settings.seed_entries > 0:
            # `append_many` takes the store's lock ONCE for the whole batch. Seeding 10,000
            # entries through `append` would be 10,000 uncontended lock round-trips inside the
            # container healthcheck's start_period, for no benefit.
            store.append_many(generate_entries(settings.seed_entries))
        return cls(
            settings=settings,
            store=store,
            limiter=_build_limiter(settings, limiter_clock),
        )


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Stamp ``X-Request-ID`` and the ``X-RateLimit-*`` triple on **every** response.

    Both are middleware concerns for the same reason: they are only useful if they survive the
    whole request, error paths included. This wraps 200s, 401s, 403s, 429s and unhandled 500s
    alike, because it sees the response that actually leaves the app rather than the one a
    handler intended to return.

    The alternative — a dependency writing to its injected ``response: Response`` — silently
    drops everything the moment a dependency raises, and the raising paths (``401``, ``403``,
    ``429``) are exactly the ones where a client most needs to be told what its ceiling is. A
    ``429`` that does not say when to come back is barely better than a connection reset.

    An id supplied by the client is echoed (so a caller can correlate across services);
    otherwise a fresh hex uuid4 is minted.
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        request_id = request.headers.get("x-request-id") or uuid4().hex
        request.state.request_id = request_id

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id

        # `request.state` is backed by the ASGI `scope`, and `call_next` hands the SAME scope
        # object to the app underneath — so the Decision the `rate_limit` gate stashed several
        # layers in is readable right here, on the way back out.
        #
        # `rate_limit_headers` returns an EMPTY mapping when this request had no principal
        # (a 401, /health, /docs, the token endpoint), and nothing is emitted in that case. That
        # is deliberate: with no principal there is no bucket, so any value would be invented,
        # and a header claiming a ceiling that was never evaluated is worse than a missing one —
        # a client can handle absence but cannot detect fiction. See `src.deps` for the full
        # three-case rule, including why a 403 reports a peeked (non-consuming) allowance.
        for header, value in rate_limit_headers(request).items():
            response.headers[header] = value

        return response


#: The ``detail`` every rejected body carries. A fixed string rather than a rendering of the
#: failure, because :class:`~src.models.ErrorBody` types ``detail`` as ``str`` and the per-field
#: specifics belong in ``errors`` below. FastAPI's default handler puts a *list* under ``detail``,
#: which every route in ``src/api/v1.py`` already contradicts by publishing ``ErrorBody`` as its
#: ``422`` model — so this constant also closes a gap between the document and the wire.
VALIDATION_ERROR_DETAIL = "request body failed validation; see `errors` for the offending fields"

#: The machine-readable half, for a client that would rather branch on a code than parse prose.
VALIDATION_ERROR_CODE = "validation_error"

#: How many individual field errors a single ``422`` will report. A body can fail validation in
#: as many places as it has fields, and a hostile one can have a great many; twenty is far more
#: than a human debugging a request needs and bounds the response regardless.
MAX_REPORTED_VALIDATION_ERRORS = 20

#: Per-error ``msg`` (and ``loc`` component) ceiling. Pydantic messages quote the offending value
#: in some cases, so this is the second place a pathological body could inflate the response.
MAX_VALIDATION_MESSAGE_CHARS = 300


def _validation_error_payload(
    request: Request, exc: RequestValidationError
) -> dict[str, Any]:
    """Render a :class:`~fastapi.exceptions.RequestValidationError` as an ``ErrorBody`` payload.

    Only three scalars survive from each entry in ``exc.errors()`` — ``type``, ``loc`` and
    ``msg``. ``input`` and ``ctx`` are dropped entirely, and that is the whole point of this
    function rather than an incidental tidy-up:

    * **It removes the recursion instead of moving it.** ``input`` is the *rejected value*, so for
      a filter tree nested five hundred levels deep it is a five-hundred-level structure — and
      FastAPI's default handler runs :func:`~fastapi.encoders.jsonable_encoder` over it, which
      recurses once per level. That is a ``RecursionError`` raised *inside the error handler*,
      which the server can only report as a ``500``: the documented ``MAX_FILTER_DEPTH`` guard
      correctly refuses the body and the client is told the server broke. Raising
      ``sys.setrecursionlimit`` would buy a deeper cliff at the cost of trading a caught exception
      for a hard interpreter crash on stack exhaustion — the cliff moves, it does not disappear.
      Never encoding the input at all is the only version with no cliff in it.
    * **It is a privacy win.** ``POST /logs/search`` takes its filter in the body partly so that
      sensitive search terms stay out of proxy access logs and browser history, which query
      strings do not. Echoing the rejected body back inside an error payload would hand those
      same terms to every error-tracking sink the client pipes ``422``s into, and undo the reason
      the route is a ``POST``.

    ``loc`` is kept in full (bounded only by length) because it is what makes an ordinary mistake
    diagnosable: without it a client is told *that* its body is wrong and never *where*.
    """
    errors: list[dict[str, Any]] = []
    for raw in list(exc.errors())[:MAX_REPORTED_VALIDATION_ERRORS]:
        errors.append(
            {
                "type": str(raw.get("type", VALIDATION_ERROR_CODE)),
                # Pydantic hands back a tuple of str|int; ints are array indexes and stay ints so
                # `body.filter.all.0` is still walkable programmatically. Anything exotic is
                # stringified rather than trusted to serialise.
                "loc": [
                    part
                    if isinstance(part, int)
                    else str(part)[:MAX_VALIDATION_MESSAGE_CHARS]
                    for part in (raw.get("loc") or ())
                ],
                "msg": str(raw.get("msg", ""))[:MAX_VALIDATION_MESSAGE_CHARS],
            }
        )

    return {
        "detail": VALIDATION_ERROR_DETAIL,
        "code": VALIDATION_ERROR_CODE,
        # Read exactly the way every other consumer of the correlation id reads it, and defaulted
        # rather than indexed: an exception handler runs on paths where the middleware may not
        # have (a malformed ASGI scope, a handler swapped in by a test), and an `AttributeError`
        # raised *here* would turn this 422 straight back into the 500 it exists to prevent.
        "request_id": getattr(request.state, "request_id", None),
        "errors": errors,
    }


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> Response:
    """Return ``422`` for a body FastAPI refused, without ever encoding the body itself.

    Registered in :func:`create_app` (not on the module-level ``app``) so the injected-runtime
    test path and the lifespan production path get identical behaviour — a hardening that only
    applied to one of them would be worse than none, because the suite would prove the wrong one.

    The whole payload construction is wrapped: an exception handler that can itself raise is just
    a slower ``500``, so if anything at all goes wrong — an unexpected ``errors()`` shape, an
    encoder refusing a value — the fallback is a minimal, statically-built body that cannot fail
    to serialise. The status code is the contract; the diagnostics are best-effort.

    Worth knowing when reading the tests: a body can be deep enough that this handler never runs.
    The stdlib JSON decoder recurses once per container, so past a few hundred levels on CPython
    3.11 ``json.loads`` itself refuses the document and FastAPI answers ``400`` ("there was an
    error parsing the body") before any model is consulted. Where that ceiling sits moves between
    interpreter versions — 3.12 and 3.14 parse thousands of levels, which is exactly the range
    where the default handler used to answer ``500``. Both ``400`` and ``422`` are the server
    refusing the *client's* body; only ``500`` was the server admitting it broke.
    """
    try:
        return ORJSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=_validation_error_payload(request, exc),
        )
    except Exception:  # pragma: no cover - defensive; nothing above is expected to raise
        logger.warning("could not render validation error detail", exc_info=True)
        return ORJSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "detail": VALIDATION_ERROR_DETAIL,
                "code": VALIDATION_ERROR_CODE,
                "request_id": None,
                "errors": [],
            },
        )


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
    # `seed_entries` is what was REQUESTED; `store_entries` is what actually landed. The two
    # differ whenever SEED_ENTRIES exceeds STORE_CAPACITY (the ring evicts while it is being
    # filled), and the resident count is the number every later question — page.total, /health,
    # /stats — is actually answered from, so it is the one worth having in the startup line.
    seeded = len(runtime.store) if runtime.store is not None else 0
    logger.info(
        "runtime initialised (log_level=%s, store_capacity=%d, seed_entries=%d, "
        "store_entries=%d, rate_limit_enabled=%s, tiers=%s)",
        settings.log_level,
        settings.store_capacity,
        settings.seed_entries,
        seeded,
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

    # Every route that takes a body — `POST /logs`, `POST /logs/search`, the token form —
    # rejects a bad one through the same exception, so the handler is registered once here
    # rather than per-route. Registering it inside `create_app` is what makes it unconditional:
    # both the lifespan path and the injected-runtime test path are built through this function.
    app.add_exception_handler(
        RequestValidationError,
        validation_exception_handler,  # type: ignore[arg-type]
    )

    # Unversioned liveness, then the versioned data surface. They are two routers rather than
    # one so `/health` cannot drift under `/api/v1` — see `src/api/health.py`. A future v2 adds
    # a THIRD `include_router` line here beside v1; it never edits v1's shapes.
    app.include_router(health_router)
    app.include_router(v1_router)

    return app


#: Served by uvicorn (see the Dockerfile CMD). Built without an explicit Runtime, so
#: :func:`lifespan` constructs and seeds one on startup.
app = create_app()
