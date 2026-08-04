"""The enforcement layer itself: one pure-ASGI middleware, six decisions, one round trip.

Everything C1-C5 built is inert until something calls it on the request path. This is that
something. It classifies the endpoint, resolves the caller, runs the one ``EVALSHA``, and then
either short-circuits with a 429 or lets the request through with its allowance advertised in
headers.

.. rubric:: Pure ASGI, and NOT ``BaseHTTPMiddleware``. This is the decisive design choice here

``BaseHTTPMiddleware`` exists to adapt the ASGI three-callable protocol into the far friendlier
``async def dispatch(request, call_next)`` shape. It buys that ergonomics with real machinery **per
request**: an ``anyio`` task group plus a *pair* of memory-object streams, so the downstream app can
run concurrently with the ``call_next`` coroutine and stream its response back through a queue.

Starlette's own documentation and benchmarks put that overhead at roughly **0.3-1 ms per request**.
This project's stated budget for the entire rate-limit check is **5 ms**, so the adapter alone would
consume **6-20% of the whole budget** — on the one code path whose sole reason for existing is that
budget. Paying a fifth of the latency allowance to avoid writing thirty lines of ASGI is not a
trade; it is the feature being spent on its own plumbing.

The budget argument is decisive on its own, and there is a second, non-negotiable one:
``BaseHTTPMiddleware`` **breaks ``contextvars`` propagation**. Because the downstream app runs in a
child task, anything a handler sets in a context variable is not visible to the middleware after
``call_next`` returns, and anything the middleware sets is not reliably visible downstream. C9's
analytics fires *after* the response body is sent and wants request-scoped context; a structured
logger or a tracing span would want the same. Building on a base class that severs that link would
mean discovering the limitation two commits later, from the wrong end.

Written as a plain ASGI callable, the cost of this middleware is: two ``dict`` lookups, one set
membership test, one memoised ``classify``, and a closure. The awaits are the ones that were always
going to be there — the identity lookup and the decision script.

.. rubric:: The ordered flow, and why each step is where it is

1. ``scope["type"] != "http"`` -> pass through. **The #1 bug in hand-written ASGI middleware.**
2. Exempt paths -> pass through, unmetered and unauthenticated.
3. ``RATE_LIMIT_ENABLED=false`` -> pass through, with **no** rate-limit headers.
4. Classify the endpoint and look up its weighted cost. Pure, free, and needed regardless.
5. Resolve the principal. No principal -> 401 with ``WWW-Authenticate`` and no limit headers.
6. Read the tier snapshot (synchronously) and run the one ``EVALSHA``.
7. Stash the decision on ``scope["state"]`` so a handler *may* read it.
8. Denied -> emit the 429 directly. **The wrapped app is never invoked.**
9. Allowed -> wrap ``send`` and append the headers to whatever the app produces.

Each step is commented at its own site with why it sits where it does; the ordering is a contract,
not an accident, and three of the four "why is this before that?" answers are security properties.

.. rubric:: The path this file reasons about is ``get_route_path(scope)``, never ``scope["path"]``

Both the exemption check and :func:`src.keys.classify` are given
:func:`starlette.routing.get_route_path`, which is ``scope["path"]`` with ``scope["root_path"]``
removed. That is not a stylistic preference — it is **the same function Starlette's own
``Route.matches`` calls**, so what this middleware thinks a request is and what the router
dispatches it to are the same string by construction rather than by two files happening to agree.

The two strings diverge the moment the application is mounted under a prefix, which is a
deployment decision made outside this repository (an ASGI server started with ``--root-path``, a
proxy that forwards one, a sub-app mount). With ``root_path="/gw"``, ``scope["path"]`` is
``/gw/api/v1/logs/query`` while the router sees ``/api/v1/logs/query``, and reading the raw path
produces two live bugs at once:

* **A 5x pricing bypass.** ``GET /gw/api/v1/logs/query`` is served by the real, expensive handler
  and classified as ``("other", "default")`` — 1 token instead of 5, charged to
  ``rate_limit:{user}:other``, a bucket that has nothing to do with the endpoint being used. The
  overspend is invisible in that endpoint's own metering. This is the identical failure
  :func:`src.keys.classify` documents for a trailing ``%0A`` and for ``HEAD``, reached through a
  third door.
* **A healthcheck that 401s.** ``/gw/health`` misses :func:`is_exempt`, so the container
  ``HEALTHCHECK`` starts getting 401s and compose restarts a replica that is serving perfectly —
  the same restart loop C12's "nginx must not add a path prefix" note is about, except triggered
  from inside the app instead of from the proxy.

Note what does **not** catch this: ``src.api.protected.verify_route_pricing`` compares path
*templates*, and a template is root_path-independent, so every one of its checks passes while the
bypass is live. A cross-check cannot cover a divergence in the input both sides are given. Only
taking the path from the router's own accessor can.

Nothing in this repository sets a ``root_path`` today (the Dockerfile's ``CMD`` is a bare
``uvicorn src.main:app``), which is exactly why this is worth pinning: the bug is not reachable
now, is one deployment flag away, and would be silent when it arrived.

.. rubric:: The 429 is emitted as raw ASGI messages, and it HAS to be

The obvious implementation is ``raise HTTPException(429, ...)``. It does not work here, and it does
not fail loudly either — it produces a 500.

FastAPI's exception handlers are installed by ``ExceptionMiddleware``, which Starlette places at the
**innermost** end of the stack, immediately around the router. This middleware is registered
*outside* that (it has to be: it must run before routing, so an unrouted path is still metered), so
an ``HTTPException`` raised here sails straight past every handler that would have turned it into a
response and lands on ``ServerErrorMiddleware`` as an unhandled exception. The rejection a client
should have been able to retry becomes an error the service looks broken for.

So the denial path writes ``http.response.start`` and ``http.response.body`` itself, with an
explicit ``content-length``: without it some servers fall back to chunked transfer encoding for a
tiny JSON body, which is wasted framing and confuses the simplest possible client (a shell loop
with ``curl``) at exactly the moment it is trying to read a rejection.

.. rubric:: EVERY per-request value is a local in :meth:`RateLimitMiddleware.__call__`

The principal, the decision, the cost, the label, the captured status code: all locals, none on
``self``. Starlette's own middleware documentation calls this out, and the reason is not style. One
middleware instance serves every concurrent request in the process. A field on ``self`` written at
the start of request A and read at the end of request A is read *after* request B has overwritten it
the moment the event loop suspends at either ``await`` — and both awaits here are I/O. The
observable result is one caller receiving another caller's ``X-RateLimit-Remaining``, or worse being
metered against another caller's bucket: a cross-request data leak whose likelihood rises with load,
which is precisely when nobody is reading logs.

``tests/unit/test_middleware.py`` pins this with a test that would fail if any of it moved onto the
instance: N concurrent requests with N different principals, rendezvoused so that *all* of them are
suspended inside ``__call__`` simultaneously, each asserting it got its own numbers back.

.. rubric:: What this middleware deliberately does NOT do

* **It does not build a Starlette ``Request``.** Constructing one to read two headers costs ~30 us
  of object graph per request; :func:`src.identity.header_value` takes the raw
  ``scope["headers"]`` list precisely so that never has to happen. See that function.
* **It does not re-derive header logic.** Every header value and the whole 429 body come from
  :meth:`~src.models.LimitDecision.headers` and
  :meth:`~src.models.LimitDecision.error_body`. A second place that formatted
  ``X-RateLimit-Remaining`` would be a second definition of what the number means.
* **It does not decide the fail-open policy.** :meth:`src.limiter.Limiter.check` does, once, in
  the module that owns ``FAIL_MODE``, and it returns a decision rather than raising — so this file
  has **no** ``except`` around the limiter call. What it does own is the *rendering*: which status
  code a refusal gets. See the next rubric.
* **It does not record analytics.** C9 does, at the marked seam at the bottom of
  :meth:`RateLimitMiddleware.__call__`, where the response body has already been sent.

.. rubric:: Two failures, two branches — the C8 decision this file DOES own

Read the "READ THIS BEFORE WRITING C8" rubric in :mod:`src.identity` alongside this. The two
awaits on this path fail for different reasons and get different answers, and collapsing them into
one ``except`` would be an authentication bypass:

**Identity could not be resolved -> 503, never a principal, never a pass-through.** Nothing is
known about the caller. Serving them would be serving an unauthenticated request to anyone holding
any string, for as long as the store is down — and identity resolution is a *pre-auth, unmetered*
path sharing the limiter's pool, so an attacker with no credential can manufacture the condition
on demand and then walk in. That is not degradation; it is an authentication bypass whose timing
the attacker chooses. Failing closed on identity while the limiter fails open is a coherent policy
and it is the one this service ships.

**The JWT path is unaffected, and that is worth stating because it is easy to break.** A Bearer
token is verified with one HMAC over bytes already in memory: :meth:`src.identity.IdentityResolver.resolve`
touches Redis **only** on the API-key branch, so a JWT-authenticated caller keeps being
authenticated — and then metered through the fallback bucket — for the whole outage, while API-key
callers get a 503. That is strictly better than 503-ing everyone, and it is why the identity
concurrency bound lives inside the resolver, wrapped around the *lookup*, rather than around the
``resolve`` call here: a semaphore at this level would make the credential form that needs no Redis
queue behind the one that does.

**Limits could not be checked -> the limiter's own decision flows through normally.** Under
``FAIL_MODE=open`` that is a degraded 200 (or a degraded 429) carrying ``X-RateLimit-Degraded: 1``
via the existing :meth:`~src.models.LimitDecision.headers`. Under ``FAIL_MODE=closed``, and for a
saturated connection pool in *either* mode, it is a refusal carrying
:attr:`~src.models.DenyReason.BACKING_STORE`, which this file renders as a **503 with
``Retry-After`` — not a 429**. The distinction is not pedantry: 429 means "you are over your
limit", and a client library will treat it as a signal about *its own* behaviour. This caller is
not over any limit; we were unable to find out. A 503 says that, and it says it in the one status
code every HTTP client already understands as "the server, not you".
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Final, Protocol

import orjson
from starlette.datastructures import MutableHeaders
from starlette.routing import get_route_path
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from src.config import DEFAULT_COST_CATEGORY, Settings
from src.identity import ACCEPTED_SCHEMES, WWW_AUTHENTICATE, IdentityResolver
from src.keys import classify
from src.limiter import Limiter
from src.models import (
    DEGRADED_HEADER,
    DEGRADED_HEADER_VALUE,
    RETRY_AFTER_HEADER,
    DenyReason,
    LimitDecision,
)
from src.redis_client import BackingStoreUnavailable

logger = logging.getLogger(__name__)

__all__ = [
    "EXEMPT_EXACT_PATHS",
    "EXEMPT_PATH_PREFIXES",
    "IDENTITY_UNAVAILABLE_DETAIL",
    "JSON_CONTENT_TYPE",
    "LIMITER_UNAVAILABLE_DETAIL",
    "LimiterRuntime",
    "RateLimitMiddleware",
    "SCOPE_DECISION_KEY",
    "SCOPE_ENDPOINT_KEY",
    "SERVICE_UNAVAILABLE_ERROR",
    "STATUS_SERVICE_UNAVAILABLE",
    "STATUS_TOO_MANY_REQUESTS",
    "STATUS_UNAUTHORIZED",
    "UNAUTHORIZED_DETAIL",
    "UNAUTHORIZED_ERROR",
    "WWW_AUTHENTICATE_HEADER",
    "is_exempt",
]


# ---------------------------------------------------------------------------------------------
# Exempt paths
#
# An allowlist, and each entry earns its place below. Note what is NOT here: no wildcard, no
# regex, no "starts with /api/v1 and contains 'public'". Every exemption is a hole in the
# enforcement layer, so the set of holes is a literal list a reviewer can read in one glance.
# ---------------------------------------------------------------------------------------------

#: Exempt on an **exact** match only.
#:
#: ``/health``
#:     A liveness probe that can return 429 is not a liveness probe. This is not theoretical: the
#:     container ``HEALTHCHECK`` polls this path every 10 seconds from a single source address, and
#:     compose, nginx (C12) and any orchestrator treat a non-200 as "restart this replica". Meter it
#:     and a busy-but-healthy replica eventually gets restarted *by its own rate limiter* — the
#:     limiter taking the service down by working correctly. It is also unauthenticated, which is
#:     the second half of the same point: the healthcheck carries no credential, so metering it
#:     would 401 it long before it ever 429'd.
#:
#: ``/docs`` / ``/redoc`` / ``/openapi.json``
#:     The API's own documentation. Gating the description of how to authenticate behind
#:     authentication is a closed loop, and these are static, cacheable bytes with no user data in
#:     them. (``/docs/oauth2-redirect`` is deliberately absent: this service declares no OAuth2
#:     flow, so FastAPI's redirect page is unreachable in practice and exempting a path nothing
#:     serves would be a hole with no user.)
#:
#: ``/favicon.ico``
#:     Every browser requests it, unprompted, on every page load — including the dashboard's. It is
#:     not a call the caller chose to make, so charging their quota for it would meter the browser
#:     rather than the client.
EXEMPT_EXACT_PATHS: Final[frozenset[str]] = frozenset(
    {
        "/health",
        "/docs",
        "/redoc",
        "/openapi.json",
        "/favicon.ico",
    }
)

#: Exempt on the path itself **or any path segment below it** — never on a bare string prefix. See
#: :func:`is_exempt` for why that distinction is the whole point of this tuple.
#:
#: ``/dashboard``
#:     C15's dashboard shell. Two reasons, and the second is the load-bearing one: it is static
#:     bytes carrying no user data, and it is the surface from which a human authenticates. Rate
#:     limiting the page that renders the limiter's own state means that during the incident you
#:     built the dashboard for, the dashboard is the first thing to stop working — and a caller
#:     handed a 429 for the shell has no page left to authenticate from.
#:
#: ``/static``
#:     The CSS and JS the dashboard is made of. Exempting the document and metering its assets
#:     would produce a page that half-loads, which is a worse failure than either extreme.
#:
#: ``/api/v1/admin``
#:     **The most important exemption in this list.** The admin API is how an operator *raises* a
#:     limit. If it were metered, then during exactly the incident where every request is 429ing,
#:     the call that would fix it would 429 too — the control plane locked behind the failure it
#:     controls. C10 puts this surface behind ``ADMIN_TOKEN``; it is exempt from *metering*, not
#:     from *authentication*, and those are different words.
#:
#:     **Note for C10.** Until that commit lands this prefix is unmetered *and* unauthenticated, so
#:     it is an anonymous 404 factory today and an anonymous 401 factory afterwards. That is
#:     acceptable only while the check that refuses those callers is **free**: ``ADMIN_TOKEN`` must
#:     be compared in-process with :func:`hmac.compare_digest` and must **not** touch Redis. An
#:     admin auth check that issued a lookup would turn an exempt, unauthenticated surface into an
#:     amplifier against the shared connection pool and circuit breaker — the same pre-auth
#:     exhaustion vector :mod:`src.identity` documents for the identity path, but without even the
#:     rate limiter in front of it.
EXEMPT_PATH_PREFIXES: Final[tuple[str, ...]] = (
    "/dashboard",
    "/static",
    "/api/v1/admin",
)

#: Path segments that must never appear in a path that wins exemption. See :func:`is_exempt`.
_DOT_SEGMENTS: Final[frozenset[str]] = frozenset({".", ".."})


def is_exempt(path: str) -> bool:
    """Whether ``path`` bypasses the whole enforcement layer. Exact match or path-segment prefix.

    .. rubric:: ``startswith`` on a bare prefix is the bug this function exists to not have

    ``path.startswith("/api/v1/admin")`` is true for ``/api/v1/administrator``, and
    ``path.startswith("/health")`` is true for ``/healthz-of-mine``. Either one is an **unmetered,
    unauthenticated route created by a naming coincidence** — and it is a route an attacker can go
    looking for, because the exempt list is in the README. So a prefix matches only when the next
    character is a ``/``, i.e. when the prefix is a real path *segment* boundary, and everything
    else is compared for equality.

    ``tests/unit/test_middleware.py`` asserts the three near misses directly (``/healthz``,
    ``/dashboardx``, ``/api/v1/administrator``) rather than trusting the reasoning above.

    .. rubric:: One trailing slash is not a different resource

    Starlette's router 307-redirects ``/health/`` to ``/health``, but this middleware runs *above*
    the router and sees the raw path — so without normalising, ``/health/`` would be metered (and
    therefore 401'd) instead of redirected. The same rule :func:`src.keys._normalise_path` applies
    to the classifier, applied here for the same reason: one endpoint, one spelling.

    Note the direction this can and cannot go wrong in. Normalising *widens* the exemption for the
    listed paths only; it cannot pull a metered route into the allowlist, because a metered route's
    normalised form is still not in the list. A path padded with control bytes (``/health%0A``)
    goes the other way: it is not normalised here, so it falls through to metering even though
    Starlette's ``$``-anchored router would serve it. That is the safe asymmetry
    :func:`src.keys.classify` documents at length — over-charging a padded request is a bug report,
    under-charging one is a bypass available to anyone.

    .. rubric:: A path carrying a dot segment can NEVER win exemption — defence in depth

    ``/api/v1/admin/../../v1/logs/query`` string-prefixes an exempt entry while naming a metered
    endpoint, so a matcher that looked only at prefixes would serve it **unmetered and
    unauthenticated**. The percent-encoded spellings (``%2e%2e``, ``/./``) arrive here already
    decoded — the ASGI scope carries the decoded path — so they are the same string by the time
    this function sees them.

    Nothing actually serves those paths today, and that is precisely the argument for the guard
    rather than against it: the safety comes from two facts this module does not own — Starlette
    never resolves dot segments (so they 404) and ``StaticFiles`` carries its own traversal check.
    Either can change without this file being edited. A ``Mount`` under ``/dashboard`` or
    ``/static`` that resolves its own segments, or a proxy in front that normalises before
    forwarding while the app does not, converts a coincidence into a live bypass. "Safe because a
    component I do not control happens not to do the dangerous thing" is not a security property.
    The guard costs one substring scan on the common path; the alternative costs an audit of
    everything anyone ever mounts.

    Note the *direction*: dot segments are **refused exemption**, not normalised away. Normalising
    would make this function decide what ``/dashboard/../health`` "really means" and then require
    it to agree with whatever the router decides — reintroducing the exact disagreement being
    avoided. Falling through to the metered path is the answer that cannot be wrong in the
    dangerous direction.

    A literal ``%`` is refused for the same reason and at the same price: the scope path is already
    decoded, so an exempt path never legitimately contains one, and refusing a double-encoded
    ``%252e%252e`` costs a single ``in`` test.
    """
    # `"." in path` first: it is one substring scan, and it short-circuits the list allocation of
    # `split` for the overwhelming majority of real paths, which contain no dot at all. The dotted
    # exempt entries (`/openapi.json`, `/favicon.ico`, `/static/app.css`) pay the split and pass.
    if "%" in path or ("." in path and _DOT_SEGMENTS.intersection(path.split("/"))):
        return False

    if len(path) > 1 and path.endswith("/"):
        # `or "/"` because a path of nothing but slashes ("//") rstrips to the empty string, and an
        # empty path is not a path — it would match no exact entry and no prefix, which is right by
        # accident rather than by construction. Root is explicit.
        path = path.rstrip("/") or "/"

    if path in EXEMPT_EXACT_PATHS:
        return True

    return any(
        path == prefix or path.startswith(prefix + "/") for prefix in EXEMPT_PATH_PREFIXES
    )


# ---------------------------------------------------------------------------------------------
# Wire constants
# ---------------------------------------------------------------------------------------------

STATUS_UNAUTHORIZED: Final = 401
STATUS_TOO_MANY_REQUESTS: Final = 429

#: Emitted when the enforcement layer could not reach a verdict: identity was unresolvable, the
#: store was unreachable under ``FAIL_MODE=closed``, or this process's connection pool was
#: saturated. **Never 429** — see the "two failures, two branches" rubric in the module docstring.
STATUS_SERVICE_UNAVAILABLE: Final = 503

JSON_CONTENT_TYPE: Final = "application/json"

#: RFC 9110 §11.6.1. The challenge itself is :data:`src.identity.WWW_AUTHENTICATE`, generated from
#: :data:`~src.identity.ACCEPTED_SCHEMES` so that what this service *accepts* and what it *says* it
#: accepts cannot drift apart.
WWW_AUTHENTICATE_HEADER: Final = "WWW-Authenticate"

#: The 401 body's ``error``. Deliberately **not** one of the two spec strings
#: (:data:`~src.models.ERROR_RATE_LIMIT` / :data:`~src.models.ERROR_QUOTA`): a 401 is not a limit
#: decision, no bucket was consulted, and a client that pattern-matches on those literals must not
#: be told an authentication failure was a rate-limit failure.
UNAUTHORIZED_ERROR: Final = "Unauthorized"

#: Generated from :data:`~src.identity.ACCEPTED_SCHEMES`, for the same anti-drift reason
#: :data:`~src.identity.WWW_AUTHENTICATE` is: a hand-written sentence here would still say
#: "Bearer" the day a third scheme is added.
#:
#: It says which schemes exist and nothing else. It does NOT say whether the presented credential
#: was unknown, expired, malformed or revoked — see :meth:`~src.identity.IdentityResolver.resolve`:
#: every one of those is one fact to the caller ("this credential is not usable here"), and
#: distinguishing them on the wire is a credential-enumeration oracle.
UNAUTHORIZED_DETAIL: Final = (
    "Send an API key as 'X-API-Key: <key>', or an Authorization header using one of: "
    + ", ".join(scheme.display for scheme in ACCEPTED_SCHEMES)
    + ". The WWW-Authenticate header on this response carries the same list."
)

#: Serialised once at import. The 401 body is a constant — it names no caller, echoes nothing back
#: and carries no per-request number — so re-encoding it per rejection would be JSON serialisation
#: on the one path an unauthenticated flood consists entirely of.
_UNAUTHORIZED_BODY: Final[bytes] = orjson.dumps(
    {"error": UNAUTHORIZED_ERROR, "detail": UNAUTHORIZED_DETAIL}
)

#: The 503 body's ``error``. Deliberately **not** one of the two spec literals
#: (:data:`~src.models.ERROR_RATE_LIMIT` / :data:`~src.models.ERROR_QUOTA`), for the same reason
#: :data:`UNAUTHORIZED_ERROR` is not: a client that pattern-matches those strings must never be
#: told that an unavailable enforcement layer was a limit it exceeded. It is also why the 503 body
#: is built here rather than from :meth:`~src.models.LimitDecision.error_body`, which renders
#: ``"Rate limit exceeded"`` for any non-quota reason and would say exactly that.
SERVICE_UNAVAILABLE_ERROR: Final = "Service Unavailable"

#: Why an identity failure is a 503 and not a pass-through, in one sentence a caller can act on.
#: It names no credential, no digest and no store detail — the caller's remedy is identical
#: whatever the cause, and enumerating causes on the wire is what turns an error body into an
#: oracle.
IDENTITY_UNAVAILABLE_DETAIL: Final = (
    "The credential store could not be reached, so this request could not be authenticated. "
    "This is refused rather than served: serving it would mean admitting a caller whose identity "
    "was never established. Retry after the interval below."
)

#: Why a limiter failure is a 503 and not a 429. Says explicitly that the caller is *not* over
#: their limit, because that is the exact wrong conclusion for them to draw and the one a 429 would
#: have invited.
LIMITER_UNAVAILABLE_DETAIL: Final = (
    "The rate limiter could not reach a verdict for this request, so it was refused rather than "
    "admitted unmetered. This does not mean you are over your limit — it means the limit could "
    "not be checked. Retry after the interval below."
)

# Lower-case bytes, as ASGI requires header names to be. Pre-encoded because these three never
# change and encoding them per response is pure waste on the denial path.
_RAW_CONTENT_TYPE: Final = b"content-type"
_RAW_CONTENT_LENGTH: Final = b"content-length"
_RAW_JSON_CONTENT_TYPE: Final = JSON_CONTENT_TYPE.encode("latin-1")

# ---------------------------------------------------------------------------------------------
# `scope["state"]` keys
#
# Namespaced with `rlq_` because `scope["state"]` is shared with every other middleware and with
# uvicorn's own lifespan state. A key called "decision" would be a collision waiting for the second
# middleware anyone adds.
# ---------------------------------------------------------------------------------------------

#: Where the :class:`~src.models.LimitDecision` is stashed. Read it from a handler as
#: ``request.state.rlq_decision`` (Starlette's ``Request.state`` is a view over ``scope["state"]``).
SCOPE_DECISION_KEY: Final = "rlq_decision"

#: Where the classified endpoint label is stashed. Set on **every** metered request, including the
#: ones that never get a decision (a 401) and the ones where enforcement is switched off — which is
#: the whole reason it is a separate key rather than a field read off the decision.
SCOPE_ENDPOINT_KEY: Final = "rlq_endpoint"


class LimiterRuntime(Protocol):
    """The slice of :class:`src.main.Runtime` this middleware actually uses.

    A :class:`~typing.Protocol` rather than an import of ``Runtime``, for two reasons. The
    structural one: ``src.main`` imports this module, so importing ``Runtime`` back would be a
    circular import. The useful one: it states in the type system that this middleware needs
    exactly two collaborators, so a reader does not have to grep to find out whether it also
    reaches for the gateway or the tier registry behind their backs.
    """

    identity: IdentityResolver
    limiter: Limiter


def _runtime_of(scope: Scope) -> LimiterRuntime:
    """Read the runtime off ``scope["app"].state``, or raise naming the wiring bug.

    Starlette sets ``scope["app"]`` before the middleware stack runs, so this is the same object a
    handler would reach through ``request.app.state.runtime`` — one source of truth, read at request
    time rather than captured at construction time. That matters because ``create_app()`` on the
    production path registers this middleware *before* :func:`src.main.lifespan` has built a
    runtime at all; capturing one in ``__init__`` would only work on the injected-runtime test seam,
    which is the wrong half of the pair to be correct for.

    .. rubric:: A missing runtime raises. It does not degrade

    Every other read of ``app.state.runtime`` in this project (``/health``, C10's admin surface)
    degrades to a documented fallback rather than raising, and this one deliberately does not.
    There is no safe fallback available here:

    * Passing the request through unmetered is a **silent unmetered request** — the exact failure
      this whole project exists to make impossible, arriving through a wiring mistake.
    * Answering 401 would be a lie: nothing was asked about the caller's credential.
    * Answering 503 would dress a bug in this process up as a store outage, which is precisely the
      confusion :meth:`src.limiter.Limiter._ensure_registered` refuses to introduce — and it would
      make C8's fail-open logic apply to a missing ``await runtime.start()``.

    A ``RuntimeError`` naming the cause is the only answer that cannot be mistaken for a policy.
    It is unreachable in both shipped construction paths (the lifespan attaches the runtime before
    uvicorn accepts a connection; ``create_app(runtime=...)`` attaches it synchronously), so this
    is a guard against a future fourth path, not against today's two.
    """
    runtime = getattr(getattr(scope.get("app"), "state", None), "runtime", None)
    if runtime is None:
        raise RuntimeError(
            "RateLimitMiddleware found no runtime on app.state — the enforcement layer cannot "
            "meter a request without one, and passing it through unmetered would silently "
            "disable rate limiting. Build the app with create_app(runtime=...) or let the "
            "lifespan attach one."
        )
    return runtime


def _scope_state(scope: Scope) -> dict[str, Any]:
    """Return ``scope["state"]``, creating it if the server did not.

    ``state`` is optional in the ASGI HTTP scope and the servers disagree: uvicorn supplies a
    per-request shallow copy of the lifespan state, while ``httpx.ASGITransport`` and Starlette's
    ``TestClient`` supply nothing at all. Creating it here rather than requiring it means a handler
    can read ``request.state.rlq_decision`` identically under all three, instead of the stash
    working in production and silently not in tests.
    """
    state = scope.get("state")
    if state is None:
        state = scope["state"] = {}
    return state


async def _send_json(
    send: Send, *, status: int, body: bytes, headers: Mapping[str, str]
) -> None:
    """Emit a complete JSON response as raw ASGI messages. The short-circuit path's only writer.

    Used by both refusals (401 and 429) so there is one definition of "what a short-circuited
    response looks like on the wire" rather than two that agree by inspection.

    Header names are lower-cased and encoded to **bytes**, because that is what an ASGI
    ``http.response.start`` message carries — a ``str`` here does not raise, it produces a header
    the server may drop or mangle depending on which server it is.

    ``content-length`` is set explicitly and computed from the body that is actually sent. Without
    it, servers fall back to chunked transfer encoding for a ~400-byte JSON document: wasted
    framing, and a needlessly awkward read for the simplest client anyone points at a rate limiter
    (a shell loop with ``curl``). Computing it from ``len(body)`` rather than tracking it alongside
    is the only way it cannot be wrong.

    The header list is rebuilt per response rather than shared from a module constant: it is handed
    to the server, and the CORS middleware above us takes a copy of it — but "takes a copy" is a
    property of somebody else's implementation, and a shared mutable list on the response path is
    not a thing to be right about by luck.
    """
    raw: list[tuple[bytes, bytes]] = [
        (name.lower().encode("latin-1"), value.encode("latin-1"))
        for name, value in headers.items()
    ]
    raw.append((_RAW_CONTENT_TYPE, _RAW_JSON_CONTENT_TYPE))
    raw.append((_RAW_CONTENT_LENGTH, str(len(body)).encode("latin-1")))

    await send({"type": "http.response.start", "status": status, "headers": raw})
    await send({"type": "http.response.body", "body": body})


async def _send_unauthorized(send: Send) -> None:
    """Emit the 401: a challenge, a JSON body, and **no** ``X-RateLimit-*`` / ``X-Quota-*``.

    The absent headers are the point. With no principal there is no bucket and no quota counter, so
    every number those headers could carry would be fabricated — and a fabricated
    ``X-RateLimit-Remaining`` is worse than a missing one, because a client can detect a missing
    header and cannot detect a wrong one. It will happily pace itself off the fiction.

    ``WWW-Authenticate`` is mandatory on a 401 (RFC 9110 §11.6.1 — a 401 without it is malformed)
    and carries **both** accepted schemes, generated from
    :data:`~src.identity.ACCEPTED_SCHEMES`. A challenge naming only ``Bearer`` would tell a client
    holding a perfectly valid API key that its credential type is not supported here.
    """
    await _send_json(
        send,
        status=STATUS_UNAUTHORIZED,
        body=_UNAUTHORIZED_BODY,
        headers={WWW_AUTHENTICATE_HEADER: WWW_AUTHENTICATE},
    )


async def _send_unavailable(
    send: Send, *, detail: str, retry_after: int, degraded: bool
) -> None:
    """Emit the 503 for a request the enforcement layer could not decide.

    .. rubric:: No ``X-RateLimit-*`` and no ``X-Quota-*``, on purpose

    The same rule as the 401, for the same reason: **no gate was evaluated**, so every number those
    headers could carry would be invented. A client cannot detect a wrong header and will pace
    itself off it; it can detect a missing one. The only numbers on this response are ones we
    actually know — the retry interval, and (when the fallback policy is what refused) the fact
    that this replica is degraded.

    ``Retry-After`` is floored at 1 second at the call sites and again here. A ``Retry-After: 0``
    handed to a client that is already being refused is a retry storm the service manufactured for
    itself, and a 503 is precisely the response type clients retry hardest against.

    ``X-RateLimit-Degraded`` appears only when the refusal *came from* the degraded policy
    (``FAIL_MODE=closed`` with the store down). A pool-exhaustion 503 does not carry it: nothing
    was degraded, the store is healthy, and this replica simply ran out of connections — reporting
    it as degradation would be the misdiagnosis the whole overload/outage split exists to prevent.
    """
    headers = {RETRY_AFTER_HEADER: str(max(1, retry_after))}
    if degraded:
        headers[DEGRADED_HEADER] = DEGRADED_HEADER_VALUE
    await _send_json(
        send,
        status=STATUS_SERVICE_UNAVAILABLE,
        body=orjson.dumps({"error": SERVICE_UNAVAILABLE_ERROR, "detail": detail}),
        headers=headers,
    )


async def _send_denied(send: Send, decision: LimitDecision) -> None:
    """Emit the 429 for ``decision``. Both the body and every header come from the decision.

    Not one number is re-derived here — see :meth:`~src.models.LimitDecision.headers` and
    :meth:`~src.models.LimitDecision.error_body`. That is the contract that keeps the response path
    free of I/O: the decision already carries every quantity a header or a body field could want,
    read at the single instant the decision was made, so there is no second Redis round trip and no
    chance of reporting a number that raced the request it describes.

    The status is 429 for **both** families — "you are going too fast" and "you have spent your
    allowance". The distinction lives in the body's ``reason`` and in the ``error`` string
    (``"Rate limit exceeded"`` vs ``"Quota exceeded"``, both spec literals). Using a different
    status code for the quota family would be defensible in a green-field API and is wrong here:
    the spec names 429, and every HTTP client library on earth already has retry behaviour attached
    to it.
    """
    await _send_json(
        send,
        status=STATUS_TOO_MANY_REQUESTS,
        body=orjson.dumps(decision.error_body()),
        headers=decision.headers(),
    )


class RateLimitMiddleware:
    """Meters every non-exempt HTTP request. A plain ASGI callable — see the module docstring.

    Constructed with the process's :class:`~src.config.Settings` and nothing else. The runtime's
    collaborators are read per request off ``scope["app"].state.runtime`` (see :func:`_runtime_of`),
    because on the production path they do not exist yet when this is registered.

    Settings, by contrast, *are* known at construction on both paths and are immutable for the life
    of the process — :func:`src.main.create_app` passes ``runtime.settings`` on the injected seam
    and the ``get_settings()`` singleton on the production one, which is the same object the
    lifespan's runtime is built from. Taking them here rather than off the runtime is what lets
    steps 1-3 of the flow (including the kill switch) work on an app whose runtime is not wired
    yet, instead of failing before reaching the switch that would have turned this off.
    """

    def __init__(self, app: ASGIApp, settings: Settings) -> None:
        self.app = app
        self._settings = settings

        # Resolved once. `parse_endpoint_costs` guarantees the "default" category exists (it
        # refuses to parse a spec without one), so this cannot KeyError — and binding it here means
        # the guarantee is checked when the app is built rather than on the first request to an
        # unclassified path, which is where a missing default would otherwise surface.
        self._default_cost: int = settings.endpoint_costs[DEFAULT_COST_CATEGORY]

        # `Retry-After` for the identity 503. The breaker's cooldown, because that is exactly how
        # long this process will wait before it next discovers whether the credential store is
        # back — advising a caller to return sooner is advising them to be refused again. Floored
        # at 1: `Retry-After: 0` on a 503 is a retry storm, and 503 is the status clients retry
        # against hardest.
        self._identity_retry_after: int = max(1, int(settings.breaker_cooldown_sec))

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Run the ordered flow for one request. Every value below is a local. See the docstring."""
        # ----------------------------------------------------------------------------------- #
        # 1. Not an HTTP request -> nothing to meter.
        #
        # THE first thing any hand-written ASGI middleware must do, and the most common bug in one
        # that skips it. This callable is invoked for the `lifespan` scope at startup and shutdown,
        # and for `websocket` scopes — neither of which has `scope["path"]` in the sense used
        # below (`lifespan` has no path at all), so without this guard the process raises a
        # KeyError *during startup*, before it has served anything, and the traceback points at a
        # rate limiter rather than at the missing guard.
        #
        # It is also correct on the merits and not merely defensive: a websocket has one handshake
        # and then an unbounded number of frames, so metering the handshake would charge one token
        # for an arbitrarily long session. Metering websockets means counting frames, which is a
        # different design, not a missing branch.
        # ----------------------------------------------------------------------------------- #
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # ----------------------------------------------------------------------------------- #
        # 1b. THE path this middleware reasons about is `get_route_path(scope)`, NOT
        #     `scope["path"]`. Read the rubric in the module docstring before changing this line.
        #
        # `get_route_path` is `scope["path"]` with `scope["root_path"]` stripped, and it is
        # literally the function `starlette.routing.Route.matches` calls to decide what a request
        # is. Using it here is what makes classifier/router agreement STRUCTURAL rather than a
        # coincidence that holds only while nothing sets a root_path.
        # ----------------------------------------------------------------------------------- #
        path: str = get_route_path(scope)

        # ----------------------------------------------------------------------------------- #
        # 2. Exempt paths -> straight through, unmetered and unauthenticated.
        #
        # BEFORE identity, deliberately. `/health` has no credential to present, and an exemption
        # that still required authentication would 401 the container healthcheck — the same
        # restart loop metering it would cause, arriving one status code earlier. See
        # `EXEMPT_EXACT_PATHS` for what each entry buys.
        # ----------------------------------------------------------------------------------- #
        if is_exempt(path):
            await self.app(scope, receive, send)
            return

        # ----------------------------------------------------------------------------------- #
        # 3 & 4. Classify, then consult the operability switch, then price.
        #
        # Classification runs above the switch because BOTH branches need it: when enforcement is
        # off, C9's analytics still wants to know which endpoint was called, and a request recorded
        # against no label is a hole in the graph exactly when someone is comparing "with the
        # limiter" against "without it" (which is what the switch is for — it is C14's overhead
        # baseline). It is pure, memoised and free, so computing it once above the branch costs
        # nothing and duplicating the call below would cost a reader.
        #
        # Everything the switch actually gates is still gated: no cost is looked up, no principal is
        # resolved, no script runs, and — the part worth stating — **no rate-limit headers are
        # emitted**. A header describing a limit that was never evaluated is a lie, and it is the
        # kind of lie a client builds pacing logic on top of.
        #
        # Classifying before identity is also deliberate, and it is not just ordering hygiene: the
        # label is a component of the bucket key, so it has to exist regardless of who is calling,
        # and it means an unauthenticated request to a path that does not exist is still classified
        # (as "other") and still countable by C9 — which is the traffic you most want on the graph.
        #
        # One consequence worth stating because it reads as a bug: a METERED path written with a
        # trailing slash is charged twice for one logical call. `GET /api/v1/logs/query/` is
        # metered here (5 tokens), Starlette's router then answers it with a 307 to
        # `/api/v1/logs/query`, and the client's follow-up is metered again (5 more). Both are real
        # requests this service really served, so neither charge is fabricated — and it is the safe
        # side of the same asymmetry `src.keys.classify` documents: over-charging an unusual
        # spelling is a bug report, under-charging it is a bypass. Deliberately NOT "fixed" by
        # having the middleware resolve redirects itself, which would mean this file predicting the
        # router's answer. The exempt paths do not have this property, because `is_exempt`
        # normalises one trailing slash before matching.
        # ----------------------------------------------------------------------------------- #
        label, category = classify(scope["method"], path)
        state = _scope_state(scope)
        state[SCOPE_ENDPOINT_KEY] = label

        if not self._settings.rate_limit_enabled:
            await self.app(scope, receive, send)
            return

        # `.get(category, default)` and not `[category]`: `classify` can return a category that
        # `ENDPOINT_COSTS` does not price (an operator can drop a category from the env without
        # touching the route table), and a KeyError on the hot path would turn a configuration gap
        # into a 500 for every caller of that endpoint. The default is the same weight an
        # unclassified path is charged, which is the honest answer to "we do not have a price for
        # this".
        cost: int = self._settings.endpoint_costs.get(category, self._default_cost)

        # ----------------------------------------------------------------------------------- #
        # 5. Who is calling?
        #
        # Raw `scope["headers"]` — no Starlette `Request` is built anywhere in this file. See
        # `src.identity.header_value` for the measurement behind that.
        #
        # `BackingStoreUnavailable` here is a **503, never a pass-through and never a principal**.
        # This is the first half of the C8 decision (the module docstring argues it in full): the
        # limiter failing open serves an unmetered request to a KNOWN customer, which is the
        # documented degradation; identity failing open would serve an unauthenticated request to
        # anyone holding any string — an authentication bypass, on a pre-auth path an attacker with
        # no credential can saturate on demand and then walk through.
        #
        # `FAIL_MODE` deliberately gets no vote here. It configures what happens when *limits*
        # cannot be checked; there is no deployment for which "we could not establish who you are,
        # so come in" is the right answer, and making it configurable would be offering it.
        #
        # Note what is NOT refused: a Bearer token never reaches Redis (see
        # `IdentityResolver._resolve_jwt`), so JWT callers keep authenticating straight through the
        # outage and are then metered by the fallback bucket. Only the API-key branch 503s.
        # ----------------------------------------------------------------------------------- #
        runtime = _runtime_of(scope)
        try:
            principal = await runtime.identity.resolve(scope["headers"])
        except BackingStoreUnavailable as exc:
            # WARNING, not ERROR: this is a dependency failure being handled exactly as designed,
            # and it can arrive at request rate. The op name distinguishes the store outage from
            # the saturated pool (`BackingStoreOverloaded` carries the same `op` shape), and the
            # gateway has already logged the underlying cause once at its own site.
            logger.warning(
                "identity could not be resolved (%s); refusing with %d rather than admitting an "
                "unauthenticated request",
                exc.op or "identity",
                STATUS_SERVICE_UNAVAILABLE,
            )
            await _send_unavailable(
                send,
                detail=IDENTITY_UNAVAILABLE_DETAIL,
                retry_after=self._identity_retry_after,
                degraded=False,
            )
            return
        if principal is None:
            await _send_unauthorized(send)
            return

        # ----------------------------------------------------------------------------------- #
        # 6 & 7. The tier snapshot, then the ONE EVALSHA.
        #
        # The plan's flow lists `tiers.snapshot()` as its own step here, and it happens exactly
        # here — inside `Limiter.check`, which opens with `self._tiers.snapshot().argv_tail`.
        # Calling it a second time from this file would read the tier table twice per request and
        # then discard one of the two answers; worse, the two reads can legitimately straddle the
        # snapshot's TTL, so this middleware and the limiter would be looking at different tables
        # while appearing to share one. The property the step exists to guarantee — that reading
        # the tier table is **synchronous, with no await and no Redis on the hot path** — is a
        # property of `TierRegistry.snapshot` and is asserted in `tests/unit/test_tiers.py`; it is
        # not made truer by calling it from a second place.
        #
        # `check()` is the only await on this path that touches the store, and it is one round
        # trip: four gates read and four counters written, atomically, on Redis's single thread.
        # ----------------------------------------------------------------------------------- #
        decision = await runtime.limiter.check(principal.user_id, label, cost)

        # ----------------------------------------------------------------------------------- #
        # 8. Stash the decision where a handler *may* find it.
        #
        # This is what the spec's "transparent to route handlers" means, precisely: **available,
        # never mandatory**. No handler is required to read it, no dependency has to be declared to
        # get it, and a route added by someone who has never heard of this middleware behaves
        # correctly. A handler that *does* want it (an echo endpoint, a debug view) reads
        # `request.state.rlq_decision`.
        #
        # Set BEFORE the denial check, so it is present on the 429 path too — for a future error
        # handler, and for C9, which wants the decision whether or not the request was admitted.
        # ----------------------------------------------------------------------------------- #
        state[SCOPE_DECISION_KEY] = decision

        # ----------------------------------------------------------------------------------- #
        # 9. Denied -> emit the refusal here and return. The wrapped app is NEVER invoked.
        #
        # That is the property, not a side effect: a rejected request costs zero downstream work.
        # No routing, no dependency resolution, no handler, no database. It matters most under
        # exactly the load that produces 429s — if a refused request still cost a trip through the
        # router and a handler, a caller could saturate the service *with requests it is refusing*,
        # and the rate limiter would be a queue rather than a gate.
        #
        # **Which** refusal is the second half of the C8 decision. `BACKING_STORE` is the one
        # reason the decision script cannot produce: the limiter builds it by hand when it could
        # not reach a verdict at all — `FAIL_MODE=closed` with the store down, or a saturated
        # connection pool in either mode. Those get a 503, because 429 is a statement about the
        # CALLER ("you are going too fast") and this caller has done nothing measurable. Every
        # other reason is a real gate refusing a real overage, and keeps the spec's 429.
        # ----------------------------------------------------------------------------------- #
        if not decision.allowed:
            if decision.reason is DenyReason.BACKING_STORE:
                await _send_unavailable(
                    send,
                    detail=LIMITER_UNAVAILABLE_DETAIL,
                    # From the decision, not re-derived: the fail-closed path advertises the
                    # breaker's cooldown (when we next learn anything) while the pool-exhaustion
                    # path advertises 1 second (contention clears in milliseconds). One number,
                    # decided where the cause is known.
                    retry_after=decision.retry_after_sec,
                    degraded=decision.degraded,
                )
                return
            await _send_denied(send, decision)
            return

        # ----------------------------------------------------------------------------------- #
        # 10. Allowed -> wrap `send` and decorate the app's own response.
        #
        # The headers go on `http.response.start` because that is the only message that carries
        # any, and they are applied with `MutableHeaders` — Starlette's own writer for a raw ASGI
        # message, which lower-cases and latin-1 encodes each name it sets.
        #
        # `.update()` and not `.append()`: appending would emit a *second* `X-RateLimit-Remaining`
        # if a handler had already set one, and two contradictory values for the same header is the
        # one outcome worse than either value alone — an HTTP client takes whichever its parser
        # reaches first, so the caller's pacing would depend on library internals. `update`
        # replaces only the names we own and leaves every other header the app produced untouched.
        #
        # That guarantee holds for a CONFORMANT name only, and the gap is worth closing rather than
        # documenting: `MutableHeaders.__setitem__` compares the STORED bytes against the
        # lower-cased name it is setting, so an app that emitted `b"X-RateLimit-Limit"` would be
        # appended *alongside* rather than replaced — the exact duplicate this paragraph claims to
        # prevent. ASGI requires lower-case header names and neither Starlette nor FastAPI can
        # produce anything else, so this is defence against a raw-ASGI sub-app someone mounts
        # later, not against the framework.
        #
        # So the app's names are lower-cased in one pass first. That is NOT an extra allocation:
        # `MutableHeaders(scope=message)` copies the list internally anyway, so building the copy
        # here and handing it over with `raw=` (which takes the list by reference and mutates it in
        # place) is the same single list build plus n `bytes.lower()` calls. It also subsumes the
        # `headers`-is-absent case — that key is optional in an ASGI response-start message and a
        # handful of raw-ASGI apps omit it.
        # ----------------------------------------------------------------------------------- #
        status_code = 0

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                raw: list[tuple[bytes, bytes]] = [
                    (name.lower(), value) for name, value in message.get("headers", ())
                ]
                message["headers"] = raw
                MutableHeaders(raw=raw).update(decision.headers())
            await send(message)

        await self.app(scope, receive, send_wrapper)

        # =================================== C9 SEAM ======================================= #
        # The response body has been sent. `status_code` above is captured here and nowhere else,
        # and it is the one dimension of C9's analytics record that only the response knows.
        #
        # C9 adds one awaited call at this exact point:
        #
        #     await runtime.analytics.record(decision, status_code=status_code)
        #
        # Here, rather than before `await self.app(...)`, because the status is not known until
        # afterwards and because the write takes ~220 us off perceived latency when it happens
        # after the body is on the wire. **Awaited**, not fire-and-forget: a bare `create_task`
        # would drop backpressure and leak tasks under load, which is a memory leak that only
        # appears in production. Every exception it raises must be swallowed and counted —
        # analytics may never fail a request that was already served.
        # =================================================================================== #
        logger.debug(
            "metered %s -> %d (endpoint=%s, tier=%s, cost=%d)",
            scope["method"],
            status_code,
            label,
            decision.tier,
            cost,
        )
