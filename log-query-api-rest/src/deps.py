"""FastAPI dependencies — the request pipeline's gates, expressed declaratively.

Auth is a **dependency**, never an ad-hoc check inside a handler. That is not a style
preference, it buys three concrete things:

* **The contract is published.** ``Depends(current_principal)`` on a route makes the security
  requirement part of the generated OpenAPI 3.1 document, so ``/docs`` shows a padlock and a
  working *Authorize* button. An ``if not request.headers.get("authorization")`` at the top of a
  handler is invisible to every client, every code generator, and every reviewer.
* **The contract is uniform.** One implementation produces the ``401`` for every route, so the
  status code, the body, and the ``WWW-Authenticate`` header cannot drift between handlers.
* **A missing gate is visible.** A route that forgot to declare its dependency is a diff you can
  see; a route that forgot an inline check looks exactly like a route that is meant to be public.

The chain a guarded request walks is ``current_principal`` (who are you? → ``401``) →
``require_role`` (may you? → ``403``) → ``rate_limit`` (how hard may you ask? → ``429``), in
that order. The ordering is load-bearing: a ``403`` must not drain the caller's token bucket,
because a client that is permanently forbidden from a route would otherwise burn its whole
quota discovering that fact.

Settings are read from the running app (``request.app.state.runtime.settings``) rather than from
a module-level global, so a test injecting a Runtime via ``create_app(runtime=...)`` gets its own
signing key and TTL with no environment mutation and no cache clearing.

This module must not import ``src.api.v1`` or ``src.main``: both import the router that imports
these dependencies, and either edge would close an import cycle.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from functools import lru_cache
from typing import Annotated, Any

from fastapi import Depends, HTTPException, Request, status
from fastapi.routing import APIRoute
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.auth import (
    ROLE_ORDER,
    AuthError,
    Principal,
    Role,
    Tier,
    decode_token,
    role_satisfies,
)
from src.config import Settings, get_settings
from src.ratelimit import HEADER_RETRY_AFTER, Decision, RateLimiter

logger = logging.getLogger(__name__)

#: RFC 9110 §11.6.1 requires a ``401`` response to carry a ``WWW-Authenticate`` challenge naming
#: the scheme the client should use. Without it a ``401`` is technically malformed, and a client
#: (or the Swagger UI) has no machine-readable way to know that retrying with a bearer token is
#: the fix. Defined once here so every ``401`` this app emits carries the identical challenge.
WWW_AUTHENTICATE = {"WWW-Authenticate": "Bearer"}

#: ``auto_error=False`` is the whole reason this is declared rather than used inline.
#:
#: With the default ``auto_error=True``, FastAPI's ``HTTPBearer`` raises the ``401``/``403``
#: itself the moment the header is absent or malformed — and it raises a bare ``403`` for a
#: *missing* Authorization header, which flatly contradicts the README's "``401`` means I don't
#: know who you are; ``403`` means I know, and no". It also emits no ``WWW-Authenticate`` header.
#: Turning ``auto_error`` off makes the scheme purely declarative: it still publishes the
#: ``bearerAuth`` security scheme into the OpenAPI document (which is what lights up the
#: *Authorize* button in ``/docs``), but it hands us ``None`` instead of raising, so the status
#: code, the body and the challenge header are all ours to control.
bearer_scheme = HTTPBearer(
    scheme_name="bearerAuth",
    description="A JWT issued by POST /api/v1/auth/token, sent as `Authorization: Bearer <jwt>`.",
    auto_error=False,
)


def settings_from_request(request: Request) -> Settings:
    """Read :class:`~src.config.Settings` off the running app, falling back to the global.

    Defensive ``getattr`` chain, matching the convention the handlers use for the store: a
    half-wired runtime degrades to a documented fallback instead of raising ``AttributeError``
    and surfacing as a ``500``. The fallback is the process-wide cached
    :func:`~src.config.get_settings`, which is what production would have used anyway — so the
    degraded path is *correct*, not merely non-crashing.

    Going through the app rather than calling ``get_settings()`` directly is what makes the
    signing key injectable: ``create_app(runtime=Runtime.build(Settings(jwt_secret=...)))`` in a
    test is honoured here with no environment mutation and no ``cache_clear()`` dance.
    """
    runtime = getattr(request.app.state, "runtime", None)
    settings = getattr(runtime, "settings", None)
    if isinstance(settings, Settings):
        return settings
    logger.warning("no Settings on app.state.runtime; falling back to get_settings()")
    return get_settings()


def _unauthorized(reason: str) -> HTTPException:
    """Build the one ``401`` this application emits.

    The ``reason`` comes from :class:`~src.auth.AuthError` and is deliberately coarse ("token
    expired", "token is malformed"). It is useful enough for a developer holding a stale token
    to fix their client, and carries nothing an attacker can use to steer a forgery attempt.
    """
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=reason,
        headers=dict(WWW_AUTHENTICATE),
    )


async def current_principal(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> Principal:
    """Decode ``Authorization: Bearer <jwt>`` into a :class:`~src.auth.Principal`.

    Every failure — no header, a header that is not a bearer credential, an empty token, an
    expired token, a tampered payload, a signature from another key, a forged ``alg`` — produces
    the same ``401`` with a ``WWW-Authenticate: Bearer`` challenge. They are one status code on
    purpose: distinguishing "no token" from "bad token" in the response tells an attacker
    whether their forgery got as far as signature verification, and tells a legitimate client
    nothing it cannot see for itself.

    What this dependency never returns is a ``403``. Being unable to identify the caller is not
    the same as identifying them and refusing — that distinction is C7's ``require_role``, and
    the README calls out that the two must never be conflated.
    """
    if credentials is None or not credentials.credentials:
        raise _unauthorized("missing bearer token")

    settings = settings_from_request(request)
    try:
        principal = decode_token(credentials.credentials, settings=settings)
    except AuthError as exc:
        # Logged at INFO, not WARNING: an expired token is an ordinary event on a 30-minute TTL,
        # and a log line per rejected request at WARNING would drown the real signal.
        logger.info("rejected token: %s", exc.reason)
        raise _unauthorized(exc.reason) from exc

    # Stashed for the middleware and for the C8 limiter, both of which need to know *who* the
    # caller is on paths where re-running the dependency chain is not an option.
    request.state.principal = principal
    return principal


# =================================================================================================
# Role-based access control
# =================================================================================================

#: The OpenAPI **operation-level extension** that carries a route's minimum role, e.g.
#: ``"x-required-role": "writer"``. The ``x-`` prefix is what OpenAPI 3.1 reserves for vendor
#: extensions, so it is legal in any operation object and every generator/validator ignores it
#: rather than choking on it.
#:
#: This exists because the obvious mechanism does **not** work here.
#: ``fastapi.Security(current_principal, scopes=["writer"])`` looks like the right answer, and it
#: is the answer for an OAuth2 flow — but FastAPI only copies the requested scopes into an
#: operation's ``security`` block when the underlying scheme is an ``OAuth2`` or ``OpenIdConnect``
#: instance (``get_sub_dependant`` sets ``use_scopes = []`` for every other ``SecurityBase``).
#: Our scheme is :data:`bearer_scheme`, an ``HTTPBearer``, so the scopes would be silently dropped
#: and every operation would publish a bare ``[{"bearerAuth": []}]`` — indistinguishable from a
#: route with no role requirement at all. Publishing the requirement therefore has to be done
#: explicitly, which is what this extension and :func:`role_requirement_note` are for.
REQUIRED_ROLE_EXTENSION = "x-required-role"

#: Attribute stamped on each guard closure produced by :func:`require_role`, so
#: :class:`RoleDocumentedRoute` can recover a route's requirement **from the enforcement itself**
#: rather than from a second declaration on the decorator that could drift away from it.
_MINIMUM_ROLE_ATTR = "__minimum_role__"


def role_requirement_note(minimum: Role) -> str:
    """The one sentence that documents a route's role requirement, in Markdown.

    Appended to the route's ``description`` by :class:`RoleDocumentedRoute`, so it renders in
    ``/docs`` and ``/redoc`` directly under the handler's own prose. Defined here rather than
    inlined so the test that asserts it reached ``/openapi.json`` compares against the same
    string the app emits, instead of a hand-copied approximation of it.
    """
    return (
        f"**Requires role:** `{Role(minimum).value}` or higher "
        "(the ladder is inclusive: viewer < analyst < writer < admin)."
    )


def role_denied_detail(held: Role, minimum: Role) -> str:
    """The ``403`` body for a caller whose role is below a route's minimum.

    It names the requirement and nothing else. Saying "``'writer'`` or higher required" is
    actionable — the caller now knows what to ask an administrator for — while saying *who* they
    are, what else exists, or which other routes they could reach would be handing out a map of
    the authorisation surface to someone who has just been told no.
    """
    return (
        f"role {Role(held).value!r} is not permitted; "
        f"{Role(minimum).value!r} or higher required"
    )


@lru_cache(maxsize=len(Role))
def require_role(minimum: Role) -> Callable[..., Awaitable[Principal]]:
    """Build the dependency that admits a caller only if their role satisfies ``minimum``.

    A dependency **factory**: each route declares its own floor, and the returned guard nests
    :func:`current_principal` rather than re-decoding the token, so a request pays for exactly
    one signature verification no matter how many gates it walks.

    .. rubric:: ``401`` and ``403`` are never conflated, and that split is the whole point

    "I don't know who you are" and "I know, and no" are different facts about a request, they
    have different remedies, and a client cannot act correctly if the API blurs them. A ``401``
    means *get a token* — retrying with fresh credentials is expected to work. A ``403`` means
    *this token will never work here*; retrying is pointless, and re-authenticating as the same
    principal is a loop. That is why every failure to *identify* the caller comes out of
    :func:`current_principal` as a ``401``, this guard only ever produces a ``403``, and the
    ``403`` deliberately carries **no** ``WWW-Authenticate`` header: offering an authentication
    challenge to someone who already authenticated successfully is an invitation into exactly
    that loop.

    .. rubric:: Why the result is cached

    ``lru_cache`` makes ``require_role(Role.VIEWER)`` return the *same* callable everywhere, which
    matters for two mechanical reasons. FastAPI's per-request dependency cache is keyed on the
    callable, so one shared guard is resolved once per request instead of once per route-level
    closure; and :class:`RoleDocumentedRoute` identifies the guard by attribute, which is far
    easier to reason about when there are four guards in the process rather than one per route.
    Four roles, four entries — the cache can never grow past :data:`~src.auth.ROLE_ORDER`.
    """
    minimum = Role(minimum)

    async def _guard(principal: Principal = Depends(current_principal)) -> Principal:
        if not role_satisfies(principal.role, minimum):
            # INFO, not WARNING: a dashboard rendering a control its principal cannot use is an
            # ordinary event, and a 403 is the API working exactly as designed.
            logger.info(
                "denied %r (role=%s) on a route requiring %s",
                principal.subject,
                principal.role.value,
                minimum.value,
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=role_denied_detail(principal.role, minimum),
                # No `headers=WWW_AUTHENTICATE` here, on purpose. See the docstring.
            )
        return principal

    # Stamped on the closure so the route class can read the requirement back off the dependency
    # graph. Naming the function after its role also makes FastAPI's own diagnostics legible:
    # a dependency-resolution error names `require_writer`, not `require_role.<locals>._guard`.
    setattr(_guard, _MINIMUM_ROLE_ATTR, minimum)
    _guard.__name__ = f"require_{minimum.value}"
    _guard.__qualname__ = _guard.__name__
    _guard.__doc__ = f"Admit callers whose role is {minimum.value!r} or higher; else 403."
    return _guard


def _declared_minimum_role(dependant: Any) -> Role | None:
    """Recover the strictest role requirement declared anywhere in a route's dependency tree.

    Walks :class:`fastapi.dependencies.models.Dependant` nodes looking for a guard stamped by
    :func:`require_role`. Typed as ``Any`` rather than importing ``Dependant``: that class lives
    in an undocumented FastAPI module, and this function needs nothing from it but the public
    ``dependencies`` / ``call`` attributes.

    Returns the **strictest** requirement found, not the first. A route carrying two gates is
    already an oddity, but if one ever appears the effective requirement is the higher of the
    two, and the published document must say so rather than advertise the weaker one.
    """
    strictest: Role | None = None
    for sub in getattr(dependant, "dependencies", ()) or ():
        candidates = (
            getattr(getattr(sub, "call", None), _MINIMUM_ROLE_ATTR, None),
            _declared_minimum_role(sub),
        )
        for candidate in candidates:
            if isinstance(candidate, Role) and (
                strictest is None or ROLE_ORDER[candidate] > ROLE_ORDER[strictest]
            ):
                strictest = candidate
    return strictest


class RoleDocumentedRoute(APIRoute):
    """An ``APIRoute`` that publishes its own role requirement into the OpenAPI document.

    The README claims that "enforcement is a route dependency, so the required role is part of
    the generated OpenAPI document rather than tribal knowledge". This class is what makes that
    literally true instead of aspirationally true: it reads the requirement **out of the
    dependency tree it is about to enforce**, so the published document cannot disagree with the
    code. A hand-written ``description="requires writer"`` on the decorator would be a second
    declaration of the same fact, and the two would eventually drift — silently, because nothing
    would fail when they did.

    Two things are emitted, because they serve different readers:

    * ``description`` gains :func:`role_requirement_note` — the human-readable half, rendered by
      ``/docs`` and ``/redoc`` right under the handler's own prose.
    * ``openapi_extra`` gains :data:`REQUIRED_ROLE_EXTENSION` — the machine-readable half, so a
      generated client or a policy linter can read the ladder without parsing English.

    .. rubric:: Why the description append must be idempotent

    ``APIRouter.include_router`` does not re-use route objects; it **re-creates** each one via
    ``add_api_route(..., description=route.description, route_class_override=type(route))``. So
    every route in this file is constructed twice — once on the sub-router, once on the app — and
    the second construction is handed a description that already carries the note. Appending
    unconditionally would publish the sentence twice on every route.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # `*args`/`**kwargs` passthrough rather than a restated signature: `APIRoute` takes ~30
        # keyword arguments and `include_router` forwards all of them, so mirroring the list here
        # would be a maintenance liability that buys nothing.
        super().__init__(*args, **kwargs)

        # `getattr` rather than `self.dependant`: this class reads a FastAPI internal to produce a
        # *documentation* nicety, and app construction must never fail because that nicety could
        # not find its input. `_declared_minimum_role(None)` returns None, so the worst case is an
        # undocumented (but still fully enforced) route.
        minimum = _declared_minimum_role(getattr(self, "dependant", None))
        if minimum is None:
            # Ungated by design (`POST /auth/token`, and every route before its gate lands).
            # Publishing "requires nothing" would be noise.
            return

        self.openapi_extra = {
            **(self.openapi_extra or {}),
            REQUIRED_ROLE_EXTENSION: minimum.value,
        }

        note = role_requirement_note(minimum)
        if note not in self.description:
            self.description = f"{self.description}\n\n{note}".strip()


# The four aliases the routes actually spell. ``Annotated[Principal, Depends(...)]`` rather than a
# bare ``Depends`` default, so a handler's parameter is typed as the :class:`~src.auth.Principal`
# it receives and the gate travels with the type — a route cannot accept the principal without
# also accepting the check that produced it.
#
# ``RequireViewer`` is the floor of the ladder and therefore never actually rejects anybody: every
# role satisfies it. It exists anyway, and the read routes declare it, because "this route is
# gated at the lowest level" and "somebody forgot to gate this route" must not look identical in
# the source — that is the same argument the module docstring makes for dependencies over inline
# checks, applied one level up.
RequireViewer = Annotated[Principal, Depends(require_role(Role.VIEWER))]
RequireAnalyst = Annotated[Principal, Depends(require_role(Role.ANALYST))]
RequireWriter = Annotated[Principal, Depends(require_role(Role.WRITER))]
RequireAdmin = Annotated[Principal, Depends(require_role(Role.ADMIN))]


# =================================================================================================
# Rate limiting
# =================================================================================================

#: ``request.state`` attribute carrying this request's :class:`~src.ratelimit.Decision`.
#:
#: The decision travels on the *request*, not on an injected ``response: Response``, and that is
#: the whole design. Headers written to a dependency's injected Response do not survive the
#: exception path — and ``429``/``403`` are exactly the responses where a client most needs to be
#: told its ceiling. ``RequestContextMiddleware`` reads this stash and decorates whatever response
#: actually leaves the app; see :func:`rate_limit_headers`.
RATE_DECISION_ATTR = "rate_decision"


def limiter_from_request(request: Request) -> RateLimiter | None:
    """Read the :class:`~src.ratelimit.RateLimiter` off ``app.state.runtime``, defensively.

    Same ``getattr`` convention as :func:`settings_from_request` and the handlers' store reads.
    ``None`` means *no limiter is wired*, which :func:`rate_limit` treats as allow-and-say-nothing
    rather than as a ``500``: a limiter that takes the API down when it is misconfigured has
    inverted its own purpose. The ``isinstance`` check is what makes the return type honest —
    ``Runtime.limiter`` is a plain dataclass field that a test could set to anything.
    """
    runtime = getattr(request.app.state, "runtime", None)
    limiter = getattr(runtime, "limiter", None)
    return limiter if isinstance(limiter, RateLimiter) else None


def rate_limit_detail(tier: Tier, decision: Decision) -> str:
    """The ``429`` body: what the ceiling is, and when to come back.

    Names the caller's own tier and its burst — both facts the principal already holds in its
    token and can read from ``GET /auth/me`` — so the message is actionable ("upgrade, or slow
    down to this number") without disclosing anything about anyone else's limits. Mirrors
    :func:`role_denied_detail`: state the requirement, nothing more.
    """
    return (
        f"rate limit exceeded for tier {Tier(tier).value!r} "
        f"({decision.limit} requests burst); retry in {decision.retry_after}s"
    )


def rate_limit(request: Request, principal: Principal) -> Decision | None:
    """Spend one token from ``principal``'s bucket. Raises ``429`` when the bucket is empty.

    .. rubric:: Deliberately a plain function, not a ``Depends``-able dependency

    Every other gate in this module is a dependency; this one is not, and the reason is the
    ordering guarantee. FastAPI resolves a route's dependency **graph**, and while sibling
    dependencies do happen to be solved in declaration order, that is an implementation detail of
    ``solve_dependencies`` rather than a documented contract. A ``Depends``-able ``rate_limit``
    would therefore be usable as ``dependencies=[Depends(require_role(...)), Depends(rate_limit)]``
    — which *looks* ordered, is not guaranteed to be, and silently starts draining buckets on
    ``403`` the day that detail changes. Shipping that form would ship the footgun next to the
    fix. The only way to reach this function is through :func:`guarded`, where the role check is a
    genuine parent in the graph and therefore *must* have completed first.

    A missing limiter degrades to allow-and-emit-nothing. ``None`` is returned rather than a
    synthetic full decision because a header advertising a ceiling that was never evaluated is
    worse than no header at all — a client would pace itself against a number nobody computed.

    Raises:
        HTTPException: ``429`` with ``Retry-After`` **and** the three ``X-RateLimit-*`` headers.
            The triple is attached to the exception as well as by the middleware, so the response
            is self-describing even if middleware ordering is ever changed; both copies come from
            the same :class:`~src.ratelimit.Decision`, so they cannot disagree.
    """
    limiter = limiter_from_request(request)
    if limiter is None:
        logger.warning(
            "no RateLimiter on app.state.runtime; %r was not metered", principal.subject
        )
        return None

    decision = limiter.acquire(principal.subject, principal.tier)
    setattr(request.state, RATE_DECISION_ATTR, decision)

    if not decision.allowed:
        # INFO, not WARNING: a client hitting its documented ceiling is the limiter working, not
        # a fault. It is logged at all because "which principal is saturating" is the first
        # question anyone asks when a dashboard starts showing 429s.
        logger.info(
            "rate limited %r (tier=%s, limit=%d, retry_after=%ds)",
            principal.subject,
            principal.tier.value,
            decision.limit,
            decision.retry_after,
        )
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=rate_limit_detail(principal.tier, decision),
            headers={
                **decision.headers(),
                HEADER_RETRY_AFTER: str(decision.retry_after),
            },
        )
    return decision


def rate_limit_headers(request: Request) -> dict[str, str]:
    """The ``X-RateLimit-*`` triple for whatever response is about to leave the app.

    Called by ``RequestContextMiddleware`` on **every** response. Three cases, in order:

    * **A decision was stashed** (``200``, ``429``) — report it verbatim. This is the number that
      describes what this very request cost.
    * **No decision, but a principal was resolved** (``403``, and any error raised after the auth
      gate) — :meth:`~src.ratelimit.RateLimiter.peek` the caller's bucket. A ``403`` never reaches
      :func:`rate_limit`, by design, so there is nothing stashed; but the caller is known, their
      allowance is a real number, and refusing to report it would mean the one class of response a
      client is most likely to retry carries no pacing information. ``peek`` consumes nothing, so
      this cannot turn the documented "a 403 does not drain the bucket" into a lie.
    * **Neither** (``401``, ``/health``, ``/docs``, the token endpoint) — **emit nothing.** There
      is no principal, so there is no bucket, so any number would be invented. A header claiming
      a ceiling that was never evaluated is strictly worse than a missing header: a client can
      handle absence, but it cannot detect fiction.
    """
    decision = getattr(request.state, RATE_DECISION_ATTR, None)
    if isinstance(decision, Decision):
        return decision.headers()

    principal = getattr(request.state, "principal", None)
    limiter = limiter_from_request(request)
    if isinstance(principal, Principal) and limiter is not None:
        return limiter.peek(principal.subject, principal.tier).headers()

    return {}


@lru_cache(maxsize=len(Role))
def guarded(minimum: Role) -> Callable[..., Awaitable[Principal]]:
    """The full gate: authenticate, check the role, **then** spend a token.

    ``401`` (who?) → ``403`` (may you?) → ``429`` (how hard?), in that order and no other.

    .. rubric:: The ordering is a property of the graph, not of a list

    ``rate_limit`` is invoked from the body of a dependency whose own parameter is
    ``Depends(require_role(minimum))``. FastAPI cannot call this function until that parameter is
    resolved, and ``require_role`` raises the ``403`` during resolution — so on a forbidden
    request the limiter is never consulted at all. That is a structural guarantee: it holds
    because the two gates are *parent and child* in the dependency tree, not because they happen
    to be listed in a helpful order.

    Why it matters: a route a caller is permanently forbidden from is the one they are most
    likely to hammer (a dashboard rendering a control its principal cannot use, a retry loop that
    never learns). If a ``403`` drained the bucket, that caller would be locked out of every route
    they *are* entitled to as collateral damage — the authorization failure would escalate itself
    into an availability failure. ``test_403_does_not_consume_a_token`` is the pin.

    The ``lru_cache`` serves the same two purposes as it does on :func:`require_role`: one shared
    callable per role means FastAPI's per-request dependency cache resolves the gate once no
    matter how many routes share it, and :class:`RoleDocumentedRoute` has four guards to
    recognise rather than one per route.
    """
    minimum = Role(minimum)
    role_guard = require_role(minimum)

    async def _guarded(
        request: Request, principal: Principal = Depends(role_guard)
    ) -> Principal:
        rate_limit(request, principal)
        return principal

    # Stamped so `RoleDocumentedRoute` finds the requirement at the first level of the tree rather
    # than only by recursing into the nested `require_role` closure. Both carry the same role, and
    # `_declared_minimum_role` takes the strictest, so the duplicate is inert — it just means the
    # published documentation does not depend on how deeply the guard happens to nest.
    setattr(_guarded, _MINIMUM_ROLE_ATTR, minimum)
    _guarded.__name__ = f"guard_{minimum.value}"
    _guarded.__qualname__ = _guarded.__name__
    _guarded.__doc__ = (
        f"Authenticate, require role {minimum.value!r} or higher, then spend a rate-limit token."
    )
    return _guarded


# What every metered route in v1 actually spells. The `Require*` aliases above remain the
# role-gate-only form: they are what a route composes with when its principal does NOT come from
# the Authorization header — C10's `GET /logs/stream` and its `?access_token=` escape hatch — and
# keeping them separate is what stops that route from having to re-implement the ladder.
ViewerGuard = Annotated[Principal, Depends(guarded(Role.VIEWER))]
AnalystGuard = Annotated[Principal, Depends(guarded(Role.ANALYST))]
WriterGuard = Annotated[Principal, Depends(guarded(Role.WRITER))]
AdminGuard = Annotated[Principal, Depends(guarded(Role.ADMIN))]


# =================================================================================================
# C10 NOTE — the SSE query-parameter escape hatch
#
# The browser's native `EventSource` cannot set an Authorization header, so `GET /logs/stream`
# (and only that route) additionally accepts `?access_token=`. That belongs in a separate,
# narrowly-scoped dependency next to the stream route — NOT in `current_principal`, which would
# quietly extend the query-param path to every route in the API, including `POST /logs/search`
# whose entire rationale is keeping search terms out of proxy access logs.
# =================================================================================================
