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

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.auth import AuthError, Principal, decode_token
from src.config import Settings, get_settings

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
# C7 HOOK — require_role(minimum)
#
#   def require_role(minimum: Role) -> Callable[..., Awaitable[Principal]]:
#       async def _guard(principal: Principal = Depends(current_principal)) -> Principal:
#           if not role_satisfies(principal.role, minimum):
#               raise HTTPException(403, detail=...)   # NOT 401, and no WWW-Authenticate
#           return principal
#       return _guard
#
# A dependency *factory*, so each route declares its own minimum and that minimum lands in the
# generated OpenAPI document. It nests `current_principal` rather than re-decoding, so the token
# is verified exactly once per request. `403` carries no `WWW-Authenticate` header: re-
# authenticating is not the remedy for insufficient privilege, and offering the challenge would
# invite a client into a retry loop that can never succeed.
#
#
# C8 HOOK — rate_limit
#
#   async def rate_limit(request: Request, principal: Principal = Depends(current_principal)):
#       decision = limiter.check(principal)          # bucket sized by principal.tier
#       request.state.rate_decision = decision       # middleware turns this into headers
#       if not decision.allowed:
#           raise HTTPException(429, headers={"Retry-After": ...})
#
# Declared AFTER the role check on every route, so a `403` never consumes a token. The decision
# is stashed on `request.state` rather than written to a `response: Response` parameter, because
# headers set that way do not survive the exception path — and the exception path (429) is
# exactly where the client most needs to be told the ceiling. `RequestContextMiddleware` in
# `src/main.py` reads the stash and attaches X-RateLimit-Limit / -Remaining / -Reset to whatever
# response actually leaves the app.
#
#
# C10 NOTE — the SSE query-parameter escape hatch
#
# The browser's native `EventSource` cannot set an Authorization header, so `GET /logs/stream`
# (and only that route) additionally accepts `?access_token=`. That belongs in a separate,
# narrowly-scoped dependency next to the stream route — NOT in `current_principal`, which would
# quietly extend the query-param path to every route in the API, including `POST /logs/search`
# whose entire rationale is keeping search terms out of proxy access logs.
# =================================================================================================
