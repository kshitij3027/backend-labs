"""The control plane — runtime tier/quota configuration, mounted at ``/api/v1/admin``.

Core requirement §2 is "a configuration service for limit rules, quota policies and tier configs,
supporting updates **without a service restart**". This module is the operator-facing half of it;
:mod:`src.tiers` is the half that makes a change reach every replica.

.. rubric:: Two clocks of propagation, and only one of them is cached

The split is the reason this surface can honestly claim "no restart", and it is worth stating
before any route:

* **What a tier *means*** (its four numbers) lives in ``config:tiers`` and is read into each
  replica's :class:`~src.tiers.TierRegistry` snapshot. ``PUT /tiers/{tier}`` writes it, bumps
  ``config:version``, and then re-reads on *this* replica synchronously — so the replica that
  served the write is already enforcing the new numbers when the response is written. Every other
  replica converges within ``TIER_CACHE_TTL_SEC`` (5 s), which is a **deterministic** bound a test
  can assert rather than a pub/sub delivery it would have to hope for. :mod:`src.tiers` argues that
  choice at length.
* **Who is on which tier** is read from ``user:{id}`` *inside the decision script*, on every
  request, with no cache anywhere in the path. ``PUT /users/{id}/tier`` is therefore one ``HSET``
  that takes effect on the **very next request on every replica** — no reload, no TTL, no restart.
  That is the property the spec's "limits applied from the tier at request time" actually asks for,
  and it is free precisely because nothing here caches it.

.. rubric:: Authentication is in-process and MUST stay that way

``/api/v1/admin`` is on :data:`src.middleware.EXEMPT_PATH_PREFIXES`, and that exemption is not
negotiable: the admin API is how an operator *raises* a limit, so metering it would lock the
control plane behind the incident it controls — during the outage where everything 429s, the call
that fixes it would 429 too.

The cost of that exemption is that this is the one authenticated surface an anonymous caller can
hit **unmetered**. So the token check is compared with :func:`hmac.compare_digest` against
``ADMIN_TOKEN`` **in this process, before any store access**, and a rejection issues *zero* Redis
commands. If rejecting a bad token touched Redis, the exemption would become an amplifier against
the shared connection pool — the same pool the limiter needs and the same circuit breaker it
depends on. That is not hypothetical: C5's verification measured the identical pre-auth vector on
the identity path at **200 concurrent unknown API keys → 168 errors and the shared breaker OPEN**,
and the identity path at least has the rate limiter in front of it. This one would not.
``tests/integration/test_admin_api.py`` asserts the Redis call counter is unchanged across a
rejected request rather than trusting this paragraph.

.. rubric:: 401, not 403

A missing or wrong ``ADMIN_TOKEN`` is "you have not proved who you are", not "you are known and
not permitted". There is exactly one principal on this surface — the operator holding the token —
so there is no authenticated-but-forbidden caller for a 403 to describe (RFC 9110 §15.5.3 vs
§15.5.4). 401 is also the status that carries a ``WWW-Authenticate`` challenge, which is what tells
a client *how* to authenticate instead of only that it failed.

.. rubric:: Redis unavailable is a 503 here, never a fail-open

:mod:`src.middleware` fails **open** when the store is down, and that is right for a metered
request: serving a known customer unmetered for a few seconds beats refusing them. None of the
reasoning transfers to this module. These are not metered paths, nobody's traffic is riding on
them, and an admin write that silently no-ops is strictly worse than an error — an operator who
believes they lowered a limit and did not is worse off than one who got a 503 and knows to retry.
So every route that touches the store reports :class:`~src.redis_client.BackingStoreUnavailable` as
a 503 with a ``Retry-After``.

.. rubric:: The error envelope here is FastAPI's, not the middleware's

Rejections from this router are ``{"detail": ...}`` — FastAPI's shape, the same one
:mod:`src.api.protected`'s 404 produces. The ``{"error": "...", "detail": "..."}`` envelope is the
**limiter's** wire contract (the spec pins ``"Rate limit exceeded"`` character-for-character), and
an admin 401 is not a limit decision. A client pattern-matching those literals must never be handed
an authentication failure wearing them.
"""

from __future__ import annotations

import hmac
import logging
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Final, NoReturn

import psutil
from fastapi import APIRouter, Depends, HTTPException, Path, Request, status
from pydantic import BaseModel, ConfigDict, Field

from src.api.health import SERVED_BY
from src.config import Settings, TierConfig
from src.identity import FIELD_TIER
from src.keys import (
    daily_quota_key,
    day_expire_at,
    month_expire_at,
    monthly_quota_key,
    sanitise_user_id,
    user_key,
)
from src.models import (
    UNLIMITED,
    QuotaPeriodState,
    QuotaUsage,
    TierUpdate,
    UserTierUpdate,
    UserUsage,
)
from src.redis_client import BackingStoreUnavailable, RedisGateway
from src.tiers import TierRegistry

logger = logging.getLogger(__name__)

__all__ = [
    "ADMIN_TOKEN_HEADER",
    "AUTHORIZATION_HEADER",
    "BEARER_SCHEME",
    "ConfigReloaded",
    "MemoryProbe",
    "OP_USAGE",
    "OP_USER_TIER",
    "STORE_UNAVAILABLE_RETRY_AFTER_SEC",
    "TierTable",
    "TierUpdated",
    "UserTierAssigned",
    "period_state",
    "presented_admin_token",
    "require_admin_token",
    "router",
]

# ---------------------------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------------------------

#: The primary admin credential header.
#:
#: A **dedicated** header rather than only ``Authorization``, and the reason is not taste. This
#: service already speaks ``Authorization: Bearer <jwt>`` on every metered route, where the token is
#: a *caller* credential resolved by :class:`~src.identity.IdentityResolver`. Two different secrets
#: under one header name is how an operator ends up pasting the admin token into a ``curl`` aimed at
#: ``/api/v1/whoami`` — sending the one credential that can re-price the whole service to a route
#: that logs, meters and attributes it as an ordinary caller token. A distinct header makes the two
#: impossible to confuse in a shell history, in a proxy log and in a reviewer's eye.
ADMIN_TOKEN_HEADER: Final = "X-Admin-Token"

#: Also accepted, because it is what every HTTP client, SDK and API-testing tool reaches for by
#: default, and an admin surface that only speaks a bespoke header is one people work around. The
#: ambiguity above is contained by *order*: :data:`ADMIN_TOKEN_HEADER` wins when both are present,
#: so the explicit spelling is never overridden by whatever an interactive tool happened to attach.
AUTHORIZATION_HEADER: Final = "Authorization"

#: Compared lower-cased — RFC 9110 §11.1 makes the auth scheme case-insensitive.
BEARER_SCHEME: Final = "bearer"

#: The ``WWW-Authenticate`` challenge on a 401 from this router. Mandatory on a 401 (RFC 9110
#: §11.6.1 — a 401 without one is malformed) and deliberately its **own** realm: the challenge the
#: metered surface emits lists the caller schemes, and answering an admin rejection with that list
#: would send an operator looking for an API key.
ADMIN_WWW_AUTHENTICATE: Final = f'Bearer realm="admin", {ADMIN_TOKEN_HEADER}'

#: The 401 detail. It says how to authenticate and nothing about what was wrong with what was
#: presented — "missing", "too short" and "wrong" are one fact to the caller and three bits to
#: someone guessing.
UNAUTHORIZED_DETAIL: Final = (
    f"Admin authentication required. Present the ADMIN_TOKEN as "
    f"'{ADMIN_TOKEN_HEADER}: <token>' or '{AUTHORIZATION_HEADER}: Bearer <token>'."
)

#: Returned when this replica cannot *perform* the check — no runtime wired, or an ``ADMIN_TOKEN``
#: that is somehow empty. Refusing (rather than passing, and rather than 401ing) is the only safe
#: answer: an empty expected token would make :func:`hmac.compare_digest` return **true** for a
#: caller who also sent nothing, i.e. an open admin API. :class:`~src.config.Settings` already
#: rejects an empty, short or placeholder token at construction, so this is unreachable in a
#: correctly built process — which is exactly why it must fail closed rather than be assumed.
MISCONFIGURED_DETAIL: Final = (
    "The admin API is not configured on this replica (ADMIN_TOKEN is unavailable), so no "
    "credential can be verified. Refusing rather than serving."
)

#: Longest presented credential this router will encode and compare, in characters.
#:
#: An anonymous caller can put a megabyte in ``X-Admin-Token`` and, without this, every rejection
#: would UTF-8 encode a megabyte and hand two megabytes to :func:`hmac.compare_digest` — on an
#: **unmetered** surface, which is the same amplification argument this module's docstring makes
#: about the connection pool, relocated from Redis into this process's CPU and heap.
#:
#: It is not a complete bound and does not claim to be: the ASGI server has already buffered and
#: decoded the header by the time this code runs (uvicorn's h11 caps a request's headers well below
#: this anyway). What it removes is the work *this* router would otherwise do per rejection, which
#: is the only part of the cost this module owns.
#:
#: 512 is far above any real token — :class:`~src.config.Settings` requires at least 16 characters
#: and the documented generator is ``openssl rand -hex 32`` (64) — and the check is written against
#: ``max(this, len(expected))`` so an operator with a longer token than the author imagined can
#: still authenticate. It reveals nothing new: :func:`hmac.compare_digest` already runs in time
#: proportional to the shorter operand and so already does not hide length.
MAX_PRESENTED_TOKEN_CHARS: Final = 512

#: ``Retry-After`` on a 503 from this router, in seconds. The breaker's cooldown is the honest
#: interval — it is how long this process will wait before it next discovers whether the store is
#: back — but it is per-deployment configuration, and a control-plane 503 wants a number an
#: operator retries against by hand. One second, floored, because ``Retry-After: 0`` is a retry
#: storm and a 503 is the status clients retry hardest against.
STORE_UNAVAILABLE_RETRY_AFTER_SEC: Final = 1

#: Logical operation names, as they appear in gateway logs and in a raised
#: :class:`~src.redis_client.BackingStoreUnavailable`.
OP_USER_TIER: Final = "admin:user_tier"
OP_USAGE: Final = "admin:usage"


def presented_admin_token(headers: Mapping[str, str]) -> str:
    """Extract the admin credential from ``headers``. Returns ``""`` when none was presented.

    Pure, header-only, and **it never returns ``None``**: an absent credential becomes the empty
    string so the caller compares it like any other value instead of branching to an early return.
    That is not stylistic — an early return on "no header" is a path that skips the comparison
    entirely, and the whole point of :func:`require_admin_token` is that every rejection costs the
    same work and touches the same nothing.

    :data:`ADMIN_TOKEN_HEADER` is checked first; ``Authorization`` is consulted only when it is
    absent, and only for the ``Bearer`` scheme. An ``Authorization`` header this router does not
    speak (``Basic``, a bare ``Bearer``) yields ``""`` rather than being treated as a token —
    falling through to "no credential", which is what it is.
    """
    direct = headers.get(ADMIN_TOKEN_HEADER)
    if direct is not None:
        return direct

    authorization = headers.get(AUTHORIZATION_HEADER)
    if authorization is not None:
        scheme, separator, token = authorization.partition(" ")
        if separator and scheme.lower() == BEARER_SCHEME:
            return token.strip()
    return ""


def _tokens_match(presented: str, expected: str) -> bool:
    """Constant-time comparison of two tokens.

    :func:`hmac.compare_digest` rather than ``==``, because ``==`` on a ``str`` short-circuits at
    the first differing byte and the comparison is against attacker-controlled input. That makes it
    a timing oracle: a caller who can measure the difference between "wrong at byte 0" and "wrong at
    byte 20" can recover the token one byte at a time, and this token can re-price every tier in the
    service. ``tests/integration/test_admin_api.py`` therefore includes a token differing only in
    its **last** byte and a token that is a **prefix** of the real one — the two shapes a
    short-circuiting comparison distinguishes and a constant-time one does not.

    Both sides are encoded to UTF-8 first. :func:`hmac.compare_digest` raises ``TypeError`` on a
    ``str`` containing a non-ASCII character, and a caller sending ``X-Admin-Token: café`` must be
    answered with a 401, not a 500 — a comparison that can be crashed by its input is a denial of
    service on the control plane.

    What this does **not** hide is length: ``compare_digest`` runs in time proportional to the
    shorter operand. That is accepted and stated rather than papered over — the token is
    high-entropy and at least 16 characters by :class:`~src.config.Settings`' own validation, so
    knowing its length narrows nothing usable.
    """
    return hmac.compare_digest(presented.encode("utf-8"), expected.encode("utf-8"))


async def require_admin_token(request: Request) -> None:
    """Verify ``ADMIN_TOKEN`` in-process. **The only gate on this router, and it touches no store.**

    Attached to the :data:`router` itself rather than to each route, so a route added later
    inherits it by existing rather than by someone remembering the decorator. An unauthenticated
    admin route is not a missing feature, it is an anonymous caller re-pricing the service.

    ``async``, deliberately: FastAPI runs a *synchronous* dependency in a worker thread, and a
    thread hop per rejected request on an unmetered, anonymous surface is a second amplifier of
    exactly the shape this check exists to remove. Nothing here awaits anything — the whole body is
    a dict lookup and a constant-time compare.

    Raises:
        HTTPException: 401 on a missing or wrong token, 503 when this replica cannot perform the
            check at all (see :data:`MISCONFIGURED_DETAIL`). Never anything else, and never after
            touching Redis.

    .. rubric:: One honest caveat about ordering

    FastAPI parses a request **body** before it solves dependencies, so ``PUT`` with syntactically
    invalid JSON *and* a bad token is answered 422 rather than 401. That is a cosmetic ordering
    quirk, not a hole: JSON parsing is pure CPU on bytes already in memory, so the property that
    matters — **a rejected admin request issues zero Redis commands** — holds identically on that
    path.
    """
    settings: Settings | None = getattr(
        getattr(request.app.state, "runtime", None), "settings", None
    )
    expected = getattr(settings, "admin_token", "") or ""
    if not expected:
        # Fail CLOSED. See MISCONFIGURED_DETAIL: with an empty expectation the compare below would
        # succeed for a caller who also sent nothing.
        logger.error(
            "admin request refused: no ADMIN_TOKEN is available on this replica, so the "
            "credential could not be verified"
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=MISCONFIGURED_DETAIL,
            headers={"Retry-After": str(STORE_UNAVAILABLE_RETRY_AFTER_SEC)},
        )

    presented = presented_admin_token(request.headers)
    # Length gate FIRST, and short-circuiting on purpose — see `MAX_PRESENTED_TOKEN_CHARS`. A
    # credential longer than the real one cannot match, so skipping the compare costs no
    # correctness and removes the only unbounded work an anonymous caller could ask of this route.
    # `max(..., len(expected))` keeps a longer-than-expected deployment token usable.
    if len(presented) > max(MAX_PRESENTED_TOKEN_CHARS, len(expected)) or not _tokens_match(
        presented, expected
    ):
        # The token is never logged, in any form — not the presented one, not a prefix of it, not
        # its length. A control-plane credential in a log aggregator is a credential in every
        # incident ticket and every stdout scrape that follows.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=UNAUTHORIZED_DETAIL,
            headers={"WWW-Authenticate": ADMIN_WWW_AUTHENTICATE},
        )


# ---------------------------------------------------------------------------------------------
# Response models
#
# Declared here rather than in `src.models` on purpose. `src.models` carries the types more than
# one module speaks — `TierUpdate`, `UserTierUpdate`, `QuotaUsage`, `UserUsage` are all read by
# C13's verifier as well as by this router. The shapes below are this endpoint's own envelope and
# nothing else consumes them, so they live next to the handler that builds them, exactly as
# `HealthResponse` lives in `src/api/health.py`.
# ---------------------------------------------------------------------------------------------


class TierTable(BaseModel):
    """Body of ``GET /tiers`` — **what this replica is enforcing right now**.

    Note the wording. This is served from :meth:`~src.tiers.TierRegistry.snapshot`, i.e. from the
    in-process table the decision script is actually being handed, not from a fresh read of
    ``config:tiers``.

    *Rejected: reading Redis on every GET.* It would make this endpoint answer a question nobody
    asked — "what is stored?" — while the operationally useful one is "what is this replica
    doing?", and those differ for up to ``TIER_CACHE_TTL_SEC`` by design. Worse, it would make the
    convergence property unobservable through the API: a ``GET`` on the replica that did *not*
    serve a ``PUT`` would report the new numbers while still enforcing the old ones, which is
    precisely the state an operator needs to be able to see. ``snapshot_age_sec`` and
    ``config_version`` are what make it visible, and ``POST /config/reload`` is how you end it.

    It also means this route keeps answering while Redis is down — the honest reading being "here
    is what I am enforcing, and I have not been able to check for updates for N seconds".
    """

    model_config = ConfigDict(frozen=True)

    tiers: dict[str, TierConfig] = Field(description="Tier name -> the four enforced numbers.")
    config_version: int = Field(
        description="`config:version` behind this snapshot. Climbs on every accepted tier write."
    )
    default_tier: str = Field(
        description="Tier applied to a principal with no `tier` field in `user:{id}`."
    )
    cache_ttl_sec: int = Field(
        description=(
            "TIER_CACHE_TTL_SEC — the deterministic bound within which a tier change made on "
            "another replica reaches this one."
        )
    )
    snapshot_age_sec: float = Field(
        description=(
            "Seconds since this replica last read `config:tiers`. Greater than `cache_ttl_sec` "
            "means a refresh is due or failing, not that the numbers are wrong."
        )
    )
    served_by: str = Field(description="Hostname of the replica that answered.")


class TierUpdated(BaseModel):
    """Body of ``PUT /tiers/{tier}`` — the applied config, what it replaced, and the new version.

    ``previous`` is included because a partial ``PUT`` is the one write where the operator does not
    already know the whole result: they sent one number and four came back. Having the before and
    after in one response makes it an audit record rather than a receipt.

    It is the row read **inside Redis at the instant of the merge**, not what this replica believed
    was stored. That distinction is the entire point of :data:`~src.lua.RLQ_MERGE_TIER`: a snapshot
    is up to ``TIER_CACHE_TTL_SEC`` old, so a cached ``previous`` would be wrong precisely on the
    write that landed second — which is the one an operator would go looking this up about.
    """

    model_config = ConfigDict(frozen=True)

    tier: str = Field(description="The tier that was written.")
    config: TierConfig = Field(description="The tier's complete config after the update.")
    previous: TierConfig = Field(
        description=(
            "The committed config this update was merged onto, read inside Redis at merge time."
        )
    )
    merge: str = Field(
        description=(
            "How the merge base was obtained. `merged` — the stored row (the normal case). "
            "`seeded` — there was no row, so TIER_LIMITS' default was used and the row now "
            "exists. `repaired` — the stored row was unreadable and was rebuilt from that same "
            "default. The last two mean fields you did not send came from the shipped "
            "configuration rather than from what was in the store."
        )
    )
    config_version: int = Field(
        description=(
            "`config:version` produced by this write. Under concurrent writes this is the version "
            "*your* change created; the version this replica now serves is on `GET /tiers`."
        )
    )
    cache_ttl_sec: int = Field(
        description=(
            "Seconds within which every OTHER replica converges on this change. This replica is "
            "already enforcing it — its snapshot was invalidated and re-read before this response."
        )
    )
    served_by: str = Field(description="Hostname of the replica that served the write.")


class UserTierAssigned(BaseModel):
    """Body of ``PUT /users/{user_id}/tier``.

    There is no ``cache_ttl_sec`` here and that absence is the feature: nothing caches
    ``user -> tier``, so there is no window to report.
    """

    model_config = ConfigDict(frozen=True)

    user_id: str = Field(description="The principal that was assigned.")
    tier: str = Field(description="The tier now recorded in `user:{id}`.")
    created: bool = Field(
        description=(
            "True when `user:{id}` had no `tier` field before this call — i.e. this principal "
            "was previously running on DEFAULT_TIER."
        )
    )
    served_by: str = Field(description="Hostname of the replica that served the write.")


class ConfigReloaded(BaseModel):
    """Body of ``POST /config/reload`` — the version this replica converged on."""

    model_config = ConfigDict(frozen=True)

    config_version: int = Field(description="`config:version` after the refresh.")
    previous_version: int = Field(
        description="What this replica had before. Equal to `config_version` when nothing moved."
    )
    changed: bool = Field(description="Whether the reload actually picked anything up.")
    tiers: dict[str, TierConfig] = Field(description="The table now in force on this replica.")
    served_by: str = Field(description="Hostname of the replica that reloaded.")


class MemoryProbe(BaseModel):
    """Body of ``GET /debug/memory`` — this process's vitals, for C14's memory gate.

    ``rss_mb`` is the number the load harness gates on: resident set size is what an orchestrator
    OOM-kills on, where ``vms`` counts address space the process may never touch. Both are reported
    because a large gap between them is itself diagnostic.
    """

    model_config = ConfigDict(frozen=True)

    pid: int = Field(description="OS process id inside the container.")
    served_by: str = Field(description="Hostname of the replica that answered.")
    rss_bytes: int = Field(description="Resident set size, in bytes.")
    rss_mb: float = Field(description="Resident set size, in MiB. C14 gates on this.")
    vms_mb: float = Field(description="Virtual memory size, in MiB.")
    num_threads: int = Field(description="Threads in this process.")
    num_fds: int = Field(description="Open file descriptors, or -1 where unavailable.")
    uptime_sec: float = Field(description="Seconds since this replica's Runtime was constructed.")


# ---------------------------------------------------------------------------------------------
# Runtime access and error mapping
# ---------------------------------------------------------------------------------------------

#: One process handle, resolved at import. ``psutil.Process()`` walks ``/proc`` on construction and
#: the pid cannot change under a running process, so rebuilding it per request would pay a syscall
#: for a constant — the same reasoning that resolves ``SERVED_BY`` once in :mod:`src.api.health`.
_PROCESS: Final = psutil.Process()

#: Bytes per MiB. Named because "1024 * 1024" appearing next to a gate threshold is how MB and MiB
#: get silently mixed: C14's ``MAX_BACKEND_MEM_MB`` is compared against ``rss_mb``, so the two must
#: agree on which one they mean, and they mean MiB.
_BYTES_PER_MIB: Final = 1024 * 1024


def _runtime(request: Request) -> Any:
    """The process Runtime, or a 503.

    Read off ``app.state`` with ``getattr`` for the same reason ``/health`` does — a half-wired app
    must produce an answer rather than an ``AttributeError`` — but the answer here is the opposite
    one. ``/health`` degrades to zeroed vitals because a liveness probe that 500s gets the replica
    restarted; an admin *write* with no runtime has nowhere to write to, and reporting success for
    it would be the silent no-op this whole module refuses.
    """
    runtime = getattr(request.app.state, "runtime", None)
    if runtime is None:  # pragma: no cover - create_app always attaches one
        _unavailable("this replica has no runtime wired, so no store can be reached")
    return runtime


def _unavailable(detail: str) -> NoReturn:
    """Raise the 503 used for every store failure on this router.

    One function, so the status, the ``Retry-After`` and the "we did not pretend this worked"
    contract cannot drift between the four routes that need them.
    """
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail=detail,
        headers={"Retry-After": str(STORE_UNAVAILABLE_RETRY_AFTER_SEC)},
    )


def _unknown_tier(registry: TierRegistry, tier: str) -> NoReturn:
    """Raise the 404 for a tier that has no committed row and no configured default.

    .. rubric:: Creating a tier through this endpoint is a deliberate non-feature

    A ``PUT`` to an unknown name *could* create a tier — ``TierRegistry._parse_tiers`` overlays
    whatever ``config:tiers`` holds on top of ``TIER_LIMITS``, so a new row would appear in the
    snapshot and become assignable. It is refused anyway, because the overwhelmingly likely way to
    reach this branch is a typo: ``PUT /tiers/primium {"rate_limit_per_min": 10}`` would answer
    ``200``, create an inert tier nobody is on, and leave ``premium`` untouched at 300 — an operator
    mid-incident believing they had lowered a limit that never moved. That is the exact failure
    :class:`~src.models.TierUpdate`'s ``extra="forbid"`` exists to prevent one level down, and it
    would be silly to close it on field names and leave it open on tier names.

    The legitimate way to add a tier is ``TIER_LIMITS`` plus a deploy, where it is reviewable and
    where the seed's ``HSETNX`` gives it a definition on first boot. Adding one is a pricing
    decision; this endpoint exists for *re-sizing* under time pressure.

    **404, not 422.** The tier is a path segment, so an unknown one names a resource that does not
    exist; 422 would say the *body* was unprocessable, and the body may well have been perfect.

    .. rubric:: The DECISION is the script's; only the MESSAGE is built from the snapshot

    Whether the tier exists is answered inside :data:`~src.lua.RLQ_MERGE_TIER`, against the store,
    because that is the only place the answer is current — this replica's snapshot could be up to
    ``TIER_CACHE_TTL_SEC`` behind a tier another replica just defined. The snapshot is read here
    solely to list the names an operator can try, which is a help string rather than a verdict.
    """
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=(
            f"unknown tier {tier!r}; this replica knows {sorted(registry.snapshot().tiers)}. This "
            "endpoint re-sizes an existing tier — creating one is a TIER_LIMITS change plus a "
            "deploy, so that a typo cannot silently mint a tier while leaving the real one "
            "untouched. Nothing was written."
        ),
    )


# ---------------------------------------------------------------------------------------------
# The router
#
# `dependencies=[Depends(require_admin_token)]` on the ROUTER, never per route. Every path below is
# unmetered by `src.middleware.EXEMPT_PATH_PREFIXES`, so a route that forgot the decorator would be
# anonymous *and* unthrottled. Attaching the gate to the container means a new route inherits it by
# being added.
# ---------------------------------------------------------------------------------------------

router = APIRouter(
    prefix="/api/v1/admin",
    tags=["admin"],
    dependencies=[Depends(require_admin_token)],
    responses={
        status.HTTP_401_UNAUTHORIZED: {
            "description": "Missing or invalid ADMIN_TOKEN. Verified in-process; no store touched."
        },
        status.HTTP_503_SERVICE_UNAVAILABLE: {
            "description": "The store could not be reached. Never a silent no-op."
        },
    },
)


@router.get(
    "/tiers",
    response_model=TierTable,
    summary="Read the tier table this replica is enforcing",
    description=(
        "Served from this replica's in-process snapshot, so it answers 'what am I enforcing?' "
        "rather than 'what is stored?'. The two differ for at most `cache_ttl_sec` after a change "
        "made on another replica — use `snapshot_age_sec` and `config_version` to see it, and "
        "`POST /config/reload` to end it. Touches Redis only if the snapshot is due a background "
        "refresh."
    ),
)
async def read_tiers(request: Request) -> TierTable:
    """Return the live tier table, the config version behind it, and how old it is."""
    runtime = _runtime(request)
    registry: TierRegistry = runtime.tiers
    settings: Settings = runtime.settings
    snapshot = registry.snapshot()
    return TierTable(
        tiers=dict(snapshot.tiers),
        config_version=snapshot.version,
        default_tier=settings.default_tier,
        cache_ttl_sec=settings.tier_cache_ttl_sec,
        # `time.monotonic` because `fetched_monotonic` is monotonic — mixing clocks here would
        # produce an age of several decades, which is the shape of bug that gets read as "the
        # cache is broken".
        snapshot_age_sec=round(max(0.0, time.monotonic() - snapshot.fetched_monotonic), 3),
        served_by=SERVED_BY,
    )


@router.put(
    "/tiers/{tier}",
    response_model=TierUpdated,
    summary="Re-size a tier at runtime, with no restart",
    description=(
        "A **partial** update: send only the numbers you are changing. Writes `config:tiers`, "
        "INCRs `config:version`, then re-reads on this replica so the response describes limits "
        "that are already in force here. Other replicas converge within `cache_ttl_sec`. Every "
        "value must be > 0 — the decision script reads a non-positive limit as 'not enforced', so "
        "a zero would quietly make the tier unlimited."
    ),
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "No such tier. Creating one is not a feature."},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {
            "description": "Empty body, unknown field, or a limit <= 0. Nothing was written."
        },
    },
)
async def update_tier(
    request: Request,
    update: TierUpdate,
    tier: str = Path(description="Name of an existing tier, e.g. 'premium'."),
) -> TierUpdated:
    """Merge ``update`` into ``tier``'s **committed** config and make it live.

    .. rubric:: The merge happens inside Redis, and the reason is a measured data-loss bug

    The obvious implementation — read the tier out of this replica's snapshot, apply
    :meth:`~src.models.TierUpdate.apply_to`, ``HSET`` the result — is wrong, and wrong
    deterministically rather than occasionally. A partial ``PUT`` rewrites all four fields, so it
    needs a base for the three it is not changing, and :meth:`~src.tiers.TierRegistry.snapshot` is
    **by design** up to ``TIER_CACHE_TTL_SEC`` behind on any replica that did not serve the previous
    write. With two replicas behind the C12 load balancer:

    .. code-block:: text

       A: PUT premium {"daily_quota": 99999}      -> store 300|300|99999|1250000
       B: PUT premium {"rate_limit_per_min": 77}  -> store  77|300|50000|1250000
                                                                  ^^^^^ A's change silently gone

    Two operator ``PUT``s seconds apart hit that by default, not by bad luck, and B answers ``200``
    with a ``previous`` that describes a row nobody has held for five seconds. The same shape loses
    updates between concurrent ``PUT``s on a single replica.

    So the read-modify-write runs inside :data:`~src.lua.RLQ_MERGE_TIER`: the merge base is whatever
    ``config:tiers`` holds at the instant of the merge, and the script returns the true prior row,
    which is what makes ``previous`` an audit record instead of a guess.

    .. rubric:: What is still enforced before the script runs

    The four ``gt=0`` constraints fire in pydantic, so an ``rpm`` of ``0`` is a 422 before this
    function is entered and **nothing is written** — which matters because
    :func:`src.tiers.decode_tier` treats a non-positive value as malformed and falls back to the
    configured default, so storing one would produce a config that does not mean what the operator
    typed and does not error either. ``extra="forbid"`` plus strict integer typing close the other
    two shapes of "a body that looks applied and was not".

    Whether the tier *exists* is decided by the script, against the store, for the same staleness
    reason — a snapshot check could 404 a tier another replica defined four seconds ago. Nothing is
    written on that path; the script returns before its ``HSET``.
    """
    runtime = _runtime(request)
    registry: TierRegistry = runtime.tiers

    try:
        result = await registry.merge_tier(
            tier,
            rate_limit_per_min=update.rate_limit_per_min,
            burst=update.burst,
            daily_quota=update.daily_quota,
            monthly_quota=update.monthly_quota,
        )
    except BackingStoreUnavailable as exc:
        # 503, and NOT a fail-open. The middleware fails open on this exception because a metered
        # request has a caller waiting; an admin write has an operator waiting for the truth.
        logger.warning("admin tier write for %r could not reach the store: %s", tier, exc)
        _unavailable(
            f"could not write tier {tier!r}: the configuration store is unreachable. Nothing "
            "was changed — retry rather than assuming this took effect."
        )

    if result.previous is None or result.config is None:
        # `MERGE_TIER_STATUS_ABSENT` (equivalently `not result.written`): the script returned before
        # its HSET, so `config:tiers` and `config:version` are byte-identical and this 404 has no
        # side effect. Spelled as a ``None`` check rather than as `not result.written` so the
        # narrowing below belongs to the type checker instead of to an ``assert`` — which ``python
        # -O`` strips, per the rule ``src/main.py`` states for the pricing cross-check.
        _unknown_tier(registry, tier)

    logger.info(
        "tier %r updated at runtime (%s): %s -> %s (config_version=%d)",
        tier,
        result.status,
        result.previous.model_dump(exclude={"name"}),
        result.config.model_dump(exclude={"name"}),
        result.version,
    )
    return TierUpdated(
        tier=tier,
        config=result.config,
        previous=result.previous,
        merge=result.status,
        config_version=result.version,
        cache_ttl_sec=runtime.settings.tier_cache_ttl_sec,
        served_by=SERVED_BY,
    )


@router.put(
    "/users/{user_id}/tier",
    response_model=UserTierAssigned,
    summary="Move a principal to another tier — effective on the very next request",
    description=(
        "One `HSET user:{id} tier <tier>`. **No cache anywhere in this path**: the decision script "
        "reads the principal's tier from `user:{id}` on every request, so this takes effect on the "
        "next request on *every* replica — no reload, no TTL, no restart. The tier must exist in "
        "the live table."
    ),
    responses={
        status.HTTP_422_UNPROCESSABLE_ENTITY: {
            "description": "Unusable user id, or a tier that is not in the live table. Nothing "
            "was written."
        },
    },
)
async def assign_user_tier(
    request: Request,
    update: UserTierUpdate,
    user_id: str = Path(description="Principal id, as it appears in `user:{id}`."),
) -> UserTierAssigned:
    """Record ``update.tier`` against ``user_id``.

    .. rubric:: This is the half of hot reload that is instant, and it is instant by *omission*

    ``PUT /tiers/{tier}`` has a 5-second convergence story because a tier's four numbers are cached
    per replica. This route has none, because ``user -> tier`` is read inside the Lua script on
    every single request and is cached nowhere: not in the registry (which caches what a tier
    *means*, never who is on one), not in :class:`~src.identity.IdentityResolver` (which resolves a
    credential to a *user id* and deliberately never looks up their tier — see its module
    docstring), and not in a JWT claim (a signed ``tier`` claim is ignored precisely so that a
    downgrade cannot wait for expiry). *Identity from the token, authority from the store.*

    So the next request on any replica is priced at the new tier. That is what the spec's "limits
    applied from the tier at request time" asks for, and it is a property of the design rather than
    of this handler.

    .. rubric:: Validated against the live table, not against :class:`~src.models.Tier`

    Tiers are runtime data. An enum check here would reject a tier this very API had just defined.
    The check is against :meth:`~src.tiers.TierRegistry.snapshot`, which is also the table that will
    *price* this principal on their next request — so "accepted" and "enforceable" are the same
    question asked once.

    A tier missing from this replica's snapshot but present on another (a sub-``TIER_CACHE_TTL_SEC``
    window after a tier was defined elsewhere) is refused. Refusing a valid assignment for at most
    5 seconds is a retry; accepting an invalid one silently demotes a principal to ``DEFAULT_TIER``
    with a 200 in the operator's hand.

    Exactly **one** command is issued. A read-back to report the previous tier would double the
    round trips on the write this endpoint exists to make trivially cheap; ``created`` — straight
    off the ``HSET`` reply — already answers the question that actually matters, which is whether
    this principal was running on ``DEFAULT_TIER`` until now.
    """
    runtime = _runtime(request)
    registry: TierRegistry = runtime.tiers
    gateway: RedisGateway = runtime.redis

    try:
        # Refused rather than normalised: a braced id can forge or collide with another
        # principal's Redis Cluster slot, and an empty one puts every unidentified caller in a
        # single shared bucket. `src.keys.sanitise_user_id` argues both.
        safe_user_id = sanitise_user_id(user_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc

    known = registry.snapshot().tiers
    if update.tier not in known:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"unknown tier {update.tier!r}; this replica's live table has {sorted(known)}. "
                "Assigning a tier that does not exist would silently price this principal at "
                "DEFAULT_TIER."
            ),
        )

    key = user_key(safe_user_id)
    try:
        created = await gateway.run(
            lambda: gateway.client.hset(key, FIELD_TIER, update.tier), op=OP_USER_TIER
        )
    except BackingStoreUnavailable as exc:
        logger.warning("admin tier assignment for %r could not reach the store: %s", user_id, exc)
        _unavailable(
            f"could not assign tier {update.tier!r} to {safe_user_id!r}: the store is "
            "unreachable. Nothing was changed — retry rather than assuming this took effect."
        )

    logger.info(
        "principal %r assigned to tier %r (new field: %s) — effective on the next request on "
        "every replica",
        safe_user_id,
        update.tier,
        bool(created),
    )
    return UserTierAssigned(
        user_id=safe_user_id,
        tier=update.tier,
        # HSET returns the number of fields that were CREATED, so 1 means this principal had no
        # tier recorded and was therefore running on DEFAULT_TIER until now.
        created=bool(created),
        served_by=SERVED_BY,
    )


def period_state(limit: int, used: int) -> QuotaPeriodState:
    """Where a quota period stands, from the counter alone. The read-only analogue of the script's.

    The Lua side computes this from ``(limit, before, after)`` — the counter either side of the
    request it is deciding — and checks the cases in the order ``unenforced``, ``exhausted``,
    ``reset``, ``active``. This function keeps that order exactly, with ``used`` playing both roles,
    because the ordering carries meaning rather than being an implementation detail:

    * ``unenforced`` **first**, because a period with no ceiling has no other state that is true.
      ``reset`` is a *claim* — "a period boundary has just rolled over" — and making it about a
      quota nobody counts invites a client to render "your quota just refreshed" for a limit that
      does not exist.
    * ``exhausted`` next, so a period that is both untouched-since-rollover and already at its
      ceiling (a limit of zero-after-validation cannot occur, but a limit *lowered* below the
      current usage certainly can) reports the fact that binds.
    * ``reset`` for a counter that does not exist yet.

    .. rubric:: One deliberate difference from the script, worth stating

    The script reports ``reset`` for the *first* request of a period — it knows ``before == 0``.
    This function reports ``active`` for a principal who has made exactly one request, because by
    then ``used == 1`` and the period is, plainly, in use. Both are correct about different
    questions: the script answers "did this request start a new period?", this endpoint answers
    "where does the period stand now?". A never-seen principal still reports ``reset`` from here,
    which is the case the two agree on and the one C13 asserts.
    """
    if limit <= 0:
        return QuotaPeriodState.UNENFORCED
    if used >= limit:
        return QuotaPeriodState.EXHAUSTED
    if used == 0:
        return QuotaPeriodState.RESET
    return QuotaPeriodState.ACTIVE


def _as_int(value: object) -> int:
    """Coerce one Redis reply element to ``int``; a missing key reads as ``0``.

    The gateway runs with ``decode_responses=False``, so a counter arrives as ``bytes``. ``None``
    means the key does not exist, which for a counter means zero — the quota period exists whether
    or not anyone has spent against it.

    A value that is not a number is reported as ``0`` and logged, rather than raised. Only an
    out-of-band write can produce one (the decision script's ``INCRBY`` cannot), and a read-out is
    not worth a 500 to the operator who is looking at it *because* something is wrong.
    """
    if value is None:
        return 0
    text = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
    try:
        return int(text)
    except ValueError:
        logger.error("quota counter %r is not an integer; reporting 0", text)
        return 0


def _usage(
    *, limit: int, used: int, expire_time: int, fallback_reset_at: int
) -> QuotaUsage:
    """Assemble one period's report from its counter and its expiry.

    ``expire_time`` is an ``EXPIRETIME`` reply: absolute unix seconds, ``-1`` for a key with no
    expiry, ``-2`` for a key that does not exist. ``EXPIRETIME`` rather than ``TTL`` because
    ``reset_at`` **is** an absolute instant (matching ``X-Quota-Reset``), and deriving one from a
    relative TTL would need this replica's wall clock — reintroducing, on a read-out whose whole
    job is to be checkable from outside, the per-replica clock the decision script exists to
    remove.

    ``-2`` (no counter yet) falls back to the boundary the *next* write will set, which is the
    honest answer: the period is real and has a known end even though nobody has spent in it.
    ``-1`` is a genuine anomaly — a counter with no expiry never rolls over — so it is logged and
    reported with the computed boundary rather than as "never".

    ``remaining`` carries :data:`~src.models.UNLIMITED` (``-1``) for an unenforced period, exactly
    as :attr:`~src.models.LimitDecision.daily_remaining` does. ``0`` would say "you have nothing
    left" about a period with no ceiling, which is the opposite fact.
    """
    if expire_time >= 0:
        reset_at = expire_time
    else:
        reset_at = fallback_reset_at
        if expire_time == -1:
            logger.error(
                "a quota counter exists with no expiry, so its period will never roll over; "
                "reporting the computed boundary (%d) instead",
                fallback_reset_at,
            )

    remaining = UNLIMITED if limit <= 0 else max(0, limit - used)
    return QuotaUsage(
        limit=limit,
        used=used,
        remaining=remaining,
        reset_at=reset_at,
        state=period_state(limit, used),
    )


@router.get(
    "/users/{user_id}/usage",
    response_model=UserUsage,
    summary="Read a principal's daily and monthly quota counters",
    description=(
        "Both periods, with the limit their tier imposes, what has been spent, when the counter "
        "rolls over, and the period's state. A principal nobody has ever seen reports zeros with "
        "`state: reset` — the quota period exists whether or not anyone has used it — never a 404."
    ),
)
async def read_usage(
    request: Request,
    user_id: str = Path(description="Principal id, as it appears in `user:{id}`."),
) -> UserUsage:
    """Report both quota periods for ``user_id``.

    .. rubric:: One pipeline, five commands, and the calendar comes from this replica

    ``HGET user:{id} tier``, then ``GET`` + ``EXPIRETIME`` for each period, issued as a single
    non-transactional pipeline: five independent reads, one round trip. ``MULTI``/``EXEC`` would
    hold Redis's single thread for the batch to buy an atomicity nothing here needs — a counter
    that ticks between two reads changes a number by one, which is well inside the uncertainty an
    operator reading a live counter already has.

    Which day and month the counters belong to is derived from ``datetime.now(timezone.utc)`` —
    **the same source** :meth:`src.limiter.Limiter.check` uses to name the keys it increments. The
    alternative, a ``TIME`` call for Redis's clock, would be one more round trip and would only
    matter for a replica whose clock has drifted across a UTC midnight, in which case the request
    it just metered went into the "wrong" key too and this read-out agreeing with it is the useful
    behaviour rather than the correct-in-isolation one.

    .. rubric:: 503 rather than zeros when the store is down

    A quota read-out that answers "0 used" during an outage is worse than an error: it is the
    number an operator would act on, and it says the principal has their whole allowance left. The
    middleware's fail-open reasoning does not reach here — nobody's traffic depends on this route.
    """
    runtime = _runtime(request)
    registry: TierRegistry = runtime.tiers
    settings: Settings = runtime.settings
    gateway: RedisGateway = runtime.redis

    try:
        safe_user_id = sanitise_user_id(user_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc

    moment = datetime.now(timezone.utc)
    daily_key = daily_quota_key(safe_user_id, moment)
    monthly_key = monthly_quota_key(safe_user_id, moment)
    principal_key = user_key(safe_user_id)

    async def _pipelined() -> list[Any]:
        async with gateway.client.pipeline(transaction=False) as pipe:
            pipe.hget(principal_key, FIELD_TIER)
            pipe.get(daily_key)
            pipe.expiretime(daily_key)
            pipe.get(monthly_key)
            pipe.expiretime(monthly_key)
            return list(await pipe.execute())

    try:
        raw_tier, raw_daily, daily_expire, raw_monthly, monthly_expire = await gateway.run(
            _pipelined, op=OP_USAGE
        )
    except BackingStoreUnavailable as exc:
        logger.warning("admin usage read for %r could not reach the store: %s", user_id, exc)
        _unavailable(
            f"could not read usage for {safe_user_id!r}: the store is unreachable. Refused "
            "rather than answered with zeros, which would read as 'full allowance remaining'."
        )

    stored_tier = raw_tier.decode("utf-8", errors="replace") if raw_tier else ""
    tiers = registry.snapshot().tiers

    # The SAME fallback ladder the decision script walks: the stored tier, else DEFAULT_TIER. A
    # principal with no record, or one on a tier that has since been removed from the table, is
    # priced at DEFAULT_TIER on their next request — so reporting the tier that *would* apply is
    # what keeps `tier` and `limit` describing the same thing. Reporting the stored-but-unknown
    # name beside DEFAULT_TIER's numbers would be two facts that contradict each other.
    effective_tier = stored_tier if stored_tier in tiers else settings.default_tier
    config = tiers.get(effective_tier)

    # `config is None` needs `DEFAULT_TIER` itself to be absent from the snapshot, which
    # `Settings._default_tier_must_exist` and `TierRegistry._parse_tiers` between them prevent.
    # Reported as "no ceiling known" rather than crashed on, because a read-out is not worth a 500.
    daily_limit = config.daily_quota if config is not None else 0
    monthly_limit = config.monthly_quota if config is not None else 0

    # A period switched off in configuration reaches the decision script as an EXPIREAT of 0, which
    # forces its limit to 0 and stops the counter being read or written at all. Mirroring that here
    # is what makes this endpoint report `unenforced` for exactly the periods that are unenforced,
    # rather than advertising a tier ceiling nothing is applying.
    if not settings.quota_daily_enabled:
        daily_limit = 0
    if not settings.quota_monthly_enabled:
        monthly_limit = 0

    return UserUsage(
        user_id=safe_user_id,
        tier=effective_tier,
        daily=_usage(
            limit=daily_limit,
            used=_as_int(raw_daily),
            expire_time=int(daily_expire),
            fallback_reset_at=day_expire_at(moment),
        ),
        monthly=_usage(
            limit=monthly_limit,
            used=_as_int(raw_monthly),
            expire_time=int(monthly_expire),
            fallback_reset_at=month_expire_at(moment),
        ),
    )


@router.post(
    "/config/reload",
    response_model=ConfigReloaded,
    summary="Force this replica to re-read the tier table now",
    description=(
        "Bypasses the TTL and the post-failure backoff: an operator pressing reload is an explicit "
        "action and always attempts a read. Affects **this replica only** — with a load balancer "
        "in front, the deterministic way to converge every replica is to wait `cache_ttl_sec`."
    ),
)
async def reload_config(request: Request) -> ConfigReloaded:
    """Re-read ``config:tiers`` and ``config:version`` immediately.

    .. rubric:: A reload button that lies is worse than no reload button

    :meth:`~src.tiers.TierRegistry.refresh` deliberately **swallows**
    :class:`~src.redis_client.BackingStoreUnavailable` and returns the previous snapshot: on the
    background path that is exactly right, because forgetting a table we already have would leave
    every principal with no ceiling to look up. On *this* path it would mean answering a
    "reload now" with a ``200`` and a version number that never moved, which is the silent no-op
    this module refuses everywhere else — so this is the one caller that passes ``strict=True``.

    The failure bookkeeping on the registry side is unchanged by that flag: the counter still
    moves, the backoff window is still armed, the log line is still written. Only the *report*
    differs, which is the whole point — the operator is told, and the replica keeps enforcing the
    last table it successfully read.

    Note what is deliberately *not* done here: :meth:`~src.tiers.TierRegistry.invalidate` is not
    called first. ``refresh`` is awaited directly, which per :mod:`src.tiers` always attempts
    regardless of the backoff window — invalidating as well would only mark the snapshot stale in
    the failure case, scheduling a background retry the backoff is there to suppress.
    """
    runtime = _runtime(request)
    registry: TierRegistry = runtime.tiers

    previous_version = registry.snapshot().version
    try:
        snapshot = await registry.refresh(strict=True)
    except BackingStoreUnavailable as exc:
        logger.warning("admin config reload failed: %s", exc)
        _unavailable(
            "could not reload the tier table: the configuration store is unreachable. This "
            "replica is still enforcing the last table it read "
            f"(config_version={previous_version})."
        )

    logger.info(
        "tier table reloaded on operator request (config_version %d -> %d)",
        previous_version,
        snapshot.version,
    )
    return ConfigReloaded(
        config_version=snapshot.version,
        previous_version=previous_version,
        changed=snapshot.version != previous_version,
        tiers=dict(snapshot.tiers),
        served_by=SERVED_BY,
    )


@router.get(
    "/debug/memory",
    response_model=MemoryProbe,
    summary="This replica's process vitals",
    description=(
        "Resident set size and a few process vitals, straight from psutil. Read by C14's load "
        "harness for its memory gate: RSS before a run, RSS after, and both an absolute and a "
        "growth threshold. Behind ADMIN_TOKEN because process internals are operator information."
    ),
)
async def debug_memory(request: Request) -> MemoryProbe:
    """Report RSS and a few vitals for the process serving this request.

    Deliberately **this** process, not an aggregate: C14 measures memory growth under load, and
    with two replicas behind a load balancer the number is only meaningful next to the
    ``served_by`` that produced it. An averaged figure would hide the one replica that is leaking.

    ``psutil.Process.memory_info()`` is a single ``/proc/self/statm`` read on Linux — microseconds,
    no allocation worth naming — so there is nothing here to cache or rate limit.
    """
    memory = _PROCESS.memory_info()
    try:
        num_fds = _PROCESS.num_fds()
    except (AttributeError, psutil.Error):  # pragma: no cover - Linux always answers
        # `num_fds` does not exist on Windows and can race a dying process. A missing vital is
        # reported as -1 rather than failing the probe: C14 gates on RSS, and losing the whole
        # measurement because a secondary field was unavailable would be an own goal.
        num_fds = -1

    runtime = getattr(request.app.state, "runtime", None)
    return MemoryProbe(
        pid=_PROCESS.pid,
        served_by=SERVED_BY,
        rss_bytes=memory.rss,
        rss_mb=round(memory.rss / _BYTES_PER_MIB, 3),
        vms_mb=round(memory.vms / _BYTES_PER_MIB, 3),
        num_threads=_PROCESS.num_threads(),
        num_fds=num_fds,
        uptime_sec=round(float(getattr(runtime, "uptime_sec", 0.0) or 0.0), 3),
    )


# ---------------------------------------------------------------------------------------------
# The catch-all — declared LAST, deliberately
#
# Starlette matches routes before FastAPI solves dependencies, so without this an anonymous caller
# maps the whole control plane for free: `/api/v1/admin/nope` answers 404, `/api/v1/admin/tiers`
# answers 401, `DELETE /api/v1/admin/tiers` answers 405, and `PUT /api/v1/admin/tiers/` answers a
# 307 with a `Location` naming the real path. Four distinguishable replies, on a prefix that is
# exempt from metering, is a free map of every route and method this service exposes.
#
# It is disclosure rather than amplification — every one of those replies costs zero Redis commands
# — but it flatly contradicts what `UNAUTHORIZED_DETAIL` is careful about one screen up, where a
# rejection deliberately does not say whether the credential was missing, wrong or malformed. Being
# scrupulous about credential shape while narrating the route table is a strange place to land.
#
# Declaration order is what makes this safe: FastAPI appends routes in the order they are declared
# and Starlette takes the FIRST full match, so every real route above wins and this only sees what
# none of them matched. It inherits the router's `Depends(require_admin_token)` like every other
# route, so an anonymous caller gets the same 401 everywhere and an authenticated operator still
# gets an honest 404 for a path that does not exist.
#
# What it does NOT do is change anything the middleware sees. `src.middleware.is_exempt` reads the
# request path, never the route table, so the exempt prefix behaves identically; and
# `src.api.protected.mounted_v1_routes` filters through that same `is_exempt`, so this route is
# invisible to the pricing cross-check without a second exclusion list.
#
# One residual: `GET /api/v1/admin` (no trailing slash) still 307s to `/api/v1/admin/`, because
# Starlette's redirect_slashes runs against the mounted paths and this route's path is
# `/api/v1/admin/{_path:path}`. That redirect names only the prefix the README already documents,
# so it discloses nothing about which routes exist.
# ---------------------------------------------------------------------------------------------

#: Methods the catch-all answers. `OPTIONS` is included because CORS answers a *preflight* above
#: the limiter and never reaches the router — so an `OPTIONS` arriving here is an ordinary request
#: and should be treated like one, not left as the single method that still enumerates.
_CATCH_ALL_METHODS: Final = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"]


@router.api_route(
    "/{_path:path}",
    methods=_CATCH_ALL_METHODS,
    include_in_schema=False,
    # `NoReturn` is not a pydantic field type, and FastAPI builds a response model from the return
    # annotation unless told otherwise. `None` here says "there is no response model", which is the
    # truth: this handler only ever raises.
    response_model=None,
)
async def admin_not_found(_path: str) -> NoReturn:
    """Answer 404 for any authenticated request to a path this router does not serve.

    Reached only after :func:`require_admin_token` has passed, which is the whole purpose: an
    anonymous caller gets 401 for *every* spelling under the prefix — real route, misspelled route,
    wrong method, stray trailing slash — and learns nothing from the difference between them.

    Takes ``_path`` rather than ``Request`` so FastAPI binds the wildcard rather than treating it as
    an unfilled path parameter. The value is deliberately **not** echoed: reflecting arbitrary
    caller input back at arbitrary length on an unmetered surface is a free amplifier, and the
    caller already knows what they asked for.
    """
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=(
            "no such admin route. See GET /api/v1/admin/tiers, PUT /api/v1/admin/tiers/{tier}, "
            "PUT /api/v1/admin/users/{user_id}/tier, GET /api/v1/admin/users/{user_id}/usage, "
            "POST /api/v1/admin/config/reload, GET /api/v1/admin/debug/memory."
        ),
    )
