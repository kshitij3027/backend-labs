"""Black-box end-to-end verifier for the API Rate Limiter & Quota Manager (C13).

Runs **inside Docker** (the profile-gated ``e2e`` compose service) against the LIVE stack over
HTTP only, reached by SERVICE NAME over the compose network rather than by a published host port:

* ``TARGET_URL=http://lb:80``     — through the nginx load balancer, i.e. across **both** replicas.
* ``DIRECT_URL=http://api1:8000`` — one replica, addressed directly, for check 8's control run.

This is the commit that proves the project's central claim **from outside the process**. Every
in-process test in ``tests/`` drives two ``httpx.ASGITransport`` apps over one Redis; that is a
strong test and it is still a test of Python objects the suite constructed itself. Here the two
replicas are two real uvicorn processes, in two containers, with two connection pools, behind a
real proxy, and the only thing this file can see is what a customer's HTTP client can see.

It never imports the app, the middleware, the limiter or the Lua. The **only** things it reaches
into are :func:`src.identity.issue_token`, the :data:`src.identity.DEMO_CREDENTIALS` declaration
and :class:`src.config.Settings` — the credentials the server itself authenticates against, the
signing function the server itself verifies with, and the configuration the server itself was
started from. The tester image ships ``src/`` precisely so those imports resolve; a second
hard-coded copy of any of them would let a rename drift the harness away from the thing it is
supposed to be checking. Everything else — tier ceilings, quota limits, the default tier, the
tier-cache TTL — is read back off the **live admin API**, so this file never asserts against its
own idea of the configuration.

That last sentence is a closed loop if left alone, and it is opened exactly once, at bootstrap:
:func:`_assert_live_table_matches_config` grades the live table against the configured
``TIER_LIMITS`` before a single check runs. Without it, a regression that dropped the configured
tiers and fell back to built-in defaults would be reported consistently by the admin API *and*
enforced consistently by the limiter, and all 14 checks below would agree with a service running
on limits nobody asked for.

Signing its own tokens is what lets it mint a **throwaway principal per check**: a fresh
``uuid4`` has an empty bucket and a zeroed quota by construction, so a capacity assertion is exact
rather than "exact modulo whatever the previous run spent".

The first failing check prints a loud ``FAIL`` line and exits 1 immediately, so ``make e2e``
propagates it.

The 14 checks, in order:

 1. ``GET /health`` carries the spec's two keys **verbatim** (``status: "healthy"``,
    ``rate_limiter: "active"``) and **no** ``X-RateLimit-*`` — proving the exemption survived the
    proxy hop. See the C12 note: a proxy that added a path prefix would make ``/health`` miss the
    exempt list, and the container healthcheck would start collecting 401s.
 2. **Both replicas answer.** ``E2E_REPLICA_PROBES`` probes yield >= 2 distinct ``X-Served-By``,
    and the header agrees with the body's ``served_by`` on every one of them.
 3. Identity: every :data:`~src.identity.DEMO_CREDENTIALS` API key resolves to its declared
    ``user_id``/``tier``; a self-signed JWT resolves as ``jwt``; a bogus key -> ``401`` whose
    ``WWW-Authenticate`` advertises **both** schemes; no credential at all -> ``401``. Neither
    ``401`` carries ``X-RateLimit-*`` (there is no principal, so every number would be fabricated).
 4. An allowed response carries all six headers well-formed, and the documented **unit asymmetry**
    holds: ``X-RateLimit-Reset`` is delay-seconds, ``X-Quota-Reset`` is absolute unix seconds.
 5. Free-tier ``429``: body ``error`` is **exactly** ``"Rate limit exceeded"``, ``Retry-After`` >=
    1, ``X-RateLimit-Remaining: 0``.
 6. Per-tier enforcement: premium's measured ceiling is **strictly higher** than free's, and each
    lands inside ``[ceiling, ceiling + refill]`` for its own tier — the actual counts, not "more".
 7. **THE DISTRIBUTED DOUBLE-SPEND CHECK.** See :func:`check_double_spend`.
 8. Single-replica control against ``DIRECT_URL`` with a second fresh principal: same ceiling. If
    the control says 60 and the LB says 120, the shared-state claim is false.
 9. Weighted cost: ``GET /api/v1/logs/query`` costs 5 and ``GET /api/v1/whoami`` costs 1, read off
    ``X-RateLimit-Remaining`` deltas and cross-checked against the charged ``daily.used``.
10. Quota exhaustion with **no phantom spend**: a tiny daily quota set through the admin API is
    exhausted, the refusal is a ``429`` with the quota reason and ``X-Quota-Remaining: 0``, and the
    token bucket still holds tokens across two consecutive refusals.
11. A runtime tier change with **no restart** is enforced by **both** replicas within
    ``TIER_CACHE_TTL_SEC`` + ``E2E_TIER_SLACK_SEC``, with **zero** requests dropped meanwhile.
12. Analytics: ``/dashboard/api/stats`` totals move by **exactly** the number of requests fired and
    ``by_status`` matches, agreed by two distinct replicas.
13. ``/dashboard/api/stats`` is 200 JSON carrying ``rate_limit_enabled``, and is itself unmetered.
14. Rate-limit **overhead**: p95 of the metered path minus p95 of an exempt path on the same
    stack, gated at ``MAX_OVERHEAD_MS``. See :func:`check_overhead` for what that number is and,
    more importantly, what it is not.

.. rubric:: NEVER USE A TRAILING SLASH ON A METERED PATH

From C7's verification, and it is the one footgun that would make this file *manufacture* the bug
it exists to catch. A metered path with a trailing slash is charged **twice**: ``GET
/api/v1/logs/query/`` costs 5 for the 307 and 5 more for the redirected request — 10 tokens for one
logical call. That is the documented safe asymmetry (over-charging is a bug report, under-charging
is a bypass), not a defect. But checks 7, 9 and 10 assert *exact* usage counts, so one stray
trailing slash would present as a double-spend and send someone hunting a bug that is not there.
:func:`_guard_trailing_slash` turns that from a convention into a refusal, on every metered request
this file issues.

.. rubric:: What this file may and may not mutate

Checks 10 and 11 write to the **tier table**, because "a limit can be changed at runtime with no
restart" is not observable any other way. Both restore the tier they touched in a ``finally``, so a
failing assertion cannot leave the stack mis-configured for the checks after it. Nothing else here
writes shared state: every other check operates on a principal that did not exist a millisecond
earlier and will never be seen again.

Environment knobs (all optional, ``${VAR:-default}`` in compose):

* ``TARGET_URL``              base URL through the LOAD BALANCER (default ``http://lb:80``)
* ``DIRECT_URL``              base URL of ONE replica (default ``http://api1:8000``)
* ``E2E_READY_TIMEOUT``       seconds to wait for both URLs' ``/health`` (default 90)
* ``E2E_REPLICA_PROBES``      ``/health`` probes fired for the fan-out check (default 20)
* ``E2E_BURST_REQUESTS``      requests in the double-spend burst (default 200, ~3.3x capacity)
* ``E2E_BURST_CONCURRENCY``   in-flight requests during that burst (default 50)
* ``E2E_RATE_LIMIT_MARGIN``   requests fired PAST a tier's ceiling to provoke a 429 (default 30)
* ``E2E_QUOTA_LIMIT``         tiny daily quota installed for check 10 (default 3)
* ``E2E_TIER_RPM``            distinctive rpm installed for check 11 (default 137)
* ``E2E_TIER_SLACK_SEC``      slack ON TOP of the server's own TIER_CACHE_TTL_SEC (default 5)
* ``E2E_TIER_MAX_WAIT_SEC``   ABSOLUTE ceiling on tier convergence, ms-independent of the server's
                              own TTL (default 15) — see :func:`convergence_budget`
* ``E2E_ANALYTICS_REQUESTS``  allowed requests fired for the analytics delta (default 12)
* ``E2E_ANALYTICS_UNAUTH``    unauthenticated requests fired for the same delta (default 5)
* ``E2E_ANALYTICS_TIMEOUT``   seconds to wait for the analytics delta to settle (default 20)
* ``E2E_STATS_MINUTES``       minute buckets requested from the stats endpoint (default 60)
* ``E2E_LATENCY_SAMPLES``     paired samples backing the overhead p95 (default 40)
* ``MAX_OVERHEAD_MS``         ceiling on the measured p95 overhead, ms (default 5)

Every gate is host-overridable, which is how we prove the gates are real rather than decorative:
``MAX_OVERHEAD_MS=0 make e2e`` **MUST** exit non-zero, and so must ``E2E_BURST_REQUESTS=1 make
e2e`` — a burst of one request cannot distinguish a shared bucket from two per-process ones, and a
harness that reported ``PASS`` for it would be worse than no harness. Exit code 0 with
``E2E PASSED (14/14)`` only when all 14 hold.
"""

from __future__ import annotations

import asyncio
import math
import os
import sys
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Final

import httpx

from src.config import Settings, get_settings
from src.identity import DEMO_CREDENTIALS, issue_token

# --------------------------------------------------------------------------------------------- #
# Configuration (env-driven; documented in the module docstring)
# --------------------------------------------------------------------------------------------- #
BASE_URL = os.environ.get("TARGET_URL", "http://lb:80").rstrip("/")
DIRECT_URL = os.environ.get("DIRECT_URL", "http://api1:8000").rstrip("/")
READY_TIMEOUT = float(os.environ.get("E2E_READY_TIMEOUT", "90"))
REPLICA_PROBES = int(os.environ.get("E2E_REPLICA_PROBES", "20"))
BURST_REQUESTS = int(os.environ.get("E2E_BURST_REQUESTS", "200"))
BURST_CONCURRENCY = int(os.environ.get("E2E_BURST_CONCURRENCY", "50"))
RATE_LIMIT_MARGIN = int(os.environ.get("E2E_RATE_LIMIT_MARGIN", "30"))
QUOTA_LIMIT = int(os.environ.get("E2E_QUOTA_LIMIT", "3"))
TIER_RPM = int(os.environ.get("E2E_TIER_RPM", "137"))
TIER_SLACK_SEC = float(os.environ.get("E2E_TIER_SLACK_SEC", "5"))
TIER_MAX_WAIT_SEC = float(os.environ.get("E2E_TIER_MAX_WAIT_SEC", "15"))
ANALYTICS_REQUESTS = int(os.environ.get("E2E_ANALYTICS_REQUESTS", "12"))
ANALYTICS_UNAUTH = int(os.environ.get("E2E_ANALYTICS_UNAUTH", "5"))
ANALYTICS_TIMEOUT = float(os.environ.get("E2E_ANALYTICS_TIMEOUT", "20"))
STATS_MINUTES = int(os.environ.get("E2E_STATS_MINUTES", "60"))
LATENCY_SAMPLES = int(os.environ.get("E2E_LATENCY_SAMPLES", "40"))
MAX_OVERHEAD_MS = float(os.environ.get("MAX_OVERHEAD_MS", "5"))

TOTAL_CHECKS = 14

API = "/api/v1"
ADMIN = f"{API}/admin"

#: The metered namespace. Everything under it is priced by :data:`src.keys.ROUTE_TABLE` except the
#: admin sub-tree, which is exempt from metering (and only from metering — it is still
#: ``ADMIN_TOKEN``-gated).
METERED_PREFIX: Final = f"{API}/"

#: The three priced paths this file uses, spelled EXACTLY as :data:`src.keys.ROUTE_TABLE` matches
#: them — anchored, no trailing slash. See the trailing-slash rubric in the module docstring.
PATH_WHOAMI: Final = f"{API}/whoami"
PATH_LOGS_QUERY: Final = f"{API}/logs/query"

#: What those two cost, per ``ENDPOINT_COSTS`` (``logs_query:5,logs_ingest:2,default:1``). Asserted
#: rather than assumed by check 9 — these are the *expected* values the measurement is graded
#: against, not values read from the server.
COST_WHOAMI: Final = 1
COST_LOGS_QUERY: Final = 5

#: The six headers every allowed, non-degraded, quota-enforced response must carry (check 4).
RATELIMIT_HEADERS: Final = ("X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset")
QUOTA_HEADERS: Final = ("X-Quota-Limit", "X-Quota-Remaining", "X-Quota-Reset")

#: Prefix used to detect *any* limiter header, ``X-RateLimit-Degraded`` included. Checks 1, 3 and
#: 13 assert its total absence, which is a different (and stronger) claim than "the three named
#: ones are missing".
RATELIMIT_HEADER_PREFIX: Final = "x-ratelimit-"

#: The spec's two literal 429 strings, duplicated here **on purpose**. :data:`src.models
#: .ERROR_RATE_LIMIT` is the server's definition; these are the contract as a *client* knows it,
#: and importing the server's constant would make this assertion tautological — a rewording would
#: change both sides at once and the check would still pass.
ERROR_RATE_LIMIT: Final = "Rate limit exceeded"
ERROR_QUOTA: Final = "Quota exceeded"

#: ``DenyReason`` values, likewise spelled as a client sees them.
REASON_QUOTA_DAILY: Final = "quota_daily"
RATE_REASONS: Final = frozenset({"rate_limit", "sliding_window"})

#: The multiplier that defines the bug. Two replicas each keeping their own token bucket would
#: admit ``2 x capacity``; one shared bucket admits ``capacity`` (plus refill). Check 7's headline
#: assertion is that the measured number is on the correct side of this.
PER_PROCESS_MULTIPLIER: Final = 2

#: The smallest burst that can refute the per-process hypothesis, as a multiple of capacity. Firing
#: fewer than ``PER_PROCESS_MULTIPLIER x capacity`` requests cannot distinguish one shared bucket
#: from two per-process ones for the arithmetic reason that it cannot even *reach* the number it is
#: meant to refute — 100 requests against two 60-token buckets admit 100, which is indistinguishable
#: from a working limiter that never got the chance to refuse. Check 7 refuses to run below this,
#: which is what makes ``E2E_BURST_REQUESTS=1 make e2e`` fail loudly instead of passing vacuously.
MIN_BURST_FACTOR: Final = PER_PROCESS_MULTIPLIER

#: Cross-check state. Facts a later check needs from an earlier one — the replica hostnames seen in
#: check 2, the live tier table, the capacity check 8 grades its control run against.
STATE: dict[str, Any] = {}


class CheckFailure(AssertionError):
    """Raised inside a check to fail it with a single clear detail line."""


def _load_settings() -> Settings:
    """Build :class:`~src.config.Settings` from the environment, or fail the bootstrap loudly.

    The verifier needs exactly three values out of it — ``JWT_SECRET`` and ``JWT_ALGORITHM`` to
    sign the throwaway principals, ``ADMIN_TOKEN`` to reach the control plane — and every one of
    them must be **the same value the replicas were started with**, which is why the compose
    service declares them from the same ``${VAR:-default}`` expressions the ``x-api`` anchor does.

    ``Settings`` refuses to construct without them (``validate_default=True``), so a mis-wired
    compose service surfaces here as one readable line rather than as a wall of 401s from check 3.
    """
    try:
        return get_settings()
    except Exception as exc:  # noqa: BLE001 — a config failure is a bootstrap failure
        print(
            "FAIL bootstrap: could not build Settings from the environment "
            f"({type(exc).__name__}: {exc}). JWT_SECRET, API_KEY_PEPPER and ADMIN_TOKEN must be "
            "declared on the e2e compose service with the SAME values the api replicas got.",
            flush=True,
        )
        raise SystemExit(1) from exc


SETTINGS: Final[Settings] = _load_settings()


# --------------------------------------------------------------------------------------------- #
# HTTP plumbing
# --------------------------------------------------------------------------------------------- #
#: Through the load balancer. `follow_redirects` is left at httpx's default of False, deliberately:
#: a 307 must surface as an unexpected status rather than being silently followed, because the one
#: redirect this API can produce is the trailing-slash double charge.
CLIENT = httpx.Client(base_url=BASE_URL, timeout=60.0)

#: One replica, addressed directly over the compose network. Only check 8 uses it, and only to
#: establish that a single process enforces the SAME ceiling the pair does.
DIRECT_CLIENT = httpx.Client(base_url=DIRECT_URL, timeout=60.0)


def _guard_trailing_slash(path: str) -> None:
    """Refuse a metered path with a trailing slash, before it is ever sent.

    See the rubric in the module docstring. ``/dashboard/`` and the admin sub-tree are exempt from
    metering, so a trailing slash there costs nothing and is allowed through — the guard is scoped
    to exactly the paths where the double charge is real.
    """
    if path.startswith(METERED_PREFIX) and not path.startswith(ADMIN) and path.endswith("/"):
        raise CheckFailure(
            f"refusing to send {path!r}: a metered path with a trailing slash is charged TWICE "
            "(the 307 and the redirected request). Use the exact path — every count in this file "
            "would otherwise read as a double-spend that is not there."
        )


def request(
    method: str,
    path: str,
    *,
    client: httpx.Client = CLIENT,
    principal: str | None = None,
    api_key: str | None = None,
    headers: dict[str, str] | None = None,
    **kwargs: Any,
) -> httpx.Response:
    """Issue one request, turning a transport failure into a check failure rather than a crash.

    ``principal`` mints and attaches a bearer token for that user id; ``api_key`` attaches a raw
    demo key. Passing neither sends the request anonymously, which is what checks 3 and 12 want.
    """
    _guard_trailing_slash(path)
    merged = dict(headers or {})
    if principal is not None:
        merged["Authorization"] = f"Bearer {token_for(principal)}"
    if api_key is not None:
        merged["X-API-Key"] = api_key
    try:
        return client.request(method, path, headers=merged, **kwargs)
    except Exception as exc:  # noqa: BLE001 — a network failure is a check failure
        raise CheckFailure(f"{method} {path} raised {type(exc).__name__}: {exc}") from exc


def expect(method: str, path: str, status: int, **kwargs: Any) -> httpx.Response:
    """Issue one request and fail the check unless it returned exactly ``status``."""
    resp = request(method, path, **kwargs)
    if resp.status_code != status:
        raise CheckFailure(
            f"{method} {path} -> HTTP {resp.status_code} (expected {status}): {resp.text[:200]}"
        )
    return resp


def body_of(resp: httpx.Response) -> Any:
    """Decode a JSON body, failing the check when the response is not JSON at all."""
    try:
        return resp.json()
    except ValueError as exc:
        raise CheckFailure(
            f"{resp.request.method} {resp.request.url.path} returned non-JSON: {resp.text[:200]}"
        ) from exc


def token_for(user_id: str) -> str:
    """Sign a bearer token for ``user_id`` using the server's own issuer.

    :func:`src.identity.issue_token` rather than a hand-rolled ``jwt.encode``: the claim set the
    resolver *requires* (``sub`` + ``exp``) is declared once, in the module that verifies it, and a
    harness that built its own payload would be asserting against its own idea of a valid token.
    """
    return issue_token(user_id, settings=SETTINGS)


def new_principal(label: str) -> str:
    """A user id nothing has ever seen: empty bucket, zeroed quota, no tier record.

    ``uuid4`` and not a counter, because the point is that the state this principal accumulates
    during a check is **only** what the check itself put there. A reused id would make every
    capacity assertion depend on what the previous run spent, which is the difference between "the
    bucket admitted exactly 60" and "the bucket admitted 60 given some history nobody recorded".
    """
    return f"e2e-{label}-{uuid.uuid4().hex[:16]}"


def admin(method: str, path: str, **kwargs: Any) -> httpx.Response:
    """Call the control plane through the LB with the operator token attached.

    Deliberately through ``lb`` like everything else. The admin surface is exempt from metering but
    not from the load balancer, so a tier write lands on whichever replica nginx picked — which is
    precisely the asymmetry checks 10 and 11 are built to observe (the writing replica is already
    converged; the other one has ``TIER_CACHE_TTL_SEC`` to catch up).
    """
    headers = dict(kwargs.pop("headers", None) or {})
    headers["X-Admin-Token"] = SETTINGS.admin_token
    return request(method, path, headers=headers, **kwargs)


def admin_json(method: str, path: str, status: int = 200, **kwargs: Any) -> Any:
    """Call the control plane and return the decoded body, asserting the status."""
    resp = admin(method, path, **kwargs)
    if resp.status_code != status:
        raise CheckFailure(
            f"{method} {path} -> HTTP {resp.status_code} (expected {status}): {resp.text[:200]}"
        )
    return body_of(resp)


def tier_table() -> dict[str, Any]:
    """``GET /api/v1/admin/tiers`` — the table the answering replica is enforcing right now."""
    return admin_json("GET", f"{ADMIN}/tiers")


def assign_tier(user_id: str, tier: str) -> None:
    """Put ``user_id`` on ``tier``. Takes effect on the very next request, on every replica."""
    body = admin_json("PUT", f"{ADMIN}/users/{user_id}/tier", json={"tier": tier})
    if body.get("tier") != tier:
        raise CheckFailure(f"assigning {user_id!r} to {tier!r} reported {body.get('tier')!r}")


def usage_of(user_id: str) -> dict[str, Any]:
    """``GET /api/v1/admin/users/{id}/usage`` — the charged daily and monthly counters."""
    return admin_json("GET", f"{ADMIN}/users/{user_id}/usage")


def set_tier_limits(tier: str, **fields: int) -> dict[str, Any]:
    """``PUT /api/v1/admin/tiers/{tier}`` — a partial update, merged inside Redis."""
    return admin_json("PUT", f"{ADMIN}/tiers/{tier}", json=fields)


def ceiling_of(tier: str) -> int:
    """The number of cost-1 requests ``tier`` admits from cold, before any refill.

    ``min(burst, rate_limit_per_min)`` — the two rate gates are independent and either can bind.
    The token bucket's capacity is ``burst``; the account-wide sliding window's ceiling over its
    60-second window is ``rate_limit_per_min``. The shipped tiers set the two equal, so this
    usually *is* ``burst`` — but reading it as a minimum means an operator who raises burst alone
    does not silently turn every capacity assertion in this file into a measurement of the other
    gate.
    """
    config = STATE["tiers"].get(tier)
    if config is None:
        raise CheckFailure(f"tier {tier!r} is not in the live table {sorted(STATE['tiers'])}")
    return min(int(config["burst"]), int(config["rate_limit_per_min"]))


def convergence_budget() -> tuple[float, float, bool]:
    """How long a tier change may take to reach every replica: ``(budget, relative, capped)``.

    Two bounds, and the tighter one wins.

    * **relative** — ``TIER_CACHE_TTL_SEC + E2E_TIER_SLACK_SEC``, with the TTL read from the live
      admin API. It is the *documented* convergence bound, so it is the right thing to grade
      against on a default stack.
    * **absolute** — ``E2E_TIER_MAX_WAIT_SEC``, and it exists because the relative bound alone is
      **self-defeating**. Propagation here is pure TTL expiry — ``src/tiers.py`` has no pub/sub, no
      invalidation broadcast, nothing but a snapshot that goes stale — so ``TIER_CACHE_TTL_SEC``
      is simultaneously the mechanism under test and the number the test tolerates. Raise it and
      the harness widens its own tolerance by exactly as much: measured live, a stack at
      ``TIER_CACHE_TTL_SEC=45`` converged in **32.7 s and passed**, and at 600 this check would sit
      there for ten minutes before agreeing that everything was fine. A regression that made
      propagation ten times slower would be absorbed rather than reported.

    So a stack configured to converge more slowly than this harness tolerates is **itself the
    finding**, and the failure message says so rather than quietly stretching. ``capped`` is what
    lets the caller phrase that: it is true exactly when the absolute ceiling is the binding one.
    """
    relative = float(STATE["cache_ttl_sec"]) + TIER_SLACK_SEC
    budget = min(relative, TIER_MAX_WAIT_SEC)
    return budget, relative, TIER_MAX_WAIT_SEC < relative


def slow_convergence_detail(relative: float, capped: bool) -> str:
    """The sentence a convergence failure adds when the ABSOLUTE ceiling was the binding bound."""
    if not capped:
        return ""
    return (
        f" This is the ABSOLUTE ceiling E2E_TIER_MAX_WAIT_SEC={TIER_MAX_WAIT_SEC:.0f}s, which is "
        f"tighter than the server's own TIER_CACHE_TTL_SEC={STATE['cache_ttl_sec']:.0f}s + slack "
        f"{TIER_SLACK_SEC:.0f}s = {relative:.0f}s — so this stack would have been allowed to "
        "converge more slowly and was not. That is the finding, not an artefact of the gate: "
        "propagation is pure TTL expiry with no pub/sub, so every second added to the TTL is a "
        "second during which some replica enforces a limit an operator has already changed."
    )


def refill_bound(tier: str, wall_ms: float) -> int:
    """Tokens the bucket can have refilled over ``wall_ms``, from the tier's own rate.

    ``floor(wall_ms * rpm / 60000)``. This is the whole reason check 7's upper bound is *derived*
    rather than guessed: a burst that takes 3 seconds against a 60/min tier legitimately admits
    three more requests than one that takes 300 ms, and a hard-coded "capacity + 5" would either
    flake on a slow machine or stop being able to see a real over-admission on a fast one.
    """
    rpm = int(STATE["tiers"][tier]["rate_limit_per_min"])
    return math.floor(max(0.0, wall_ms) * rpm / 60_000.0)


def header_int(resp: httpx.Response, name: str) -> int:
    """Read one response header as an integer, or fail the check naming what arrived instead."""
    raw = resp.headers.get(name)
    if raw is None:
        raise CheckFailure(f"{resp.request.url.path} response carries no {name}")
    try:
        return int(raw)
    except ValueError as exc:
        raise CheckFailure(f"{name} is {raw!r} (want an integer)") from exc


def leaked_ratelimit_headers(resp: httpx.Response) -> list[str]:
    """Every ``X-RateLimit-*`` header on ``resp``. Empty is the assertion; the list is the message."""
    return sorted(h for h in resp.headers if h.lower().startswith(RATELIMIT_HEADER_PREFIX))


def served_by(resp: httpx.Response) -> str:
    """The ``X-Served-By`` stamp, or a check failure. Every response in this stack carries one."""
    host = resp.headers.get("X-Served-By")
    if not host:
        raise CheckFailure(
            f"{resp.request.method} {resp.request.url.path} carries no X-Served-By — "
            "ServedByMiddleware is the outermost middleware and must stamp every response"
        )
    return host


def percentile(values: list[float], pct: float) -> float:
    """The ceil-rank percentile of ``values`` (0 < pct <= 100); 0.0 for an empty list."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(pct / 100.0 * len(ordered)) - 1)]


# --------------------------------------------------------------------------------------------- #
# The burst engine
#
# Checks 6, 7 and 8 all need "fire N requests at concurrency C as one principal and tell me what
# came back", so it is one function. It returns a projection rather than the responses themselves:
# holding 200 `httpx.Response` objects alive to read four fields off each is memory this harness
# has no reason to spend, and the projection is what makes the assertions readable.
# --------------------------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class Reply:
    """The five things every assertion in this file asks of a burst response."""

    status: int
    host: str
    retry_after: str | None
    remaining: str | None
    error: str | None
    reason: str | None


async def _fire(
    base_url: str, path: str, headers: dict[str, str], total: int, concurrency: int
) -> list[Reply]:
    """Fire ``total`` requests at ``base_url + path``, at most ``concurrency`` in flight."""
    gate = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)

    async with httpx.AsyncClient(base_url=base_url, timeout=60.0, limits=limits) as client:

        async def one() -> Reply:
            async with gate:
                resp = await client.get(path, headers=headers)
            error: str | None = None
            reason: str | None = None
            if resp.status_code != 200:
                # Only parsed off the refusal path: a 200 body carries no `error` key, and decoding
                # every allowed response would put JSON parsing inside the measured wall time.
                # Both fields are taken from the SAME response the status came from, so a check
                # never has to fire an extra request to find out why a refusal happened — which
                # would race the window boundary the burst just filled.
                try:
                    payload = resp.json()
                except ValueError:
                    payload = {}
                if isinstance(payload, dict):
                    error = payload.get("error") if isinstance(payload.get("error"), str) else None
                    reason = (
                        payload.get("reason") if isinstance(payload.get("reason"), str) else None
                    )
            return Reply(
                status=resp.status_code,
                host=resp.headers.get("X-Served-By", ""),
                retry_after=resp.headers.get("Retry-After"),
                remaining=resp.headers.get("X-RateLimit-Remaining"),
                error=error,
                reason=reason,
            )

        return list(await asyncio.gather(*(one() for _ in range(total))))


def burst(
    principal: str,
    *,
    total: int,
    concurrency: int,
    base_url: str = BASE_URL,
    path: str = PATH_WHOAMI,
) -> tuple[list[Reply], float]:
    """Fire a burst as ``principal`` and return ``(replies, wall_ms)``.

    ``time.perf_counter`` around the whole gather, because the upper bound on how many requests may
    legitimately be admitted is a function of how long the burst took — see :func:`refill_bound`.
    """
    _guard_trailing_slash(path)
    headers = {"Authorization": f"Bearer {token_for(principal)}"}
    started = time.perf_counter()
    replies = asyncio.run(_fire(base_url, path, headers, total, concurrency))
    return replies, (time.perf_counter() - started) * 1000.0


def partition(replies: Iterable[Reply]) -> tuple[list[Reply], list[Reply], list[Reply]]:
    """Split replies into ``(allowed, rejected, other)`` — 200s, 429s, and anything else.

    ``other`` exists so "no third status code" is an assertion with evidence rather than an
    inference from two counts that happen to add up.
    """
    allowed = [r for r in replies if r.status == 200]
    rejected = [r for r in replies if r.status == 429]
    other = [r for r in replies if r.status not in (200, 429)]
    return allowed, rejected, other


# --------------------------------------------------------------------------------------------- #
# The check harness
# --------------------------------------------------------------------------------------------- #
_counter = 0


def check(name: str, fn: Callable[[], str]) -> None:
    """Run one check; print PASS with evidence, or FAIL + exit 1 immediately."""
    global _counter
    _counter += 1
    prefix = f"[{_counter:2d}/{TOTAL_CHECKS}]"
    try:
        evidence = fn()
    except CheckFailure as exc:
        print(f"{prefix} FAIL {name}: {exc}", flush=True)
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001 — an unexpected error is still a failure
        print(f"{prefix} FAIL {name}: unexpected {type(exc).__name__}: {exc}", flush=True)
        sys.exit(1)
    print(f"{prefix} PASS {name} ({evidence})", flush=True)


def info(msg: str) -> None:
    """Print a progress line (flushed so Docker shows it live rather than at exit)."""
    print(f"[e2e] {msg}", flush=True)


def wait_ready(timeout: float = READY_TIMEOUT) -> None:
    """Poll ``/health`` on BOTH base URLs until each answers 200, or exit 1 at the timeout.

    Compose already gates the ``e2e`` service on ``lb`` and both replicas being *healthy*, so this
    normally returns on the first poll. It stays for the same reason the sibling project's does —
    ``TARGET_URL`` can point at something compose does not manage — and it polls ``DIRECT_URL`` too
    because check 8's control run is the one thing here that does not go through the proxy, and a
    verifier that discovers at check 8 that ``api1`` was never reachable has wasted seven checks.
    """
    for label, base, client in (("lb", BASE_URL, CLIENT), ("api1", DIRECT_URL, DIRECT_CLIENT)):
        info(f"waiting for {base}/health ({label}, up to {timeout:.0f}s)...")
        deadline = time.time() + timeout
        last = "no response"
        while time.time() < deadline:
            try:
                resp = client.get("/health", timeout=5.0)
                if resp.status_code == 200:
                    info(f"{label} is ready")
                    break
            except Exception as exc:  # noqa: BLE001 — the service may still be starting
                last = type(exc).__name__
            else:
                last = f"HTTP {resp.status_code}"
            time.sleep(2.0)
        else:
            print(
                f"FAIL bootstrap: {label} /health not ready after {timeout:.0f}s (last: {last})",
                flush=True,
            )
            sys.exit(1)


#: The four numbers a tier is. Compared field by field between the live table and the configured
#: one, so a mismatch names the field rather than dumping two objects.
TIER_FIELDS: Final = ("rate_limit_per_min", "burst", "daily_quota", "monthly_quota")


def _assert_live_table_matches_config(live: dict[str, Any]) -> str:
    """Fail the bootstrap unless the LIVE tier table equals the CONFIGURED ``TIER_LIMITS``.

    .. rubric:: The one thing reading everything off the admin API cannot catch

    Every expected value in checks 4, 6, 7, 10 and 11 comes from ``GET /api/v1/admin/tiers``. That
    is deliberate — it is what stops this file and the replicas disagreeing about a tier somebody
    resized at runtime — but taken alone it is a closed loop: a regression in the **seed / store /
    decode** path that dropped the configured table and fell back to built-in defaults would be
    reported identically by the admin API *and* enforced identically by the limiter, and all 14
    checks would pass while the service ran on limits nobody configured.

    So exactly one number in this file comes from somewhere else: ``Settings().tier_limits``, parsed
    from the ``TIER_LIMITS`` environment variable that the compose ``e2e`` service receives from the
    **same** ``${TIER_LIMITS:-...}`` expression the replicas do. That crosses the whole path the
    runtime table actually travels — parse, seed into ``config:tiers`` with ``HSETNX``, read back,
    ``decode_tier`` — none of which the verifier shares with the server.

    .. rubric:: The honest limit of this assertion

    :func:`src.config.parse_tier_limits` **is** shared: both sides call it on the same string. A bug
    in the parser itself would therefore change both sides at once and pass. What this catches is
    everything downstream of the parse — which is where the interesting failure modes are, because
    that is the half involving a network, a store, and a fallback that is designed to be silent
    (``decode_tier`` treats a malformed row as "use the configured default", by design).

    Asserted **before any check runs**, and therefore before checks 10 and 11 mutate anything.
    After those, the live table legitimately differs from the configured one for as long as the
    mutation is in force, and re-checking would be asserting that hot reload does not work.
    """
    configured = SETTINGS.tier_limits
    if set(live) != set(configured):
        raise CheckFailure(
            f"the live tier table has {sorted(live)} but TIER_LIMITS configures "
            f"{sorted(configured)}. A tier that is configured and not enforced (or enforced and "
            "not configured) means the seeded table is not the one this stack was started with."
        )
    for name in sorted(configured):
        want = configured[name]
        got = live[name]
        for field in TIER_FIELDS:
            if int(got[field]) != int(getattr(want, field)):
                raise CheckFailure(
                    f"tier {name!r}: the live table enforces {field}={got[field]} but TIER_LIMITS "
                    f"configures {getattr(want, field)}. The seeded/decoded table has drifted from "
                    "the configured one — every ceiling this verifier grades against comes from "
                    "the live table, so without this assertion the whole run would agree with a "
                    "service running on limits nobody asked for."
                )
    return f"{len(configured)} tiers match TIER_LIMITS on all {len(TIER_FIELDS)} fields"


def load_tier_table() -> None:
    """Read the live tier table into :data:`STATE` and grade it against ``TIER_LIMITS``. Bootstrap.

    **Nothing in this file hard-codes a limit.** ``free`` being 60/60/1000/25000 is the shipped
    configuration, not a law, and an operator who exports a different ``TIER_LIMITS`` for the stack
    must get a verifier that grades against *their* numbers rather than one that fails on
    arithmetic. So the ceilings, the quota limits, the default tier and ``TIER_CACHE_TTL_SEC`` all
    come from the control plane — which is also the only source that reflects a tier somebody
    changed at runtime.

    The one exception, and the reason it is an exception, is in
    :func:`_assert_live_table_matches_config`.

    A failure here is a **bootstrap** failure rather than a numbered check: every check after this
    point grades against this table, so a table that cannot be trusted is not a failing assertion,
    it is a missing precondition. It prints ``FAIL bootstrap:`` and exits 1 exactly as
    :func:`wait_ready` does.
    """
    table = tier_table()
    STATE["tiers"] = table["tiers"]
    STATE["default_tier"] = table["default_tier"]
    STATE["cache_ttl_sec"] = float(table["cache_ttl_sec"])
    STATE["config_version"] = table["config_version"]
    shipped = ", ".join(
        f"{name}={cfg['rate_limit_per_min']}/{cfg['burst']}/{cfg['daily_quota']}"
        for name, cfg in sorted(STATE["tiers"].items())
    )
    info(f"live tiers (rpm/burst/daily): {shipped}")

    try:
        agreement = _assert_live_table_matches_config(STATE["tiers"])
        if STATE["default_tier"] != SETTINGS.default_tier:
            raise CheckFailure(
                f"the live table reports default_tier={STATE['default_tier']!r} but DEFAULT_TIER "
                f"is {SETTINGS.default_tier!r}; an unrecognised caller would be priced at a tier "
                "nobody configured"
            )
    except CheckFailure as exc:
        print(f"FAIL bootstrap: {exc}", flush=True)
        sys.exit(1)

    budget, relative, capped = convergence_budget()
    info(
        f"default_tier={STATE['default_tier']}, TIER_CACHE_TTL_SEC={STATE['cache_ttl_sec']:.0f}, "
        f"config_version={STATE['config_version']}; live table vs TIER_LIMITS: {agreement}"
    )
    info(
        f"tier convergence budget {budget:.0f}s = "
        + (
            f"E2E_TIER_MAX_WAIT_SEC (capping TTL+slack = {relative:.0f}s)"
            if capped
            else f"TTL+slack (under the E2E_TIER_MAX_WAIT_SEC ceiling of {TIER_MAX_WAIT_SEC:.0f}s)"
        )
    )


# --------------------------------------------------------------------------------------------- #
# 1 — health shape, and the exemption surviving the proxy
# --------------------------------------------------------------------------------------------- #
def check_health() -> str:
    """1. ``/health`` carries the spec's two keys verbatim and no limiter headers.

    Two claims, and the second is the one C12's verification asked for. The middleware's exempt
    check reads ``scope["path"]`` **literally**: if nginx forwarded a path prefix without stripping
    it, ``/health`` would arrive as ``/prefix/health``, miss the exemption, and be metered — at
    which point this unauthenticated probe becomes a 401 and compose starts restarting two replicas
    that are serving perfectly. The absence of ``X-RateLimit-*`` on this response is what proves the
    exemption is still in force *after* the hop, which is not observable from inside the app.
    """
    resp = expect("GET", "/health", 200)
    body = body_of(resp)

    # The spec names these two keys and these two values. Compared with `!=` against literals
    # rather than against an imported constant: this is the published contract as a client reads
    # it, and a check that imported the server's own string would pass through a rewording.
    if body.get("status") != "healthy":
        raise CheckFailure(f"/health status is {body.get('status')!r} (the spec says 'healthy')")
    if body.get("rate_limiter") != "active":
        raise CheckFailure(
            f"/health rate_limiter is {body.get('rate_limiter')!r} (the spec says 'active'). "
            "'degraded' means this replica is on the C8 fallback bucket — Redis is unreachable "
            "and the stack is serving at N x the intended rate."
        )
    for field in ("version", "uptime_sec", "served_by", "redis", "pool", "config_version"):
        if field not in body:
            raise CheckFailure(f"/health is missing {field!r}: {sorted(body)}")
    if body["redis"] != "ok":
        raise CheckFailure(
            f"/health reports redis={body['redis']!r}; every count in this run would be measuring "
            "the degraded fallback rather than the shared store"
        )

    leaked = leaked_ratelimit_headers(resp)
    if leaked:
        raise CheckFailure(
            f"/health carries {leaked} — it is exempt from metering, so either the exemption "
            "stopped applying or the proxy is rewriting the path before the app sees it"
        )
    return (
        f"status={body['status']!r}, rate_limiter={body['rate_limiter']!r}, redis={body['redis']!r},"
        f" served_by={body['served_by']}, no X-RateLimit-* through the proxy"
    )


# --------------------------------------------------------------------------------------------- #
# 2 — both replicas answer
# --------------------------------------------------------------------------------------------- #
def check_replica_fanout() -> str:
    """2. The LB fans out: >= 2 distinct ``X-Served-By`` over ``E2E_REPLICA_PROBES`` probes.

    This is the precondition for check 7 and it is asserted here, early and on its own, so that a
    stack running one replica fails with *this* message rather than silently turning the
    double-spend check into a single-process test that passes trivially.

    The header is cross-checked against the body's ``served_by`` on every probe. They are produced
    by different code paths — ``ServedByMiddleware`` stamps the header from outside the whole
    stack, the handler puts the field in the body — and if they ever disagreed, every replica
    attribution in this file (and on C15's fan-out strip) would be reading a different fact than it
    thinks it is.
    """
    hosts: dict[str, int] = {}
    for index in range(REPLICA_PROBES):
        resp = expect("GET", "/health", 200)
        host = served_by(resp)
        body_host = body_of(resp).get("served_by")
        if body_host != host:
            raise CheckFailure(
                f"probe {index + 1}: X-Served-By={host!r} but the body says served_by={body_host!r}"
            )
        hosts[host] = hosts.get(host, 0) + 1

    if len(hosts) < 2:
        raise CheckFailure(
            f"{REPLICA_PROBES} probes were all served by {sorted(hosts)} — the load balancer is "
            "not fanning out, so nothing after this can distinguish one shared bucket from two "
            "per-process ones. Check that api1 AND api2 are up and healthy."
        )
    STATE["replicas"] = set(hosts)
    spread = ", ".join(f"{host}={count}" for host, count in sorted(hosts.items()))
    return f"{len(hosts)} replicas over {REPLICA_PROBES} probes ({spread}), header == body"


# --------------------------------------------------------------------------------------------- #
# 3 — identity: keys, tokens, and the two 401s
# --------------------------------------------------------------------------------------------- #
def check_identity() -> str:
    """3. Every shipped API key resolves as declared; a JWT resolves; two 401 shapes hold.

    The demo keys are iterated straight off :data:`src.identity.DEMO_CREDENTIALS` rather than
    listed here, so this check grades the server against *the declaration the server seeded from*.
    A rename or a re-tiering in that tuple changes both sides at once — which is the point: the
    thing being verified is that the seeded record and the resolver agree, not that a string
    someone typed twice matches itself.

    Each key costs exactly one request. That matters more than it looks: the demo principals are
    the only ids in this file that are **not** freshly minted, so their daily quota is shared with
    every previous run against the same store. Three requests per run is a rounding error against
    free's 1000/day; a capacity probe on ``demo-free`` would not be.
    """
    resolved = []
    for credential in DEMO_CREDENTIALS:
        body = body_of(expect("GET", PATH_WHOAMI, 200, api_key=credential.raw_key))
        if body.get("user_id") != credential.user_id:
            raise CheckFailure(
                f"key {credential.raw_key!r} resolved to user_id {body.get('user_id')!r}, "
                f"DEMO_CREDENTIALS declares {credential.user_id!r}"
            )
        if body.get("tier") != credential.tier.value:
            raise CheckFailure(
                f"{credential.user_id!r} was metered on tier {body.get('tier')!r}, "
                f"DEMO_CREDENTIALS declares {credential.tier.value!r}"
            )
        if body.get("credential") != "api_key":
            raise CheckFailure(
                f"{credential.user_id!r} authenticated as {body.get('credential')!r} (want 'api_key')"
            )
        if body.get("metered") is not True:
            raise CheckFailure(
                f"{credential.user_id!r} reports metered={body.get('metered')!r} — "
                "RATE_LIMIT_ENABLED is off and this stack is serving every request unmetered"
            )
        resolved.append(f"{credential.user_id}/{credential.tier.value}")

    # A self-signed token for a principal that has never existed. It resolves because identity
    # comes from the signature and authority comes from the store — the token carries no `tier`
    # claim and the resolver would ignore one if it did.
    stranger = new_principal("jwt")
    jwt_body = body_of(expect("GET", PATH_WHOAMI, 200, principal=stranger))
    if jwt_body.get("user_id") != stranger:
        raise CheckFailure(f"JWT for {stranger!r} resolved to {jwt_body.get('user_id')!r}")
    if jwt_body.get("credential") != "jwt":
        raise CheckFailure(f"JWT authenticated as {jwt_body.get('credential')!r} (want 'jwt')")
    if jwt_body.get("tier") != STATE["default_tier"]:
        raise CheckFailure(
            f"a principal with no `user:{{id}}` record was priced at {jwt_body.get('tier')!r}; "
            f"DEFAULT_TIER is {STATE['default_tier']!r}"
        )

    # A credential that is well-formed and simply unknown. The challenge must advertise BOTH
    # schemes: a 401 that names only Bearer tells a client holding an API key that its credential
    # type is not supported here, which is a support ticket with an incorrect answer in it.
    bogus = expect("GET", PATH_WHOAMI, 401, api_key=f"not-a-real-key-{uuid.uuid4().hex}")
    challenge = bogus.headers.get("WWW-Authenticate", "")
    for scheme in ("Bearer", "ApiKey"):
        if scheme not in challenge:
            raise CheckFailure(
                f"the 401 challenge is {challenge!r} and does not advertise {scheme!r}; "
                "the service accepts both schemes and must say so"
            )
    if body_of(bogus).get("error") != "Unauthorized":
        raise CheckFailure(
            f"the 401 body's error is {body_of(bogus).get('error')!r} (want 'Unauthorized'); a "
            "client pattern-matching the two 429 literals must never be told auth was a limit"
        )

    anonymous = expect("GET", PATH_WHOAMI, 401)
    if "Bearer" not in anonymous.headers.get("WWW-Authenticate", ""):
        raise CheckFailure("a request with no credential got a 401 with no WWW-Authenticate")

    # Neither 401 may report a bucket. There is no principal, so every number would be invented —
    # and a *wrong* header is one a client cannot detect, unlike a missing one.
    for label, resp in (("bogus key", bogus), ("no credential", anonymous)):
        leaked = leaked_ratelimit_headers(resp)
        if leaked:
            raise CheckFailure(f"the {label} 401 leaked {leaked} (there is no principal to meter)")

    return (
        f"{len(resolved)} demo keys resolve as declared ({', '.join(resolved)}); self-signed JWT "
        f"-> jwt/{jwt_body['tier']}; bogus key + no credential -> 401 with "
        f"WWW-Authenticate: {challenge}, no X-RateLimit-*"
    )


# --------------------------------------------------------------------------------------------- #
# 4 — the header contract on an allowed response
# --------------------------------------------------------------------------------------------- #
def check_headers() -> str:
    """4. All six headers are present, well-formed, and mean what the docs say they mean.

    The interesting assertion is the last one. ``X-RateLimit-Reset`` is **delay-seconds** and
    ``X-Quota-Reset`` is **absolute unix seconds** — a deliberate asymmetry (the rate window is a
    short relative recovery, the quota period is a calendar instant) and exactly the kind of thing
    that gets "tidied" into one unit by someone who has only read one of them. A client that slept
    until ``X-Quota-Reset`` read as a delay would sleep for 56 years; one that treated
    ``X-RateLimit-Reset`` as an instant would never sleep at all. So the two are asserted to be in
    different magnitudes, from outside, on the same response.

    Fired as a **fresh** principal so ``Remaining`` is exactly ``ceiling - 1``: on a used bucket
    the only assertable property would be "some number", which asserts nothing.
    """
    principal = new_principal("hdr")
    default_tier = STATE["default_tier"]
    resp = expect("GET", PATH_WHOAMI, 200, principal=principal)

    missing = [h for h in RATELIMIT_HEADERS + QUOTA_HEADERS if h not in resp.headers]
    if missing:
        raise CheckFailure(f"an allowed response is missing {missing}")
    if "Retry-After" in resp.headers:
        raise CheckFailure("an ALLOWED response carries Retry-After — that header is denial-only")
    if "X-RateLimit-Degraded" in resp.headers:
        raise CheckFailure(
            "the response is stamped X-RateLimit-Degraded: this replica is on the C8 fallback "
            "bucket, so it is enforcing a per-process limit and nothing below can be trusted"
        )

    config = STATE["tiers"][default_tier]
    limit = header_int(resp, "X-RateLimit-Limit")
    remaining = header_int(resp, "X-RateLimit-Remaining")
    reset = header_int(resp, "X-RateLimit-Reset")
    quota_limit = header_int(resp, "X-Quota-Limit")
    quota_remaining = header_int(resp, "X-Quota-Remaining")
    quota_reset = header_int(resp, "X-Quota-Reset")

    # `Limit` is the TIER's per-minute number, not the bucket capacity. They are equal on the
    # shipped tiers, which is exactly why this must be checked against the tier rather than
    # against `ceiling_of` — the two would agree by coincidence and drift silently.
    if limit != int(config["rate_limit_per_min"]):
        raise CheckFailure(
            f"X-RateLimit-Limit is {limit}, but tier {default_tier!r} declares "
            f"rate_limit_per_min={config['rate_limit_per_min']}"
        )
    expected_remaining = ceiling_of(default_tier) - COST_WHOAMI
    if remaining != expected_remaining:
        raise CheckFailure(
            f"a fresh principal's first request reports X-RateLimit-Remaining={remaining}, "
            f"want {expected_remaining} (= ceiling {ceiling_of(default_tier)} - cost {COST_WHOAMI})"
        )
    if quota_limit != int(config["daily_quota"]):
        raise CheckFailure(
            f"X-Quota-Limit is {quota_limit}, tier {default_tier!r} declares "
            f"daily_quota={config['daily_quota']}"
        )
    if quota_remaining != quota_limit - COST_WHOAMI:
        raise CheckFailure(
            f"X-Quota-Remaining is {quota_remaining} after one cost-{COST_WHOAMI} request against "
            f"a limit of {quota_limit}"
        )

    now = time.time()
    if not 0 <= reset <= 3600:
        raise CheckFailure(
            f"X-RateLimit-Reset is {reset}; it is DELAY-SECONDS and cannot exceed the bucket TTL"
        )
    if quota_reset <= now:
        raise CheckFailure(
            f"X-Quota-Reset is {quota_reset}, which is in the past. It is ABSOLUTE unix seconds "
            "(the next UTC midnight), not a delay — see the unit asymmetry in src/models.py"
        )
    if quota_reset - now > 86_400 + 60:
        raise CheckFailure(
            f"X-Quota-Reset is {quota_reset}, more than a day out; the daily period rolls at the "
            "next UTC midnight"
        )
    return (
        f"Limit={limit} Remaining={remaining} Reset={reset}s (delay) | Quota-Limit={quota_limit} "
        f"Quota-Remaining={quota_remaining} Quota-Reset={quota_reset} (absolute, "
        f"+{(quota_reset - now) / 3600:.1f}h), no Retry-After on a 200"
    )


# --------------------------------------------------------------------------------------------- #
# 5 — the free-tier 429
# --------------------------------------------------------------------------------------------- #
def check_free_tier_429() -> str:
    """5. Exhausting the default tier produces the spec's literal refusal, with a usable Retry-After.

    ``error`` is compared **character for character** against ``"Rate limit exceeded"``. That is
    the whole reason :data:`src.models.ERROR_RATE_LIMIT` is a named constant on the server side:
    the string is a public contract, clients pattern-match it, and no Python in this repo would
    catch a "harmless" rewording — only a check that crosses the container boundary would.

    A 429 without ``Retry-After`` tells a client to back off without saying for how long, which is
    the same as telling it to spin. Floored at 1 second on the server, asserted here.
    """
    tier = STATE["default_tier"]
    principal = new_principal("429")
    ceiling = ceiling_of(tier)
    total = ceiling + RATE_LIMIT_MARGIN
    replies, wall_ms = burst(principal, total=total, concurrency=BURST_CONCURRENCY)
    allowed, rejected, other = partition(replies)

    if other:
        raise CheckFailure(
            f"{len(other)} of {total} requests answered with neither 200 nor 429 "
            f"(statuses: {sorted({r.status for r in other})})"
        )
    if not rejected:
        raise CheckFailure(
            f"{total} requests against a ceiling of {ceiling} produced no 429 at all "
            f"({len(allowed)} allowed in {wall_ms:.0f}ms) — is RATE_LIMIT_ENABLED off?"
        )

    first = rejected[0]
    if first.error != ERROR_RATE_LIMIT:
        raise CheckFailure(
            f"the 429 body's error is {first.error!r}; the spec's literal is {ERROR_RATE_LIMIT!r}"
        )
    if first.remaining != "0":
        raise CheckFailure(f"the 429 reports X-RateLimit-Remaining={first.remaining!r} (want '0')")
    if first.retry_after is None:
        raise CheckFailure("the 429 carries no Retry-After")
    try:
        delay = float(first.retry_after)
    except ValueError as exc:
        raise CheckFailure(f"Retry-After is {first.retry_after!r} (want delay-seconds)") from exc
    if delay < 1:
        raise CheckFailure(f"Retry-After is {delay} (< 1s is an invitation to spin)")

    # The reason is machine-readable and must be a RATE reason, never a quota one: this principal
    # has spent ~90 of a 1000/day allowance, so a `quota_daily` here would mean the gates are
    # reporting each other's verdicts. Read off the SAME 429 the assertions above used, rather than
    # from a fresh request — a follow-up probe would be racing the window rollover that this
    # principal is now sitting against, and would 200 roughly once in every few thousand runs.
    if first.reason not in RATE_REASONS:
        raise CheckFailure(
            f"the 429's reason is {first.reason!r}, want one of {sorted(RATE_REASONS)}"
        )
    return (
        f"{len(rejected)}/{total} were 429 on tier {tier} (ceiling {ceiling}); "
        f"error={first.error!r} exactly, reason={first.reason!r}, Retry-After={delay:.0f}s, "
        f"X-RateLimit-Remaining=0"
    )


# --------------------------------------------------------------------------------------------- #
# 6 — per-tier enforcement
# --------------------------------------------------------------------------------------------- #
def _measure_ceiling(principal: str, tier: str) -> tuple[int, int, float]:
    """Drain a fresh principal's allowance and return ``(admitted, ceiling, wall_ms)``.

    Asserts the partition (200 | 429 and nothing else) and both bounds, so every caller gets the
    same grading. The bounds are the same pair check 7 uses, minus the distributed assertions.
    """
    ceiling = ceiling_of(tier)
    total = ceiling + RATE_LIMIT_MARGIN
    replies, wall_ms = burst(principal, total=total, concurrency=BURST_CONCURRENCY)
    allowed, rejected, other = partition(replies)
    if other:
        raise CheckFailure(
            f"tier {tier}: {len(other)} of {total} requests were neither 200 nor 429 "
            f"(statuses: {sorted({r.status for r in other})})"
        )
    if not rejected:
        raise CheckFailure(
            f"tier {tier}: {total} requests against a ceiling of {ceiling} produced no 429 — "
            "the margin is too small to observe the ceiling, or the limiter is not enforcing"
        )
    admitted = len(allowed)
    upper = ceiling + refill_bound(tier, wall_ms)
    if not ceiling <= admitted <= upper:
        raise CheckFailure(
            f"tier {tier} admitted {admitted}, outside [{ceiling}, {upper}] "
            f"(ceiling {ceiling} + {upper - ceiling} refilled over {wall_ms:.0f}ms)"
        )
    return admitted, ceiling, wall_ms


def check_per_tier() -> str:
    """6. A premium principal's measured ceiling is strictly higher than a free one's.

    "Higher" is the weak half of this check and the counts are the strong half. A limiter that
    ignored tiers entirely and gave everyone 1000 would satisfy ``premium > free`` trivially; only
    asserting that each principal landed inside **its own tier's** ``[ceiling, ceiling + refill]``
    band shows that the number came from the tier table rather than from a constant.

    Both principals are minted here and assigned through the admin API, so neither shares state
    with any other check — and the premium one exercises the "tier read from ``user:{id}`` inside
    the decision script on every request" path rather than a cached claim.
    """
    free_tier = STATE["default_tier"]
    free_principal = new_principal("tier-free")
    free_admitted, free_ceiling, free_ms = _measure_ceiling(free_principal, free_tier)

    premium_principal = new_principal("tier-prem")
    assign_tier(premium_principal, "premium")
    premium_admitted, premium_ceiling, premium_ms = _measure_ceiling(premium_principal, "premium")

    if premium_admitted <= free_admitted:
        raise CheckFailure(
            f"premium admitted {premium_admitted} and {free_tier} admitted {free_admitted}: the "
            "paid tier is not being enforced any higher than the free one"
        )
    if premium_ceiling <= free_ceiling:
        raise CheckFailure(
            f"the live table gives premium a ceiling of {premium_ceiling} and {free_tier} "
            f"{free_ceiling}; this check cannot distinguish the tiers"
        )
    return (
        f"{free_tier} admitted {free_admitted} (ceiling {free_ceiling}, {free_ms:.0f}ms), "
        f"premium admitted {premium_admitted} (ceiling {premium_ceiling}, {premium_ms:.0f}ms) — "
        f"{premium_admitted} > {free_admitted}, each inside its own tier's band"
    )


# --------------------------------------------------------------------------------------------- #
# 7 — THE DISTRIBUTED DOUBLE-SPEND CHECK
# --------------------------------------------------------------------------------------------- #
def check_double_spend() -> str:
    """7. Two replicas, one principal, one bucket: the burst admits ``capacity``, not ``2 x capacity``.

    **This is the check the project exists to pass.** Everything else here verifies an API; this
    verifies the one property that is invisible from inside a single process and that a
    dict-in-memory rate limiter gets catastrophically wrong the moment a second replica exists.

    The recipe, and why each step is load-bearing:

    1. A **fresh** principal on the smallest tier. ``uuid4`` because the bucket must be provably
       untouched, and the small tier because 60 is a number a human can check by eye.
    2. **One probe first**, asserting ``Remaining == ceiling - 1``. A burst against a
       partially-drained bucket proves nothing: if the bucket already held 40 tokens, admitting 40
       is consistent with both a shared bucket and two broken ones.
    3. The burst goes at the **load balancer**, so nginx round-robins it across both replicas.
    4. ``len({X-Served-By}) >= 2`` is asserted **before** any count. Without it, a stack that
       happened to be running one replica would pass this check trivially — and the bug it exists
       to catch would go undetected precisely in the configuration where it cannot occur.
    5. Two upper bounds, and the **per-process one is checked first** so that the canonical bug
       report is the sentence a 2x over-admission actually produces — see assertion (d) for why
       the other order makes it unreachable. The second bound is **derived from measured wall
       time**: a bucket legitimately refills at ``rpm/60000`` tokens per millisecond while the
       burst is in flight, so the ceiling is ``capacity + floor(wall_ms * rpm / 60000)`` and not a
       guessed slack.
    6. ``daily.used == admitted`` from the admin API — **not** ``== total``. This is the assertion
       that a rejected request burned neither a token nor a unit of quota, and it is read back out
       of the store from outside the process that made the decision.

    The wall clock starts at the probe, not at the burst: the probe's own elapsed time is time the
    bucket spent refilling, so measuring from there is what makes the upper bound a bound rather
    than an approximation.
    """
    tier = STATE["default_tier"]
    principal = new_principal("burst")
    assign_tier(principal, tier)
    capacity = ceiling_of(tier)

    # Refused before a single request is fired, because a burst this check cannot learn anything
    # from must not be allowed to report PASS. See MIN_BURST_FACTOR.
    if BURST_REQUESTS < MIN_BURST_FACTOR * capacity:
        raise CheckFailure(
            f"E2E_BURST_REQUESTS={BURST_REQUESTS} is below {MIN_BURST_FACTOR} x capacity "
            f"{capacity} = {MIN_BURST_FACTOR * capacity}. A burst that small cannot reach "
            f"{PER_PROCESS_MULTIPLIER} x {capacity} even if every replica kept its own bucket, so "
            "it cannot refute the hypothesis this check exists to refute. Raise it, or accept that "
            "the project's central claim is untested."
        )

    started = time.perf_counter()
    probe = expect("GET", PATH_WHOAMI, 200, principal=principal)
    probe_remaining = header_int(probe, "X-RateLimit-Remaining")
    if probe_remaining != capacity - COST_WHOAMI:
        raise CheckFailure(
            f"the opening probe reports Remaining={probe_remaining}, want "
            f"{capacity - COST_WHOAMI} — this principal's bucket was NOT full, so a burst against "
            "it would prove nothing about capacity"
        )

    replies, _ = burst(principal, total=BURST_REQUESTS, concurrency=BURST_CONCURRENCY)
    wall_ms = (time.perf_counter() - started) * 1000.0
    allowed, rejected, other = partition(replies)

    # (a) No third status code. A 503 here would mean the limiter could not reach a verdict, and a
    #     burst that produced them would be measuring the connection pool rather than the bucket.
    if len(allowed) + len(rejected) != BURST_REQUESTS:
        raise CheckFailure(
            f"{len(allowed)} allowed + {len(rejected)} rejected != {BURST_REQUESTS} fired; "
            f"{len(other)} answered with {sorted({r.status for r in other})}"
        )

    # (b) The burst actually crossed both replicas. Asserted before any arithmetic — see rubric.
    hosts = {r.host for r in replies if r.host}
    if len(hosts) < 2:
        raise CheckFailure(
            f"all {BURST_REQUESTS} burst requests were served by {sorted(hosts)}. With one replica "
            "answering, this check passes trivially and the bug it exists to catch goes "
            "undetected — the burst must cross both."
        )

    admitted = len(allowed) + 1  # the probe was admitted too, and it spent a token.
    refill = refill_bound(tier, wall_ms)
    upper = capacity + refill

    # (c) The lower bound. It catches a limiter that is simply refusing everything, which would
    #     satisfy every "<=" assertion in this function trivially.
    if admitted < capacity:
        raise CheckFailure(
            f"only {admitted} of {BURST_REQUESTS + 1} requests were admitted against a capacity of "
            f"{capacity} — the bucket is refusing traffic it should have allowed"
        )

    # (d) THE BUG REPORT, and it is checked BEFORE the derived upper bound on purpose.
    #
    #     The plan lists the derived bound first. Written that way this assertion is DEAD CODE:
    #     `admitted >= 2 * capacity` can only be reached when `admitted <= capacity + refill`
    #     already held, which requires `refill >= capacity` — a burst lasting a full minute. So the
    #     one sentence this whole project exists to be able to print would never have been printed.
    #     Verified: pointing one replica at a different Redis logical DB produces exactly 2 x
    #     capacity, and in plan order it surfaced as "more than the derived bound" rather than as
    #     "per-process buckets".
    #
    #     Ordered this way, the two assertions divide the space properly: an over-admission that is
    #     *exactly* a multiple of capacity is named as what it is, and anything else falls through
    #     to (e), where the bound is derived from measured wall time and the diagnosis is open.
    if admitted >= PER_PROCESS_MULTIPLIER * capacity:
        raise CheckFailure(
            f"{PER_PROCESS_MULTIPLIER} replicas x capacity {capacity} = "
            f"{PER_PROCESS_MULTIPLIER * capacity} would mean per-process buckets; got {admitted}"
        )

    # (e) The derived upper bound, from measured wall time rather than a guessed slack. Reached
    #     only for an over-admission smaller than a whole extra bucket — a partially shared store,
    #     a lost round trip, a refill that is running fast.
    if admitted > upper:
        raise CheckFailure(
            f"admitted {admitted} > capacity {capacity} + {refill} legitimately refilled over "
            f"{wall_ms:.0f}ms. The bucket handed out more than it holds, but by less than a whole "
            f"second bucket ({PER_PROCESS_MULTIPLIER} x {capacity}), so this is not the "
            "per-process case — look at the refill rate and at whether every replica reached the "
            "same store for every request."
        )

    # (e) Every refusal is a well-formed one. A 429 that omits Retry-After, or reports a non-zero
    #     Remaining, or uses a different error string, is a refusal a client cannot act on.
    for index, reply in enumerate(rejected):
        if reply.error != ERROR_RATE_LIMIT:
            raise CheckFailure(f"429 #{index + 1} carries error={reply.error!r}")
        if reply.remaining != "0":
            raise CheckFailure(
                f"429 #{index + 1} reports X-RateLimit-Remaining={reply.remaining!r} (want '0')"
            )
        if reply.retry_after is None or float(reply.retry_after) < 1:
            raise CheckFailure(
                f"429 #{index + 1} carries Retry-After={reply.retry_after!r} (want >= 1)"
            )

    # (f) Verified from OUTSIDE the process that decided: the store charged exactly the admitted
    #     requests. `daily.used` counts weighted COST, and /whoami costs 1, so it is a request
    #     count here by construction.
    usage = usage_of(principal)
    used = int(usage["daily"]["used"])
    if used != admitted:
        raise CheckFailure(
            f"daily.used is {used} but {admitted} requests were admitted out of "
            f"{BURST_REQUESTS + 1}. Equal to {BURST_REQUESTS + 1} would mean rejected requests "
            "still burned quota; anything else means the charge and the verdict disagree."
        )

    STATE["lb_admitted"] = admitted
    STATE["lb_refill"] = refill
    return (
        f"{BURST_REQUESTS + 1} requests at c={BURST_CONCURRENCY} across {len(hosts)} replicas "
        f"({', '.join(sorted(hosts))}) in {wall_ms:.0f}ms: {admitted} admitted, {len(rejected)} "
        f"refused; capacity {capacity} <= {admitted} <= {upper} (+{refill} refill), and "
        f"{admitted} < {PER_PROCESS_MULTIPLIER * capacity}; daily.used={used} == admitted"
    )


# --------------------------------------------------------------------------------------------- #
# 8 — the single-replica control
# --------------------------------------------------------------------------------------------- #
def check_single_replica_control() -> str:
    """8. One replica, addressed directly, enforces the SAME ceiling the pair does.

    This is the control that makes check 7 an experiment rather than an observation. Check 7 alone
    shows the LB admitted ~60; it does not show that ~60 is what *one* process would have admitted.
    If the control said 60 and the load-balanced run said 120, the shared-state claim would be
    false and the only honest reading of check 7 would be "the burst was small".

    ``DIRECT_URL`` is ``api1:8000`` over the compose network — a service name, never a published
    host port. Neither replica publishes one, and that is why C12 used two named services instead
    of ``deploy.replicas``: compose-generated names cannot be addressed stably.
    """
    tier = STATE["default_tier"]
    principal = new_principal("control")
    assign_tier(principal, tier)
    capacity = ceiling_of(tier)

    started = time.perf_counter()
    probe = expect("GET", PATH_WHOAMI, 200, client=DIRECT_CLIENT, principal=principal)
    probe_host = served_by(probe)
    probe_remaining = header_int(probe, "X-RateLimit-Remaining")
    if probe_remaining != capacity - COST_WHOAMI:
        raise CheckFailure(
            f"the control probe reports Remaining={probe_remaining}, want {capacity - COST_WHOAMI}"
        )

    replies, _ = burst(
        principal, total=BURST_REQUESTS, concurrency=BURST_CONCURRENCY, base_url=DIRECT_URL
    )
    wall_ms = (time.perf_counter() - started) * 1000.0
    allowed, rejected, other = partition(replies)
    if other:
        raise CheckFailure(
            f"{len(other)} of {BURST_REQUESTS} control requests were neither 200 nor 429 "
            f"({sorted({r.status for r in other})})"
        )

    hosts = {r.host for r in replies if r.host} | {probe_host}
    if len(hosts) != 1:
        raise CheckFailure(
            f"the control run was served by {sorted(hosts)}; DIRECT_URL={DIRECT_URL} must reach "
            "exactly ONE replica or this is not a single-process control"
        )
    # And it must be one of the replicas the LB is fanning out to. A control run against some
    # fourth process — a stale container, a different image, an `api1` that is not in nginx's
    # upstream block — would be comparing two unrelated services and calling the agreement
    # evidence for shared state.
    pool = STATE.get("replicas", set())
    if not hosts <= pool:
        raise CheckFailure(
            f"the control replica {sorted(hosts)} is not one of the replicas behind the LB "
            f"({sorted(pool)}); this control run is not measuring a member of the same pool"
        )

    admitted = len(allowed) + 1
    refill = refill_bound(tier, wall_ms)
    upper = capacity + refill
    if not capacity <= admitted <= upper:
        raise CheckFailure(
            f"the single replica admitted {admitted}, outside [{capacity}, {upper}] over "
            f"{wall_ms:.0f}ms"
        )

    lb_admitted = STATE.get("lb_admitted")
    if lb_admitted is None:
        raise CheckFailure("check 7 did not record its admitted count")
    # The two runs are seconds apart and each refilled for its own duration, so they are compared
    # with a tolerance of BOTH runs' refill bounds rather than against each other exactly. What must
    # not happen is a *factor*: 60 here and 120 there is the whole hypothesis, and it is a mile
    # outside this window whatever the two bursts' wall times were.
    tolerance = refill + int(STATE["lb_refill"])
    if abs(admitted - lb_admitted) > tolerance:
        raise CheckFailure(
            f"the load-balanced run admitted {lb_admitted} and the single replica {admitted}, a "
            f"gap of {abs(admitted - lb_admitted)} against a refill tolerance of {tolerance}. Two "
            "replicas over one Redis must admit what one replica admits; a gap this size means the "
            "buckets are not shared."
        )
    return (
        f"{BURST_REQUESTS + 1} requests at api1 only ({', '.join(sorted(hosts))}) in "
        f"{wall_ms:.0f}ms: {admitted} admitted vs {lb_admitted} through the LB — same ceiling "
        f"{capacity}, not {PER_PROCESS_MULTIPLIER * capacity}"
    )


# --------------------------------------------------------------------------------------------- #
# 9 — weighted endpoint cost
# --------------------------------------------------------------------------------------------- #
def check_weighted_cost() -> str:
    """9. ``/logs/query`` costs 5 and ``/whoami`` costs 1, measured off ``Remaining`` deltas.

    Read as **deltas** on the account-wide number rather than as absolutes, because the two
    endpoints have different token buckets: ``X-RateLimit-Remaining`` is
    ``min(bucket_remaining, window_limit - window_used)`` and the sliding window is the only one of
    those two that both endpoints share. A first request to ``/logs/query`` therefore moves the
    reported number by 5 even though its own bucket has never been touched.

    Cross-checked against ``daily.used``, which is charged the same weighted cost inside the
    decision script. Two independent read-outs of the same charge, and a discrepancy between them
    would mean the number a client paces itself off is not the number it is billed.

    .. rubric:: The window boundary is stepped around rather than tolerated

    The account-wide window is 60 seconds wide. A rollover between two samples resets ``window_used``
    to zero and the delta comes out negative — a real event, at a known instant, that would present
    as "the cost is wrong". So the check reads ``X-RateLimit-Reset`` (delay-seconds to that
    rollover) first and, if the boundary is closer than the measurement needs, waits it out and
    starts over on a fresh principal. Deterministic, and never a retry loop over a flaky assertion.
    """
    tier = STATE["default_tier"]
    for attempt in range(3):
        principal = new_principal("cost")
        first = expect("GET", PATH_WHOAMI, 200, principal=principal)
        reset = header_int(first, "X-RateLimit-Reset")
        if reset < 3:
            # Too close to the rollover to take four more samples inside the same window.
            time.sleep(reset + 0.5)
            continue

        r1 = header_int(first, "X-RateLimit-Remaining")
        r2 = header_int(expect("GET", PATH_WHOAMI, 200, principal=principal), "X-RateLimit-Remaining")
        r3 = header_int(
            expect("GET", PATH_LOGS_QUERY, 200, principal=principal, params={"limit": 1}),
            "X-RateLimit-Remaining",
        )
        r4 = header_int(
            expect("GET", PATH_LOGS_QUERY, 200, principal=principal, params={"limit": 1}),
            "X-RateLimit-Remaining",
        )
        deltas = (r1 - r2, r2 - r3, r3 - r4)
        if min(deltas) < 0:
            # Only reachable if the window rolled over mid-measurement despite the guard above.
            continue

        if deltas[0] != COST_WHOAMI:
            raise CheckFailure(
                f"{PATH_WHOAMI} moved Remaining by {deltas[0]}, want {COST_WHOAMI}"
            )
        for index, delta in enumerate(deltas[1:], start=1):
            if delta != COST_LOGS_QUERY:
                raise CheckFailure(
                    f"{PATH_LOGS_QUERY} call {index} moved Remaining by {delta}, want "
                    f"{COST_LOGS_QUERY}. A delta of {2 * COST_LOGS_QUERY} is the trailing-slash "
                    "double charge (the 307 plus the redirected request)."
                )

        charged = int(usage_of(principal)["daily"]["used"])
        expected = 2 * COST_WHOAMI + 2 * COST_LOGS_QUERY
        if charged != expected:
            raise CheckFailure(
                f"daily.used is {charged} after 2 x {PATH_WHOAMI} + 2 x {PATH_LOGS_QUERY}; the "
                f"weighted cost of those four calls is {expected}"
            )
        return (
            f"Remaining deltas {deltas} on tier {tier}: whoami={COST_WHOAMI}, "
            f"logs/query={COST_LOGS_QUERY} twice; daily.used={charged} == "
            f"2x{COST_WHOAMI} + 2x{COST_LOGS_QUERY} (exact paths, no trailing slash)"
        )
    raise CheckFailure(
        "could not take four samples inside one sliding window after 3 attempts; the window "
        "boundary kept landing mid-measurement"
    )


# --------------------------------------------------------------------------------------------- #
# 10 — quota exhaustion, with no phantom spend
# --------------------------------------------------------------------------------------------- #
def _await_tier_convergence(tier: str, field: str, want: int) -> float:
    """Poll ``GET /admin/tiers`` until >= 2 distinct replicas report ``tier.field == want``.

    Returns the elapsed seconds. Every reply carries ``served_by``, and the LB round-robins, so
    polling the control plane is enough to observe both replicas' snapshots without needing a
    second base URL. The budget is :func:`convergence_budget`'s — the TTL-relative bound *and* the
    absolute ceiling, whichever is tighter.

    This is check 10's **setup**, not its subject, and the failure says so: check 10 is about quota
    enforcement, and a stack whose config takes longer to propagate than this harness will wait has
    not failed the quota check, it has failed to reach the state the quota check needs. Check 11 is
    where convergence is the thing under test — and it shares this budget, so on a slow stack this
    setup step is simply the first place the same finding surfaces.
    """
    budget, relative, capped = convergence_budget()
    deadline = time.monotonic() + budget
    started = time.monotonic()
    converged: dict[str, int] = {}
    while time.monotonic() < deadline:
        table = tier_table()
        converged[table["served_by"]] = int(table["tiers"][tier][field])
        if len(converged) >= 2 and all(value == want for value in converged.values()):
            return time.monotonic() - started
        time.sleep(0.25)
    raise CheckFailure(
        f"SETUP could not converge: {tier}.{field} did not reach {want} on both replicas within "
        f"{budget:.1f}s; last seen {converged}. (This is the precondition, not the quota gate — "
        "check 11 is where convergence is the subject, and it is bounded by this same budget.)"
        + slow_convergence_detail(relative, capped)
    )


def check_quota() -> str:
    """10. A daily quota is enforced across replicas, and a refusal spends nothing.

    The tier's daily quota is dropped to ``E2E_QUOTA_LIMIT`` through the admin API, which is the
    only way to observe quota exhaustion inside one run — the shipped ceilings are 1000/day and up,
    and a harness that fired a thousand requests to prove a counter increments would be a load test
    wearing a check's clothes.

    Two things are asserted that a simpler check would miss:

    * **``X-RateLimit-Remaining`` is still positive on the quota 429.** Quota and rate are separate
      gates with separate headers, and a refusal from one must not report the other as exhausted.
    * **No phantom spend.** The Lua script's C4 property is that a denial writes *nothing* — not
      the spent token, not the counter, not even the refilled bucket — so what is asserted is
      **exact equality** across two consecutive refusals, not an ordering.

    .. rubric:: Why the phantom-spend half has to buy its own precision

    ``tokens_second < tokens_first`` looks like the assertion and is not one. Enterprise refills at
    1000/min, i.e. ~16.7 tokens per second, so a bucket that quietly lost one token to a refused
    request would be back above its previous reading within 60 ms — a single phantom spend sits
    comfortably inside that tolerance and the check would pass. The ``daily.used`` assertion would
    then be carrying the whole claim on its own.

    So the two refusals are **timed**, and the pair is only accepted when the elapsed time is
    provably shorter than one refill quantum (``60000/rpm`` ms — 60 ms at 1000 rpm). With the
    legitimate refill floored at **zero tokens**, "a denial writes nothing" becomes
    ``tokens_second == tokens_first`` exactly, and one phantom token is the difference between
    passing and failing. The arithmetic goes in the evidence string so the reader can check that
    the sample was actually discriminating rather than take the word "exact" for it.

    A pair where the reading went *up* is discarded and retried: the account-wide window can roll
    over mid-pair, which is benign and simply not a discriminating sample. A reading that went
    *down* is never retried — that is the bug.

    The tier is restored in a ``finally``. A failed assertion must not leave a 3-request-a-day
    ceiling installed for the checks after this one.
    """
    tier = "enterprise"
    original = int(STATE["tiers"][tier]["daily_quota"])
    rpm = int(STATE["tiers"][tier]["rate_limit_per_min"])
    #: Milliseconds in which this tier refills exactly one whole token. A refusal pair measured
    #: inside this window cannot have refilled anything, which is what makes the equality below an
    #: assertion rather than a formality.
    refill_quantum_ms = 60_000.0 / rpm
    try:
        set_tier_limits(tier, daily_quota=QUOTA_LIMIT)
        converge_sec = _await_tier_convergence(tier, "daily_quota", QUOTA_LIMIT)

        principal = new_principal("quota")
        assign_tier(principal, tier)

        remaining_seen = []
        for index in range(QUOTA_LIMIT):
            resp = expect("GET", PATH_WHOAMI, 200, principal=principal)
            remaining_seen.append(header_int(resp, "X-Quota-Remaining"))
            expected = QUOTA_LIMIT - (index + 1)
            if remaining_seen[-1] != expected:
                raise CheckFailure(
                    f"request {index + 1} of {QUOTA_LIMIT} reports X-Quota-Remaining="
                    f"{remaining_seen[-1]}, want {expected}"
                )

        # The refusal PAIR, timed, and retried only for a non-discriminating sample. `denied` is
        # the first of the pair, so every body/header assertion below and the phantom-spend
        # equality describe the same two requests.
        for attempt in range(5):
            started_pair = time.perf_counter()
            denied = expect("GET", PATH_WHOAMI, 429, principal=principal)
            second = expect("GET", PATH_WHOAMI, 429, principal=principal)
            pair_ms = (time.perf_counter() - started_pair) * 1000.0
            tokens_first = header_int(denied, "X-RateLimit-Remaining")
            tokens_second = header_int(second, "X-RateLimit-Remaining")
            if tokens_second < tokens_first:
                raise CheckFailure(
                    f"a REFUSED request spent a token: X-RateLimit-Remaining went {tokens_first} "
                    f"-> {tokens_second} across two consecutive quota 429s {pair_ms:.1f}ms apart. "
                    "A denial must write nothing — not the token, not the counter."
                )
            if pair_ms < refill_quantum_ms and tokens_second == tokens_first:
                break
            # Either the pair straddled a refill quantum (so equality would not have been
            # discriminating) or the account-wide window rolled over between the two readings and
            # the allowance went up. Neither is a failure; neither is evidence.
        else:
            raise CheckFailure(
                f"could not take two refusals inside one refill quantum ({refill_quantum_ms:.0f}ms "
                f"at {rpm} rpm) after 5 attempts; without that the equality below cannot "
                "distinguish 'nothing was spent' from 'one token was spent and refilled'"
            )

        body = body_of(denied)
        if body.get("error") != ERROR_QUOTA:
            raise CheckFailure(
                f"the quota refusal's error is {body.get('error')!r}, the spec's literal is "
                f"{ERROR_QUOTA!r}"
            )
        if body.get("reason") != REASON_QUOTA_DAILY:
            raise CheckFailure(
                f"the quota refusal's reason is {body.get('reason')!r}, want "
                f"{REASON_QUOTA_DAILY!r} — 'slow down' and 'you are out of allowance' need "
                "different client behaviour"
            )
        if denied.headers.get("X-Quota-Remaining") != "0":
            raise CheckFailure(
                f"the quota 429 reports X-Quota-Remaining="
                f"{denied.headers.get('X-Quota-Remaining')!r} (want '0')"
            )
        if float(denied.headers.get("Retry-After", "0")) < 1:
            raise CheckFailure(
                f"the quota 429 carries Retry-After={denied.headers.get('Retry-After')!r}"
            )

        # The token bucket is untouched: enterprise holds ~1000 tokens and this principal has
        # spent QUOTA_LIMIT of them. A zero here would mean the quota gate had reported itself
        # through the rate headers, and a client would back off on the wrong signal — sleeping for
        # a bucket refill when what it actually needs is tomorrow.
        expected_tokens = ceiling_of(tier) - QUOTA_LIMIT * COST_WHOAMI
        if tokens_first <= 0:
            raise CheckFailure(
                f"the quota 429 reports X-RateLimit-Remaining={tokens_first}: a quota refusal is "
                "being presented as an exhausted rate limit"
            )
        if tokens_first != expected_tokens:
            raise CheckFailure(
                f"the quota 429 reports X-RateLimit-Remaining={tokens_first}, want "
                f"{expected_tokens} (= ceiling {ceiling_of(tier)} - {QUOTA_LIMIT} admitted "
                f"requests at cost {COST_WHOAMI}). The bucket has spent something other than "
                "exactly the admitted traffic."
            )

        charged = int(usage_of(principal)["daily"]["used"])
        if charged != QUOTA_LIMIT:
            raise CheckFailure(
                f"daily.used is {charged} after {QUOTA_LIMIT} admitted and "
                f"{2 * (attempt + 1)} refused requests; want {QUOTA_LIMIT} — the refusals must "
                "not have been charged"
            )
        return (
            f"{tier}.daily_quota {original} -> {QUOTA_LIMIT} converged on 2 replicas in "
            f"{converge_sec:.1f}s; {QUOTA_LIMIT} allowed (Quota-Remaining {remaining_seen}), then "
            f"429 error={ERROR_QUOTA!r} reason={REASON_QUOTA_DAILY} Quota-Remaining=0. No phantom "
            f"spend: two refusals {pair_ms:.1f}ms apart, under one refill quantum "
            f"({refill_quantum_ms:.0f}ms at {rpm} rpm) so the legitimate refill floor is 0 tokens, "
            f"and RateLimit-Remaining held at EXACTLY {tokens_first} == {tokens_second} "
            f"(= {ceiling_of(tier)} - {QUOTA_LIMIT}); daily.used={charged}"
        )
    finally:
        set_tier_limits(tier, daily_quota=original)


# --------------------------------------------------------------------------------------------- #
# 11 — a runtime tier change, enforced by both replicas, with no restart
# --------------------------------------------------------------------------------------------- #
def check_runtime_tier_change() -> str:
    """11. ``PUT /admin/tiers/{tier}`` reaches BOTH replicas within the documented TTL, dropping nothing.

    The convergence bound is the product's claim, not an implementation detail: a tier's four
    numbers are cached per replica for ``TIER_CACHE_TTL_SEC``, the replica that served the write
    invalidates its own snapshot immediately, and every other replica catches up within the TTL.
    Only a black-box run across a load balancer can see the second half of that sentence.

    What is observed is ``X-RateLimit-Limit``, which is the tier's ``rate_limit_per_min`` — so the
    new number is read off a **metered response** rather than off the control plane. That
    distinction is the check: ``GET /admin/tiers`` reports what a replica *believes*, while
    ``X-RateLimit-Limit`` on a served request reports what it actually **priced the request at**.

    "Zero requests dropped meanwhile" is asserted as: every poll during the convergence window
    returned 200. A hot config change that reached both replicas but 503'd a caller on the way is
    not a hot config change.

    .. rubric:: The budget does NOT scale with the thing it is measuring

    The obvious budget — ``TIER_CACHE_TTL_SEC + slack``, with the TTL read from the live admin API
    — is the documented bound and is also, on its own, a gate that cannot fail. Propagation *is*
    TTL expiry (``src/tiers.py``: a stale snapshot, a background refresh, no pub/sub), so the
    number being tolerated and the mechanism under test are the same number. Measured: at
    ``TIER_CACHE_TTL_SEC=45`` the slow replica took **32.7 s and this check passed**, because the
    budget had widened to 50 s along with it — and at 600 it would wait ten minutes to report
    health. :func:`convergence_budget` therefore takes the tighter of that bound and an absolute
    ``E2E_TIER_MAX_WAIT_SEC``, and a stack that needs longer than the absolute one is reported as
    the finding it is rather than waited out.
    """
    tier = "premium"
    config = STATE["tiers"][tier]
    original_rpm = int(config["rate_limit_per_min"])
    original_burst = int(config["burst"])
    if TIER_RPM in (original_rpm, original_burst):
        raise CheckFailure(
            f"E2E_TIER_RPM={TIER_RPM} equals {tier}'s current value; the new number must be "
            "distinguishable from the old one or convergence is unobservable"
        )
    ttl = STATE["cache_ttl_sec"]
    budget, relative, capped = convergence_budget()
    principal = new_principal("hotcfg")
    assign_tier(principal, tier)

    try:
        write = set_tier_limits(tier, rate_limit_per_min=TIER_RPM, burst=TIER_RPM)
        started = time.monotonic()
        first_seen: dict[str, float] = {}
        statuses: dict[int, int] = {}
        polls = 0
        while time.monotonic() - started < budget:
            resp = request("GET", PATH_WHOAMI, principal=principal)
            polls += 1
            statuses[resp.status_code] = statuses.get(resp.status_code, 0) + 1
            if resp.status_code != 200:
                raise CheckFailure(
                    f"poll {polls} during the config change returned HTTP {resp.status_code}: "
                    f"{resp.text[:200]}. A runtime tier change must drop nothing."
                )
            host = served_by(resp)
            if header_int(resp, "X-RateLimit-Limit") == TIER_RPM and host not in first_seen:
                first_seen[host] = time.monotonic() - started
            if len(first_seen) >= 2:
                break
            time.sleep(0.25)

        if len(first_seen) < 2:
            raise CheckFailure(
                f"only {sorted(first_seen)} of the replicas enforced {tier}={TIER_RPM} within "
                f"{budget:.1f}s; {polls} polls, statuses {statuses}."
                + slow_convergence_detail(relative, capped)
            )
        slowest = max(first_seen.values())
        if slowest > budget:
            raise CheckFailure(
                f"the slowest replica took {slowest:.1f}s to enforce the change, past the "
                f"{budget:.1f}s budget." + slow_convergence_detail(relative, capped)
            )
        timings = ", ".join(f"{host} at {secs:.1f}s" for host, secs in sorted(first_seen.items()))
        return (
            f"{tier}.rate_limit_per_min {original_rpm} -> {TIER_RPM} (config_version "
            f"{write['config_version']}) enforced by {len(first_seen)} replicas: {timings} "
            f"(budget {budget:.1f}s = min(TTL {ttl:.0f}s + slack {TIER_SLACK_SEC:.0f}s, absolute "
            f"ceiling {TIER_MAX_WAIT_SEC:.0f}s)); {polls}/{polls} polls returned 200, 0 dropped — "
            "no restart"
        )
    finally:
        set_tier_limits(tier, rate_limit_per_min=original_rpm, burst=original_burst)


# --------------------------------------------------------------------------------------------- #
# 12 — analytics move by exactly the traffic fired
# --------------------------------------------------------------------------------------------- #
def stats(minutes: int = STATS_MINUTES) -> dict[str, Any]:
    """``GET /dashboard/api/stats`` — unauthenticated, unmetered, and load balanced like everything."""
    resp = expect("GET", "/dashboard/api/stats", 200, params={"minutes": minutes, "hours": 1})
    body = body_of(resp)
    body["_served_by"] = served_by(resp)
    return body


def check_analytics() -> str:
    """12. ``totals.requests`` and ``by_status`` move by exactly what was fired — on both replicas.

    .. rubric:: The endpoint is load balanced too, and that is handled rather than hoped around

    Consecutive polls land on different replicas. The buckets themselves are global keys in the
    shared Redis and the read side takes its window from Redis's own ``TIME``, so both replicas
    *should* fold the same numbers — but "should" is what a check is for. So the after-poll is
    repeated until **two distinct replicas report the same totals**, and only then is the delta
    graded. A disagreement between replicas would show up as a timeout naming both answers rather
    than as an intermittent off-by-N that gets rerun until it passes.

    .. rubric:: Why polling for convergence is correct rather than a sleep in disguise

    The analytics write is issued *after* the response body is on the wire (deliberately — it is 16
    Redis commands and the client is not waiting for them), so the last record of a batch can land
    microseconds after the client sees its status line. Polling until the counter reaches the
    expected value converges the instant it actually has; the deadline is what turns a genuinely
    lost record into a failure instead of a hang.

    Requests are fired **sequentially**, at concurrency 1, on purpose. The collector drops records
    rather than queueing them when more than ``REDIS_MAX_CONNECTIONS // 8`` are already in flight —
    correct behaviour (a statistic must never outbid a decision for a connection) that would make
    an exact-count assertion under concurrency a coin flip.
    """
    before = stats()
    baseline_requests = int(before["totals"]["requests"])
    baseline_status = dict(before["by_status"])

    principal = new_principal("stats")
    for _ in range(ANALYTICS_REQUESTS):
        expect("GET", PATH_WHOAMI, 200, principal=principal)
    for _ in range(ANALYTICS_UNAUTH):
        expect("GET", PATH_WHOAMI, 401, api_key=f"nope-{uuid.uuid4().hex}")

    fired = ANALYTICS_REQUESTS + ANALYTICS_UNAUTH
    want_requests = baseline_requests + fired
    deadline = time.monotonic() + ANALYTICS_TIMEOUT
    agreed: dict[str, int] = {}
    after: dict[str, Any] = {}
    while time.monotonic() < deadline:
        after = stats()
        agreed[after["_served_by"]] = int(after["totals"]["requests"])
        if len(agreed) >= 2 and set(agreed.values()) == {want_requests}:
            break
        time.sleep(0.5)
    else:
        raise CheckFailure(
            f"totals.requests did not settle at {want_requests} on two replicas within "
            f"{ANALYTICS_TIMEOUT:.0f}s (baseline {baseline_requests} + {fired} fired); last "
            f"readings {agreed}. A short-fall means analytics records were dropped; a disagreement "
            "between replicas means the two are folding different windows."
        )

    delta_200 = int(after["by_status"].get("200", 0)) - int(baseline_status.get("200", 0))
    delta_401 = int(after["by_status"].get("401", 0)) - int(baseline_status.get("401", 0))
    if delta_200 != ANALYTICS_REQUESTS:
        raise CheckFailure(
            f"by_status['200'] moved by {delta_200}, want {ANALYTICS_REQUESTS}"
        )
    if delta_401 != ANALYTICS_UNAUTH:
        raise CheckFailure(
            f"by_status['401'] moved by {delta_401}, want {ANALYTICS_UNAUTH} — a 401 is a served "
            "request and must appear on the dashboard"
        )
    window = after["window"]
    return (
        f"fired {ANALYTICS_REQUESTS} x 200 + {ANALYTICS_UNAUTH} x 401; totals.requests "
        f"{baseline_requests} -> {want_requests} (+{fired}) agreed by {len(agreed)} replicas "
        f"({', '.join(sorted(agreed))}); by_status +{delta_200}/200 +{delta_401}/401 over "
        f"{window['minutes_covered']} minute buckets"
    )


# --------------------------------------------------------------------------------------------- #
# 13 — the dashboard feed
# --------------------------------------------------------------------------------------------- #
def check_dashboard() -> str:
    """13. ``/dashboard/api/stats`` is 200 JSON carrying ``rate_limit_enabled``, and is unmetered.

    ``rate_limit_enabled`` is asserted **present** because it is the only field on any surface this
    service exposes that separates "metering is switched off" from "nobody is calling us": with the
    switch off the middleware records nothing, so every number on this payload is byte-identical to
    an idle service while ``/health`` still reports ``rate_limiter: "active"``.

    .. rubric:: Scope, deliberately: this checks the FEED, not the page

    ``GET /dashboard/`` is a 404 shell until C15 lands ``src/static/index.html``. Asserting that
    404 would ship a gate that C15 must delete, and asserting a 200 would ship a red gate today —
    so what is asserted is the property that holds across both commits: the path answers, and never
    with a 5xx. The status it answered with is printed, so the transition from 404 to 200 is
    visible in this line rather than hidden. **C15 extends this check** to the HTML and the JS.
    """
    resp = expect("GET", "/dashboard/api/stats", 200, params={"minutes": 5, "hours": 1})
    body = body_of(resp)
    if not isinstance(body, dict):
        raise CheckFailure(f"/dashboard/api/stats returned a {type(body).__name__}, want an object")
    if "rate_limit_enabled" not in body:
        raise CheckFailure(
            "the stats payload has no rate_limit_enabled; without it a service with metering "
            "switched off is indistinguishable from one nobody is calling"
        )
    if body["rate_limit_enabled"] is not True:
        raise CheckFailure(
            "rate_limit_enabled is false — this stack is serving every request unmetered and "
            "unauthenticated, and every count in this run measured nothing"
        )
    for field in ("totals", "per_minute", "by_status", "tiers", "poll_ms", "served_by", "degraded"):
        if field not in body:
            raise CheckFailure(f"the stats payload is missing {field!r}: {sorted(body)}")
    if int(body["poll_ms"]) <= 0:
        raise CheckFailure(f"poll_ms is {body['poll_ms']}; the page would poll in a tight loop")
    if body["degraded"].get("stats_unavailable"):
        raise CheckFailure(
            "degraded.stats_unavailable is set: the payload is zeroed because the store could not "
            "be read, so check 12's numbers came from a fallback rather than from Redis"
        )

    # The observability surface must not be metered by the thing it observes.
    leaked = leaked_ratelimit_headers(resp)
    if leaked:
        raise CheckFailure(
            f"/dashboard/api/stats carries {leaked}; it is the endpoint an operator opens BECAUSE "
            "everything else is 429ing and must never be rate limited itself"
        )

    shell = request("GET", "/dashboard/")
    if shell.status_code >= 500:
        raise CheckFailure(
            f"GET /dashboard/ -> HTTP {shell.status_code}; the shell may be absent (404 until C15) "
            "but it must never be a server error"
        )
    return (
        f"stats 200 JSON: rate_limit_enabled={body['rate_limit_enabled']}, poll_ms="
        f"{body['poll_ms']}, {len(body['tiers'])} tiers, served_by={body['served_by']}, unmetered; "
        f"GET /dashboard/ -> {shell.status_code} (the HTML shell arrives in C15)"
    )


# --------------------------------------------------------------------------------------------- #
# 14 — overhead
# --------------------------------------------------------------------------------------------- #
def check_overhead() -> str:
    """14. p95 of the enforcement layer's added latency, measured against an exempt path.

    .. rubric:: What this number is, and what it is NOT — read before quoting it

    A black-box harness cannot see inside the process, so it cannot time the rate-limit check in
    isolation. What it *can* do is time two requests that traverse the identical stack — the same
    nginx hop, the same uvicorn worker, the same middleware chain — and differ in exactly one
    thing: whether the enforcement layer ran.

    * **metered**: ``GET /api/v1/whoami`` — resolves a principal, runs the decision script against
      Redis, writes an analytics record. The route itself touches no store and does no downstream
      work, so what is left in the delta is the limiter.
    * **baseline**: ``GET /docs`` — exempt from metering, a real 200, no Redis, no identity.

    ``p95(metered) - p95(baseline)``, and the honest caveats, stated rather than buried:

    1. It is a **difference of percentiles**, not a percentile of differences. The two samples are
       interleaved so they see the same host conditions, but the p95 of a difference is a different
       statistic and this is not it.
    2. It includes the **analytics** round trip, not only the decision. That write is issued after
       the body is sent, so it is not in the response the client measured — but on a keep-alive
       connection uvicorn does not read the next request until it finishes, so it lands in the
       *following* sample. It is real cost the enforcement layer imposes, and it is counted here
       rather than argued away.
    3. The two handlers are not identical work. ``/docs`` serves a small static HTML body;
       ``/whoami`` serialises a JSON model. The residual is small next to a Redis round trip and it
       is not subtracted out.

    The clean isolation — the same application with ``RATE_LIMIT_ENABLED=false`` as the baseline —
    needs a second stack, and that is **C14's** overhead phase. This check is the black-box lower
    bound on the same quantity, and it is gated at the same ``MAX_OVERHEAD_MS``.

    Principals are rotated every ``ceiling/2`` samples so the bucket can never be what is being
    measured: a run long enough to drain a free-tier bucket would otherwise start timing 429s.
    """
    tier = STATE["default_tier"]
    rotate_every = max(1, ceiling_of(tier) // 2)
    principal = new_principal("perf")
    metered_ms: list[float] = []
    baseline_ms: list[float] = []

    for index in range(LATENCY_SAMPLES):
        if index and index % rotate_every == 0:
            principal = new_principal("perf")

        started = time.perf_counter()
        exempt = request("GET", "/docs")
        baseline_ms.append((time.perf_counter() - started) * 1000.0)
        if exempt.status_code != 200:
            raise CheckFailure(f"baseline GET /docs -> HTTP {exempt.status_code}")

        started = time.perf_counter()
        resp = request("GET", PATH_WHOAMI, principal=principal)
        metered_ms.append((time.perf_counter() - started) * 1000.0)
        if resp.status_code != 200:
            raise CheckFailure(
                f"metered sample {index + 1} -> HTTP {resp.status_code}: {resp.text[:160]}. "
                f"Principals rotate every {rotate_every} samples, so this is not the bucket."
            )

    base_p50, base_p95 = percentile(baseline_ms, 50), percentile(baseline_ms, 95)
    met_p50, met_p95 = percentile(metered_ms, 50), percentile(metered_ms, 95)
    overhead_p95 = met_p95 - base_p95
    overhead_p50 = met_p50 - base_p50
    if overhead_p95 > MAX_OVERHEAD_MS:
        raise CheckFailure(
            f"limiter overhead p95 {overhead_p95:.2f}ms > gate {MAX_OVERHEAD_MS:.2f}ms "
            f"(metered p95 {met_p95:.2f}ms - exempt p95 {base_p95:.2f}ms over n={LATENCY_SAMPLES})"
        )
    return (
        f"metered p50/p95 {met_p50:.2f}/{met_p95:.2f}ms vs exempt /docs {base_p50:.2f}/"
        f"{base_p95:.2f}ms over n={LATENCY_SAMPLES} paired samples -> added p50 "
        f"{overhead_p50:.2f}ms, p95 {overhead_p95:.2f}ms <= {MAX_OVERHEAD_MS:.2f}ms. "
        "DIFFERENCE OF p95s against an exempt path, decision + analytics included; the "
        "RATE_LIMIT_ENABLED=false baseline is C14's"
    )


# --------------------------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------------------------- #
def main() -> None:
    info(f"== API Rate Limiter black-box verifier vs {BASE_URL} (control: {DIRECT_URL}) ==")
    info(
        f"gates: burst {BURST_REQUESTS} at c={BURST_CONCURRENCY}; quota {QUOTA_LIMIT}/day; "
        f"tier change to {TIER_RPM} rpm within min(TTL + {TIER_SLACK_SEC:.0f}s, "
        f"{TIER_MAX_WAIT_SEC:.0f}s absolute); analytics settle within {ANALYTICS_TIMEOUT:.0f}s; "
        f"overhead p95 <= {MAX_OVERHEAD_MS:.0f}ms over {LATENCY_SAMPLES} samples"
    )
    wait_ready()
    load_tier_table()

    check("health shape + exemption survived the proxy", check_health)
    check("both replicas answer through the LB", check_replica_fanout)
    check("api keys, JWT, and both 401 shapes", check_identity)
    check("rate-limit + quota headers on an allowed response", check_headers)
    check("free-tier 429 body, Retry-After and Remaining", check_free_tier_429)
    check("per-tier ceilings are enforced and ordered", check_per_tier)
    check("DISTRIBUTED DOUBLE-SPEND: one bucket across two replicas", check_double_spend)
    check("single-replica control enforces the same ceiling", check_single_replica_control)
    check("weighted endpoint cost (logs/query = 5, whoami = 1)", check_weighted_cost)
    check("daily quota exhausts with no phantom token spend", check_quota)
    check("runtime tier change reaches both replicas, no restart", check_runtime_tier_change)
    check("analytics move by exactly the traffic fired", check_analytics)
    check("dashboard stats feed is 200 JSON and unmetered", check_dashboard)
    check("rate-limit overhead p95 within budget", check_overhead)

    print(f"E2E PASSED ({TOTAL_CHECKS}/{TOTAL_CHECKS})", flush=True)


if __name__ == "__main__":
    main()
