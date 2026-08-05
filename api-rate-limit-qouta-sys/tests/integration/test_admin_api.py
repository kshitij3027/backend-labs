"""The control plane, against a real Redis and the real middleware.

Three properties in this file earn their run time; the rest is shape-checking that keeps them
honest.

1. **A rejected admin request issues ZERO Redis commands.** ``/api/v1/admin`` is exempt from
   metering, so it is the one authenticated surface an anonymous caller can hit unthrottled. If a
   bad token cost a store round trip, that exemption would be an amplifier against the shared
   connection pool the limiter depends on — the pre-auth exhaustion vector C5's verification
   measured on the identity path (200 concurrent unknown keys → 168 errors, breaker OPEN), except
   without even a rate limiter in front of it. Asserted against **two** counters: this process's
   :attr:`~src.redis_client.RedisGateway.calls`, and the *server's* own
   ``total_commands_processed``, so a command issued around the gateway would still be caught.

2. **A tier change alters enforcement with no restart.** The payoff of the whole commit. Not
   asserted by reading the value back — that only proves a write happened — but by driving real
   requests through the real middleware and watching a premium principal start being refused at the
   new ceiling, in the same process, with ``/health`` proving the uptime never reset.

3. **A user's tier change takes effect on the very next request**, with no reload and no TTL,
   because ``user -> tier`` is read inside the Lua script and cached nowhere.

Everything that rejects a write also asserts **nothing was written** — a 422 that left a partially
applied config behind is worse than the write it refused.

Driven through ``httpx.ASGITransport`` — no socket, no server — against ``redis:7-alpine`` over the
compose network.
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from src.api.admin import (
    ADMIN_TOKEN_HEADER,
    AUTHORIZATION_HEADER,
    MAX_PRESENTED_TOKEN_CHARS,
    period_state,
)
from src.api.health import SERVED_BY
from src.config import Settings, TierConfig
from src.identity import FIELD_TIER, issue_token
from src.keys import (
    CONFIG_TIERS_KEY,
    CONFIG_VERSION_KEY,
    daily_quota_key,
    day_expire_at,
    month_expire_at,
    user_key,
)
from src.lua import (
    MERGE_TIER_FIELD_COUNT,
    MERGE_TIER_SEPARATOR,
    MERGE_TIER_STATUS_MERGED,
    MERGE_TIER_STATUS_REPAIRED,
    MERGE_TIER_STATUS_SEEDED,
    RLQ_MERGE_TIER_NAME,
)
from src.main import Runtime, create_app
from src.models import UNLIMITED, QuotaPeriodState, RATELIMIT_LIMIT_HEADER, TierUpdate
from src.redis_client import RedisGateway
from src.tiers import (
    TIER_FIELD_COUNT,
    TIER_FIELD_SEPARATOR,
    TierRegistry,
    decode_tier,
    encode_tier,
)
from tests.conftest import TEST_ADMIN_TOKEN

#: A port nothing listens on inside the test container: connection *refused*, which is what a
#: stopped `redis` container produces. Same constant, same reasoning, as
#: ``tests/integration/test_degradation.py``.
DEAD_URL = "redis://127.0.0.1:6390/0"

ADMIN = "/api/v1/admin"
WHOAMI = "/api/v1/whoami"

#: The shipped table, written out rather than read off ``Settings`` so the suite states what the
#: numbers *are* instead of asserting that they equal themselves.
FREE_RPM = 60
PREMIUM_RPM = 300
ENTERPRISE_RPM = 1000
FREE_DAILY = 1000
FREE_MONTHLY = 25_000
PREMIUM_BURST = 300

#: **Every** admin route as ``(method, mounted template, a concrete path, a valid body or None)``.
#:
#: Complete by assertion, not by hope: ``test_this_table_is_every_admin_route_mounted`` compares the
#: templates against the app's real routes, so a route added later without an entry here fails
#: rather than quietly skipping the authentication tests below — which are the tests that make the
#: exempt prefix safe.
ADMIN_ROUTES: tuple[tuple[str, str, str, dict[str, Any] | None], ...] = (
    ("GET", f"{ADMIN}/tiers", f"{ADMIN}/tiers", None),
    ("PUT", f"{ADMIN}/tiers/{{tier}}", f"{ADMIN}/tiers/free", {"burst": 11}),
    (
        "PUT",
        f"{ADMIN}/users/{{user_id}}/tier",
        f"{ADMIN}/users/alice/tier",
        {"tier": "free"},
    ),
    ("GET", f"{ADMIN}/users/{{user_id}}/usage", f"{ADMIN}/users/alice/usage", None),
    ("POST", f"{ADMIN}/config/reload", f"{ADMIN}/config/reload", None),
    ("GET", f"{ADMIN}/debug/memory", f"{ADMIN}/debug/memory", None),
)

#: The authenticated catch-all, declared last so every real route above wins. Listed separately
#: from :data:`ADMIN_ROUTES` because it is not an admin *operation* — it exists so that an
#: anonymous caller gets the same 401 for every spelling under the prefix instead of a free map of
#: the route table.
CATCH_ALL_TEMPLATE = f"{ADMIN}/{{_path:path}}"
CATCH_ALL_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS")


# =============================================================================================
# Fixtures and helpers
# =============================================================================================


def auth(token: str = TEST_ADMIN_TOKEN) -> dict[str, str]:
    """The admin credential header. Defaults to the real token from ``tests/conftest.py``."""
    return {ADMIN_TOKEN_HEADER: token}


def bearer(settings: Settings, user_id: str) -> dict[str, str]:
    """Headers for a JWT caller.

    JWT rather than an API key, for two reasons that both matter here: a fresh principal needs no
    ``apikey:v1:*`` record seeded, and a JWT resolves with **no Redis lookup** — so a test that
    fires traffic to observe a limit is measuring the limiter rather than the credential cache.
    """
    return {"Authorization": f"Bearer {issue_token(user_id, settings=settings)}"}


async def build_app(settings: Settings, *, flush: bool = True) -> tuple[FastAPI, Runtime]:
    """A real app over a real Redis, with the demo credentials seeded.

    ``create_app(runtime=...)`` skips the FastAPI lifespan by design, so this takes the lifespan's
    two jobs on explicitly. The flush happens **between** connecting and seeding: the other order
    would delete the records that were just written.

    ``flush=False`` builds a **second replica** joining a store the first one already seeded — its
    own Runtime, its own gateway, its own connection pool and its own tier snapshot, which is what
    makes a two-replica assertion mean anything.
    """
    runtime = Runtime.build(settings)
    await runtime.redis.connect()
    if flush:
        await runtime.redis.client.flushdb()
    await runtime.start()
    return create_app(runtime=runtime), runtime


async def cut_redis(runtime: Runtime) -> None:
    """Take the store away for real: reopen the gateway against a refused port."""
    await runtime.redis.aclose()
    runtime.redis.settings = runtime.settings.model_copy(update={"redis_url": DEAD_URL})
    await runtime.redis.connect()


async def restore_redis(runtime: Runtime) -> None:
    """Give it back. Called by the fixture teardown too, so a cut test still cleans up."""
    await runtime.redis.aclose()
    runtime.redis.settings = runtime.settings
    await runtime.redis.connect()


async def _serve(settings: Settings):
    """Yield ``(client, runtime)`` for an app built on ``settings``, restoring Redis on the way out.

    The restore is unconditional and comes *before* the flush: a test that cut the store leaves the
    gateway pointed at a dead port, and a teardown that tried to ``FLUSHDB`` through it would fail
    on cleanup and mask the test's own result.
    """
    app, runtime = await build_app(settings)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        try:
            yield client, runtime
        finally:
            await restore_redis(runtime)
            try:
                await runtime.redis.client.flushdb()
            finally:
                await runtime.stop()


@pytest.fixture()
async def admin(redis_settings: Settings):
    """The shipped configuration: an app, a real Redis, and the real middleware in front."""
    async for pair in _serve(redis_settings):
        yield pair


@pytest.fixture()
async def two_replicas(redis_settings: Settings):
    """Two independently constructed apps over one Redis — the C12 shape, in one process.

    Each has its own :class:`~src.redis_client.RedisGateway`, its own pool and its own
    :class:`~src.tiers.TierRegistry` snapshot, so replica B is genuinely up to
    ``TIER_CACHE_TTL_SEC`` behind whatever A committed. That staleness is the *point*: it is the
    condition under which a cached merge base reverts a committed change.
    """
    app_a, runtime_a = await build_app(redis_settings)
    app_b, runtime_b = await build_app(redis_settings, flush=False)
    transport_a = httpx.ASGITransport(app=app_a)
    transport_b = httpx.ASGITransport(app=app_b)
    async with (
        httpx.AsyncClient(transport=transport_a, base_url="http://replica-a") as client_a,
        httpx.AsyncClient(transport=transport_b, base_url="http://replica-b") as client_b,
    ):
        try:
            yield client_a, client_b, runtime_a
        finally:
            try:
                await runtime_a.redis.client.flushdb()
            finally:
                await runtime_b.stop()
                await runtime_a.stop()


@pytest.fixture()
async def quota_free_admin(redis_settings: Settings):
    """An app with both quota periods switched off — the ``unenforced`` state's only source."""
    settings = redis_settings.model_copy(
        update={"quota_daily_enabled": False, "quota_monthly_enabled": False}
    )
    async for pair in _serve(settings):
        yield pair


class FakeClock:
    """A monotonic clock a test moves by hand.

    :class:`~src.tiers.TierRegistry` takes ``clock`` precisely so the TTL boundary is a value a
    test can land on exactly rather than approach with a ``sleep``. Every convergence assertion
    that does not need wall time uses this.
    """

    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


async def second_replica(
    settings: Settings, *, clock: FakeClock | None = None
) -> tuple[RedisGateway, TierRegistry]:
    """A registry with **its own gateway and its own pool**, over the same Redis.

    Its own gateway is the point rather than an implementation detail: the property under test is
    that two processes sharing one store converge, and a registry sharing the writer's client would
    prove nothing about that. This is the same construction C12's distributed test uses.

    Deliberately **not** flushed — it is joining a store the writer already seeded.
    """
    gateway = RedisGateway(settings)
    await gateway.connect()
    registry = (
        TierRegistry(settings, gateway)
        if clock is None
        else TierRegistry(settings, gateway, clock=clock)
    )
    await registry.start()
    return gateway, registry


async def stored_tiers(runtime: Runtime) -> dict[str, str]:
    """``config:tiers`` as raw ``name -> "rpm|burst|daily|monthly"``, read straight off the client.

    Around :meth:`~src.redis_client.RedisGateway.run` on purpose: after a test has cut and restored
    the store the breaker may still be open, and a "nothing was written" assertion must be able to
    read the store regardless of what this process believes about its health.
    """
    raw = await runtime.redis.client.hgetall(CONFIG_TIERS_KEY)
    return {key.decode(): value.decode() for key, value in raw.items()}


async def stored_version(runtime: Runtime) -> int:
    """``config:version``, or 0 when it has never been written."""
    raw = await runtime.redis.client.get(CONFIG_VERSION_KEY)
    return int(raw) if raw is not None else 0


async def stored_user_tier(runtime: Runtime, user_id: str) -> str | None:
    """The ``tier`` field of ``user:{id}``, or ``None`` when the principal has no record."""
    raw = await runtime.redis.client.hget(user_key(user_id), FIELD_TIER)
    return raw.decode() if raw is not None else None


async def server_commands(runtime: Runtime) -> int:
    """Redis's own ``total_commands_processed``.

    Read through ``client.info`` rather than through the gateway, so taking the measurement does
    not move the in-process counter the same test is asserting on.
    """
    stats = await runtime.redis.client.info("stats")
    return int(stats["total_commands_processed"])


async def quiesce(runtime: Runtime) -> None:
    """Leave the registry freshly refreshed and with nothing scheduled.

    Called before every zero-command measurement. :meth:`~src.tiers.TierRegistry.snapshot` schedules
    a background refresh once the snapshot passes ``TIER_CACHE_TTL_SEC``, and a task left over from
    fixture setup could land inside a measurement window and be attributed to the request under
    test. An explicit refresh resets both the staleness flag and the fetch timestamp; the
    ``sleep(0)`` lets anything already in flight finish first.
    """
    await asyncio.sleep(0)
    await runtime.tiers.refresh()
    await asyncio.sleep(0)


async def drain(
    client: httpx.AsyncClient, headers: dict[str, str], attempts: int
) -> tuple[int, int]:
    """Fire ``attempts`` sequential ``/whoami`` calls; return ``(allowed, refused)``.

    Sequential rather than concurrent, so "how many got through" is a statement about the limits
    rather than about the event loop.
    """
    allowed = refused = 0
    for _ in range(attempts):
        response = await client.get(WHOAMI, headers=headers)
        if response.status_code == 200:
            allowed += 1
        else:
            assert response.status_code == 429, response.text
            refused += 1
    return allowed, refused


# =============================================================================================
# Authentication — the hard requirement
# =============================================================================================


def test_this_table_is_every_admin_route_mounted(admin):
    """:data:`ADMIN_ROUTES` must list every mounted admin path, or the auth tests have a hole.

    The tests below prove "an anonymous caller is refused, for free" by walking this table. A route
    added later and forgotten here would simply not be walked — and the one thing that must never
    happen on an unmetered prefix is an authentication gate that a new route quietly opts out of.
    """
    client, runtime = admin
    mounted = {
        (method, route.path)
        for route in client._transport.app.routes  # type: ignore[attr-defined]
        for method in getattr(route, "methods", None) or ()
        if route.path.startswith(ADMIN)
    }
    catch_all = {(method, CATCH_ALL_TEMPLATE) for method in CATCH_ALL_METHODS}
    assert mounted == {(method, template) for method, template, _, _ in ADMIN_ROUTES} | catch_all
    assert runtime is not None


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [(method, path, body) for method, _template, path, body in ADMIN_ROUTES],
)
async def test_every_admin_route_refuses_a_missing_token_with_zero_redis_commands(
    admin, method: str, path: str, body: dict[str, Any] | None
):
    """**The hard requirement.** No credential -> 401, and not one command reaches the store.

    Two independent counters, because either alone has a blind spot. The gateway's ``calls`` would
    miss a command issued directly on ``gateway.client``; the server's ``total_commands_processed``
    would miss nothing at all, but only if the baseline is measured rather than assumed — hence the
    two back-to-back ``INFO`` calls that establish what "no work happened" costs on this server
    build before the request is made.
    """
    client, runtime = admin
    await quiesce(runtime)

    # What an empty window costs, measured rather than assumed: an INFO reply may or may not count
    # itself depending on the server build, and this test must not depend on which.
    first = await server_commands(runtime)
    second = await server_commands(runtime)
    baseline = second - first

    calls_before = runtime.redis.calls
    opened = await server_commands(runtime)
    response = await client.request(method, path, json=body)
    closed = await server_commands(runtime)

    assert response.status_code == 401, response.text
    assert runtime.redis.calls == calls_before, "a rejected admin request went through the gateway"
    assert closed - opened == baseline, (
        f"{method} {path} issued {closed - opened - baseline} Redis command(s) while rejecting an "
        "anonymous caller — the exempt admin prefix is an amplifier against the shared pool"
    )


@pytest.mark.parametrize(
    ("label", "token"),
    [
        ("wrong entirely", "not-the-admin-token-at-all-0123456789"),
        # The two shapes a short-circuiting `==` distinguishes and `hmac.compare_digest` does not.
        ("last byte differs", TEST_ADMIN_TOKEN[:-1] + ("X" if TEST_ADMIN_TOKEN[-1] != "X" else "Y")),
        ("a prefix of the real token", TEST_ADMIN_TOKEN[:-1]),
        ("the real token plus a byte", TEST_ADMIN_TOKEN + "9"),
        ("empty", ""),
        ("whitespace", "   "),
    ],
)
async def test_a_wrong_token_is_refused_with_zero_redis_commands(admin, label: str, token: str):
    """Every near miss is refused, and none of them costs a round trip.

    The last-byte and prefix cases are the ones that matter: they are exactly the inputs whose
    timing differs under a short-circuiting comparison, which is what makes a byte-at-a-time
    recovery of the token possible.
    """
    client, runtime = admin
    await quiesce(runtime)

    first = await server_commands(runtime)
    baseline = await server_commands(runtime) - first

    calls_before = runtime.redis.calls
    opened = await server_commands(runtime)
    response = await client.get(f"{ADMIN}/tiers", headers=auth(token))
    closed = await server_commands(runtime)

    assert response.status_code == 401, f"{label!r} was accepted: {response.text}"
    assert runtime.redis.calls == calls_before
    assert closed - opened == baseline


async def test_a_non_ascii_token_is_a_401_and_not_a_500(admin):
    """A byte a caller can send must not be able to crash the control plane.

    :func:`hmac.compare_digest` raises ``TypeError`` on a ``str`` containing a non-ASCII character,
    which — on a route reached before any other validation — would turn one header byte into a 500
    on every admin request that carried it. Both sides are encoded to UTF-8 first, so this is an
    ordinary rejection.

    Sent as **raw bytes**, because httpx refuses to encode a non-ASCII ``str`` header at all. That
    is the accurate reproduction anyway: the wire carries bytes, and Starlette decodes them as
    latin-1, which is exactly how a non-ASCII character reaches the comparison as a ``str``.
    """
    client, runtime = admin
    calls_before = runtime.redis.calls
    response = await client.get(
        f"{ADMIN}/tiers",
        headers={ADMIN_TOKEN_HEADER.encode(): "café-not-the-admin-token-01234".encode()},
    )
    assert response.status_code == 401, response.text
    assert runtime.redis.calls == calls_before


async def test_an_oversized_token_is_refused_without_encoding_or_comparing_it(admin):
    """A 1 MiB ``X-Admin-Token`` must not buy a megabyte of encode plus a compare, per request.

    Zero Redis either way, so the connection pool is safe — but this is an **unmetered** surface, so
    unbounded per-request work in this process is the same amplification argument the module
    docstring makes about the pool, relocated into CPU and heap. A credential longer than the real
    one cannot match, so the length gate costs no correctness.
    """
    client, runtime = admin
    await quiesce(runtime)
    calls_before = runtime.redis.calls

    oversized = "z" * (MAX_PRESENTED_TOKEN_CHARS * 2048)
    response = await client.get(f"{ADMIN}/tiers", headers=auth(oversized))

    assert response.status_code == 401
    assert runtime.redis.calls == calls_before
    # And a token one byte over the cap is refused for the same reason, not just an absurd one.
    just_over = "z" * (MAX_PRESENTED_TOKEN_CHARS + 1)
    assert (await client.get(f"{ADMIN}/tiers", headers=auth(just_over))).status_code == 401


@pytest.mark.parametrize(
    ("method", "path"),
    [
        # Before the catch-all these answered 404 / 401 / 405 / 307-with-a-Location respectively —
        # four distinguishable replies that map every route and method on a prefix exempt from
        # metering. Disclosure rather than amplification (all four cost zero Redis commands), but
        # it contradicts the care `UNAUTHORIZED_DETAIL` takes not to say what was wrong with a
        # credential.
        ("GET", f"{ADMIN}/nope"),
        ("GET", f"{ADMIN}/tiers"),
        ("DELETE", f"{ADMIN}/tiers"),
        ("PUT", f"{ADMIN}/tiers/"),
        ("POST", f"{ADMIN}/users/alice/tier"),
        ("GET", f"{ADMIN}/debug"),
        ("PATCH", f"{ADMIN}/config/reload"),
    ],
)
async def test_an_anonymous_caller_cannot_tell_a_real_admin_route_from_a_missing_one(
    admin, method: str, path: str
):
    """**Every** spelling under the prefix answers 401 with no credential — same status, same body.

    Route existence, method support and trailing-slash handling all stop being observable without
    the token, which is what makes the "a rejection reveals nothing" claim true rather than
    aspirational.
    """
    client, runtime = admin
    await quiesce(runtime)
    calls_before = runtime.redis.calls

    response = await client.request(method, path)

    assert response.status_code == 401, f"{method} {path} -> {response.status_code}"
    assert "WWW-Authenticate" in response.headers
    assert runtime.redis.calls == calls_before


@pytest.mark.parametrize(
    ("method", "path"),
    [("GET", f"{ADMIN}/nope"), ("DELETE", f"{ADMIN}/tiers"), ("PUT", f"{ADMIN}/tiers/")],
)
async def test_an_authenticated_operator_still_gets_an_honest_404(admin, method: str, path: str):
    """The catch-all hides the route table from strangers, not from the person holding the token."""
    client, _runtime = admin
    response = await client.request(method, path, headers=auth())
    assert response.status_code == 404, response.text
    assert "no such admin route" in response.json()["detail"]


async def test_the_404_does_not_echo_the_requested_path(admin):
    """Reflecting caller input at arbitrary length on an unmetered surface is a free amplifier.

    The body lists the routes that *do* exist, which is operator-useful and fixed-length; what it
    never contains is whatever the caller made up, which they already know.
    """
    client, _runtime = admin
    nonce = "x" * 4096
    response = await client.get(f"{ADMIN}/{nonce}", headers=auth())

    assert response.status_code == 404
    assert nonce not in response.text
    assert len(response.text) < 1024


async def test_the_catch_all_does_not_shadow_a_real_admin_route(admin):
    """Declaration order is what makes the catch-all safe — every real route above it still wins."""
    client, _runtime = admin
    for method, _template, path, body in ADMIN_ROUTES:
        response = await client.request(method, path, json=body, headers=auth())
        assert response.status_code == 200, f"{method} {path} fell through: {response.text}"


async def test_the_401_carries_a_challenge_and_never_echoes_the_credential(admin):
    """A 401 without ``WWW-Authenticate`` is malformed (RFC 9110 §11.6.1), and a leaked token is worse.

    The body says how to authenticate and nothing about what was wrong with what was presented:
    "missing", "too short" and "wrong" are one fact to the caller and three bits to someone guessing.
    """
    client, _runtime = admin
    secret = "super-secret-guess-value-0123456789"
    response = await client.get(f"{ADMIN}/tiers", headers=auth(secret))

    assert response.status_code == 401
    assert "Bearer" in response.headers["WWW-Authenticate"]
    assert ADMIN_TOKEN_HEADER in response.headers["WWW-Authenticate"]
    assert secret not in response.text
    assert TEST_ADMIN_TOKEN not in response.text


async def test_the_dedicated_header_is_accepted(admin):
    """``X-Admin-Token`` — the primary spelling, distinct from the caller-facing ``Authorization``."""
    client, _runtime = admin
    response = await client.get(f"{ADMIN}/tiers", headers=auth())
    assert response.status_code == 200, response.text


@pytest.mark.parametrize("scheme", ["Bearer", "bearer", "BEARER"])
async def test_an_authorization_bearer_is_accepted_case_insensitively(admin, scheme: str):
    """Accepted because it is what every HTTP client reaches for; case-insensitive per RFC 9110."""
    client, _runtime = admin
    response = await client.get(
        f"{ADMIN}/tiers", headers={AUTHORIZATION_HEADER: f"{scheme} {TEST_ADMIN_TOKEN}"}
    )
    assert response.status_code == 200, response.text


@pytest.mark.parametrize(
    "value",
    [
        "Basic " + TEST_ADMIN_TOKEN,  # a scheme this router does not speak
        "Bearer",  # no separator, so no token
        TEST_ADMIN_TOKEN,  # the bare token with no scheme
    ],
)
async def test_an_authorization_header_this_router_does_not_speak_is_not_a_credential(
    admin, value: str
):
    """Falls through to "no credential presented" rather than being treated as a token."""
    client, _runtime = admin
    response = await client.get(f"{ADMIN}/tiers", headers={AUTHORIZATION_HEADER: value})
    assert response.status_code == 401


async def test_the_dedicated_header_wins_when_both_are_present(admin):
    """Order resolves the ambiguity of two secrets under one header name.

    The explicit spelling must never be overridden by whatever an interactive tool happened to
    attach to ``Authorization``, so a correct ``X-Admin-Token`` beside a wrong ``Bearer`` is
    accepted — and a wrong ``X-Admin-Token`` beside a correct ``Bearer`` is not.
    """
    client, _runtime = admin

    accepted = await client.get(
        f"{ADMIN}/tiers",
        headers={ADMIN_TOKEN_HEADER: TEST_ADMIN_TOKEN, AUTHORIZATION_HEADER: "Bearer nonsense"},
    )
    refused = await client.get(
        f"{ADMIN}/tiers",
        headers={
            ADMIN_TOKEN_HEADER: "nonsense",
            AUTHORIZATION_HEADER: f"Bearer {TEST_ADMIN_TOKEN}",
        },
    )

    assert accepted.status_code == 200
    assert refused.status_code == 401


async def test_a_replica_with_no_admin_token_refuses_everything_with_a_503(admin):
    """Fail **closed** when the check itself cannot be performed.

    An empty expectation would make :func:`hmac.compare_digest` return true for a caller who also
    sent nothing — an open admin API. :class:`~src.config.Settings` rejects an empty token at
    construction, so this state is unreachable in a correctly built process; it is asserted anyway,
    because "unreachable" is the reason a guard gets deleted and this is the one guard whose absence
    is an open control plane.
    """
    client, runtime = admin
    original = runtime.settings.admin_token
    runtime.settings.admin_token = ""
    try:
        with_token = await client.get(f"{ADMIN}/tiers", headers=auth(original))
        without = await client.get(f"{ADMIN}/tiers")
    finally:
        runtime.settings.admin_token = original

    assert with_token.status_code == 503, "an empty ADMIN_TOKEN must not accept the old one"
    assert without.status_code == 503, "an empty ADMIN_TOKEN must not accept an empty credential"
    assert with_token.headers["Retry-After"] == "1"


# =============================================================================================
# GET /tiers
# =============================================================================================


async def test_get_tiers_reports_the_table_this_replica_is_enforcing(admin):
    """Shape, the shipped numbers, and the two fields that make convergence observable."""
    client, runtime = admin
    response = await client.get(f"{ADMIN}/tiers", headers=auth())
    assert response.status_code == 200, response.text
    body = response.json()

    assert set(body["tiers"]) == {"free", "premium", "enterprise"}
    assert body["tiers"]["free"]["rate_limit_per_min"] == FREE_RPM
    assert body["tiers"]["premium"]["rate_limit_per_min"] == PREMIUM_RPM
    assert body["tiers"]["enterprise"]["rate_limit_per_min"] == ENTERPRISE_RPM
    assert body["default_tier"] == "free"
    assert body["cache_ttl_sec"] == runtime.settings.tier_cache_ttl_sec
    assert body["served_by"] == SERVED_BY
    # Seeded on a flushed store, so exactly one INCR has happened.
    assert body["config_version"] == 1
    assert 0.0 <= body["snapshot_age_sec"] < 30.0


# =============================================================================================
# PUT /tiers/{tier} — the runtime re-size
# =============================================================================================


async def test_a_partial_update_changes_only_what_was_sent_and_bumps_the_version(admin):
    """One field in, four out — and the other three are the ones that were already stored."""
    client, runtime = admin
    before_version = await stored_version(runtime)

    response = await client.put(
        f"{ADMIN}/tiers/premium", json={"rate_limit_per_min": 7}, headers=auth()
    )
    assert response.status_code == 200, response.text
    body = response.json()

    assert body["tier"] == "premium"
    assert body["config"]["rate_limit_per_min"] == 7
    assert body["config"]["burst"] == PREMIUM_BURST, "burst was not sent and must not have moved"
    assert body["previous"]["rate_limit_per_min"] == PREMIUM_RPM
    assert body["merge"] == MERGE_TIER_STATUS_MERGED, "the base must be the stored row"
    assert body["config_version"] == before_version + 1
    assert body["served_by"] == SERVED_BY

    stored = await stored_tiers(runtime)
    assert stored["premium"].split("|")[0] == "7"
    assert stored["premium"].split("|")[1] == str(PREMIUM_BURST)
    assert await stored_version(runtime) == before_version + 1


async def test_the_replica_that_served_the_put_sees_it_immediately(admin):
    """Synchronous invalidation: **no sleep**, no TTL, no second request.

    The registry's snapshot and the ``GET`` that reads it both show the new number on the very next
    line. Without :meth:`~src.tiers.TierRegistry.store_tier` awaiting its own refresh, this replica
    would serve the value it just overwrote for up to ``TIER_CACHE_TTL_SEC`` — which is how an
    operator watching the response of their own change concludes hot reload does not work.
    """
    client, runtime = admin
    assert runtime.tiers.snapshot().tiers["premium"].rate_limit_per_min == PREMIUM_RPM

    put = await client.put(f"{ADMIN}/tiers/premium", json={"rate_limit_per_min": 9}, headers=auth())
    assert put.status_code == 200

    assert runtime.tiers.snapshot().tiers["premium"].rate_limit_per_min == 9
    read_back = await client.get(f"{ADMIN}/tiers", headers=auth())
    assert read_back.json()["tiers"]["premium"]["rate_limit_per_min"] == 9
    assert read_back.json()["config_version"] == put.json()["config_version"]


async def test_a_tier_change_alters_ENFORCEMENT_with_no_restart(admin):
    """**The payoff test.** The behaviour changes, in the same process, with no restart.

    Deliberately not asserted by reading the stored value back — that only proves a write landed.
    Two fresh premium principals are driven through the real middleware either side of the change:
    the first is admitted well past 5 requests at the shipped ceiling of 300, the second is admitted
    exactly 5 times and then refused. ``/health`` is read either side to prove the process is the
    same one: the hostname is unchanged, uptime went **up** rather than resetting to zero, and
    ``config_version`` climbed.
    """
    client, runtime = admin
    settings = runtime.settings
    new_rpm = 5

    before_health = (await client.get("/health")).json()

    # Before: the shipped premium ceiling. Six requests is more than the new limit will allow, so
    # this run is only meaningful because it succeeds.
    early = bearer(settings, "payoff-before")
    assert (
        await client.put(f"{ADMIN}/users/payoff-before/tier", json={"tier": "premium"}, headers=auth())
    ).status_code == 200
    first = await client.get(WHOAMI, headers=early)
    assert first.status_code == 200
    assert first.headers[RATELIMIT_LIMIT_HEADER] == str(PREMIUM_RPM)
    allowed_before, refused_before = await drain(client, early, attempts=new_rpm + 1)
    assert (allowed_before, refused_before) == (new_rpm + 1, 0)

    # The runtime change. No restart, no reload, no redeploy.
    put = await client.put(
        f"{ADMIN}/tiers/premium",
        json={"rate_limit_per_min": new_rpm, "burst": new_rpm},
        headers=auth(),
    )
    assert put.status_code == 200, put.text

    # After: a fresh premium principal, so the ceiling is observed from a full allowance rather
    # than from whatever the first principal had left.
    late = bearer(settings, "payoff-after")
    assert (
        await client.put(f"{ADMIN}/users/payoff-after/tier", json={"tier": "premium"}, headers=auth())
    ).status_code == 200
    probe = await client.get(WHOAMI, headers=late)
    assert probe.status_code == 200
    assert probe.headers[RATELIMIT_LIMIT_HEADER] == str(new_rpm), "the advertised ceiling is stale"

    allowed_after, refused_after = await drain(client, late, attempts=new_rpm + 3)
    assert allowed_after == new_rpm - 1, "the new limit is not being enforced"
    assert refused_after == 4

    denied = await client.get(WHOAMI, headers=late)
    assert denied.status_code == 429
    assert denied.json()["error"] == "Rate limit exceeded"

    after_health = (await client.get("/health")).json()
    assert after_health["served_by"] == before_health["served_by"], "a different process answered"
    assert after_health["uptime_sec"] >= before_health["uptime_sec"], "the process restarted"
    assert after_health["config_version"] > before_health["config_version"]


async def test_a_second_replica_does_not_converge_until_its_cache_ttl_elapses(admin, redis_settings):
    """The two-replica property C12 depends on, asserted **deterministically** at the shipped TTL.

    An injected monotonic clock rather than a ``sleep``, so the boundary is a value this test lands
    on exactly: before the TTL the second replica is *still enforcing the old table* (which is the
    documented behaviour, not a bug), and the first snapshot past it schedules the read that closes
    the gap. A wall-clock version of this test could only assert "eventually", and "eventually" is
    exactly the guarantee pub/sub would have given — the reason :mod:`src.tiers` chose a TTL is that
    the bound is assertable.
    """
    client, runtime = admin
    clock = FakeClock()
    gateway, replica = await second_replica(redis_settings, clock=clock)
    try:
        before = replica.snapshot()
        assert before.tiers["premium"].rate_limit_per_min == PREMIUM_RPM

        put = await client.put(
            f"{ADMIN}/tiers/premium", json={"rate_limit_per_min": 4}, headers=auth()
        )
        assert put.status_code == 200
        assert runtime.tiers.snapshot().version == put.json()["config_version"]

        # The write is invisible here: no time has passed on this replica's clock.
        assert replica.snapshot().version == before.version
        assert replica.snapshot().tiers["premium"].rate_limit_per_min == PREMIUM_RPM

        # One tick past the TTL, the next snapshot() serves stale AND schedules the refresh.
        clock.advance(float(redis_settings.tier_cache_ttl_sec))
        assert replica.snapshot().tiers["premium"].rate_limit_per_min == PREMIUM_RPM

        for _ in range(200):
            await asyncio.sleep(0.005)
            if replica.snapshot().version != before.version:
                break

        assert replica.snapshot().version == put.json()["config_version"]
        assert replica.snapshot().tiers["premium"].rate_limit_per_min == 4
    finally:
        await replica.stop()
        await gateway.aclose()


async def test_a_second_replica_converges_in_real_time_within_its_cache_ttl(redis_settings):
    """The same convergence, on the **real** clock, measured.

    ``TIER_CACHE_TTL_SEC=2`` rather than the shipped 5 so the suite pays two seconds instead of
    five; the property under test is "converges within its configured TTL", and the shipped value's
    boundary is pinned deterministically by the test above. The elapsed time is printed so a
    verification run (``pytest -s``) can report the measured number rather than an assertion that it
    was under a bound.
    """
    settings = redis_settings.model_copy(update={"tier_cache_ttl_sec": 2})
    async for client, _runtime in _serve(settings):
        gateway, replica = await second_replica(settings)
        try:
            before = replica.snapshot().version
            put = await client.put(
                f"{ADMIN}/tiers/enterprise", json={"daily_quota": 4242}, headers=auth()
            )
            assert put.status_code == 200

            started = time.perf_counter()
            deadline = started + settings.tier_cache_ttl_sec + 3.0
            while time.perf_counter() < deadline:
                if replica.snapshot().version != before:
                    break
                await asyncio.sleep(0.02)
            elapsed = time.perf_counter() - started

            assert replica.snapshot().version == put.json()["config_version"], (
                f"a second replica never converged (waited {elapsed:.3f}s)"
            )
            assert replica.snapshot().tiers["enterprise"].daily_quota == 4242
            print(
                f"\n[C10] second-replica convergence: {elapsed:.3f}s "
                f"(TIER_CACHE_TTL_SEC={settings.tier_cache_ttl_sec})"
            )
            assert elapsed <= settings.tier_cache_ttl_sec + 1.5
        finally:
            await replica.stop()
            await gateway.aclose()


# =============================================================================================
# The merge base — the bug this commit's second round fixed
#
# A partial PUT rewrites all four fields, so it needs a base for the three it is not changing. If
# that base is the replica's snapshot — which is BY DESIGN up to TIER_CACHE_TTL_SEC old — the write
# silently reverts whatever another replica committed inside that window. With two replicas behind
# a load balancer that is the default path for two operator PUTs seconds apart, not a race.
# =============================================================================================


async def test_sequential_partial_puts_across_two_replicas_preserve_every_field(two_replicas):
    """**The regression.** Two partial PUTs, two replicas, both changes survive.

    Measured before the fix, with both replicas settled::

        A1 (replica A): PUT {"daily_quota": 99999}     -> 300|300|99999|1250000
        A2 (replica B): PUT {"rate_limit_per_min": 77} ->  77|300|50000|1250000
                                                                   ^^^^^ A1 reverted

    Replica B's snapshot still said ``daily_quota=50000`` because it had not refreshed yet, and the
    write rewrote all four fields from it. The merge now happens inside Redis, so B's base is what
    is committed rather than what B remembers.

    **No sleep and no reload between the two PUTs** — the point is that B is stale, so making it
    fresh first would test the opposite of what broke.
    """
    client_a, client_b, runtime = two_replicas

    first = await client_a.put(
        f"{ADMIN}/tiers/premium", json={"daily_quota": 99999}, headers=auth()
    )
    assert first.status_code == 200, first.text
    assert (await stored_tiers(runtime))["premium"] == "300|300|99999|1250000"

    second = await client_b.put(
        f"{ADMIN}/tiers/premium", json={"rate_limit_per_min": 77}, headers=auth()
    )
    assert second.status_code == 200, second.text

    stored = (await stored_tiers(runtime))["premium"]
    assert stored == "77|300|99999|1250000", (
        "the second replica merged onto its stale snapshot and reverted the first write"
    )


async def test_the_previous_field_reports_the_true_committed_row(two_replicas):
    """``previous`` is an audit record, so it must not be the writing replica's stale belief.

    Before the fix, the second PUT reported ``daily_quota: 50000`` — a row nobody had held for
    several seconds — which is worse than reporting nothing at all: it is wrong precisely on the
    write someone would later go looking this up about.
    """
    client_a, client_b, _runtime = two_replicas

    await client_a.put(f"{ADMIN}/tiers/premium", json={"daily_quota": 99999}, headers=auth())
    second = await client_b.put(
        f"{ADMIN}/tiers/premium", json={"rate_limit_per_min": 77}, headers=auth()
    )

    assert second.json()["previous"] == {
        "name": "premium",
        "rate_limit_per_min": PREMIUM_RPM,
        "burst": PREMIUM_BURST,
        "daily_quota": 99999,
        "monthly_quota": 1_250_000,
    }
    assert second.json()["merge"] == MERGE_TIER_STATUS_MERGED


async def test_concurrent_partial_puts_to_distinct_fields_all_survive(admin):
    """Three partial PUTs in flight at once must not lose two of them and answer 200 three times.

    Measured before the fix on a single replica: ``rpm=111``, ``daily_quota=77777`` and ``burst=55``
    fired together produced ``111|55|50000|1250000`` — ``daily_quota`` gone — while
    ``config:version`` still advanced by three, so a version watcher could not detect it either.

    Read-modify-write inside one script means Redis serialises the three merges; each one sees the
    previous one's output.
    """
    client, runtime = admin
    version_before = await stored_version(runtime)

    responses = await asyncio.gather(
        *(
            client.put(f"{ADMIN}/tiers/free", json=body, headers=auth())
            for body in (
                {"rate_limit_per_min": 111},
                {"daily_quota": 77777},
                {"burst": 55},
            )
        )
    )

    assert [response.status_code for response in responses] == [200, 200, 200]
    assert (await stored_tiers(runtime))["free"] == f"111|55|77777|{FREE_MONTHLY}"
    assert await stored_version(runtime) == version_before + 3
    # Each write reports the version IT produced, so three concurrent writes report three distinct
    # versions rather than all three reporting whatever the last refresh happened to read.
    assert sorted(response.json()["config_version"] for response in responses) == [
        version_before + 1,
        version_before + 2,
        version_before + 3,
    ]


@pytest.mark.parametrize(
    "body",
    [
        {"rate_limit_per_min": 1},
        {"burst": 2},
        {"daily_quota": 3},
        {"monthly_quota": 4},
        {"rate_limit_per_min": 5, "burst": 6},
        {"burst": 7, "daily_quota": 8, "monthly_quota": 9},
        {"rate_limit_per_min": 1, "burst": 2, "daily_quota": 3, "monthly_quota": 4},
    ],
)
async def test_the_server_side_merge_agrees_with_apply_to(admin, body: dict):
    """The Lua merge and :meth:`~src.models.TierUpdate.apply_to` must be the same rule.

    ``apply_to`` can no longer *be* the write — the merge base has to be the committed row — so the
    risk is that the Python statement of "supplied replaces, absent keeps" and the script's drift
    apart, and nothing notices because each is internally consistent. Pinned here against the real
    script rather than reasoned about.
    """
    client, runtime = admin
    before = runtime.tiers.snapshot().tiers["premium"]

    response = await client.put(f"{ADMIN}/tiers/premium", json=body, headers=auth())
    assert response.status_code == 200, response.text

    expected = TierUpdate(**body).apply_to(before)
    assert TierConfig(**response.json()["config"]) == expected
    assert (await stored_tiers(runtime))["premium"] == encode_tier(expected)


async def test_a_deleted_row_is_rebuilt_from_the_configured_default(admin):
    """No committed row means there is nothing to merge onto, so ``TIER_LIMITS`` is the base.

    Refusing instead would 404 a tier this replica's snapshot legitimately lists (``_parse_tiers``
    overlays the store on the shipped defaults) and that the next replica boot would recreate with
    ``HSETNX`` anyway. The outcome is reported as ``seeded`` rather than silently succeeding,
    because three of the four numbers came from the shipped configuration and not from whatever the
    operator believed was stored.
    """
    client, runtime = admin
    await runtime.redis.client.hdel(CONFIG_TIERS_KEY, "free")

    response = await client.put(f"{ADMIN}/tiers/free", json={"burst": 42}, headers=auth())
    assert response.status_code == 200, response.text
    assert response.json()["merge"] == MERGE_TIER_STATUS_SEEDED
    assert response.json()["previous"]["burst"] == FREE_RPM  # the shipped default, 60
    assert response.json()["config"]["burst"] == 42
    assert (await stored_tiers(runtime))["free"] == f"{FREE_RPM}|42|{FREE_DAILY}|{FREE_MONTHLY}"


@pytest.mark.parametrize(
    "garbage",
    ["nonsense", "1|2|3", "1|2|3|4|5", "60|60|0|25000", "60|60|-1|25000", "60|6 0|1000|25000"],
)
async def test_an_unreadable_row_is_repaired_rather_than_used_as_a_merge_base(
    admin, garbage: str
):
    """A row that does not parse must never become the base of a write.

    Same policy :func:`src.tiers.decode_tier` applies on the read side, for the same reason: the
    decision script reads a non-positive limit as "this gate is not enforcing anything", so merging
    onto ``60|60|0|25000`` would carry the zero forward and hand the tier an unlimited daily
    allowance through the endpoint whose purpose is tightening limits.
    """
    client, runtime = admin
    await runtime.redis.client.hset(CONFIG_TIERS_KEY, "free", garbage)

    response = await client.put(
        f"{ADMIN}/tiers/free", json={"rate_limit_per_min": 12}, headers=auth()
    )
    assert response.status_code == 200, response.text
    assert response.json()["merge"] == MERGE_TIER_STATUS_REPAIRED
    assert (await stored_tiers(runtime))["free"] == f"12|{FREE_RPM}|{FREE_DAILY}|{FREE_MONTHLY}"
    # And the repaired row is readable by the Python parser on the next refresh — the two sides
    # agree about what a well-formed row is.
    assert decode_tier("free", (await stored_tiers(runtime))["free"]).rate_limit_per_min == 12


async def test_the_merge_script_and_the_python_encoding_agree_on_the_wire_format():
    """``src.lua`` restates the separator and field count rather than importing ``src.tiers``.

    That module is PURE by contract — it imports nothing but ``src.keys`` — so the duplication is
    unavoidable and is pinned here instead of hoped for, exactly as ``src.keys`` pins its default
    cost category against ``src.config``'s.
    """
    assert MERGE_TIER_SEPARATOR == TIER_FIELD_SEPARATOR
    assert MERGE_TIER_FIELD_COUNT == TIER_FIELD_COUNT


async def test_a_reply_of_the_wrong_arity_is_a_bug_and_not_a_plausible_answer(admin):
    """A script/decoder mismatch must raise, not produce a config built from the right-shaped bits.

    The same rule :meth:`~src.models.LimitDecision.from_lua` follows for the 19-element decision
    reply: a plausible-looking wrong answer from an enforcement layer is the failure this project
    exists to make impossible to have quietly.
    """
    client, runtime = admin
    assert client is not None
    runtime.redis.register(RLQ_MERGE_TIER_NAME, "return {'merged', '1|1|1|1'}")

    with pytest.raises(ValueError, match="returned 2 elements"):
        await runtime.tiers.merge_tier("free", burst=5)


@pytest.mark.parametrize(
    ("label", "body"),
    [
        ("rpm zero", {"rate_limit_per_min": 0}),
        ("rpm negative", {"rate_limit_per_min": -1}),
        ("burst zero", {"burst": 0}),
        ("burst negative", {"burst": -50}),
        ("daily zero", {"daily_quota": 0}),
        ("monthly zero", {"monthly_quota": 0}),
        ("empty body", {}),
        ("every field explicitly null", {"rate_limit_per_min": None, "burst": None}),
        # A plausible misspelling of `rate_limit_per_min`. A 200 that changed nothing is the worst
        # possible answer for an operator who believes they have just lowered a limit.
        ("unknown field", {"rate_limit": 10}),
        ("wrong type", {"burst": "lots"}),
        # Pydantic's LAX mode would coerce these and apply them. Harmless in effect, and refused on
        # exactly the argument `extra="forbid"` rests on one line up: a body whose shape is wrong
        # should be rejected rather than guessed at, or "it looked applied and was not" comes back
        # through a different door.
        ("numeric string", {"rate_limit_per_min": "10"}),
        ("float", {"burst": 1.5}),
        ("integral float", {"daily_quota": 10.0}),
        ("boolean", {"monthly_quota": True}),
    ],
)
async def test_a_rejected_tier_write_is_a_422_and_writes_NOTHING(admin, label: str, body: dict):
    """A refused write must leave the store byte-identical.

    ``rpm``/``burst``/``daily``/``monthly`` of zero is the case that matters:
    :func:`src.tiers.decode_tier` treats a non-positive value as *malformed* and falls back to the
    configured default, so storing one would produce a config that silently does not mean what the
    operator typed — and the decision script reads a non-positive limit as "this gate is not
    enforcing anything".
    """
    client, runtime = admin
    tiers_before = await stored_tiers(runtime)
    version_before = await stored_version(runtime)

    response = await client.put(f"{ADMIN}/tiers/free", json=body, headers=auth())

    assert response.status_code == 422, f"{label!r}: {response.text}"
    assert await stored_tiers(runtime) == tiers_before, f"{label!r} wrote to config:tiers"
    assert await stored_version(runtime) == version_before, f"{label!r} bumped config:version"


async def test_an_unknown_tier_is_a_404_and_writes_nothing(admin):
    """Creating a tier through this endpoint is a deliberate non-feature.

    ``PUT /tiers/primium`` is overwhelmingly more likely to be a typo than an intent to define a
    tier: answering 200 would mint an inert tier nobody is on and leave ``premium`` untouched at
    300, with an operator mid-incident believing they had lowered a limit that never moved.

    **404 rather than 422**: the tier is a path segment, so an unknown one names a resource that
    does not exist. The body may well have been perfect.
    """
    client, runtime = admin
    tiers_before = await stored_tiers(runtime)
    version_before = await stored_version(runtime)

    response = await client.put(
        f"{ADMIN}/tiers/primium", json={"rate_limit_per_min": 10}, headers=auth()
    )

    assert response.status_code == 404, response.text
    assert "primium" in response.json()["detail"]
    assert await stored_tiers(runtime) == tiers_before
    assert await stored_version(runtime) == version_before
    assert "primium" not in await stored_tiers(runtime)


async def test_a_tier_write_with_redis_down_is_a_503_and_not_a_silent_success(admin):
    """An admin write that silently no-ops is worse than an error.

    The middleware fails **open** on this exception because a metered request has a caller waiting.
    None of that reasoning reaches here: nobody's traffic depends on this route, and an operator who
    believes they lowered a limit and did not is worse off than one who got a 503 and knows to retry.
    """
    client, runtime = admin
    tiers_before = await stored_tiers(runtime)
    version_before = await stored_version(runtime)

    await cut_redis(runtime)
    response = await client.put(
        f"{ADMIN}/tiers/free", json={"rate_limit_per_min": 3}, headers=auth()
    )
    await restore_redis(runtime)

    assert response.status_code == 503, response.text
    assert response.headers["Retry-After"] == "1"
    assert "unreachable" in response.json()["detail"]
    assert await stored_tiers(runtime) == tiers_before, "the 503 was a lie: something was written"
    assert await stored_version(runtime) == version_before


# =============================================================================================
# PUT /users/{user_id}/tier — the instant half
# =============================================================================================


async def test_a_user_tier_change_takes_effect_on_the_very_next_request(admin):
    """**No restart, no reload, no TTL.** One request either side of one ``HSET``.

    This is the half of hot reload that is instant, and it is instant by *omission*: ``user -> tier``
    is read inside the Lua script on every request and cached nowhere — not in the registry, not in
    the identity resolver, and deliberately not in a JWT claim. The 5-second snapshot TTL bounds only
    how long a change to what a tier *means* takes to propagate.
    """
    client, runtime = admin
    headers = bearer(runtime.settings, "instant-user")

    first = await client.get(WHOAMI, headers=headers)
    assert first.status_code == 200
    assert first.headers[RATELIMIT_LIMIT_HEADER] == str(FREE_RPM), "should start on DEFAULT_TIER"

    put = await client.put(
        f"{ADMIN}/users/instant-user/tier", json={"tier": "enterprise"}, headers=auth()
    )
    assert put.status_code == 200, put.text
    assert put.json() == {
        "user_id": "instant-user",
        "tier": "enterprise",
        "created": True,
        "served_by": SERVED_BY,
    }

    # The very next request. Nothing was reloaded and nothing was waited for.
    second = await client.get(WHOAMI, headers=headers)
    assert second.status_code == 200
    assert second.headers[RATELIMIT_LIMIT_HEADER] == str(ENTERPRISE_RPM)
    assert await stored_user_tier(runtime, "instant-user") == "enterprise"


async def test_reassigning_an_existing_principal_reports_created_false(admin):
    """``created`` comes straight off the ``HSET`` reply — one command, no read-back."""
    client, runtime = admin
    await client.put(f"{ADMIN}/users/repeat-user/tier", json={"tier": "premium"}, headers=auth())
    again = await client.put(
        f"{ADMIN}/users/repeat-user/tier", json={"tier": "free"}, headers=auth()
    )

    assert again.status_code == 200
    assert again.json()["created"] is False
    assert await stored_user_tier(runtime, "repeat-user") == "free"


async def test_a_demo_principal_can_be_moved_between_tiers_at_runtime(admin):
    """The seeded API-key principals are ordinary records — the admin API can re-tier them too."""
    client, runtime = admin
    response = await client.put(
        f"{ADMIN}/users/demo-free/tier", json={"tier": "enterprise"}, headers=auth()
    )
    assert response.status_code == 200
    assert response.json()["created"] is False

    probe = await client.get(WHOAMI, headers={"X-API-Key": "demo-free-key"})
    assert probe.headers[RATELIMIT_LIMIT_HEADER] == str(ENTERPRISE_RPM)
    assert await stored_user_tier(runtime, "demo-free") == "enterprise"


@pytest.mark.parametrize(
    ("label", "body"),
    [
        ("unknown tier", {"tier": "platinum"}),
        ("blank tier", {"tier": "   "}),
        ("missing field", {}),
        ("unknown field", {"tier": "free", "rpm": 10}),
        # `"free "` would match no field in the live table and the principal would fall through to
        # DEFAULT_TIER — a silent demotion produced by a trailing space in a curl command. The model
        # strips first, so this one is *accepted* and lands on "free"; the assertion is in the
        # test below rather than here.
    ],
)
async def test_a_rejected_user_tier_write_is_a_422_and_writes_nothing(
    admin, label: str, body: dict
):
    """A tier not in the live table would silently price the principal at ``DEFAULT_TIER``."""
    client, runtime = admin
    response = await client.put(
        f"{ADMIN}/users/rejected-user/tier", json=body, headers=auth()
    )

    assert response.status_code == 422, f"{label!r}: {response.text}"
    assert await stored_user_tier(runtime, "rejected-user") is None, f"{label!r} wrote a tier"


async def test_a_padded_tier_name_is_normalised_rather_than_silently_demoting_the_principal(admin):
    """``"free "`` is stripped where it enters, so the value validated is the value stored."""
    client, runtime = admin
    response = await client.put(
        f"{ADMIN}/users/padded-user/tier", json={"tier": "  free  "}, headers=auth()
    )
    assert response.status_code == 200, response.text
    assert response.json()["tier"] == "free"
    assert await stored_user_tier(runtime, "padded-user") == "free"


async def test_a_braced_user_id_is_refused_rather_than_normalised(admin):
    """Braces delimit the Redis Cluster hash tag, so an id containing one can forge another's slot.

    ``sanitise_user_id`` raises rather than rewriting: silently normalising ``a}b`` to ``a_b``
    changes *which principal* is being metered, and a limiter that quietly meters the wrong account
    is worse than one that returns an error naming the problem.
    """
    client, runtime = admin
    response = await client.put(
        f"{ADMIN}/users/alice}}x{{bob/tier", json={"tier": "free"}, headers=auth()
    )
    assert response.status_code == 422, response.text
    assert "brace" in response.json()["detail"]
    assert await stored_user_tier(runtime, "alice") is None


async def test_a_user_tier_write_with_redis_down_is_a_503(admin):
    """Same rule as the tier write: refused loudly rather than reported as applied."""
    client, runtime = admin
    await cut_redis(runtime)
    response = await client.put(
        f"{ADMIN}/users/outage-user/tier", json={"tier": "premium"}, headers=auth()
    )
    await restore_redis(runtime)

    assert response.status_code == 503, response.text
    assert response.headers["Retry-After"] == "1"
    assert await stored_user_tier(runtime, "outage-user") is None


# =============================================================================================
# GET /users/{user_id}/usage
# =============================================================================================


async def test_a_never_seen_principal_reports_zeros_and_reset_rather_than_a_404(admin):
    """The quota period exists whether or not anyone has used it.

    A 404 would say "there is no such quota", which is false: the principal will be metered against
    exactly these numbers on their first request, and ``reset_at`` is already known.
    """
    client, _runtime = admin
    moment = datetime.now(timezone.utc)
    response = await client.get(f"{ADMIN}/users/never-seen/usage", headers=auth())

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["user_id"] == "never-seen"
    assert body["tier"] == "free", "an unrecorded principal is priced at DEFAULT_TIER"
    assert body["daily"] == {
        "limit": FREE_DAILY,
        "used": 0,
        "remaining": FREE_DAILY,
        "reset_at": day_expire_at(moment),
        "state": QuotaPeriodState.RESET.value,
    }
    assert body["monthly"]["limit"] == FREE_MONTHLY
    assert body["monthly"]["used"] == 0
    assert body["monthly"]["reset_at"] == month_expire_at(moment)
    assert body["monthly"]["state"] == QuotaPeriodState.RESET.value


async def test_usage_counts_exactly_the_requests_that_were_ADMITTED(admin):
    """``daily.used`` must track admissions, not attempts.

    This is the assertion C13's double-spend check makes from outside the process: it fires a known
    number of requests, counts the allowed ones, and asserts ``daily.used == allowed`` rather than
    ``== total`` — which is what proves a rejected request burned neither a token nor quota.
    """
    client, runtime = admin
    headers = bearer(runtime.settings, "usage-user")
    fired = 4
    allowed, refused = await drain(client, headers, attempts=fired)
    assert (allowed, refused) == (fired, 0)

    body = (await client.get(f"{ADMIN}/users/usage-user/usage", headers=auth())).json()
    assert body["daily"]["used"] == allowed
    assert body["daily"]["remaining"] == FREE_DAILY - allowed
    assert body["daily"]["state"] == QuotaPeriodState.ACTIVE.value
    assert body["monthly"]["used"] == allowed
    assert body["monthly"]["state"] == QuotaPeriodState.ACTIVE.value


async def test_usage_reports_exhausted_once_the_daily_quota_binds(admin):
    """A quota lowered at runtime binds at runtime, and the read-out says so.

    Both halves in one test on purpose: the ``exhausted`` state is only meaningful next to a request
    that was actually refused for :attr:`~src.models.DenyReason.QUOTA_DAILY`.
    """
    client, runtime = admin
    quota = 2
    assert (
        await client.put(f"{ADMIN}/tiers/free", json={"daily_quota": quota}, headers=auth())
    ).status_code == 200

    headers = bearer(runtime.settings, "exhausted-user")
    allowed, refused = await drain(client, headers, attempts=quota + 1)
    assert (allowed, refused) == (quota, 1)

    denied = await client.get(WHOAMI, headers=headers)
    assert denied.status_code == 429
    assert denied.json()["reason"] == "quota_daily"

    body = (await client.get(f"{ADMIN}/users/exhausted-user/usage", headers=auth())).json()
    assert body["daily"] == {
        "limit": quota,
        "used": quota,
        "remaining": 0,
        "reset_at": day_expire_at(datetime.now(timezone.utc)),
        "state": QuotaPeriodState.EXHAUSTED.value,
    }
    # The refused requests burned no monthly quota either.
    assert body["monthly"]["used"] == quota
    assert body["monthly"]["state"] == QuotaPeriodState.ACTIVE.value


async def test_usage_reports_unenforced_when_a_period_is_switched_off(quota_free_admin):
    """``QUOTA_DAILY_ENABLED=false`` reaches the script as an ``EXPIREAT`` of 0 and stops the gate.

    Reporting ``reset`` here — the state a naive implementation lands on — would be a false *claim*:
    ``reset`` says a period boundary has just rolled over, and a client could reasonably render
    "your quota just refreshed" for a quota nobody is counting. ``remaining`` carries the
    :data:`~src.models.UNLIMITED` sentinel for the same reason ``0`` would be the opposite fact.
    """
    client, runtime = quota_free_admin
    headers = bearer(runtime.settings, "unmetered-user")
    await drain(client, headers, attempts=2)

    body = (await client.get(f"{ADMIN}/users/unmetered-user/usage", headers=auth())).json()
    for period in ("daily", "monthly"):
        assert body[period]["limit"] == 0, period
        assert body[period]["remaining"] == UNLIMITED, period
        assert body[period]["state"] == QuotaPeriodState.UNENFORCED.value, period
        assert body[period]["used"] == 0, "a disabled period's counter is never written"


async def test_usage_reports_the_tier_the_principal_is_actually_on(admin):
    """``tier`` and ``limit`` must describe the same thing, including for an unknown stored tier."""
    client, runtime = admin

    await client.put(f"{ADMIN}/users/tiered-user/tier", json={"tier": "premium"}, headers=auth())
    assigned = (await client.get(f"{ADMIN}/users/tiered-user/usage", headers=auth())).json()
    assert assigned["tier"] == "premium"
    assert assigned["daily"]["limit"] == 50_000

    # A tier written out of band that no longer exists in the table: the decision script would
    # price this principal at DEFAULT_TIER, so reporting the stored name beside DEFAULT_TIER's
    # numbers would be two facts that contradict each other.
    await runtime.redis.client.hset(user_key("ghost-user"), FIELD_TIER, "retired-tier")
    ghost = (await client.get(f"{ADMIN}/users/ghost-user/usage", headers=auth())).json()
    assert ghost["tier"] == "free"
    assert ghost["daily"]["limit"] == FREE_DAILY


async def test_reset_at_is_read_from_the_counters_own_expiry(admin):
    """``EXPIRETIME``, not ``TTL`` plus this replica's wall clock.

    Proved by writing a counter whose expiry is deliberately **not** the computed UTC boundary: if
    ``reset_at`` were derived from the calendar rather than from the key, this would still report
    midnight and the test would pass for the wrong reason.
    """
    client, runtime = admin
    moment = datetime.now(timezone.utc)
    key = daily_quota_key("expiry-user", moment)
    odd_expiry = day_expire_at(moment) + 4321

    await runtime.redis.client.set(key, 17)
    await runtime.redis.client.expireat(key, odd_expiry)

    body = (await client.get(f"{ADMIN}/users/expiry-user/usage", headers=auth())).json()
    assert body["daily"]["used"] == 17
    assert body["daily"]["reset_at"] == odd_expiry
    # The monthly counter does not exist, so it falls back to the computed boundary.
    assert body["monthly"]["reset_at"] == month_expire_at(moment)


async def test_a_counter_with_no_expiry_falls_back_to_the_computed_boundary(admin):
    """A quota counter with no TTL never rolls over — an anomaly, reported rather than crashed on."""
    client, runtime = admin
    moment = datetime.now(timezone.utc)
    await runtime.redis.client.set(daily_quota_key("eternal-user", moment), 3)

    body = (await client.get(f"{ADMIN}/users/eternal-user/usage", headers=auth())).json()
    assert body["daily"]["used"] == 3
    assert body["daily"]["reset_at"] == day_expire_at(moment)


async def test_a_counter_that_is_not_a_number_reads_as_zero_rather_than_a_500(admin):
    """A read-out is not worth a 500. Something else wrote here; say zero and log it."""
    client, runtime = admin
    moment = datetime.now(timezone.utc)
    await runtime.redis.client.set(daily_quota_key("corrupt-user", moment), "not-a-number")

    body = (await client.get(f"{ADMIN}/users/corrupt-user/usage", headers=auth())).json()
    assert body["daily"]["used"] == 0
    assert body["daily"]["state"] == QuotaPeriodState.RESET.value


async def test_a_braced_user_id_is_refused_on_the_usage_read_too(admin):
    """The same rule on the read side — one definition of what a usable id is."""
    client, _runtime = admin
    response = await client.get(f"{ADMIN}/users/bad}}id{{x/usage", headers=auth())
    assert response.status_code == 422
    assert "brace" in response.json()["detail"]


async def test_a_usage_read_with_redis_down_is_a_503_not_a_zeroed_answer(admin):
    """Zeros during an outage read as "full allowance remaining" — the number an operator acts on."""
    client, runtime = admin
    await cut_redis(runtime)
    response = await client.get(f"{ADMIN}/users/demo-free/usage", headers=auth())
    await restore_redis(runtime)

    assert response.status_code == 503, response.text
    assert response.headers["Retry-After"] == "1"
    assert "unreachable" in response.json()["detail"]


@pytest.mark.parametrize(
    ("limit", "used", "expected"),
    [
        (0, 0, QuotaPeriodState.UNENFORCED),
        (0, 99, QuotaPeriodState.UNENFORCED),
        (-1, 0, QuotaPeriodState.UNENFORCED),
        (10, 10, QuotaPeriodState.EXHAUSTED),
        (10, 11, QuotaPeriodState.EXHAUSTED),
        (10, 0, QuotaPeriodState.RESET),
        (10, 1, QuotaPeriodState.ACTIVE),
        (10, 9, QuotaPeriodState.ACTIVE),
    ],
)
def test_period_state_walks_the_same_ladder_as_the_decision_script(
    limit: int, used: int, expected: QuotaPeriodState
):
    """``unenforced``, ``exhausted``, ``reset``, ``active`` — in that order, and the order matters.

    ``(0, 99)`` is the case that pins it: a period with no ceiling is ``unenforced`` even when the
    counter is well past what any ceiling would have been, because ``exhausted`` describes a limit
    that bound and there was none.
    """
    assert period_state(limit, used) is expected


# =============================================================================================
# POST /config/reload
# =============================================================================================


async def test_reload_picks_up_an_out_of_band_write_immediately(admin):
    """An operator editing ``config:tiers`` by hand should not have to wait out a TTL.

    The ``GET`` before the reload is half the assertion: it proves the replica really was serving
    the old table, so the reload is observed to *do* something rather than to coincide with a
    refresh that would have happened anyway.
    """
    client, runtime = admin
    before = (await client.get(f"{ADMIN}/tiers", headers=auth())).json()
    assert before["tiers"]["free"]["rate_limit_per_min"] == FREE_RPM

    await runtime.redis.client.hset(CONFIG_TIERS_KEY, "free", "7|8|900|9000")
    await runtime.redis.client.incr(CONFIG_VERSION_KEY)

    # Still the old table: nothing has told this replica to look.
    mid = (await client.get(f"{ADMIN}/tiers", headers=auth())).json()
    assert mid["tiers"]["free"]["rate_limit_per_min"] == FREE_RPM

    reload = await client.post(f"{ADMIN}/config/reload", headers=auth())
    assert reload.status_code == 200, reload.text
    body = reload.json()
    assert body["changed"] is True
    assert body["previous_version"] == before["config_version"]
    assert body["config_version"] == before["config_version"] + 1
    assert body["tiers"]["free"] == {
        "name": "free",
        "rate_limit_per_min": 7,
        "burst": 8,
        "daily_quota": 900,
        "monthly_quota": 9000,
    }
    assert body["served_by"] == SERVED_BY

    after = (await client.get(f"{ADMIN}/tiers", headers=auth())).json()
    assert after["tiers"]["free"]["rate_limit_per_min"] == 7


async def test_reload_reports_changed_false_when_nothing_moved(admin):
    """A reload that picked nothing up must say so rather than implying it applied something."""
    client, _runtime = admin
    response = await client.post(f"{ADMIN}/config/reload", headers=auth())
    assert response.status_code == 200
    body = response.json()
    assert body["changed"] is False
    assert body["config_version"] == body["previous_version"] == 1


async def test_reload_bypasses_the_post_failure_backoff(admin):
    """A directly awaited refresh always attempts — or the reload button would lie.

    The backoff exists to stop a *scheduled* refresh hammering a store that is down. An operator
    pressing reload is an explicit action, and an endpoint that silently declined to try because a
    background attempt had failed thirty seconds earlier would report success having done nothing.
    """
    client, runtime = admin

    # Arm the backoff the way a real outage would: one failed refresh through a dead store.
    await cut_redis(runtime)
    failed = await client.post(f"{ADMIN}/config/reload", headers=auth())
    assert failed.status_code == 503
    assert runtime.tiers._refresh_backoff_until > 0.0

    await restore_redis(runtime)
    await runtime.redis.client.hset(CONFIG_TIERS_KEY, "premium", "11|12|1300|14000")
    await runtime.redis.client.incr(CONFIG_VERSION_KEY)

    recovered = await client.post(f"{ADMIN}/config/reload", headers=auth())
    assert recovered.status_code == 200, recovered.text
    assert recovered.json()["tiers"]["premium"]["rate_limit_per_min"] == 11


async def test_reload_with_redis_down_is_a_503_rather_than_a_reassuring_200(admin):
    """:meth:`~src.tiers.TierRegistry.refresh` swallows an outage by design; this route must not.

    Answering "reload now" with a 200 and a version number that never moved is the silent no-op this
    whole surface refuses. The outcome is therefore read from the registry's success counter rather
    than from the value ``refresh`` returns.
    """
    client, runtime = admin
    version_before = runtime.tiers.snapshot().version

    await cut_redis(runtime)
    response = await client.post(f"{ADMIN}/config/reload", headers=auth())
    await restore_redis(runtime)

    assert response.status_code == 503, response.text
    assert response.headers["Retry-After"] == "1"
    assert str(version_before) in response.json()["detail"]
    # Still serving the last good table — the outage cost freshness, not enforcement.
    assert runtime.tiers.snapshot().tiers["free"].rate_limit_per_min == FREE_RPM


# =============================================================================================
# GET /debug/memory
# =============================================================================================


async def test_debug_memory_reports_a_plausible_rss(admin):
    """C14's memory gate reads ``rss_mb``, so it has to be a real number about *this* process."""
    client, runtime = admin
    response = await client.get(f"{ADMIN}/debug/memory", headers=auth())
    assert response.status_code == 200, response.text
    body = response.json()

    assert body["pid"] > 0
    assert body["served_by"] == SERVED_BY
    # A Python process with FastAPI, redis-py and pydantic loaded is tens of MiB; anything under
    # 1 MiB or over 4 GiB is a unit error rather than a measurement.
    assert 1.0 < body["rss_mb"] < 4096.0
    assert body["rss_bytes"] == pytest.approx(body["rss_mb"] * 1024 * 1024, rel=1e-3)
    assert body["vms_mb"] >= body["rss_mb"]
    assert body["num_threads"] >= 1
    assert body["num_fds"] >= 1
    assert body["uptime_sec"] >= 0.0
    # `+ 0.01` for the millisecond rounding on the wire, which can round *up* past the live value.
    assert body["uptime_sec"] <= runtime.uptime_sec + 0.01


# =============================================================================================
# The exemption itself
# =============================================================================================


async def test_admin_answers_while_a_principal_is_completely_rate_limited(admin):
    """**The reason the admin prefix is exempt at all.**

    If this surface were metered, then during exactly the incident where every request is 429ing,
    the call that would fix it would 429 too — the control plane locked behind the failure it
    controls. The premise is asserted first: the principal really is being refused at the moment the
    admin calls are made.
    """
    client, runtime = admin
    tiny = 2
    assert (
        await client.put(
            f"{ADMIN}/tiers/free",
            json={"rate_limit_per_min": tiny, "burst": tiny},
            headers=auth(),
        )
    ).status_code == 200

    headers = bearer(runtime.settings, "throttled-user")
    allowed, refused = await drain(client, headers, attempts=tiny + 3)
    assert allowed == tiny and refused == 3

    # The premise: this principal is refused right now.
    assert (await client.get(WHOAMI, headers=headers)).status_code == 429

    for method, _template, path, body in ADMIN_ROUTES:
        response = await client.request(method, path, json=body, headers=auth())
        assert response.status_code == 200, f"{method} {path} was throttled: {response.text}"
        assert "X-RateLimit-Limit" not in response.headers, (
            f"{method} {path} was metered — an exempt path must carry no limit headers"
        )
        assert "Retry-After" not in response.headers


async def test_admin_routes_carry_no_rate_limit_headers_even_when_healthy(admin):
    """No gate was evaluated, so any number in those headers would be fiction."""
    client, _runtime = admin
    response = await client.get(f"{ADMIN}/tiers", headers=auth())
    assert response.status_code == 200
    for header in ("X-RateLimit-Limit", "X-RateLimit-Remaining", "X-Quota-Limit", "Retry-After"):
        assert header not in response.headers


async def test_the_admin_prefix_is_exempt_but_a_lookalike_path_is_not(admin):
    """``/api/v1/administrator`` must not inherit the exemption from a naming coincidence.

    ``startswith`` on a bare prefix would make it unmetered *and* unauthenticated. It is metered
    instead, so an anonymous caller gets the middleware's 401 rather than this router's.
    """
    client, _runtime = admin
    response = await client.get("/api/v1/administrator", headers=auth())
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"].startswith("Bearer")
    # The middleware's 401 envelope, not this router's — proof it never reached the admin router.
    assert response.json()["error"] == "Unauthorized"


async def test_the_tier_registry_seed_never_undoes_a_runtime_change(admin):
    """``HSETNX`` on startup is what makes the whole feature credible.

    With ``HSET``, an operator who lowers a limit at 14:00 gets it silently reverted the next time
    any replica restarts — a deploy, an OOM kill, a node drain. The change does not error and does
    not revert immediately: it reverts at some unrelated later moment, which is the most expensive
    shape a bug can have. Simulated here by re-running the seed a boot would run.
    """
    client, runtime = admin
    assert (
        await client.put(f"{ADMIN}/tiers/free", json={"rate_limit_per_min": 13}, headers=auth())
    ).status_code == 200

    await runtime.tiers.seed()  # exactly what another replica's start() does

    assert (await stored_tiers(runtime))["free"].split("|")[0] == "13"
    reloaded = await client.post(f"{ADMIN}/config/reload", headers=auth())
    assert reloaded.json()["tiers"]["free"]["rate_limit_per_min"] == 13


async def test_encode_tier_round_trips_through_the_admin_write(admin):
    """The stored form is the one :func:`src.tiers.decode_tier` reads back — asserted, not assumed."""
    client, runtime = admin
    response = await client.put(
        f"{ADMIN}/tiers/enterprise",
        json={"rate_limit_per_min": 2, "burst": 3, "daily_quota": 4, "monthly_quota": 5},
        headers=auth(),
    )
    assert response.status_code == 200, response.text

    stored = await stored_tiers(runtime)
    written = runtime.tiers.snapshot().tiers["enterprise"]
    assert stored["enterprise"] == encode_tier(written) == "2|3|4|5"
