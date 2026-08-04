"""Pool exhaustion is not a store outage — the C4/C5 verification note, turned into assertions.

The defect this file exists to prevent is precise and was measured twice. 200 concurrent calls
against ``REDIS_MAX_CONNECTIONS=32`` raise ``ConnectionError('Too many connections')``, which C2
classifies into :class:`~src.redis_client.BackingStoreUnavailable`, which C8 would fail **open**.
At the project's 1000 rps target that means a traffic burst *silently unmeters itself*: load
removes the limiter at exactly the moment it matters most, and ``/health`` reports a Redis outage
for a Redis that is answering every other client perfectly.

C5's verification made it worse by showing the vector is reachable **pre-auth** — identity
resolution runs before the limiter, so 200 distinct unknown ``X-API-Key`` values were enough to
produce 168 errors and leave the shared circuit breaker OPEN, from a caller holding no credential.

Four properties, four sections:

1. it is **classified distinctly** (:class:`~src.redis_client.BackingStoreOverloaded`), and the
   message markers are pinned against the *installed* redis-py by provoking a real exhaustion
   rather than by trusting a string;
2. it does **not** trip the circuit breaker — opening it would take the limiter out because the
   pool was busy, converting a momentary burst into a real unmetered window;
3. it has its **own counter** and its own ``/health`` signal, separate from ``degraded``;
4. the pre-auth identity path is **bounded**, so unauthenticated traffic cannot take the pool the
   limiter needs.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest
import redis.asyncio
import redis.exceptions

from src.api.health import POOL_OK, POOL_SATURATED, RATE_LIMITER_ACTIVE, REDIS_SATURATED
from src.config import Settings
from src.identity import (
    IDENTITY_POOL_SHARE,
    IdentityResolver,
    identity_concurrency,
)
from src.limiter import Limiter
from src.models import DenyReason
from src.redis_client import (
    POOL_EXHAUSTION_MARKERS,
    BackingStoreOverloaded,
    BackingStoreUnavailable,
    BreakerState,
    RedisGateway,
    is_pool_exhaustion,
)
from src.tiers import _build_snapshot

USER = "alice"
ENDPOINT = "GET:/api/v1/whoami"
MOMENT = datetime(2026, 8, 10, 13, 45, 30, tzinfo=timezone.utc)


@pytest.fixture()
async def gateway(settings: Settings):
    """A **connected** :class:`~src.redis_client.RedisGateway` that never opens a socket.

    ``connect()`` builds the pool and dials nothing until the first command, so a real gateway —
    with its real ``run()`` classification — is available without a server. Every test below
    injects its failure as the operation itself, so no command is ever issued.
    """
    instance = RedisGateway(settings)
    await instance.connect()
    try:
        yield instance
    finally:
        await instance.aclose()


class StubTiers:
    def __init__(self, settings: Settings) -> None:
        self._snapshot = _build_snapshot(settings.tier_limits, version=1, fetched_monotonic=0.0)

    def snapshot(self):  # noqa: ANN201 - the private _Snapshot type is not exported
        return self._snapshot


class RaisingGateway:
    """The three limiter-facing gateway methods, with a fixed exception on the script call."""

    def __init__(self, error: BaseException) -> None:
        self.scripts: dict[str, str] = {}
        self.error = error

    def register(self, name: str, body: str) -> str:
        self.scripts[name] = body
        return body

    def script(self, name: str) -> str:
        try:
            return self.scripts[name]
        except KeyError:
            raise KeyError(f"lua script {name!r} was never registered") from None

    async def run_script(self, name: str, keys: list[str], args: list[str]) -> list[Any]:
        raise self.error


# =============================================================================================
# 1. Classification
# =============================================================================================


def test_the_exhaustion_markers_match_the_installed_redis_py():
    """**Pinned against the library, not against a docstring.**

    Both messages are produced by provoking the real conditions: the default ``ConnectionPool``
    refuses the ``max_connections + 1``-th caller instantly, and ``BlockingConnectionPool`` raises
    after its wait budget. Message matching is the only signal redis-py offers — it raises a plain
    ``ConnectionError`` for both — so the day a reword lands, this test fails loudly instead of the
    classification silently reverting to "outage" and the limiter silently failing open again.
    """
    pool = redis.asyncio.ConnectionPool(max_connections=1)
    pool.get_available_connection()
    with pytest.raises(redis.exceptions.ConnectionError) as refused:
        pool.get_available_connection()

    assert is_pool_exhaustion(refused.value)
    assert any(marker in str(refused.value).lower() for marker in POOL_EXHAUSTION_MARKERS)


async def test_a_blocking_pool_that_times_out_is_also_recognised():
    """The pool this service actually builds, exercised end to end against a real (empty) budget.

    ``timeout=0`` makes the wait expire immediately, which is the same code path a saturated pool
    reaches after 50 ms — without spending 50 ms of test time to get there.
    """
    pool = redis.asyncio.BlockingConnectionPool.from_url(
        "redis://127.0.0.1:6390/0", max_connections=1, timeout=0
    )
    held = pool.get_available_connection()
    assert held is not None

    with pytest.raises(redis.exceptions.ConnectionError) as exhausted:
        await pool.get_connection("PING")

    assert is_pool_exhaustion(exhausted.value)


@pytest.mark.parametrize(
    "error",
    [
        redis.exceptions.ConnectionError("Connection closed by server"),
        redis.exceptions.TimeoutError("Timeout reading from socket"),
        redis.exceptions.BusyLoadingError("LOADING Redis is loading the dataset in memory"),
        redis.exceptions.ReadOnlyError("READONLY You can't write against a read only replica"),
        redis.exceptions.ResponseError("Error compiling script"),
        redis.exceptions.AuthenticationError("WRONGPASS invalid username-password pair"),
        OSError("Name or service not known"),
        asyncio.TimeoutError("timed out"),
    ],
    ids=[
        "closed",
        "timeout",
        "loading",
        "readonly",
        "script-bug",
        "wrongpass",
        "dns",
        "asyncio-timeout",
    ],
)
def test_nothing_else_is_mistaken_for_pool_exhaustion(error):
    """**The narrowness is the safety.** A false positive here refuses a request that should degrade.

    A timeout in particular is deliberately excluded even though it is also a ``ConnectionError``
    relative: it means a socket we *held* stopped answering, which is the store's problem, while
    exhaustion means we never got a socket at all. ``AuthenticationError`` is included because it
    subclasses ``ConnectionError`` in redis-py and would be the one plausible way a correctness
    failure could leak into this branch.
    """
    assert is_pool_exhaustion(error) is False


async def test_the_gateway_raises_the_distinct_type(gateway: RedisGateway):
    """The classification reaches callers as a type, not as a flag they have to remember to read."""

    async def exhausted() -> None:
        raise redis.exceptions.ConnectionError("No connection available.")

    with pytest.raises(BackingStoreOverloaded) as raised:
        await gateway.run(exhausted, op="script:rlq")

    assert raised.value.op == "script:rlq"
    # ...and it is STILL a BackingStoreUnavailable, so the callers that only ask "did I get an
    # answer?" — the tier registry serving stale, the identity seed, /health — inherit their
    # existing behaviour unchanged rather than needing a second `except` each.
    assert isinstance(raised.value, BackingStoreUnavailable)


async def test_the_gateway_still_classifies_a_real_outage_as_an_outage(gateway: RedisGateway):
    """The existing availability/correctness split is **extended**, not changed.

    An unrecognised ``ConnectionError`` is exactly what it was before C8: an outage that degrades.
    That is also the safe failure direction if redis-py ever rewords its exhaustion messages.
    """

    async def refused() -> None:
        raise redis.exceptions.ConnectionError("Error 111 connecting to redis:6379. Refused.")

    with pytest.raises(BackingStoreUnavailable) as raised:
        await gateway.run(refused, op="script:rlq")

    assert not isinstance(raised.value, BackingStoreOverloaded)
    assert gateway.degraded_since is not None


# =============================================================================================
# 2. It must NOT trip the breaker
# =============================================================================================


async def test_pool_exhaustion_never_trips_the_circuit_breaker(
    gateway: RedisGateway, settings: Settings
):
    """**The single most important assertion in this file.**

    Opening the breaker on a saturated pool would refuse every *subsequent* call without touching
    Redis — and the limiter serves those through the local fallback. A momentary burst against a
    perfectly healthy store would therefore buy itself a full ``BREAKER_COOLDOWN_SEC`` of genuinely
    unmetered traffic, on every replica at once, for as long as the load lasted. The breaker exists
    to stop us dialling a store that is not answering; this store is answering and we never dialled.
    """

    async def exhausted() -> None:
        raise redis.exceptions.ConnectionError("Too many connections")

    for _ in range(settings.breaker_failures * 4):
        with pytest.raises(BackingStoreOverloaded):
            await gateway.run(exhausted, op="script:rlq")

    assert gateway.breaker.state is BreakerState.CLOSED
    assert gateway.breaker.consecutive_failures == 0
    assert gateway.short_circuits == 0
    # And the store is NOT reported as degraded, because the store is fine.
    assert gateway.degraded_since is None


async def test_an_overload_does_not_mask_a_real_outage_that_follows(gateway: RedisGateway):
    """Saturation leaves the breaker untouched; a genuine failure right after it still counts.

    The two bookkeeping paths are independent rather than one suppressing the other — otherwise a
    burst would buy an outage several requests of invisibility.
    """

    async def exhausted() -> None:
        raise redis.exceptions.ConnectionError("No connection available.")

    async def refused() -> None:
        raise redis.exceptions.ConnectionError("Connection refused")

    with pytest.raises(BackingStoreOverloaded):
        await gateway.run(exhausted, op="probe")
    with pytest.raises(BackingStoreUnavailable):
        await gateway.run(refused, op="probe")

    assert gateway.breaker.consecutive_failures == 1
    assert gateway.overloads == 1
    assert gateway.degraded_since is not None


# =============================================================================================
# 3. Its own counter, and its own /health signal
# =============================================================================================


async def test_the_overload_counter_increments_and_clears_on_success(gateway: RedisGateway):
    """A counter distinct from `errors`' outage share, because the two have opposite remedies.

    "Add connections / shed load" and "fix or wait out the store" are different actions, and a
    dashboard showing one number cannot tell an operator which one they are looking at.
    """

    async def exhausted() -> None:
        raise redis.exceptions.ConnectionError("No connection available.")

    async def fine() -> str:
        return "PONG"

    for _ in range(3):
        with pytest.raises(BackingStoreOverloaded):
            await gateway.run(exhausted, op="script:rlq")

    assert gateway.overloads == 3
    assert gateway.is_overloaded is True
    stats = gateway.stats()
    assert stats["overloads"] == 3
    assert stats["overloaded_for_sec"] >= 0.0
    # `errors` counts calls that did not return, so it includes these — `calls - errors` has to stay
    # "successful calls" or it stops meaning anything.
    assert stats["errors"] == 3
    # ...and the store was never blamed.
    assert stats["degraded_for_sec"] is None

    assert await gateway.run(fine, op="ping") == "PONG"

    # Getting a connection at all IS the evidence that the pool has room.
    assert gateway.is_overloaded is False
    assert gateway.overloads == 3  # a lifetime total, not a gauge that erases its own history


async def test_health_reports_saturation_on_its_own_field_and_stays_green(settings: Settings):
    """``pool: "saturated"`` while ``status`` stays healthy, ``rate_limiter`` stays active, 200.

    Three separate answers to three separate questions. Folding saturation into ``rate_limiter``
    would claim a degradation that is not happening (nothing is being served from the fallback —
    these requests are *refused*), and folding it into ``redis`` would blame a store that never saw
    a packet.
    """
    from fastapi.testclient import TestClient

    from src.main import Runtime, create_app

    runtime = Runtime.build(settings)
    runtime.redis.overloads = 4
    runtime.redis.overloaded_since = 1.0

    response = TestClient(create_app(runtime=runtime)).get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["pool"] == POOL_SATURATED
    assert body["status"] == "healthy"
    assert body["rate_limiter"] == RATE_LIMITER_ACTIVE


async def test_health_says_unknown_rather_than_unreachable_when_the_probe_cannot_get_a_connection(
    settings: Settings,
):
    """**`redis: "saturated"`, never `"unreachable"`.**

    When this process has no pooled connection the probe's own PING cannot get one either.
    Reporting that as ``unreachable`` would send an operator to debug a Redis that is answering
    every other client perfectly — the misdiagnosis C4's verification named. We could not ask, so
    we say we do not know.
    """
    import dataclasses

    from fastapi.testclient import TestClient

    from src.main import Runtime, create_app

    class SaturatedGateway:
        is_overloaded = True

        async def ping(self) -> bool:
            raise BackingStoreOverloaded("script:rlq: no connection available", op="ping")

    runtime = dataclasses.replace(Runtime.build(settings), redis=SaturatedGateway())
    response = TestClient(create_app(runtime=runtime)).get("/health")

    assert response.status_code == 200
    assert response.json()["redis"] == REDIS_SATURATED
    assert response.json()["pool"] == POOL_SATURATED


def test_a_healthy_replica_reports_an_ok_pool(client):
    assert client.get("/health").json()["pool"] == POOL_OK


# =============================================================================================
# 4. The limiter refuses rather than failing open
# =============================================================================================


@pytest.mark.parametrize("mode", ["open", "closed"])
async def test_the_limiter_refuses_a_saturated_pool_in_either_fail_mode(
    settings: Settings, mode: str
):
    """**`FAIL_MODE` deliberately gets no vote here**, and that is the fix.

    Under `FAIL_MODE=open` the fallback would serve the request, which is precisely the "load
    removes the limiter" defect: the busier the service gets, the less of it is metered. The store
    is healthy — the honest answer is 503 and a retry, not an unmetered 200.
    """
    tuned = settings.model_copy(update={"fail_mode": mode})
    gateway = RaisingGateway(
        BackingStoreOverloaded("script:rlq: no connection available", op="script:rlq")
    )
    limiter = Limiter(gateway, StubTiers(tuned), tuned)  # type: ignore[arg-type]

    verdict = await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert verdict.allowed is False
    assert verdict.reason is DenyReason.BACKING_STORE
    # NOT degraded: nothing was served from the fallback, and the store is not the problem.
    assert verdict.degraded is False
    assert limiter.degraded is False
    assert limiter.overload_denials == 1
    assert limiter.degraded_checks == 0
    assert limiter.fail_closed_denials == 0
    # Contention clears in milliseconds, so the honest advice is "come back immediately" — and 1 is
    # the smallest value RFC 9110 lets us say that with.
    assert verdict.retry_after_sec == 1


async def test_the_saturated_refusal_leaves_no_local_bucket_behind(settings: Settings):
    """No allowance was spent, so no local state is created — the refusal costs nothing to hold."""
    from src.fallback import LocalBucketCache

    fallback = LocalBucketCache(settings)
    gateway = RaisingGateway(BackingStoreOverloaded("no connection available", op="script:rlq"))
    limiter = Limiter(
        gateway, StubTiers(settings), settings, fallback=fallback  # type: ignore[arg-type]
    )

    await limiter.check(USER, ENDPOINT, 1, now=MOMENT)

    assert len(fallback) == 0


# =============================================================================================
# 5. The pre-auth identity path is bounded
# =============================================================================================


def test_the_identity_bound_is_derived_from_the_pool(settings: Settings):
    """Well under ``REDIS_MAX_CONNECTIONS``, and derived so the two cannot drift apart.

    The invariant that matters is relative. Two independent settings is how an operator raising the
    pool ends up with an identity bound that no longer bounds anything — or lowering it ends up
    with a bound larger than the pool, which is no bound at all.
    """
    assert identity_concurrency(settings) == 32 // IDENTITY_POOL_SHARE == 8
    assert identity_concurrency(settings) < settings.redis_max_connections
    assert identity_concurrency(settings.model_copy(update={"redis_max_connections": 64})) == 16
    # Floored at 1: a semaphore of zero permits would deadlock every authenticated request in the
    # process — a config typo turning into a total outage on the path everything else waits behind.
    assert identity_concurrency(settings.model_copy(update={"redis_max_connections": 1})) == 1


class BlockingGateway:
    """A gateway whose lookups park until released, so concurrency is observable rather than timed.

    ``peak`` is the assertion: it records the maximum number of lookups that were simultaneously
    inside ``run``, which is exactly the number of pooled connections the identity path could be
    holding at that instant.
    """

    def __init__(self) -> None:
        self.gate = asyncio.Event()
        self.in_flight = 0
        self.peak = 0
        self.started = asyncio.Event()

    async def run(self, factory, *, op: str):
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        self.started.set()
        try:
            await self.gate.wait()
            return {}
        finally:
            self.in_flight -= 1

    class _Client:
        def hgetall(self, key: str) -> dict[bytes, bytes]:  # pragma: no cover - never awaited
            return {}

    client = _Client()


async def test_the_identity_semaphore_bounds_concurrent_lookups(settings: Settings):
    """**The pre-auth exhaustion fix, asserted.**

    C5's verification measured 200 concurrent distinct unknown keys taking 168 errors and leaving
    the shared breaker OPEN — from a caller holding no credential, because identity resolution runs
    before the limiter and shares the limiter's pool. With the bound in place the same flood can
    occupy at most ``REDIS_MAX_CONNECTIONS // 4`` connections; the rest queue, and the limiter
    keeps the connections the *authenticated* callers need.

    Distinct keys on purpose: enumeration never repeats a digest, so the negative cache absorbs
    none of it and every one of these is a genuine round trip.
    """
    gateway = BlockingGateway()
    resolver = IdentityResolver(gateway, settings, max_concurrency=3)  # type: ignore[arg-type]

    flood = [
        asyncio.create_task(resolver.resolve([(b"x-api-key", f"guess-{index}".encode())]))
        for index in range(50)
    ]
    await gateway.started.wait()
    await asyncio.sleep(0)  # let every task reach the semaphore
    await asyncio.sleep(0)

    assert gateway.in_flight <= 3
    gateway.gate.set()
    await asyncio.gather(*flood)

    assert gateway.peak == 3
    assert resolver.peak_in_flight == 3
    assert resolver.gate_waits > 0
    assert resolver.cache_stats()["max_concurrency"] == 3


async def test_a_permit_is_released_even_when_the_lookup_fails(settings: Settings):
    """A leaked permit is permanent, and would shrink the bound by one on every outage.

    Left unreleased, the identity path would serialise itself and then deadlock — long after, and
    nowhere near, the outage that caused it. The `finally` is what makes the bound survive the very
    condition it exists for.
    """

    class BrokenGateway:
        client = None

        async def run(self, factory, *, op: str):
            raise BackingStoreUnavailable("redis is down", op=op)

    resolver = IdentityResolver(BrokenGateway(), settings, max_concurrency=2)  # type: ignore[arg-type]

    for index in range(10):
        with pytest.raises(BackingStoreUnavailable):
            await resolver.resolve([(b"x-api-key", f"guess-{index}".encode())])

    # Ten failures later the bound is still the bound: a permit is free and a lookup can start.
    assert resolver._in_flight == 0


async def test_a_jwt_never_takes_an_identity_permit(settings: Settings):
    """The bound wraps the **lookup**, not `resolve` — which is why the JWT path stays independent.

    A Bearer token is one HMAC over bytes already in memory and touches no Redis. Bounding at the
    middleware level instead would make the credential form that needs no store queue behind the
    one that does, throwing away the carve-out that keeps JWT callers served through an outage.
    """
    from src.identity import issue_token

    gateway = BlockingGateway()
    resolver = IdentityResolver(gateway, settings, max_concurrency=1)  # type: ignore[arg-type]
    token = issue_token("bearer-user", settings=settings)

    principal = await resolver.resolve([(b"authorization", f"Bearer {token}".encode())])

    assert principal is not None
    assert principal.user_id == "bearer-user"
    assert gateway.peak == 0
    assert resolver.peak_in_flight == 0


async def test_a_cache_hit_never_takes_a_permit(settings: Settings):
    """The permit protects a pooled connection, and a cache hit uses none.

    Holding one across the cache read would shrink the effective bound for the overwhelming
    majority of real traffic, which is served from the cache and never touches Redis at all.
    """
    from src.identity import apikey_digest
    from src.keys import apikey_key

    class OneShotGateway:
        def __init__(self) -> None:
            self.reads = 0

            class _Client:
                async def hgetall(_self, key: str) -> dict[bytes, bytes]:
                    return {b"user_id": b"alice", b"status": b"active"}

            self.client = _Client()

        async def run(self, factory, *, op: str):
            self.reads += 1
            return await factory()

    gateway = OneShotGateway()
    resolver = IdentityResolver(gateway, settings, max_concurrency=1)  # type: ignore[arg-type]
    headers = [(b"x-api-key", b"repeated")]

    for _ in range(20):
        assert (await resolver.resolve(headers)) is not None

    assert gateway.reads == 1
    assert resolver.gate_waits == 0
    assert apikey_key(apikey_digest("repeated", pepper=settings.api_key_pepper))
