"""Integration tests for :class:`~src.redis_client.RedisGateway` against a REAL redis:7-alpine.

Four things can only be proved against a real server, and each has a test below:

1. **``register_script`` recovers from ``NOSCRIPT``.** ``test_a_registered_script_survives_a_script_flush``
   issues a real ``SCRIPT FLUSH`` and re-runs. Nothing else in the suite can tell a correct
   implementation from a hand-rolled ``SCRIPT LOAD`` + ``EVALSHA``, and the difference in production
   is "every request 500s from the instant of a failover until someone redeploys".
2. **The socket timeout is ours, not the kernel's.** A *blackholed* address — one that swallows the
   SYN rather than refusing it — fails at the configured 250 ms rather than at the kernel's default,
   which is the only shape of unreachability that can tell the two apart.
3. **Failures are classified.** A caller sees exactly one exception type, never a raw
   ``redis.exceptions.RedisError``.
4. **The breaker turns a repeated failure into a free one.** After the threshold, calls stop
   touching the socket entirely.

A handful of tests here need no server at all (URL redaction, teardown, the unregistered-script
name). They live in this file rather than in ``tests/unit/`` because they are assertions about
*this class*, and splitting one object's contract across two directories is how half of it stops
being maintained.
"""

from __future__ import annotations

import time

import httpx
import pytest
import redis.exceptions

from src.api.health import REDIS_OK, REDIS_UNREACHABLE, STATUS_HEALTHY
from src.config import Settings
from src.keys import bucket_key, daily_quota_key, day_expire_at
from src.main import Runtime, create_app
from src.redis_client import (
    BackingStoreUnavailable,
    BreakerState,
    RedisGateway,
    redact_redis_url,
)

#: The trivial script the gateway tests exercise. The real four-gate decision script is C4; this
#: one exists only to prove the *registration and dispatch* machinery, so it is deliberately the
#: smallest thing that can round-trip an argument.
ECHO_SCRIPT = "return ARGV[1]"

#: Second script, to prove a hash-tagged key from ``src.keys`` is a key a script can actually
#: operate on — braces and all.
SET_AND_GET_SCRIPT = """
redis.call('SET', KEYS[1], ARGV[1])
return redis.call('GET', KEYS[1])
"""

#: A port nothing listens on inside the test container. Connection is *refused* rather than
#: blackholed, which is the failure a stopped `redis` container produces (`make up`,
#: `docker compose stop redis`) and therefore the one worth testing.
DEAD_URL = "redis://127.0.0.1:6390/0"

#: A BLACKHOLED address: 192.0.2.1 is TEST-NET-1 (RFC 5737), reserved for documentation and
#: guaranteed never to be routed. The SYN is swallowed — no RST, no ICMP — so the connect hangs
#: until *someone's* timeout fires, and which timeout that is becomes measurable.
#:
#: This is the whole reason it exists alongside `DEAD_URL`. A closed local port answers with an
#: instant RST (measured: ~0.5 ms), so a timing assertion against it passes whether the configured
#: timeout is 250 ms, 75 s, or deleted outright — it proves the kernel refuses fast, not that we
#: time out fast. Only a blackhole makes `socket_connect_timeout` the thing under test.
BLACKHOLE_URL = "redis://192.0.2.1:6379/0"

#: Two-sided bounds on the blackholed connect, around the configured 250 ms.
#:
#: The FLOOR is the half people leave out, and it is what proves the configured timeout is actually
#: in force: anything faster than this did not wait for a timeout at all (a refusal, an
#: unreachable-network error, a stubbed-out client), which is exactly the regression that made the
#: old refused-port version of this test vacuous.
#:
#: The CEILING catches the regression in the other direction — `socket_connect_timeout` dropped or
#: overridden, leaving the kernel's ~60-120 s TCP default in charge. It is 6x the configured value
#: rather than 1.5x so a loaded CI box cannot flake it, and still three orders of magnitude below
#: the default it is there to catch.
TIMEOUT_FLOOR_SEC = 0.2
TIMEOUT_CEILING_SEC = 1.5

#: Ceiling for a *refused* connection, which returns immediately and never involves a timeout.
REFUSAL_CEILING_SEC = 0.5


@pytest.fixture()
async def dead_gateway(redis_url: str):
    """A connected gateway pointed at a port nothing is listening on.

    ``connect()`` succeeds because ``redis.asyncio.from_url`` is lazy — no socket is opened until
    the first command — which is itself part of the design: an unreachable Redis must not prevent
    the service from booting, because the service is built to serve (degraded) without it.
    """
    settings = Settings(_env_file=None, redis_url=DEAD_URL)
    instance = RedisGateway(settings)
    await instance.connect()
    try:
        yield instance
    finally:
        await instance.aclose()


@pytest.fixture()
async def blackhole_gateway():
    """A connected gateway pointed at an address that swallows packets instead of refusing them.

    Separate from ``dead_gateway`` because the two exercise different failure *shapes*: a refused
    port returns instantly and can say nothing about timeouts, while this one can only end when a
    timeout fires — so it is the only fixture that can prove whose timeout it was.
    """
    settings = Settings(_env_file=None, redis_url=BLACKHOLE_URL)
    instance = RedisGateway(settings)
    await instance.connect()
    try:
        yield instance
    finally:
        await instance.aclose()


# --------------------------------------------------------------------------------------------
# The happy path
# --------------------------------------------------------------------------------------------


async def test_connect_and_ping(gateway: RedisGateway):
    assert await gateway.ping() is True
    assert gateway.breaker.state is BreakerState.CLOSED
    assert gateway.errors == 0
    assert gateway.degraded_since is None
    assert gateway.calls >= 1


async def test_connect_is_idempotent(gateway: RedisGateway):
    """Called twice, it must not build a second client — one `Redis` per process, for its life."""
    first = gateway.client
    await gateway.connect()
    assert gateway.client is first


async def test_the_client_is_built_with_the_settings_we_chose(
    gateway: RedisGateway, redis_settings: Settings
):
    """The three constructor arguments that are decisions rather than defaults.

    ``retry_on_timeout`` is the one that matters most and is checked as "not enabled": the decision
    script is NOT idempotent (it spends a token and increments two quota counters), so an automatic
    retry after a timeout that Redis had already executed charges the caller twice for one request.
    Turning this on would look like a resilience improvement and would be a double-spend bug.
    """
    pool = gateway.client.connection_pool
    kwargs = pool.connection_kwargs
    expected_timeout = redis_settings.redis_timeout_ms / 1000

    # Subscripted, NOT `.get(..., False)`. A default makes the assertion pass just as happily when
    # the key is ABSENT, and absent is precisely the failure worth catching: redis-py 6.x removes
    # `retry_on_timeout` entirely and retries three times with jitter by default — i.e. the exact
    # double-charge this setting exists to prevent, arriving silently in a dependency bump. So
    # `redis==5.2.1` in requirements.txt is load-bearing here, and a KeyError on this line is the
    # signal that a bump needs the equivalent `retry=Retry(..., retries=0)` wiring instead.
    assert kwargs["retry_on_timeout"] is False
    assert kwargs.get("socket_timeout") == pytest.approx(expected_timeout)
    assert kwargs.get("socket_connect_timeout") == pytest.approx(expected_timeout)
    assert pool.max_connections == redis_settings.redis_max_connections


async def test_run_dispatches_an_arbitrary_command(gateway: RedisGateway):
    """`run()` is the only path to Redis, so it has to be able to carry any command."""
    await gateway.run(lambda: gateway.client.set("probe", b"value"), op="set")
    stored = await gateway.run(lambda: gateway.client.get("probe"), op="get")

    assert stored == b"value"
    assert gateway.errors == 0


# --------------------------------------------------------------------------------------------
# Scripts
# --------------------------------------------------------------------------------------------


async def test_a_registered_script_executes_and_returns_its_value(gateway: RedisGateway):
    gateway.register("echo", ECHO_SCRIPT)

    result = await gateway.run_script("echo", keys=[], args=["pong"])

    # decode_responses=False, so a Lua string reply arrives as bytes — which is what the C4
    # decision script's positional integer parsing expects too.
    assert result == b"pong"


async def test_a_script_operates_on_a_hash_tagged_key_from_the_key_schema(gateway: RedisGateway):
    """The braces in `rate_limit:{alice}:...` are structure Redis understands, not a placeholder."""
    gateway.register("setget", SET_AND_GET_SCRIPT)
    key = bucket_key("alice", "GET:/api/v1/logs/query")

    result = await gateway.run_script("setget", keys=[key], args=["42"])

    assert key == "rate_limit:{alice}:GET:/api/v1/logs/query"
    assert result == b"42"
    assert await gateway.client.get(key) == b"42"


async def test_a_registered_script_survives_a_script_flush(gateway: RedisGateway):
    """**The test that proves ``register_script`` was used rather than hand-rolled ``EVALSHA``.**

    Redis's script cache is volatile: it is empty after a restart, it is emptied by ``SCRIPT
    FLUSH`` (which plenty of runbooks and some ``FLUSHALL`` configurations perform), and — the case
    that actually bites — a replica promoted by a failover has never seen the script at all,
    because script loads are not replicated.

    redis-py's ``Script`` handle catches the resulting ``NOSCRIPT`` and transparently re-sends the
    body with ``EVAL``. A hand-rolled ``EVALSHA`` does not, so every request 500s from the instant
    of the failover until someone notices and redeploys. This test is the only thing in the suite
    that can tell the two apart.
    """
    gateway.register("echo", ECHO_SCRIPT)
    assert await gateway.run_script("echo", keys=[], args=["before"]) == b"before"

    await gateway.client.script_flush()

    assert await gateway.run_script("echo", keys=[], args=["after"]) == b"after"
    # And the recovery is transparent: nothing was classified as a failure, so the breaker has no
    # reason to open over a cache flush.
    assert gateway.errors == 0
    assert gateway.breaker.state is BreakerState.CLOSED


async def test_an_unregistered_script_name_is_a_KeyError_not_an_outage(gateway: RedisGateway):
    """A typo is a wiring bug in this process, not a store outage.

    Classifying it as `BackingStoreUnavailable` would make the limiter fail *open* on a typo — i.e.
    silently stop enforcing anything, which is the one failure mode this project exists to prevent.
    """
    with pytest.raises(KeyError, match="never registered"):
        await gateway.run_script("nope", keys=[], args=[])

    assert gateway.errors == 0


async def test_register_requires_a_connected_gateway(redis_settings: Settings):
    instance = RedisGateway(redis_settings)

    with pytest.raises(RuntimeError, match="connect"):
        instance.register("echo", ECHO_SCRIPT)


async def test_closing_drops_the_script_handles(redis_settings: Settings):
    """Each handle holds a reference to the client it was registered against, so a reconnect must
    re-register rather than dispatch onto a disconnected client.

    Owns its gateway rather than borrowing the ``gateway`` fixture's: that fixture promises a
    ``FLUSHDB`` on both ends, and a test that closes the connection underneath it would turn the
    promise into a conditional — which is how leftover keys start reaching the next test.
    """
    instance = RedisGateway(redis_settings)
    await instance.connect()
    instance.register("echo", ECHO_SCRIPT)

    await instance.aclose()

    with pytest.raises(KeyError):
        instance.script("echo")


# --------------------------------------------------------------------------------------------
# The key schema against a real server
# --------------------------------------------------------------------------------------------


async def test_a_quota_key_expireat_lands_in_the_future(gateway: RedisGateway):
    """`day_expire_at` has to produce a timestamp the SERVER agrees is still ahead of it.

    An `EXPIREAT` in the past deletes the key on the spot, which would hand the caller a fresh
    daily allowance every time the counter was created. Asserting `PTTL > 0` against a real Redis
    is the only way to see that; a unit test can only compare our arithmetic to itself.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    key = daily_quota_key("alice", now)

    await gateway.run(lambda: gateway.client.incr(key), op="incr")
    await gateway.run(lambda: gateway.client.expireat(key, day_expire_at(now)), op="expireat")

    ttl_ms = await gateway.run(lambda: gateway.client.pttl(key), op="pttl")
    assert ttl_ms > 0
    assert ttl_ms <= 86_400_000


# --------------------------------------------------------------------------------------------
# Failure classification
# --------------------------------------------------------------------------------------------


async def test_a_dead_port_raises_the_classified_exception_not_a_raw_redis_error(
    dead_gateway: RedisGateway,
):
    """One exception type upstream, so no caller can enumerate the failure modes incompletely."""
    with pytest.raises(BackingStoreUnavailable) as caught:
        await dead_gateway.ping()

    assert caught.value.op == "ping"
    assert not isinstance(caught.value, redis.exceptions.RedisError)
    assert dead_gateway.errors == 1
    assert dead_gateway.degraded_since is not None


async def test_a_blackholed_address_fails_within_the_configured_timeout(
    blackhole_gateway: RedisGateway, redis_settings: Settings
):
    """**The test that proves the timeout is OURS.** Two-sided bounds, and both are load-bearing.

    Without `socket_connect_timeout`/`socket_timeout`, a Redis that has stopped answering blocks
    every request on the kernel's default TCP timeout (~60-120 s). The whole rate-limit check has a
    5 ms budget, so a limiter that hangs is strictly worse for the caller than one that fails open.

    The address is 192.0.2.1 — TEST-NET-1 (RFC 5737), reserved for documentation and guaranteed
    unroutable — so the SYN is swallowed rather than refused and the connect can only end when a
    timeout fires. That choice is the entire point of this test:

    * **The floor (`> 0.2 s`)** proves the wait actually happened, i.e. that the 250 ms
      `socket_connect_timeout` is what ended it. Pointed at a merely *closed* port instead, this
      assertion would be satisfied by an instant kernel RST — which is why the earlier version of
      this test would have passed with `socket_connect_timeout` deleted outright, and therefore
      proved nothing at all.
    * **The ceiling (`< 1.5 s`)** catches the regression in the other direction: the setting
      dropped, and the kernel's minute-plus default left in charge.

    Together they pin the elapsed time to the configured value rather than to "fast enough", which
    is the only claim worth making here.
    """
    assert redis_settings.redis_timeout_ms == 250

    started = time.perf_counter()
    with pytest.raises(BackingStoreUnavailable):
        await blackhole_gateway.ping()
    elapsed = time.perf_counter() - started

    assert TIMEOUT_FLOOR_SEC < elapsed < TIMEOUT_CEILING_SEC, (
        f"connect to a blackholed address took {elapsed:.3f}s; expected it to be bounded by the "
        f"configured {redis_settings.redis_timeout_ms}ms timeout"
    )


async def test_a_refused_port_fails_instantly(dead_gateway: RedisGateway):
    """The other failure shape: a stopped `redis` container, which refuses rather than blackholes.

    Kept because it is the common case in practice (`docker compose stop redis`), and because
    stating its speed next to the test above is what documents *why* that one needed a different
    address: a refusal returns in well under a millisecond, so no timing assertion against it can
    say anything about the timeout configuration.
    """
    started = time.perf_counter()
    with pytest.raises(BackingStoreUnavailable):
        await dead_gateway.ping()
    elapsed = time.perf_counter() - started

    assert elapsed < REFUSAL_CEILING_SEC


async def test_a_broken_lua_script_raises_ResponseError_rather_than_faking_an_outage(
    gateway: RedisGateway,
):
    """**A script bug is not an outage, and only a real server can raise the real error.**

    The unit suite injects a ``ResponseError``; this raises one the way production would — a Lua
    body that does not compile, executed against a Redis that is up and answering. That is the
    exact failure a typo in the C4 decision script produces.

    Classified as ``BackingStoreUnavailable`` it would reach C8's fail-open path and silently
    disable rate limiting for every request, reported on ``/health`` identically to an unplugged
    Redis. It must instead propagate, become a 500, and leave the breaker alone — a script that
    fails on every request would otherwise open the breaker and dress a permanent bug up as a
    passing degradation.
    """
    gateway.register("broken", "this is not lua")

    with pytest.raises(redis.exceptions.ResponseError) as caught:
        await gateway.run_script("broken", keys=[], args=[])

    assert not isinstance(caught.value, BackingStoreUnavailable)
    assert gateway.breaker.state is BreakerState.CLOSED
    assert gateway.breaker.consecutive_failures == 0
    assert gateway.degraded_since is None
    # Still counted, so /health shows that something is failing even though nothing is degraded.
    assert gateway.errors == 1


async def test_a_wrongtype_is_a_key_schema_bug_not_an_outage(gateway: RedisGateway):
    """The same rule, reached without Lua: the store answered, and the answer was "you are wrong"."""
    await gateway.run(lambda: gateway.client.set("a-string", b"x"), op="set")

    with pytest.raises(redis.exceptions.ResponseError, match="WRONGTYPE"):
        await gateway.run(lambda: gateway.client.lpush("a-string", b"y"), op="lpush")

    assert gateway.breaker.state is BreakerState.CLOSED
    assert gateway.breaker.consecutive_failures == 0
    assert gateway.degraded_since is None


async def test_run_on_a_never_connected_gateway_is_unavailable_and_does_not_trip_the_breaker(
    redis_settings: Settings,
):
    """No socket exists, so this failure is already free and there is nothing to protect against.

    Tripping the breaker here would only mask a wiring bug behind a 5 s cooldown; the error counter
    still moves so `/health` shows it.
    """
    instance = RedisGateway(redis_settings)

    with pytest.raises(BackingStoreUnavailable, match="never awaited"):
        await instance.run(lambda: instance.client.ping(), op="ping")

    assert instance.errors == 1
    assert instance.breaker.state is BreakerState.CLOSED
    with pytest.raises(RuntimeError, match="connect"):
        _ = instance.client


# --------------------------------------------------------------------------------------------
# The breaker, end to end
# --------------------------------------------------------------------------------------------


async def test_the_breaker_opens_after_repeated_failures_and_then_costs_nothing(
    dead_gateway: RedisGateway, redis_settings: Settings
):
    """The point of the breaker, measured: after the threshold, the socket is never touched again.

    The timing assertion is the headline, but `short_circuits` is the assertion that cannot be
    satisfied by luck — it counts refusals that happened *before* any dialling, so it is direct
    evidence that the request never reached the network.
    """
    threshold = redis_settings.breaker_failures

    for _ in range(threshold):
        with pytest.raises(BackingStoreUnavailable):
            await dead_gateway.ping()

    assert dead_gateway.breaker.is_open is True
    assert dead_gateway.errors == threshold
    assert dead_gateway.short_circuits == 0

    started = time.perf_counter()
    for _ in range(20):
        with pytest.raises(BackingStoreUnavailable, match="circuit breaker is open"):
            await dead_gateway.ping()
    elapsed = time.perf_counter() - started

    # 20 refusals, all of them free. Without the breaker this would be 20 x the connect attempt.
    assert dead_gateway.short_circuits == 20
    assert dead_gateway.errors == threshold  # unchanged: nothing new was attempted
    assert elapsed < 0.5


async def test_a_healthy_gateway_reports_clean_stats(gateway: RedisGateway):
    await gateway.ping()

    stats = gateway.stats()

    assert stats["connected"] is True
    assert stats["errors"] == 0
    assert stats["short_circuits"] == 0
    assert stats["breaker_state"] == "closed"
    assert stats["degraded_for_sec"] is None


async def test_a_failing_gateway_reports_its_degradation(dead_gateway: RedisGateway):
    """A degradation nobody can see is a degradation nobody fixes."""
    with pytest.raises(BackingStoreUnavailable):
        await dead_gateway.ping()

    stats = dead_gateway.stats()

    assert stats["errors"] == 1
    assert stats["consecutive_failures"] == 1
    assert stats["degraded_for_sec"] is not None
    assert stats["degraded_for_sec"] >= 0.0


async def test_a_success_clears_the_degraded_marker(gateway: RedisGateway):
    """`degraded_since` marks the CURRENT failure run, so recovery has to reset it."""
    gateway.errors = 3
    gateway.degraded_since = time.monotonic()

    await gateway.ping()

    assert gateway.degraded_since is None
    assert gateway.breaker.state is BreakerState.CLOSED


# --------------------------------------------------------------------------------------------
# Teardown
# --------------------------------------------------------------------------------------------


class _ExplodingClient:
    """Stand-in whose ``aclose`` fails, to exercise the never-raise-from-teardown path."""

    async def aclose(self) -> None:
        raise redis.exceptions.ConnectionError("close failed")


async def test_aclose_is_idempotent(redis_settings: Settings):
    instance = RedisGateway(redis_settings)
    await instance.connect()

    await instance.aclose()
    await instance.aclose()

    assert instance.is_connected is False


async def test_aclose_never_raises_from_teardown(redis_settings: Settings):
    """Raising here turns a shutdown into a crash and, under compose, into a crash loop — while
    the process is exiting anyway and the kernel is about to reclaim the sockets regardless."""
    instance = RedisGateway(redis_settings)
    instance._client = _ExplodingClient()  # type: ignore[assignment]

    await instance.aclose()

    assert instance.is_connected is False


# --------------------------------------------------------------------------------------------
# URL redaction
# --------------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("redis://redis:6379/0", "redis://redis:6379/0"),
        ("redis://user:hunter2@redis:6379/0", "redis://redis:6379/0"),
        ("rediss://:pepper@cache.internal:6380/3", "rediss://cache.internal:6380/3"),
        ("redis://redis/0", "redis://redis/0"),  # no explicit port
        ("not-a-url", "<unparseable redis url>"),
        ("redis://redis:notaport/0", "<unparseable redis url>"),
    ],
)
def test_redact_redis_url_keeps_the_host_and_drops_the_credentials(url, expected):
    """The startup line is the most-copied text in an incident — it must not carry a password.

    Anything unparseable returns a fixed marker rather than the input: falling back to "log it raw"
    would leak precisely the credentials in the malformed URLs that branch exists to handle.
    """
    assert redact_redis_url(url) == expected


def test_redaction_is_what_the_startup_path_actually_uses(redis_settings: Settings):
    """Guards against the log line quietly reverting to `settings.redis_url`."""
    assert "hunter2" not in redact_redis_url("redis://user:hunter2@redis:6379/0")
    assert redact_redis_url(redis_settings.redis_url).startswith("redis")


# --------------------------------------------------------------------------------------------
# /health, wired to a real gateway
# --------------------------------------------------------------------------------------------


async def _get_health(runtime: Runtime) -> dict:
    """Drive ``GET /health`` against an app with an injected, already-started runtime."""
    app = create_app(runtime=runtime)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.get("/health")
    assert response.status_code == 200
    return response.json()


async def test_health_reports_redis_ok_when_the_gateway_is_connected(redis_settings: Settings):
    """The injected-runtime seam start/stopped by the TEST, since `create_app(runtime=...)` skips
    the lifespan that would otherwise do it. That responsibility split is documented on
    `create_app`; this is the test that exercises it."""
    runtime = Runtime.build(redis_settings)
    await runtime.start()
    try:
        body = await _get_health(runtime)
    finally:
        await runtime.stop()

    assert body["redis"] == REDIS_OK
    assert body["status"] == STATUS_HEALTHY


async def test_health_stays_healthy_while_redis_is_unreachable():
    """**The decision, asserted.** A Redis outage must not turn the liveness probe red.

    `/health` is what the container HEALTHCHECK, compose's `condition: service_healthy` and (C12)
    nginx's upstream check all read, and each treats a non-200 as "restart this replica". A replica
    that cannot reach Redis is still alive and, per C8's `FAIL_MODE=open`, still correctly serving
    through the bounded local fallback. Failing the probe would restart it — and would restart every
    other replica at the same moment, since they all share the one Redis that is down. A dependency
    failure the system was built to survive would become a total outage.

    So the outage is reported in its own field and nowhere else.
    """
    runtime = Runtime.build(Settings(_env_file=None, redis_url=DEAD_URL))
    await runtime.start()
    try:
        body = await _get_health(runtime)
    finally:
        await runtime.stop()

    assert body["redis"] == REDIS_UNREACHABLE
    assert body["status"] == STATUS_HEALTHY
    assert body["rate_limiter"] == "active"
