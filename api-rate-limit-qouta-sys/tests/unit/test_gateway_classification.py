"""Unit tests for how :meth:`~src.redis_client.RedisGateway.run` CLASSIFIES a failure.

The gateway's single most consequential decision is which exceptions become
:class:`~src.redis_client.BackingStoreUnavailable`, because C8 catches that type and — under the
shipped ``FAIL_MODE=open`` — serves the request anyway. Anything wrongly classified as an outage
therefore does not merely log wrong: it **silently disables rate limiting for every request** and
reports on ``/health`` exactly as an unplugged Redis would.

So the rule is: *availability failures degrade; correctness failures raise.* A refused socket, a
hung socket, a server still ``LOADING`` — those are outages and they degrade. A ``ResponseError``
from a broken Lua script, a ``NOAUTH`` from a bad password, a ``WRONGTYPE`` from a key-schema bug —
those mean the store is up, answering, and telling us we are wrong. They propagate as themselves,
they never feed the circuit breaker, and upstream they become a 500: visible and attributable,
which is the correct outcome when the service is the thing that is broken.

The split follows what a failure *means*, not which class redis-py chose for it, and the tests below
pin it in both directions: ``AuthenticationError`` is one of redis-py's ``ConnectionError``\\ s and
is a bug, while ``ReadOnlyError`` is one of its ``ResponseError``\\ s and is an outage (a failover in
progress). Neither can be classified by inheritance alone, which is why ``run()`` spells its
``except`` clauses out in a specific order and why these tests exist to hold that order still.

Every failure here is injected into the coroutine factory, so none of this needs a server — which
is the point. The real-server behaviour (a genuinely refused port, a genuinely blackholed address)
is asserted in ``tests/integration/test_redis_gateway.py``; what is asserted *here* is the
classification itself, including the cases a healthy Redis will never produce on demand.
"""

from __future__ import annotations

import asyncio
import logging

import pytest
import redis.exceptions

from src.config import Settings
from src.redis_client import BackingStoreUnavailable, BreakerState, RedisGateway


class FakeClock:
    """A monotonic clock the test advances by hand, so the cooldown costs no wall time."""

    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _StubClient:
    """Stands in for the ``redis.asyncio.Redis`` object purely so ``_client is None`` is False.

    ``run()`` never calls a method on the client — it awaits whatever the caller's factory
    returns — so the tests below inject any failure without a server, a socket, or fakeredis.
    """


def _raises(exc: BaseException):
    """A coroutine factory whose operation fails with ``exc``."""

    async def _op() -> object:
        raise exc

    return _op


def _must_not_be_called():
    """A factory that fails the test if ``run()`` ever constructs the operation."""

    def _factory():
        raise AssertionError("run() built the operation when it should have refused first")

    return _factory


@pytest.fixture()
def gateway(settings: Settings) -> RedisGateway:
    """A gateway that believes it is connected, without ever opening a socket."""
    instance = RedisGateway(settings)
    instance._client = _StubClient()  # type: ignore[assignment]
    return instance


# --------------------------------------------------------------------------------------------
# Availability failures degrade
# --------------------------------------------------------------------------------------------


async def test_a_connection_error_is_classified_as_unavailable_and_records_a_breaker_failure(
    gateway: RedisGateway,
):
    """The store did not answer. This is the case fail-open exists for."""
    refused = redis.exceptions.ConnectionError("Error 111 connecting to redis:6379")

    with pytest.raises(BackingStoreUnavailable) as caught:
        await gateway.run(_raises(refused), op="ping")

    assert caught.value.op == "ping"
    assert isinstance(caught.value.__cause__, redis.exceptions.ConnectionError)
    assert gateway.errors == 1
    assert gateway.breaker.consecutive_failures == 1
    assert gateway.degraded_since is not None


async def test_a_readonly_reply_is_an_outage_and_records_a_breaker_failure(gateway: RedisGateway):
    """**The exception that proves the rule is about meaning, not about redis-py's class tree.**

    ``READONLY You can't write against a read only replica`` is a ``ResponseError`` subclass, and it
    is still an availability failure: it is what every client sees for the window around a failover,
    when the store is transiently unable to accept writes. That is a store we cannot use, not a
    script we got wrong, and degrading through it is the entire job of the fail-open path — raising
    a 500 instead would take the API down during precisely the event the limiter exists to survive.

    It therefore has to be caught BEFORE `CORRECTNESS_EXCEPTIONS`, since inheritance would otherwise
    put it there. This test is what pins that clause ordering.
    """
    failover = redis.exceptions.ReadOnlyError("READONLY You can't write against a read only replica")

    with pytest.raises(BackingStoreUnavailable) as caught:
        await gateway.run(_raises(failover), op="script:decide")

    assert caught.value.op == "script:decide"
    assert caught.value.__cause__ is failover
    # The breaker MUST learn from this one — an outage that does not open the breaker is an outage
    # every request pays the full timeout for.
    assert gateway.breaker.consecutive_failures == 1
    assert gateway.errors == 1
    assert gateway.degraded_since is not None


@pytest.mark.parametrize(
    "exc",
    [
        redis.exceptions.ConnectionError("connection reset by peer"),
        redis.exceptions.TimeoutError("Timeout connecting to server"),
        redis.exceptions.BusyLoadingError("LOADING Redis is loading the dataset in memory"),
        redis.exceptions.ReadOnlyError("READONLY You can't write against a read only replica"),
        asyncio.TimeoutError("a hang redis-py did not wrap"),
        OSError("name resolution failed"),
    ],
    ids=["connection", "timeout", "loading", "readonly", "asyncio-timeout", "oserror"],
)
async def test_every_availability_failure_becomes_the_one_classified_type(
    gateway: RedisGateway, exc: BaseException
):
    """One exception type upstream, so no caller can enumerate the failure modes incompletely.

    ``BusyLoadingError`` and ``ReadOnlyError`` are in here rather than with the correctness family
    on purpose: in both, the server is up and talking but is not able to serve us — a dataset still
    loading, a replica that cannot take writes. That is unavailability, not a bug in this service,
    and waiting is exactly the right response to it.
    """
    with pytest.raises(BackingStoreUnavailable):
        await gateway.run(_raises(exc), op="probe")

    assert gateway.breaker.consecutive_failures == 1


# --------------------------------------------------------------------------------------------
# Correctness failures raise
# --------------------------------------------------------------------------------------------


async def test_a_response_error_propagates_as_itself_and_is_never_an_outage(gateway: RedisGateway):
    """**The headline.** A broken Lua script is a bug in this service, not a Redis outage.

    ``ResponseError`` is what a compile error or a runtime error in the C4 decision script raises.
    Classified as ``BackingStoreUnavailable`` it would hit C8's fail-open path and turn a
    one-character typo into "rate limiting is off, for everyone, silently" — indistinguishable on
    ``/health`` from an unplugged Redis. Propagated as itself it becomes a 500: still an incident,
    but a *visible* one that names the operation that broke.
    """
    boom = redis.exceptions.ResponseError("Error compiling script: unexpected symbol near 'end'")

    with pytest.raises(redis.exceptions.ResponseError) as caught:
        await gateway.run(_raises(boom), op="script:decide")

    assert caught.value is boom
    assert not isinstance(caught.value, BackingStoreUnavailable)


async def test_a_response_error_records_no_breaker_failure(gateway: RedisGateway):
    """The breaker must learn nothing from it, and this is not a detail.

    A broken script fails on *every* request, so feeding these to the breaker would trip it within
    milliseconds and convert a permanent, fixable bug into a "degradation" that looks like the
    system working as designed. Nothing here may move: not the failure run, not the state, and not
    the degraded marker — the store is not degraded, we are.
    """
    for _ in range(50):
        with pytest.raises(redis.exceptions.ResponseError):
            await gateway.run(
                _raises(redis.exceptions.ResponseError("WRONGTYPE Operation against a key")),
                op="script:decide",
            )

    assert gateway.breaker.consecutive_failures == 0
    assert gateway.breaker.state is BreakerState.CLOSED
    assert gateway.breaker.is_open is False
    assert gateway.degraded_since is None
    # Counted, though: /health must still show that something is failing 50 times over.
    assert gateway.errors == 50


@pytest.mark.parametrize(
    "exc",
    [
        redis.exceptions.ResponseError("WRONGTYPE Operation against a key holding the wrong kind"),
        redis.exceptions.AuthenticationError("NOAUTH Authentication required"),
        redis.exceptions.AuthorizationError("WRONGPASS invalid username-password pair"),
        redis.exceptions.NoPermissionError("NOPERM this user has no permissions to run 'eval'"),
        redis.exceptions.DataError("Invalid input of type: 'object'"),
    ],
    ids=["wrongtype", "noauth", "wrongpass", "noperm", "dataerror"],
)
async def test_every_correctness_failure_propagates_untouched(
    gateway: RedisGateway, exc: BaseException
):
    """A misconfigured password must not read as an outage — and redis-py makes that easy to get
    wrong.

    ``AuthenticationError`` and ``AuthorizationError`` are subclasses of redis-py's *own*
    ``ConnectionError``, so an ``except`` ordered the other way round would classify ``NOAUTH`` and
    ``WRONGPASS`` as unavailability and fail open on a typo'd password — for every request, on
    every replica, until someone read the logs. This test is what pins the ordering.
    """
    with pytest.raises(type(exc)) as caught:
        await gateway.run(_raises(exc), op="script:decide")

    assert caught.value is exc
    assert not isinstance(caught.value, BackingStoreUnavailable)
    assert gateway.breaker.consecutive_failures == 0
    assert gateway.degraded_since is None


async def test_a_correctness_failure_is_logged_loudly_with_the_operation_name(
    gateway: RedisGateway, caplog: pytest.LogCaptureFixture
):
    """ERROR, not WARNING, and it names the operation.

    Upstream this is a 500 with no detail (deliberately — a client is never told about Lua), so
    this log line is the only place the cause is written down. At WARNING it would sit below the
    default threshold of most aggregators' alert rules, which for a bug that breaks every request
    is the difference between minutes and days.
    """
    with caplog.at_level(logging.DEBUG, logger="src.redis_client"):
        with pytest.raises(redis.exceptions.ResponseError):
            await gateway.run(
                _raises(redis.exceptions.ResponseError("Error running script")),
                op="script:decide",
            )

    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert len(errors) == 1
    assert "script:decide" in errors[0].getMessage()
    # A traceback, so the failing line is identifiable without reproducing it.
    assert errors[0].exc_info is not None


# --------------------------------------------------------------------------------------------
# The breaker must not be able to wedge in HALF_OPEN
# --------------------------------------------------------------------------------------------


def _open_the_breaker(gateway: RedisGateway, settings: Settings) -> None:
    """Trip the breaker without touching Redis."""
    for _ in range(settings.breaker_failures):
        gateway.breaker.record_failure()
    assert gateway.breaker.is_open is True


async def test_a_missing_client_does_not_consume_the_half_open_probe(settings: Settings):
    """``_client is None`` is checked BEFORE ``allow_request()``, and the order is the bug fix.

    ``allow_request()`` is a withdrawal, not a query: at the cooldown boundary it moves the breaker
    OPEN -> HALF_OPEN and hands out the single probe. If ``run()`` consumes that probe and then
    raises without a matching ``record_success()``/``record_failure()``, the probe never reports
    back — and HALF_OPEN refuses everyone, forever, with no timer left to rescue it.
    """
    clock = FakeClock()
    gateway = RedisGateway(settings, clock=clock)
    _open_the_breaker(gateway, settings)
    clock.advance(settings.breaker_cooldown_sec)  # the probe is now available

    with pytest.raises(BackingStoreUnavailable, match="never awaited"):
        await gateway.run(_must_not_be_called(), op="ping")

    # Still OPEN: the probe was NOT handed to a call that could never resolve it.
    assert gateway.breaker.state is BreakerState.OPEN
    assert gateway.short_circuits == 0
    assert gateway.errors == 1

    # And it is still there for a caller that can actually use it.
    assert gateway.breaker.allow_request() is True
    assert gateway.breaker.state is BreakerState.HALF_OPEN
    gateway.breaker.record_success()
    assert gateway.breaker.state is BreakerState.CLOSED


async def test_a_half_open_breaker_is_still_usable_after_a_missing_client(settings: Settings):
    """The wedge, asserted from the other side: HALF_OPEN must remain resolvable.

    Before the fix this combination left the breaker refusing every request for the rest of the
    process's life — not for a cooldown, but permanently, because nothing but a probe's verdict can
    leave HALF_OPEN and the probe had been spent on a call that never made one. The clock is
    advanced by a simulated 10,000 s afterwards to show the difference between "wedged" and merely
    "waiting".
    """
    clock = FakeClock()
    gateway = RedisGateway(settings, clock=clock)
    _open_the_breaker(gateway, settings)
    clock.advance(settings.breaker_cooldown_sec)
    assert gateway.breaker.allow_request() is True  # a real probe, outstanding
    assert gateway.breaker.state is BreakerState.HALF_OPEN

    with pytest.raises(BackingStoreUnavailable, match="never awaited"):
        await gateway.run(_must_not_be_called(), op="ping")

    # The outstanding probe is untouched, so its verdict still closes the breaker.
    assert gateway.breaker.state is BreakerState.HALF_OPEN
    gateway.breaker.record_success()

    clock.advance(10_000.0)
    assert gateway.breaker.state is BreakerState.CLOSED
    assert all(gateway.breaker.allow_request() for _ in range(100))


async def test_an_open_breaker_still_short_circuits_when_the_client_exists(
    gateway: RedisGateway, settings: Settings
):
    """Reordering the two checks must not cost the free refusal the breaker exists to provide."""
    _open_the_breaker(gateway, settings)

    with pytest.raises(BackingStoreUnavailable, match="circuit breaker is open"):
        await gateway.run(_must_not_be_called(), op="ping")

    assert gateway.short_circuits == 1
    assert gateway.calls == 0
