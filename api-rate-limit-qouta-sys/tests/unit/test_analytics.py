"""Unit tests for :class:`~src.analytics.AnalyticsCollector` — the write side and the read side.

These drive the collector against a **recording gateway stub** rather than a real Redis, and that
is what makes them unit tests rather than slow integration ones. Three properties are only
observable from this side of the boundary:

* **What commands were issued.** "The read side never ``SCAN``s" is a statement about the command
  stream, and a real Redis cannot be asked "did anyone scan you?" after the fact. The stub records
  every call, so the assertion is a direct read of the thing being claimed.
* **What exceptions were survived.** ``record`` must swallow every class of failure, including ones
  a real store cannot be made to produce on demand (a ``ResponseError`` from a broken script, a
  bare ``Exception`` from a bug in this module). Injecting them takes microseconds; provoking them
  takes a fixture that breaks Redis in five different ways.
* **Which clock a bucket came from.** Asserting that the index came from ``server_now_ms`` and not
  from ``time.time()`` needs the two to be far apart, which means choosing both — a real decision
  always carries a plausible one.

The things these deliberately do NOT prove are in ``tests/integration/test_analytics_redis.py``:
that ``EXPIRE ... NX`` really does not extend a TTL on a second write, that two collectors sum into
one bucket, and that the Lua script's arithmetic is what this module thinks it is. A stub cannot
answer any of those, because a stub is this module's own beliefs wearing a server's clothes.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
import redis.exceptions

from src.analytics import (
    ANALYTICS_POOL_SHARE_DIVISOR,
    ANONYMOUS_USER_ID,
    OUTCOME_ALLOWED,
    OUTCOME_DEGRADED,
    OUTCOME_DENIED,
    OUTCOMES,
    TOP_CONSUMERS_FANOUT,
    TOP_CONSUMERS_LIMIT,
    UNKNOWN_TIER,
    AnalyticsCollector,
    _count,
    _score,
    _text,
)
from src.config import Settings
from src.keys import (
    MS_PER_HOUR,
    MS_PER_MINUTE,
    hour_index,
    minute_index,
    recent_minute_indices,
    stats_hour_key,
    stats_minute_key,
    stats_top_key,
)
from src.lua import (
    RECORD_ARGV_ARITY,
    RECORD_ARGV_COST,
    RECORD_ARGV_ENDPOINT,
    RECORD_ARGV_HOUR_TTL_SEC,
    RECORD_ARGV_MINUTE_TTL_SEC,
    RECORD_ARGV_OUTCOME,
    RECORD_ARGV_STATUS,
    RECORD_ARGV_TIER,
    RECORD_ARGV_USER_ID,
    RECORD_KEYS_ARITY,
    RECORD_KEY_HOUR,
    RECORD_KEY_MINUTE,
    RECORD_KEY_TOP,
    RLQ_RECORD_REQUEST,
    RLQ_RECORD_REQUEST_NAME,
)
from src.models import DenyReason, LimitDecision, QuotaPeriodState
from src.redis_client import BackingStoreOverloaded, BackingStoreUnavailable

#: Every command a snapshot is allowed to issue. The point of the set is what is NOT in it.
ALLOWED_SNAPSHOT_COMMANDS = {"TIME", "HGETALL", "ZREVRANGE"}

#: The two commands whose absence is the property under test. ``SCAN MATCH stats:min:*`` walks the
#: entire keyspace — every ``rate_limit:*``, ``sw:*`` and ``quota:*`` key in the system — on a
#: single-threaded server, on the endpoint a dashboard polls every 5 seconds. ``KEYS`` does it
#: without even the courtesy of a cursor.
FORBIDDEN_COMMANDS = {"SCAN", "SCAN_ITER", "KEYS"}

#: A wall-clock instant with no relationship to the decision clock below, so a test can tell which
#: one a bucket index came from. 2026-08-05T12:00:00Z.
LOCAL_NOW_MS = 1_785_931_200_000

#: Far enough from :data:`LOCAL_NOW_MS` that they cannot share a minute *or* an hour bucket — a
#: skew this large is not realistic, which is exactly why it makes the assertion unambiguous.
DECISION_NOW_MS = LOCAL_NOW_MS - 9 * MS_PER_HOUR - 37 * MS_PER_MINUTE


# =============================================================================================
# Doubles
# =============================================================================================


def make_decision(**overrides: Any) -> LimitDecision:
    """A plausible allowed decision, with any field overridden.

    A local factory rather than an import from another test module: these tests care about four
    fields of a 24-field record, and reaching into another suite's fixtures couples two files that
    are about different things.
    """
    base: dict[str, Any] = {
        "allowed": True,
        "reason": DenyReason.NONE,
        "tier": "free",
        "user_id": "alice",
        "endpoint": "GET:/api/v1/whoami",
        "cost": 1,
        "bucket_limit": 60,
        "bucket_remaining": 41,
        "bucket_reset_sec": 3,
        "window_limit": 60,
        "window_used": 19,
        "window_reset_sec": 37,
        "daily_limit": 1000,
        "daily_used": 120,
        "daily_reset_at": 1_786_752_000,
        "daily_state": QuotaPeriodState.ACTIVE,
        "monthly_limit": 25_000,
        "monthly_used": 4_200,
        "monthly_reset_at": 1_788_220_800,
        "monthly_state": QuotaPeriodState.ACTIVE,
        "retry_after_sec": 0,
        "degraded": False,
        "server_now_ms": DECISION_NOW_MS,
        "latency_ms": 0.42,
    }
    base.update(overrides)
    return LimitDecision(**base)


class RecordingPipeline:
    """A stub redis-py async pipeline: buffers commands, answers them from the gateway's store.

    Command methods return ``self`` **synchronously**, exactly as redis-py's asyncio ``Pipeline``
    does (``pipeline_execute_command`` appends to the stack and returns the pipeline; only
    ``execute`` is a coroutine). A stub that made them awaitable would let production code that
    forgot an ``await`` pass here and fail against a real server — the stub would be testing a
    protocol nobody implements.
    """

    def __init__(self, gateway: RecordingGateway) -> None:
        self._gateway = gateway
        self.commands: list[tuple[Any, ...]] = []

    async def __aenter__(self) -> RecordingPipeline:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        self.reset_calls = getattr(self, "reset_calls", 0) + 1

    # -- buffered commands ------------------------------------------------------------------- #
    def hgetall(self, key: str) -> RecordingPipeline:
        self.commands.append(("HGETALL", key))
        return self

    def zrevrange(
        self, key: str, start: int, end: int, withscores: bool = False
    ) -> RecordingPipeline:
        self.commands.append(("ZREVRANGE", key, start, end, withscores))
        return self

    # -- the two that must never be reached --------------------------------------------------- #
    #
    # Recorded rather than raised, deliberately: a stub that exploded on `scan` would make the
    # "never scans" test assert that the STUB refuses to scan. Recording makes the assertion a
    # statement about the command stream the collector produced.
    def scan(self, *args: Any, **kwargs: Any) -> RecordingPipeline:  # pragma: no cover - never hit
        self.commands.append(("SCAN", *args))
        return self

    def keys(self, *args: Any, **kwargs: Any) -> RecordingPipeline:  # pragma: no cover - never hit
        self.commands.append(("KEYS", *args))
        return self

    async def execute(self) -> list[Any]:
        self._gateway.pipelines.append(list(self.commands))
        self._gateway.commands.extend(self.commands)
        return [self._gateway.answer(command) for command in self.commands]


class RecordingClient:
    """The ``gateway.client`` surface the read side touches: ``pipeline()`` and ``time()``.

    ``time()`` is here because the read side names its window from **Redis's** clock rather than
    from ``time.time()``. A stub that did not offer it would let a regression to the local clock
    pass silently, which is the one thing this surface exists to make impossible.
    """

    def __init__(self, gateway: RecordingGateway) -> None:
        self._gateway = gateway

    async def time(self) -> tuple[int, int]:
        self._gateway.commands.append(("TIME",))
        return self._gateway.now_ms // 1000, (self._gateway.now_ms % 1000) * 1000

    def pipeline(self, transaction: bool = True) -> RecordingPipeline:
        self._gateway.pipeline_transactions.append(transaction)
        return RecordingPipeline(self._gateway)

    def scan_iter(self, *args: Any, **kwargs: Any) -> Any:  # pragma: no cover - never hit
        self._gateway.commands.append(("SCAN_ITER", *args))
        raise AssertionError("the analytics read side must never scan the keyspace")


class RecordingGateway:
    """Stands in for :class:`~src.redis_client.RedisGateway`, recording everything it was asked.

    Holds an in-memory keyspace (``hashes`` and ``zsets``) so a snapshot has something to fold,
    and a ``script_error`` seam so ``record`` can be handed any exception class.
    """

    def __init__(
        self,
        *,
        connected: bool = True,
        script_error: BaseException | None = None,
        run_error: BaseException | None = None,
        now_ms: int = LOCAL_NOW_MS,
    ) -> None:
        self.is_connected = connected
        #: What this stub's `TIME` answers. Separate from the collector's injected wall clock on
        #: purpose — the two are set to DIFFERENT values wherever a test is about which one the
        #: read side used.
        self.now_ms = now_ms
        self._script_error = script_error
        self._run_error = run_error

        self.client = RecordingClient(self)
        self.scripts: dict[str, str] = {}
        self.registrations: list[str] = []
        self.script_calls: list[tuple[str, list[str], list[str]]] = []
        self.commands: list[tuple[Any, ...]] = []
        self.pipelines: list[list[tuple[Any, ...]]] = []
        self.pipeline_transactions: list[bool] = []
        self.ops: list[str] = []

        self.hashes: dict[str, dict[str, str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    # -- the RedisGateway surface the collector uses ------------------------------------------ #
    def register(self, name: str, body: str) -> object:
        if not self.is_connected:
            raise RuntimeError("RedisGateway.connect() must be awaited before use")
        self.scripts[name] = body
        self.registrations.append(name)
        return object()

    def script(self, name: str) -> object:
        try:
            return self.scripts[name]
        except KeyError:
            raise KeyError(f"lua script {name!r} was never registered") from None

    async def run(self, coro_factory: Any, *, op: str) -> Any:
        self.ops.append(op)
        if self._run_error is not None:
            raise self._run_error
        return await coro_factory()

    async def run_script(self, name: str, keys: list[str], args: list[str]) -> Any:
        self.ops.append(f"script:{name}")
        self.script_calls.append((name, list(keys), list(args)))
        if self._script_error is not None:
            raise self._script_error
        return 1

    # -- the in-memory keyspace ---------------------------------------------------------------- #
    def answer(self, command: tuple[Any, ...]) -> Any:
        name = command[0]
        key = command[1]
        if name == "HGETALL":
            # Encoded to bytes on the way out, because `decode_responses=False` is what the real
            # gateway builds its client with — a stub that returned `str` would let a missing
            # decode in the fold pass here and produce `b'requests'` keys in production.
            return {
                field.encode(): str(value).encode()
                for field, value in self.hashes.get(key, {}).items()
            }
        if name == "ZREVRANGE":
            limit = int(command[3]) + 1
            members = sorted(
                self.zsets.get(key, {}).items(), key=lambda pair: (-pair[1], pair[0])
            )
            return [(member.encode(), float(score)) for member, score in members[:limit]]
        raise AssertionError(f"unexpected command {command!r}")  # pragma: no cover

    def seed_bucket(self, key: str, **fields: int) -> None:
        """Write one bucket's hash fields, translating ``outcome_allowed=3`` to ``outcome:allowed``.

        The keyword spelling exists so a test body reads as the picture it is asserting on rather
        than as a dictionary literal full of colons.
        """
        stored = self.hashes.setdefault(key, {})
        for name, value in fields.items():
            stored[name.replace("__", ":", 1)] = str(value)


def collector(
    settings: Settings,
    gateway: RecordingGateway | None = None,
    *,
    now_ms: int = LOCAL_NOW_MS,
    **overrides: Any,
) -> tuple[AnalyticsCollector, RecordingGateway]:
    """Build a collector over a recording gateway with a frozen wall clock."""
    store = gateway if gateway is not None else RecordingGateway()
    # By default the stub's server clock and the collector's wall clock agree, so a test that is
    # not about clocks does not have to think about them. `test_snapshot_names_its_window_from...`
    # pulls them apart deliberately.
    store.now_ms = now_ms
    tuned = settings.model_copy(update=overrides) if overrides else settings
    return AnalyticsCollector(store, tuned, clock=lambda: now_ms / 1000), store


def argv(gateway: RecordingGateway, index: int = 0) -> list[str]:
    """The ARGV of one recorded script call. ``index`` is the call, not the slot."""
    return gateway.script_calls[index][2]


def keys_of(gateway: RecordingGateway, index: int = 0) -> list[str]:
    """The KEYS of one recorded script call."""
    return gateway.script_calls[index][1]


def slot(values: list[str], position: int) -> str:
    """Read one 1-based ARGV/KEYS slot by its contract constant, never by a literal index."""
    return values[position - 1]


# =============================================================================================
# 1. Keys are ARITHMETIC — computed from a time range, never discovered
# =============================================================================================


async def test_minute_and_hour_keys_come_from_the_time_index(settings):
    """The three keys one record writes are pure functions of the bucketing instant.

    Asserted against :mod:`src.keys` rather than against hand-written strings, because a test that
    spelled ``"stats:min:29765520"`` itself would be pinning this module to a literal instead of to
    the key schema — and would keep passing if the two ever disagreed.
    """
    collect, gateway = collector(settings)

    await collect.record(
                make_decision(server_now_ms=DECISION_NOW_MS),
                status_code=200,
                user_id="alice",
                endpoint="GET:/api/v1/whoami",
                tier="free",
                cost=1,
            )

    keys = keys_of(gateway)
    assert len(keys) == RECORD_KEYS_ARITY
    assert slot(keys, RECORD_KEY_MINUTE) == stats_minute_key(minute_index(DECISION_NOW_MS))
    assert slot(keys, RECORD_KEY_HOUR) == stats_hour_key(hour_index(DECISION_NOW_MS))
    # The top-consumer ZSET is a view OF the minute bucket, so it carries the MINUTE index — not
    # the hour one, and not an index of its own.
    assert slot(keys, RECORD_KEY_TOP) == stats_top_key(minute_index(DECISION_NOW_MS))


async def test_consecutive_instants_land_in_contiguous_buckets(settings):
    """Sixty consecutive minutes produce sixty contiguous indices, with no gap and no repeat.

    The property the read side depends on: it computes a *range* of names rather than asking Redis
    which ones exist, so any discontinuity in the index arithmetic would be a permanently empty
    column in the middle of the chart that no data would ever fill.
    """
    collect, gateway = collector(settings)

    for offset in range(60):
        await collect.record(
                        make_decision(server_now_ms=DECISION_NOW_MS + offset * MS_PER_MINUTE),
                        status_code=200,
                        user_id="alice",
                        endpoint="GET:/api/v1/whoami",
                        tier="free",
                        cost=1,
                    )

    indices = [
        int(slot(keys_of(gateway, call), RECORD_KEY_MINUTE).rsplit(":", 1)[1])
        for call in range(60)
    ]
    assert indices == list(range(indices[0], indices[0] + 60))


async def test_recent_minute_indices_is_descending_contiguous_and_clamped():
    """The read side's whole "never scan" property in three assertions.

    Descending because element zero must be "the minute happening now"; contiguous because the
    range is what stands in for a keyspace walk; clamped because a window wider than the epoch
    itself must not produce a negative index, which would build ``stats:min:-3``.
    """
    assert recent_minute_indices(100, 3) == [100, 99, 98]
    # Contiguous over a realistic window, with no duplicates.
    window = recent_minute_indices(29_765_520, 120)
    assert window == list(range(29_765_520, 29_765_520 - 120, -1))
    assert len(set(window)) == 120
    # Clamped at the epoch rather than running negative...
    assert recent_minute_indices(2, 10) == [2, 1, 0]
    # ...and a non-positive count is an empty list, not an error: a caller with a zeroed
    # ANALYTICS_MAX_BUCKETS is asking a legitimate question.
    assert recent_minute_indices(100, 0) == []


# =============================================================================================
# 2. The bucket index comes from the DECISION's clock
# =============================================================================================


async def test_the_bucket_index_comes_from_the_decision_not_from_local_time(settings):
    """**The two-replica property**, asserted at the only place it is decidable.

    The collector is given a wall clock nine and a half hours away from the decision's
    ``server_now_ms``. If the bucket came from ``time.time()``, two replicas whose system clocks
    differ would write one instant into two different minute buckets — a permanent saw-tooth on
    every chart that no amount of staring at the traffic explains. The decision's clock is
    ``redis.call('TIME')``, which is the same answer on every replica by construction.
    """
    collect, gateway = collector(settings, now_ms=LOCAL_NOW_MS)

    await collect.record(
                make_decision(server_now_ms=DECISION_NOW_MS),
                status_code=200,
                user_id="alice",
                endpoint="GET:/api/v1/whoami",
                tier="free",
                cost=1,
            )

    keys = keys_of(gateway)
    assert slot(keys, RECORD_KEY_MINUTE) == stats_minute_key(minute_index(DECISION_NOW_MS))
    assert slot(keys, RECORD_KEY_HOUR) == stats_hour_key(hour_index(DECISION_NOW_MS))
    # And explicitly NOT the local clock's buckets — the assertion is only meaningful because the
    # two are different, so the difference is stated rather than assumed.
    assert slot(keys, RECORD_KEY_MINUTE) != stats_minute_key(minute_index(LOCAL_NOW_MS))
    assert slot(keys, RECORD_KEY_HOUR) != stats_hour_key(hour_index(LOCAL_NOW_MS))


async def test_with_no_decision_the_local_clock_is_used(settings):
    """A 401 never ran the script, so there is no shared clock to read. Local time, documented.

    The error that introduces is bounded by this replica's skew and applies only to the paths that
    genuinely have no decision. The alternative — dropping the record — would take an
    authentication-failure flood off the graph entirely, which is the traffic most worth seeing.
    """
    collect, gateway = collector(settings, now_ms=LOCAL_NOW_MS)

    await collect.record(
                None,
                status_code=401,
                user_id=ANONYMOUS_USER_ID,
                endpoint="GET:/api/v1/whoami",
                tier=UNKNOWN_TIER,
                cost=1,
            )

    keys = keys_of(gateway)
    assert slot(keys, RECORD_KEY_MINUTE) == stats_minute_key(minute_index(LOCAL_NOW_MS))
    assert slot(keys, RECORD_KEY_HOUR) == stats_hour_key(hour_index(LOCAL_NOW_MS))


async def test_an_explicit_now_ms_cannot_override_a_decisions_clock(settings):
    """The seam is a *fallback*, not an override, and that ordering is a safety property.

    A ``now_ms`` able to beat ``server_now_ms`` would be a parameter that silently re-introduces
    per-replica bucketing — the exact failure the shared clock exists to remove — available to any
    caller who passed it by mistake. It applies only where there is no decision to read.
    """
    collect, gateway = collector(settings, now_ms=LOCAL_NOW_MS)
    unrelated = LOCAL_NOW_MS + 500 * MS_PER_MINUTE

    await collect.record(
                make_decision(server_now_ms=DECISION_NOW_MS),
                status_code=200,
                user_id="alice",
                endpoint="GET:/api/v1/whoami",
                tier="free",
                cost=1,
                now_ms=unrelated,
            )
    # ...and with no decision, the same argument IS honoured.
    await collect.record(
                None,
                status_code=401,
                user_id=ANONYMOUS_USER_ID,
                endpoint="other",
                tier=UNKNOWN_TIER,
                cost=1,
                now_ms=unrelated,
            )

    assert slot(keys_of(gateway, 0), RECORD_KEY_MINUTE) == stats_minute_key(
        minute_index(DECISION_NOW_MS)
    )
    assert slot(keys_of(gateway, 1), RECORD_KEY_MINUTE) == stats_minute_key(
        minute_index(unrelated)
    )


async def test_a_zeroed_server_now_ms_falls_through_to_the_local_clock(settings):
    """A hand-built decision carrying no clock must not bucket the record at the unix epoch.

    ``server_now_ms = 0`` is reachable from any hand-built decision, and taking it literally would
    write into ``stats:min:0`` — a bucket from 1970 that no window will ever read, so the record
    would be silently lost rather than merely mis-bucketed.
    """
    collect, gateway = collector(settings, now_ms=LOCAL_NOW_MS)

    await collect.record(
                make_decision(server_now_ms=0),
                status_code=200,
                user_id="alice",
                endpoint="GET:/api/v1/whoami",
                tier="free",
                cost=1,
            )

    assert slot(keys_of(gateway), RECORD_KEY_MINUTE) == stats_minute_key(
        minute_index(LOCAL_NOW_MS)
    )


# =============================================================================================
# 3. The ARGV contract
# =============================================================================================


async def test_record_argv_carries_every_dimension_in_its_contract_slot(settings):
    """Eight slots, read by their contract constants rather than by literal indices.

    A test that hard-coded ``args[4]`` would keep passing while silently asserting about a
    different dimension the day a slot is inserted — and a mis-slotted ARGV does not raise on the
    Lua side, it writes a hash field called ``tier:429``.
    """
    collect, gateway = collector(settings)

    await collect.record(
                make_decision(),
                status_code=429,
                user_id="alice",
                endpoint="GET:/api/v1/logs/query",
                tier="premium",
                cost=5,
            )

    args = argv(gateway)
    assert len(args) == RECORD_ARGV_ARITY
    assert slot(args, RECORD_ARGV_COST) == "5"
    assert slot(args, RECORD_ARGV_MINUTE_TTL_SEC) == str(settings.analytics_minute_ttl_sec)
    assert slot(args, RECORD_ARGV_HOUR_TTL_SEC) == str(settings.analytics_hour_ttl_sec)
    assert slot(args, RECORD_ARGV_USER_ID) == "alice"
    assert slot(args, RECORD_ARGV_OUTCOME) == OUTCOME_ALLOWED
    assert slot(args, RECORD_ARGV_TIER) == "premium"
    assert slot(args, RECORD_ARGV_ENDPOINT) == "GET:/api/v1/logs/query"
    assert slot(args, RECORD_ARGV_STATUS) == "429"


@pytest.mark.parametrize(
    ("decision", "expected"),
    [
        (make_decision(allowed=True, degraded=False), OUTCOME_ALLOWED),
        (
            make_decision(allowed=False, degraded=False, reason=DenyReason.RATE_LIMIT),
            OUTCOME_DENIED,
        ),
        (make_decision(allowed=True, degraded=True), OUTCOME_DEGRADED),
        # A degraded REFUSAL is `degraded`, not `denied`: while the store is down the question
        # worth answering is "was this authoritative?", and `status:429` still carries the refusal.
        (
            make_decision(allowed=False, degraded=True, reason=DenyReason.RATE_LIMIT),
            OUTCOME_DEGRADED,
        ),
        # No decision at all (a 401) is a refusal — it was not served.
        (None, OUTCOME_DENIED),
    ],
)
async def test_the_outcome_dimension_partitions_the_traffic(decision, expected):
    """Every request lands in exactly one of three outcomes, so they sum to ``requests``.

    That is what lets a dashboard compute a rejection *rate* without knowing every value the field
    can take — the denominator is the sum of the parts.
    """
    assert AnalyticsCollector.outcome_of(decision) == expected
    assert expected in OUTCOMES


async def test_blank_dimensions_fall_back_to_their_sentinels(settings):
    """An empty user, tier or endpoint becomes a named sentinel rather than a blank hash field.

    ``HINCRBY key 'tier:' 1`` is a perfectly valid command, and the row it creates renders on a
    dashboard as an unexplained blank bar that nobody can attribute to anything.
    """
    collect, gateway = collector(settings)

    await collect.record(
                None, status_code=401, user_id="", endpoint="", tier="", cost=1
            )

    args = argv(gateway)
    assert slot(args, RECORD_ARGV_USER_ID) == ANONYMOUS_USER_ID
    assert slot(args, RECORD_ARGV_TIER) == UNKNOWN_TIER
    assert slot(args, RECORD_ARGV_ENDPOINT) == "other"


async def test_a_non_positive_cost_is_floored_at_one(settings):
    """A zero-cost record would be invisible in the cost series and in the consumer ranking.

    The same rule :meth:`src.limiter.Limiter.check` applies at the other end of the pipe, for the
    same reason: a request that contributed nothing to the weighted total is a request the ranking
    that decides who to call about a load problem cannot see.
    """
    collect, gateway = collector(settings)

    for cost in (0, -5):
        await collect.record(
                        make_decision(), status_code=200, user_id="a", endpoint="other",
                        tier="free", cost=cost,
                    )

    assert [slot(argv(gateway, call), RECORD_ARGV_COST) for call in (0, 1)] == ["1", "1"]


async def test_the_script_is_registered_on_demand(settings):
    """Registration survives a reconnect, which drops every handle the gateway was holding.

    :meth:`~src.redis_client.RedisGateway.aclose` clears the script table precisely so a
    reconnected gateway cannot dispatch onto a dead client. A collector that registered once in
    ``__init__`` would then ``KeyError`` on every subsequent record.
    """
    gateway = RecordingGateway()
    collect, _ = collector(settings, gateway)
    assert gateway.registrations == [RLQ_RECORD_REQUEST_NAME]
    assert gateway.scripts[RLQ_RECORD_REQUEST_NAME] == RLQ_RECORD_REQUEST

    gateway.scripts.clear()  # what aclose() does
    await collect.record(
                make_decision(), status_code=200, user_id="a", endpoint="other", tier="free", cost=1
            )
    assert gateway.registrations == [RLQ_RECORD_REQUEST_NAME] * 2
    assert collect.records == 1


# =============================================================================================
# 4. `record` cannot raise. For ANY exception class
# =============================================================================================


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(BackingStoreUnavailable("redis is gone", op="script:x"), id="unavailable"),
        pytest.param(BackingStoreOverloaded("no connection", op="script:x"), id="overloaded"),
        pytest.param(redis.exceptions.ResponseError("WRONGTYPE"), id="response-error"),
        pytest.param(redis.exceptions.NoScriptError("NOSCRIPT"), id="noscript"),
        pytest.param(asyncio.TimeoutError("timed out"), id="asyncio-timeout"),
        pytest.param(TimeoutError("timed out"), id="builtin-timeout"),
        pytest.param(ValueError("bug in this module"), id="value-error"),
        pytest.param(Exception("something nobody predicted"), id="bare-exception"),
    ],
)
async def test_record_swallows_every_exception_class_and_counts_it(settings, error, caplog):
    """**Analytics may never break a request**, and the request is already served when this runs.

    Every class is covered because the rule is about the *category* of work, not about a list of
    failures somebody remembered. Two of these are deliberately ones the rest of this project goes
    out of its way to let propagate: a ``ResponseError`` becomes a 500 from the limiter, because a
    broken decision script must never be laundered into a silent fail-open. Here it must be
    swallowed, because the worst outcome of a broken *record* script is a gap in a chart, and by
    the time this coroutine runs the status line and the body are already on the wire — raising
    could not produce a 500 even in principle, only a torn connection on a request that succeeded.
    """
    gateway = RecordingGateway(script_error=error)
    collect, _ = collector(settings, gateway)

    with caplog.at_level("WARNING"):
        landed = await collect.record(
                                 make_decision(),
                                 status_code=200,
                                 user_id="alice",
                                 endpoint="GET:/api/v1/whoami",
                                 tier="free",
                                 cost=1,
                             )

    assert landed is False
    assert collect.records == 0
    assert collect.dropped == 1
    assert collect.errors == 1
    # The failure is attributable rather than merely counted: an operator seeing a non-zero
    # `errors` needs somewhere to look that is not "grep the logs of whichever replica it was".
    assert collect.last_error is not None
    assert type(error).__name__ in collect.last_error
    assert any("analytics record dropped" in message for message in caplog.messages)


async def test_record_swallows_a_cancelled_error_being_absent(settings):
    """``CancelledError`` is a ``BaseException`` and is deliberately NOT swallowed.

    Catching it would resurrect a cancelled request long enough to finish a Redis write, and would
    make a shutdown hang on analytics for traffic nobody is waiting for any more. The ``except
    Exception`` in :meth:`~src.analytics.AnalyticsCollector.record` is what keeps that true, and
    this pins it rather than leaving it to be inferred from the class hierarchy.
    """
    gateway = RecordingGateway(script_error=asyncio.CancelledError())
    collect, _ = collector(settings, gateway)

    with pytest.raises(asyncio.CancelledError):
        await collect.record(
                        make_decision(), status_code=200, user_id="a", endpoint="other",
                        tier="free", cost=1,
                    )
    # It was not counted as an ordinary drop either — a cancellation is not a failure to record,
    # it is a request that stopped existing.
    assert collect.errors == 0


async def test_an_unconnected_gateway_is_a_drop_but_not_an_error(settings):
    """``Runtime.build()`` without ``start()`` — the hermetic test seam — records nothing, quietly.

    Counted on ``dropped`` because the record genuinely did not land, and NOT on ``errors``,
    because nothing failed: nobody asked this process to talk to Redis. An ``errors`` counter that
    ticked once per request on a runtime nobody started is an alert that fires on every test run,
    and an alert that always fires is an alert nobody reads.
    """
    gateway = RecordingGateway(connected=False)
    collect, _ = collector(settings, gateway)

    landed = await collect.record(
                         make_decision(), status_code=200, user_id="a", endpoint="other", tier="free", cost=1
                     )

    assert landed is False
    assert collect.dropped == 1
    assert collect.errors == 0
    assert collect.last_error is None
    # And it did not even build a call, let alone dial.
    assert gateway.script_calls == []


async def test_the_counters_account_for_every_attempt(settings):
    """``records + dropped`` is every attempt — the invariant that makes ``dropped`` readable.

    It is what answers "what fraction of my traffic is actually on this graph?", which is a
    question a dashboard cannot answer about itself from the buckets alone.
    """
    gateway = RecordingGateway()
    collect, _ = collector(settings, gateway)
    kwargs: dict[str, Any] = {
        "status_code": 200, "user_id": "a", "endpoint": "other", "tier": "free", "cost": 1,
    }

    for _ in range(3):
        await collect.record(make_decision(), **kwargs)
    gateway._script_error = redis.exceptions.ResponseError("boom")
    for _ in range(2):
        await collect.record(make_decision(), **kwargs)

    assert (collect.records, collect.dropped, collect.errors) == (3, 2, 2)
    assert collect.records + collect.dropped == 5
    assert collect.stats()["records"] == 3
    assert collect.stats()["dropped"] == 2


# =============================================================================================
# 4b. The pool gate — analytics must never outbid the limiter for a connection
# =============================================================================================


class BlockingGateway(RecordingGateway):
    """A gateway whose script call parks until released — so in-flight records can be counted.

    The gate is about *concurrency*, and concurrency is the one thing a sequential stub cannot
    show. This one suspends every record inside ``run_script`` so a test can have N of them in
    flight simultaneously and assert exactly how many the collector allowed to get there.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.release = asyncio.Event()
        self.entered = 0
        self.peak_inflight = 0
        self._inflight = 0

    async def run_script(self, name: str, keys: list[str], args: list[str]) -> Any:
        self.entered += 1
        self._inflight += 1
        self.peak_inflight = max(self.peak_inflight, self._inflight)
        try:
            await self.release.wait()
        finally:
            self._inflight -= 1
        return await super().run_script(name, keys, args)


async def test_the_write_path_never_holds_more_than_its_share_of_the_pool(settings):
    """**The invariant Fix 1 establishes:** under contention, decisions win and records are shed.

    Twenty records are fired concurrently against a stub that parks each one inside the Redis call.
    At most ``REDIS_MAX_CONNECTIONS // 8`` may be in flight — that is the hard cap on how many
    connections the analytics writer can be holding while a limiter call is queueing for one — and
    every record over that is **dropped rather than queued**.

    Measured before this gate, at pool 6 with 240 concurrent requests: 26.2% of requests came back
    503 because analytics had taken the connections, with *zero* analytics writes dropped. Every
    statistic won and 63 enforcement decisions lost, which is exactly backwards for a subsystem
    whose own contract is that dropping a statistic beats delaying a request.
    """
    gateway = BlockingGateway()
    collect, _ = collector(settings, gateway)
    expected_slots = max(1, settings.redis_max_connections // ANALYTICS_POOL_SHARE_DIVISOR)

    fired = [
        asyncio.create_task(
            collect.record(
                make_decision(), status_code=200, user_id=f"u{i}", endpoint="other",
                tier="free", cost=1,
            )
        )
        for i in range(20)
    ]
    # Let every task reach the gate, then let the parked ones finish.
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    gateway.release.set()
    landed = await asyncio.gather(*fired)

    assert gateway.peak_inflight <= expected_slots
    assert gateway.entered == expected_slots
    assert landed.count(True) == expected_slots
    # The rest were SHED, not lost to an error — a rising `shed` beside a flat `errors` is the
    # signal that says "load, and the limiter is winning" rather than "the store is broken".
    assert collect.shed == 20 - expected_slots
    assert collect.dropped == 20 - expected_slots
    assert collect.errors == 0
    assert collect.records + collect.dropped == 20


async def test_the_gate_reopens_after_each_record_including_a_failing_one(settings):
    """A slot is released in a ``finally``, so a failing record cannot wedge the gate shut.

    Without that, one exception per in-flight slot would permanently close the gate and analytics
    would go silent for the life of the process — a failure that looks exactly like "no traffic",
    which is the single most misleading thing this subsystem can report.
    """
    gateway = RecordingGateway(script_error=redis.exceptions.ResponseError("boom"))
    collect, _ = collector(settings, gateway)
    write: dict[str, Any] = {
        "status_code": 200, "user_id": "a", "endpoint": "other", "tier": "free", "cost": 1,
    }

    for _ in range(20):
        await collect.record(make_decision(), **write)
    gateway._script_error = None
    assert await collect.record(make_decision(), **write) is True

    assert collect.shed == 0
    assert collect.errors == 20
    assert collect.records == 1


async def test_the_share_scales_with_the_pool_it_protects(settings):
    """A *share*, not a fixed count — so the bound means the same thing at any pool size.

    An absolute 4 would be an eighth of the shipped 32-connection pool and two thirds of a pool of
    six, which is precisely the configuration the contention was measured in. Floored at one,
    because a gate of zero would switch analytics off entirely on a small pool rather than
    throttling it.
    """
    for pool, expected in ((32, 4), (16, 2), (8, 1), (6, 1), (1, 1)):
        collect, _ = collector(settings, redis_max_connections=pool)
        assert collect.stats()["max_inflight"] == expected


# =============================================================================================
# 5. The read side never scans — and names its window from the SHARED clock
# =============================================================================================


async def test_snapshot_names_its_window_from_redis_clock_not_the_local_one(settings):
    """**Fix 3.** The read side buckets on the same clock the write side does.

    The collector is given a wall clock nine and a half hours from the stub's ``TIME``. If the
    window came from ``time.time()``, a replica five minutes fast would compute
    ``newest_minute_index`` five minutes ahead of anything that was ever written and report
    ``totals.requests = 0`` for data a correct replica reports normally — and with C12's two
    replicas behind round-robin nginx and C11 polling every five seconds, the dashboard would
    alternate between two different answers for the same underlying data.
    """
    gateway = RecordingGateway(now_ms=DECISION_NOW_MS)
    collect = AnalyticsCollector(gateway, settings, clock=lambda: LOCAL_NOW_MS / 1000)
    gateway.seed_bucket(
        stats_minute_key(minute_index(DECISION_NOW_MS)),
        requests=7, cost=7, outcome__allowed=7, status__200=7,
    )

    snapshot = await collect.snapshot(minutes=3, hours=1)

    assert snapshot.window.newest_minute_index == minute_index(DECISION_NOW_MS)
    assert snapshot.window.newest_minute_index != minute_index(LOCAL_NOW_MS)
    assert snapshot.window.newest_hour_index == hour_index(DECISION_NOW_MS)
    # ...and the data written at the server's instant is actually found, which is the whole point.
    assert snapshot.totals.requests == 7
    # The clock came from the store, through the gateway, so it feeds the breaker like everything
    # else on this path.
    assert gateway.ops[0] == "analytics:snapshot:time"
    assert ("TIME",) in gateway.commands


async def test_two_collectors_with_different_local_clocks_agree(settings):
    """Two replicas, two skewed wall clocks, one Redis — and one answer.

    This is the read-side half of the two-replica property. It holds trivially now *because*
    ``snapshot`` no longer consults a local clock at all, and it is pinned rather than assumed so
    that reintroducing one is a failing test rather than a dashboard that flickers between two
    numbers every time nginx picks the other replica.
    """
    gateway = RecordingGateway(now_ms=DECISION_NOW_MS)
    gateway.seed_bucket(
        stats_minute_key(minute_index(DECISION_NOW_MS)),
        requests=4, cost=9, outcome__allowed=4, status__200=4, tier__free=4,
    )
    replica_a = AnalyticsCollector(gateway, settings, clock=lambda: LOCAL_NOW_MS / 1000)
    replica_b = AnalyticsCollector(
        gateway, settings, clock=lambda: (LOCAL_NOW_MS + 5 * MS_PER_MINUTE) / 1000
    )

    first = await replica_a.snapshot(minutes=5, hours=2)
    second = await replica_b.snapshot(minutes=5, hours=2)

    assert first.model_dump() == second.model_dump()
    assert first.totals.requests == 4


async def test_a_zeroed_bucket_cap_reads_nothing_but_still_reports_the_request(settings):
    """``ANALYTICS_MAX_BUCKETS=0`` is a legitimate (if useless) configuration, not an error.

    The window is empty, everything is reported as dropped, and no ``HGETALL`` is issued — but the
    caller still gets a well-formed snapshot rather than an exception, because a config value that
    turns a dashboard blank must not turn it into a 500.
    """
    collect, gateway = collector(settings, analytics_max_buckets=0)

    snapshot = await collect.snapshot(minutes=10, hours=2)

    assert [c for c in gateway.commands if c[0] == "HGETALL"] == []
    assert snapshot.buckets_read == 0
    assert snapshot.dropped == 12
    assert snapshot.window.minutes_covered == 0
    assert snapshot.totals.requests == 0


# =============================================================================================
# 5b. The read side never scans
# =============================================================================================


async def test_snapshot_issues_only_hgetall_and_zrevrange(settings):
    """**Never ``SCAN``, never ``KEYS``** — asserted against the command stream, not reasoned about.

    ``SCAN MATCH stats:min:*`` walks the entire keyspace, including every ``rate_limit:*``,
    ``sw:*`` and ``quota:*`` key in the system, on a single-threaded server, on the endpoint a
    dashboard polls every 5 seconds. Bucket names are arithmetic, so they are computed and
    pipelined instead; a missing bucket comes back as an empty hash, which is the same answer a
    scan would have given.
    """
    collect, gateway = collector(settings)

    await collect.snapshot(minutes=10, hours=3)

    issued = {command[0] for command in gateway.commands}
    assert issued <= ALLOWED_SNAPSHOT_COMMANDS
    assert not issued & FORBIDDEN_COMMANDS
    # One pipeline, not one round trip per bucket: 10 minute HGETALLs + 3 hour HGETALLs + 10
    # top-consumer ZREVRANGEs, batched.
    assert len(gateway.pipelines) == 1
    assert len(gateway.pipelines[0]) == 10 + 3 + 10
    # ...and without MULTI/EXEC, which would hold the single thread for the whole batch to buy an
    # atomicity a stats read does not need.
    assert gateway.pipeline_transactions == [False]


async def test_snapshot_reads_exactly_the_computed_key_names(settings):
    """The keys pipelined are the ones :mod:`src.keys` computes for the window, and no others."""
    collect, gateway = collector(settings, now_ms=LOCAL_NOW_MS)

    await collect.snapshot(minutes=3, hours=2)

    expected_minutes = [stats_minute_key(i) for i in recent_minute_indices(minute_index(LOCAL_NOW_MS), 3)]
    expected_hours = [stats_hour_key(i) for i in recent_minute_indices(hour_index(LOCAL_NOW_MS), 2)]
    expected_tops = [stats_top_key(i) for i in recent_minute_indices(minute_index(LOCAL_NOW_MS), 3)]

    hgetalls = [command[1] for command in gateway.commands if command[0] == "HGETALL"]
    zranges = [command[1] for command in gateway.commands if command[0] == "ZREVRANGE"]
    assert hgetalls == expected_minutes + expected_hours
    assert zranges == expected_tops


async def test_an_empty_range_costs_zero_redis_commands_and_returns_zeros(settings):
    """Asking for nothing returns a zeroed snapshot, not an error and not a round trip.

    A caller with a zeroed ``ANALYTICS_MAX_BUCKETS``, or one that asks for a negative window, is
    making a legitimate request. The window reports ``None`` bounds rather than ``0`` — ``0`` is a
    real minute index and a real instant, so reusing it would make "we covered nothing" and "we
    covered 1970" the same payload.
    """
    collect, gateway = collector(settings)

    snapshot = await collect.snapshot(minutes=0, hours=-4)

    assert gateway.commands == []
    assert gateway.pipelines == []
    assert snapshot.totals.requests == 0
    assert snapshot.per_minute == []
    assert snapshot.per_hour == []
    assert snapshot.top_consumers == []
    assert snapshot.buckets_read == 0
    assert snapshot.dropped == 0
    assert snapshot.window.minutes_covered == 0
    assert snapshot.window.newest_minute_index is None
    assert snapshot.window.start_ms is None
    assert snapshot.window.end_ms is None
    # Every known outcome is present at zero rather than absent — a chart handed a missing key
    # renders a gap, one handed an explicit 0 renders the flat line that is the true picture.
    assert snapshot.by_outcome == dict.fromkeys(OUTCOMES, 0)


# =============================================================================================
# 6. The cap, and reporting what was dropped
# =============================================================================================


async def test_snapshot_caps_the_fan_in_and_reports_what_it_dropped(settings, caplog):
    """A truncated window is a **fact on the wire**, not something to infer from a log line.

    The payload has the same shape whether or not the cap bit, so without ``dropped`` and the
    ``*_requested`` / ``*_covered`` pair, a chart drawn from 12 of the 200 buckets that were asked
    for looks exactly like a chart drawn from all of them. It is logged as well, because a caller
    that ignores the field still leaves evidence.
    """
    collect, gateway = collector(settings, analytics_max_buckets=12)

    with caplog.at_level("WARNING"):
        snapshot = await collect.snapshot(minutes=200, hours=24)

    # Minutes are served first: the per-minute series is the live chart, the hour series is the
    # context line behind it. So the cap goes entirely to minutes and the hours are starved —
    # visibly, which is the point.
    assert snapshot.window.minutes_requested == 200
    assert snapshot.window.minutes_covered == 12
    assert snapshot.window.hours_requested == 24
    assert snapshot.window.hours_covered == 0
    assert snapshot.buckets_read == 12
    assert snapshot.dropped == (200 - 12) + 24
    # The fan-in really is bounded — 12 hash reads, plus the 12 bounded ZSET slices that ride the
    # same pipeline.
    assert len([c for c in gateway.commands if c[0] == "HGETALL"]) == 12
    assert any("analytics snapshot truncated" in message for message in caplog.messages)
    assert collect.truncated_snapshots == 1
    assert collect.buckets_dropped == snapshot.dropped


async def test_a_window_that_fits_reports_nothing_dropped(settings):
    """The mirror image, so ``dropped`` means something when it is zero as well as when it is not."""
    collect, _ = collector(settings, analytics_max_buckets=120)

    snapshot = await collect.snapshot(minutes=60, hours=24)

    assert snapshot.dropped == 0
    assert snapshot.buckets_read == 84
    assert snapshot.window.minutes_covered == 60
    assert snapshot.window.hours_covered == 24
    assert collect.truncated_snapshots == 0
    assert collect.snapshots == 1


# =============================================================================================
# 7. Folding
# =============================================================================================


def _seed_two_minutes(gateway: RecordingGateway, at_ms: int) -> tuple[str, str]:
    """Seed the current and previous minute buckets with a known, asymmetric picture."""
    now_index = minute_index(at_ms)
    current = stats_minute_key(now_index)
    previous = stats_minute_key(now_index - 1)

    gateway.seed_bucket(
        current,
        requests=10,
        cost=26,
        outcome__allowed=7,
        outcome__denied=2,
        outcome__degraded=1,
        status__200=7,
        status__429=2,
        status__401=1,
        endpoint__GET=6,
        tier__free=9,
        tier__premium=1,
    )
    gateway.seed_bucket(
        previous,
        requests=5,
        cost=9,
        outcome__allowed=5,
        status__200=5,
        endpoint__GET=5,
        tier__free=5,
    )
    return current, previous


async def test_folding_sums_every_dimension_across_the_window(settings):
    """Status, endpoint, tier and outcome each sum across the covered minute buckets.

    The dimensions are folded in the same pass that builds each bucket, so this also pins that the
    per-bucket counters and the window totals cannot drift apart — they are computed from the same
    decode of the same fields.
    """
    gateway = RecordingGateway()
    collect, _ = collector(settings, gateway, now_ms=LOCAL_NOW_MS)
    _seed_two_minutes(gateway, LOCAL_NOW_MS)

    snapshot = await collect.snapshot(minutes=2, hours=0)

    assert snapshot.totals.requests == 15
    assert snapshot.totals.cost == 35
    assert snapshot.totals.allowed == 12
    assert snapshot.totals.denied == 2
    assert snapshot.totals.degraded == 1
    assert snapshot.by_status == {"200": 12, "429": 2, "401": 1}
    assert snapshot.by_endpoint == {"GET": 11}
    assert snapshot.by_tier == {"free": 14, "premium": 1}
    assert snapshot.by_outcome == {
        OUTCOME_ALLOWED: 12,
        OUTCOME_DENIED: 2,
        OUTCOME_DEGRADED: 1,
    }
    # The three outcomes partition the traffic, which is what makes a rejection RATE computable.
    assert sum(snapshot.by_outcome.values()) == snapshot.totals.requests


async def test_the_minute_series_runs_oldest_first_and_carries_its_own_arithmetic(settings):
    """A time series is drawn left to right, so ``per_minute[0]`` is the oldest point.

    ``recent_minute_indices`` is newest-first because element zero of *that* list is "the minute
    happening now". The reversal happens once, in the collector, rather than in every consumer —
    and each bucket carries ``start_ms`` and ``width_ms`` so a client never has to know which
    multiplier belongs to which series.
    """
    gateway = RecordingGateway()
    collect, _ = collector(settings, gateway, now_ms=LOCAL_NOW_MS)
    _seed_two_minutes(gateway, LOCAL_NOW_MS)

    snapshot = await collect.snapshot(minutes=3, hours=2)

    indices = [bucket.index for bucket in snapshot.per_minute]
    assert indices == sorted(indices)
    assert indices[-1] == minute_index(LOCAL_NOW_MS)
    # The oldest bucket has never been written and folds to zeros rather than being absent.
    assert snapshot.per_minute[0].requests == 0
    assert snapshot.per_minute[-1].requests == 10
    assert snapshot.per_minute[-1].allowed == 7
    assert snapshot.per_minute[-1].denied == 2
    assert snapshot.per_minute[-1].degraded == 1

    for bucket in snapshot.per_minute:
        assert bucket.width_ms == MS_PER_MINUTE
        assert bucket.start_ms == bucket.index * MS_PER_MINUTE
    for bucket in snapshot.per_hour:
        assert bucket.width_ms == MS_PER_HOUR
        assert bucket.start_ms == bucket.index * MS_PER_HOUR


async def test_hour_buckets_do_not_double_count_the_totals(settings):
    """The two series describe the SAME requests at two resolutions. Only one may be folded.

    Counting both would inflate every total — and unevenly, because the hour window reaches
    further back than the minute window does, so some traffic would be counted twice and the rest
    once. The hour series is the context line and nothing else.
    """
    gateway = RecordingGateway()
    collect, _ = collector(settings, gateway, now_ms=LOCAL_NOW_MS)
    _seed_two_minutes(gateway, LOCAL_NOW_MS)
    gateway.seed_bucket(
        stats_hour_key(hour_index(LOCAL_NOW_MS)),
        requests=15,
        cost=35,
        outcome__allowed=12,
        outcome__denied=2,
        outcome__degraded=1,
        status__200=12,
        tier__free=14,
    )

    snapshot = await collect.snapshot(minutes=2, hours=1)

    # The hour bucket carries the same 15 requests, and the totals are still 15 rather than 30.
    assert snapshot.per_hour[-1].requests == 15
    assert snapshot.totals.requests == 15
    assert snapshot.by_status == {"200": 12, "429": 2, "401": 1}


async def test_unreadable_and_unknown_fields_are_tolerated_not_fatal(settings):
    """One malformed hash field must not take the whole dashboard down.

    Deliberately unlike :meth:`~src.models.LimitDecision.from_lua`, which raises on a malformed
    element: that decoder is deciding whether a request is allowed, and a confident wrong answer
    there is the failure this project exists to prevent. This one is drawing a chart, and the
    correct response to a field somebody hand-edited is to leave it out of the arithmetic.

    An unknown field *family* is ignored for a different reason: a bucket written by a newer
    replica mid-rollout can legitimately carry one, and failing the read for the whole window
    would make every rolling deploy an observability outage.
    """
    gateway = RecordingGateway()
    collect, _ = collector(settings, gateway, now_ms=LOCAL_NOW_MS)
    key = stats_minute_key(minute_index(LOCAL_NOW_MS))
    gateway.seed_bucket(key, requests=4, cost=4, outcome__allowed=4, status__200=4)
    gateway.hashes[key]["cost"] = "not-a-number"
    gateway.hashes[key]["histogram:p99"] = "12"
    # A bare prefix with no suffix would otherwise create a dimension row with a blank label.
    gateway.hashes[key]["status:"] = "9"

    snapshot = await collect.snapshot(minutes=1, hours=0)

    assert snapshot.totals.requests == 4
    assert snapshot.totals.cost == 0
    assert snapshot.by_status == {"200": 4}


# =============================================================================================
# 8. Top consumers
# =============================================================================================


async def test_top_consumers_are_ranked_by_cost_and_merged_across_the_window(settings):
    """Ranked by **cost**, and summed across every covered minute.

    Cost rather than request count is the whole reason the ZSET is scored the way it is: 20 calls
    to a 5-token endpoint outweigh 60 calls to a 1-token one, and a ranking by request count would
    put the cheap caller on top and send an operator after the wrong client.
    """
    gateway = RecordingGateway()
    collect, _ = collector(settings, gateway, now_ms=LOCAL_NOW_MS)
    now = minute_index(LOCAL_NOW_MS)
    gateway.zsets[stats_top_key(now)] = {"alice": 40.0, "bob": 55.0, "carol": 5.0}
    gateway.zsets[stats_top_key(now - 1)] = {"alice": 30.0, "bob": 5.0}

    snapshot = await collect.snapshot(minutes=2, hours=0)

    assert [(entry.user_id, entry.cost) for entry in snapshot.top_consumers] == [
        ("alice", 70),
        ("bob", 60),
        ("carol", 5),
    ]


async def test_top_consumers_is_bounded_and_deterministic(settings):
    """At most ten entries, and ties break on ``user_id`` so the order is assertable.

    The bound is what the ZSET buys: each per-minute slice is ``ZREVRANGE 0 9``, so the transfer is
    bounded by the window rather than by the number of distinct principals — which is the ``O(N)``
    the hash alternative would have paid on the endpoint polled every 5 seconds.
    """
    gateway = RecordingGateway()
    collect, _ = collector(settings, gateway, now_ms=LOCAL_NOW_MS)
    gateway.zsets[stats_top_key(minute_index(LOCAL_NOW_MS))] = {
        f"user-{index:02d}": 100.0 for index in range(25)
    }

    snapshot = await collect.snapshot(minutes=1, hours=0)

    assert len(snapshot.top_consumers) == TOP_CONSUMERS_LIMIT
    # Every score is equal, so the whole ordering is the tie-break — ascending by user id.
    assert [entry.user_id for entry in snapshot.top_consumers] == [
        f"user-{index:02d}" for index in range(TOP_CONSUMERS_LIMIT)
    ]
    # And only the bounded slice was ever asked for — FANOUT wide, LIMIT deep. See `_rank`: the
    # gap between the two is what stops a steady consumer who never tops a single minute from
    # vanishing out of the ranking entirely.
    zrange = next(c for c in gateway.commands if c[0] == "ZREVRANGE")
    assert zrange[2:] == (0, TOP_CONSUMERS_FANOUT - 1, True)
    assert TOP_CONSUMERS_FANOUT > TOP_CONSUMERS_LIMIT


async def test_a_steady_consumer_who_never_tops_a_minute_is_still_found(settings):
    """**Fix 4.** The reconstruction of the miss the C9 verification measured, now passing.

    Sixty minutes, ten burst callers per minute at 100 each, and one steady whale sitting *eleventh*
    every single minute at 99. Reading only the ten that get returned, the whale never enters a
    single slice: its real total was 5940 against a reported number-one of 100 — **59.4x, and
    absent from the list entirely**, with the error growing linearly in the window so that a
    60-minute panel is the worst case rather than an unusual one.

    Reading :data:`~src.analytics.TOP_CONSUMERS_FANOUT` per minute and merging down to ten finds it
    and ranks it first. This is not a proof of exactness — nothing bounded is — it is a proof that
    the shape ordinary traffic produces no longer defeats the ranking. A consumer ranked below 50th
    in *every* covered minute is still invisible; that is stated on ``_rank`` rather than tested,
    because it is the residual, not the fix.
    """
    gateway = RecordingGateway()
    collect, _ = collector(settings, gateway, now_ms=LOCAL_NOW_MS)
    newest = minute_index(LOCAL_NOW_MS)
    for offset in range(60):
        gateway.zsets[stats_top_key(newest - offset)] = {
            **{f"burst-{offset:02d}-{n}": 100.0 for n in range(10)},
            "whale": 99.0,
        }

    snapshot = await collect.snapshot(minutes=60, hours=0)

    top = snapshot.top_consumers[0]
    assert top.user_id == "whale"
    assert top.cost == 60 * 99
    # The bursts are each a single minute's worth, so the whale outranks them by ~59x — the exact
    # ratio the verifier measured while the whale was missing altogether.
    assert top.cost / snapshot.top_consumers[1].cost > 50


async def test_an_unrecognised_outcome_still_reaches_the_dimension_map(settings):
    """A bucket written by a newer replica keeps its own outcome name — and is not silently lost.

    ``by_outcome`` is the raw dimension and stays authoritative: an outcome this version has never
    heard of is folded into it under its own name. The three *named* counters on
    :class:`~src.models.StatsBucket` and :class:`~src.models.StatsTotals` cannot represent it and
    do not pretend to, which is why the map is kept alongside them rather than replaced by them.

    Rolling deploys are the whole reason: for the length of one, half the replicas are writing a
    field the other half has never seen, and the wrong behaviour is either to fail the read or to
    quietly drop the traffic out of the totals.
    """
    gateway = RecordingGateway()
    collect, _ = collector(settings, gateway, now_ms=LOCAL_NOW_MS)
    gateway.seed_bucket(
        stats_minute_key(minute_index(LOCAL_NOW_MS)),
        requests=5,
        cost=5,
        outcome__allowed=3,
        outcome__shadowed=2,
    )

    snapshot = await collect.snapshot(minutes=1, hours=0)

    assert snapshot.by_outcome == {
        OUTCOME_ALLOWED: 3,
        OUTCOME_DENIED: 0,
        OUTCOME_DEGRADED: 0,
        "shadowed": 2,
    }
    # The named counters carry only what they can name; the request total still carries everything.
    assert snapshot.per_minute[-1].allowed == 3
    assert snapshot.per_minute[-1].denied == 0
    assert snapshot.per_minute[-1].requests == 5
    assert snapshot.totals.requests == 5


# =============================================================================================
# 10. The tolerant decoders
#
# Tested directly, and that is the point rather than a shortcut: these three exist to survive
# inputs a HEALTHY server never produces, so there is no way to reach their tolerant branches
# through one. Their strict counterparts in `src.models` raise on the same inputs, deliberately —
# see `test_unreadable_and_unknown_fields_are_tolerated_not_fatal` for why the two decoders make
# opposite choices.
# =============================================================================================


def test_text_decodes_bytes_and_leaves_everything_else_readable():
    """``decode_responses`` is a gateway setting, and a deployment that flipped it must still work.

    The shipped client uses ``decode_responses=False`` so field names arrive as ``bytes``. A client
    built the other way hands back ``str``, and an integer reply hands back ``int``. A decoder that
    only handled ``bytes`` would turn either into a silently empty dashboard rather than an error —
    every field name would stringify to ``"b'requests'"`` and match nothing.
    """
    assert _text(b"requests") == "requests"
    assert _text("requests") == "requests"
    assert _text(429) == "429"
    # A mangled byte is a visibly wrong row, never a UnicodeDecodeError out of a stats read.
    assert _text(b"tier:\xff") == "tier:�"


def test_count_reads_a_counter_and_treats_the_unreadable_as_zero():
    """An unparseable counter under-reports. It does not fail the read.

    The failure direction is the safe one: a stats page showing slightly less traffic than happened
    is a smaller problem than a stats page showing an exception, and a hand-edited hash field is
    not evidence that the other 119 buckets are wrong.
    """
    assert _count(b"12") == 12
    assert _count("12") == 12
    assert _count(12) == 12
    assert _count(b"not-a-number") == 0
    assert _count(None) == 0


def test_score_rounds_rather_than_truncating():
    """ZSET scores are doubles; every score this service writes is an accumulated integer cost.

    ``round`` and not ``int``: truncation would read a score stored as ``4.999999999999999`` as
    ``4``, and the one thing a "top consumers" ranking must not do is under-report the top
    consumer.
    """
    assert _score(5.0) == 5
    assert _score(4.999999999999999) == 5
    assert _score(b"7") == 7
    assert _score(None) == 0
    assert _score("not-a-score") == 0


# =============================================================================================
# 11. Failures on the READ side propagate — unlike the write side
# =============================================================================================


async def test_snapshot_propagates_a_store_failure_instead_of_returning_zeros(settings):
    """A stats read that failed must **say so**, not report that traffic stopped.

    The opposite rule to :meth:`~src.analytics.AnalyticsCollector.record`, and for a reason that is
    about who is listening. A failed record has no caller left — the response is already sent. A
    failed snapshot has a dashboard in front of an operator, and answering it with zeros is the
    single most misleading thing an observability surface can do: it says the traffic stopped at
    exactly the moment someone is looking at this page to find out why it has not.
    """
    gateway = RecordingGateway(run_error=BackingStoreUnavailable("gone", op="analytics:snapshot"))
    collect, _ = collector(settings, gateway)

    with pytest.raises(BackingStoreUnavailable):
        await collect.snapshot(minutes=5, hours=1)
