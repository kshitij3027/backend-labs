"""Unit tests for :class:`~src.tiers.TierRegistry` — the cache in front of the tier table.

Four properties are asserted here, and each of them is a thing that would be *invisible* if it
regressed:

1. **``snapshot()`` performs no I/O and does not await.** Driven by a gateway stub that raises if
   touched, and by one test that calls it with no event loop running at all — you cannot await
   without a loop, so that call succeeding is proof rather than evidence.
2. **Single flight.** A hundred concurrent requests crossing the TTL boundary together must produce
   **one** Redis read, not a hundred. The assertion is a call counter.
3. **A stale snapshot is still served.** Past the TTL the caller gets the old value *immediately*;
   the refresh happens behind them. A registry that blocked here would put Redis latency back on
   the hot path for every in-flight request at once, every TTL.
4. **A garbage row cannot make a tier unlimited.** The decision script reads a non-positive limit
   as "unenforced", so a malformed value must fall back to the configured default rather than
   through it.

The clock is injected everywhere, so TTL behaviour is asserted exactly rather than slept for.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging

import pytest

from src.config import Settings, TierConfig
from src.keys import CONFIG_TIERS_KEY, CONFIG_VERSION_KEY
from src.redis_client import BackingStoreUnavailable
from src.tiers import TierRegistry, decode_tier, encode_tier, render_argv_tail

# --------------------------------------------------------------------------------------------
# Test doubles
# --------------------------------------------------------------------------------------------


class FakeClient:
    """The handful of commands the registry issues, backed by two plain Python values."""

    def __init__(self, owner: FakeGateway) -> None:
        self._owner = owner

    async def hgetall(self, key: str) -> dict[bytes, bytes]:
        assert key == CONFIG_TIERS_KEY
        self._owner.reads += 1
        # The reply is materialised HERE and returned later, which is what a real read does: the
        # bytes leave the server before anything that happens next. That ordering is the whole
        # subject of the invalidate-during-refresh race test — a write landing while the read is
        # in flight must NOT appear in this reply.
        reply = dict(self._owner.table)
        if self._owner.gate is not None:
            await self._owner.gate.wait()
        return reply

    async def get(self, key: str) -> bytes | None:
        assert key == CONFIG_VERSION_KEY
        return self._owner.version

    async def hset(self, key: str, field: str, value: str) -> int:
        assert key == CONFIG_TIERS_KEY
        created = field.encode() not in self._owner.table
        self._owner.table[field.encode()] = value.encode()
        self._owner.writes.append(("hset", field, value))
        return int(created)

    async def hsetnx(self, key: str, field: str, value: str) -> int:
        assert key == CONFIG_TIERS_KEY
        self._owner.writes.append(("hsetnx", field, value))
        if field.encode() in self._owner.table:
            return 0
        self._owner.table[field.encode()] = value.encode()
        return 1

    async def incr(self, key: str) -> int:
        assert key == CONFIG_VERSION_KEY
        self._owner.version = str(int(self._owner.version or b"0") + 1).encode()
        return int(self._owner.version)

    async def setnx(self, key: str, value: int) -> int:
        assert key == CONFIG_VERSION_KEY
        if self._owner.version is not None:
            return 0
        self._owner.version = str(value).encode()
        return 1


class FakeGateway:
    """A :class:`~src.redis_client.RedisGateway` stand-in that actually runs the factory.

    Running the factory rather than short-circuiting it matters: the registry's real command
    construction (``hgetall`` on the real key constant, ``hsetnx`` with the real encoded value) is
    then part of what these tests cover, instead of being replaced by a canned answer that would
    keep passing after the key name changed.
    """

    def __init__(
        self,
        *,
        table: dict[bytes, bytes] | None = None,
        version: bytes | None = b"7",
        error: Exception | None = None,
    ) -> None:
        self.table: dict[bytes, bytes] = dict(table or {})
        self.version = version
        self.error = error
        self.reads = 0
        self.writes: list[tuple[str, str, str]] = []
        #: When set, `hgetall` blocks on this event after taking its reply. Lets a test hold a
        #: refresh open at a chosen instant and drive a race deterministically instead of sleeping.
        self.gate: asyncio.Event | None = None
        self.client = FakeClient(self)

    async def run(self, coro_factory, *, op: str):
        if self.error is not None:
            raise self.error
        return await coro_factory()


class ExplodingGateway:
    """Fails the test if the registry touches Redis at all. Used to pin ``snapshot()``'s purity."""

    @property
    def client(self):  # pragma: no cover - reaching this IS the failure
        raise AssertionError("snapshot() must never touch Redis")

    async def run(self, coro_factory, *, op: str):  # pragma: no cover - same
        raise AssertionError("snapshot() must never touch Redis")


class FakeClock:
    """A monotonic clock a test can move. No sleeping, and the TTL boundary is hittable exactly."""

    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


def registry_for(settings: Settings, gateway, clock: FakeClock) -> TierRegistry:
    return TierRegistry(settings, gateway, clock=clock)  # type: ignore[arg-type]


#: `free:60:60:1000:25000,premium:...,enterprise:...` in the value form the HASH stores.
SEEDED_TABLE: dict[bytes, bytes] = {
    b"free": b"60|60|1000|25000",
    b"premium": b"300|300|50000|1250000",
    b"enterprise": b"1000|1000|500000|12500000",
}


# --------------------------------------------------------------------------------------------
# Pure encode / decode / render
# --------------------------------------------------------------------------------------------


def test_a_tier_round_trips_through_its_hash_value(settings: Settings):
    original = settings.tier_limits["free"]

    assert encode_tier(original) == "60|60|1000|25000"
    assert decode_tier("free", encode_tier(original)) == original


@pytest.mark.parametrize(
    ("raw", "match"),
    [
        ("60|60|1000", r"3 fields, expected 4"),
        ("60|60|1000|25000|9", r"5 fields, expected 4"),
        ("60|sixty|1000|25000", r"must be integers"),
        ("60|60|0|25000", r"must be positive"),
        ("60|60|-1|25000", r"must be positive"),
    ],
    ids=["too-few", "too-many", "non-numeric", "zero", "negative"],
)
def test_a_malformed_hash_value_is_refused_by_the_parser(raw, match):
    """A non-positive limit reads as 'unenforced' in the decision script, so 0 is not a number."""
    with pytest.raises(ValueError, match=match):
        decode_tier("free", raw)


def test_the_argv_tail_is_a_count_prefixed_flat_list_in_sorted_order(settings: Settings):
    """**The exact shape C4's Lua script indexes** — ARGV[10] = count, then 5 slots per tier.

    Written out literally rather than derived from the settings, because deriving it would make
    this assertion agree with the implementation by construction. Sorted by name so two replicas
    render byte-identical tails for identical config.
    """
    assert render_argv_tail(settings.tier_limits) == (
        "3",
        "enterprise",
        "1000",
        "1000",
        "500000",
        "12500000",
        "free",
        "60",
        "60",
        "1000",
        "25000",
        "premium",
        "300",
        "300",
        "50000",
        "1250000",
    )


def test_the_argv_tail_length_is_one_plus_five_per_tier(settings: Settings):
    tail = render_argv_tail(settings.tier_limits)

    assert int(tail[0]) == len(settings.tier_limits)
    assert len(tail) == 1 + 5 * len(settings.tier_limits)
    assert all(isinstance(item, str) for item in tail)


def test_an_empty_table_still_renders_a_count(settings: Settings):
    """The script reads ARGV[10] unconditionally; a missing count would shift every later index."""
    assert render_argv_tail({}) == ("0",)


# --------------------------------------------------------------------------------------------
# snapshot(): purity, staleness, single flight
# --------------------------------------------------------------------------------------------


def test_snapshot_needs_no_event_loop_and_never_touches_redis(settings: Settings, clock):
    """**The strongest available proof that `snapshot()` does not await.**

    This test is deliberately synchronous: there is no running event loop, so any `await` inside
    `snapshot()` would raise. The gateway raises on any access, so any I/O would fail too. It
    returning the configured table is therefore not "fast enough" — it is structurally I/O-free.
    """
    registry = registry_for(settings, ExplodingGateway(), clock)

    snapshot = registry.snapshot()

    assert snapshot.tiers == settings.tier_limits
    assert snapshot.version == 0
    assert snapshot.argv_tail == render_argv_tail(settings.tier_limits)
    assert registry._refresh_task is None


async def test_a_fresh_snapshot_is_served_without_scheduling_a_refresh(settings: Settings, clock):
    gateway = FakeGateway(table=SEEDED_TABLE)
    registry = registry_for(settings, gateway, clock)
    await registry.refresh()
    assert gateway.reads == 1

    clock.advance(settings.tier_cache_ttl_sec - 1)
    registry.snapshot()
    await asyncio.sleep(0)

    assert gateway.reads == 1
    assert registry._refresh_task is None


async def test_a_stale_snapshot_is_served_immediately_and_refreshed_behind_the_caller(
    settings: Settings, clock
):
    """The hot path must never pay Redis latency for tier config, even at the TTL boundary."""
    gateway = FakeGateway(table=SEEDED_TABLE, version=b"7")
    registry = registry_for(settings, gateway, clock)
    await registry.refresh()
    assert registry.snapshot().tiers["free"].rate_limit_per_min == 60

    # An operator lowers free's rpm out of band, and the TTL elapses.
    gateway.table[b"free"] = b"10|60|1000|25000"
    gateway.version = b"8"
    clock.advance(settings.tier_cache_ttl_sec)

    stale = registry.snapshot()

    # Served the OLD value, right now, without awaiting anything.
    assert stale.tiers["free"].rate_limit_per_min == 60
    assert stale.version == 7

    task = registry._refresh_task
    assert task is not None
    await task

    fresh = registry.snapshot()
    assert fresh.tiers["free"].rate_limit_per_min == 10
    assert fresh.version == 8
    assert registry.refreshes == 2


async def test_a_hundred_concurrent_stale_reads_trigger_exactly_one_refresh(
    settings: Settings, clock
):
    """**Single flight.** Without it, every in-flight request stampedes Redis at the TTL boundary.

    They all cross it in the same instant, which is precisely when a thundering herd is worst.
    """
    gateway = FakeGateway(table=SEEDED_TABLE)
    registry = registry_for(settings, gateway, clock)
    await registry.refresh()
    clock.advance(settings.tier_cache_ttl_sec)

    for _ in range(100):
        registry.snapshot()

    task = registry._refresh_task
    assert task is not None
    await task

    assert gateway.reads == 2  # the priming refresh, then exactly one more
    assert registry.refreshes == 2


async def test_invalidate_forces_the_next_snapshot_to_refresh(settings: Settings, clock):
    """C10 calls this on the replica that served a PUT so it does not serve what it just replaced.

    An operator watching the response of their own change and seeing the old number is how people
    conclude hot reload does not work.
    """
    gateway = FakeGateway(table=SEEDED_TABLE, version=b"7")
    registry = registry_for(settings, gateway, clock)
    await registry.refresh()

    # Well inside the TTL: without invalidate() this snapshot would be served for 5 more seconds.
    gateway.table[b"free"] = b"10|60|1000|25000"
    gateway.version = b"8"
    registry.invalidate()
    registry.snapshot()

    task = registry._refresh_task
    assert task is not None
    await task

    assert registry.snapshot().tiers["free"].rate_limit_per_min == 10
    assert registry.snapshot().version == 8


async def test_a_refresh_is_not_scheduled_after_the_registry_is_stopped(
    settings: Settings, clock
):
    """A task started during shutdown either gets cancelled a moment later or logs a false outage."""
    registry = registry_for(settings, FakeGateway(table=SEEDED_TABLE), clock)
    await registry.stop()

    registry.snapshot()

    assert registry._refresh_task is None


# --------------------------------------------------------------------------------------------
# refresh(): parsing, fallback, failure
# --------------------------------------------------------------------------------------------


async def test_refresh_reads_the_live_table_and_rebuilds_the_argv_tail(
    settings: Settings, clock
):
    gateway = FakeGateway(
        table={**SEEDED_TABLE, b"free": b"10|20|30|40"},
        version=b"12",
    )
    registry = registry_for(settings, gateway, clock)

    snapshot = await registry.refresh()

    assert snapshot.version == 12
    assert snapshot.tiers["free"] == TierConfig(
        name="free", rate_limit_per_min=10, burst=20, daily_quota=30, monthly_quota=40
    )
    # The tail is rebuilt from the LIVE table, not from settings.
    assert snapshot.argv_tail[snapshot.argv_tail.index("free") + 1] == "10"
    assert snapshot.fetched_monotonic == clock.now


async def test_a_tier_missing_from_redis_falls_back_to_the_configured_one(
    settings: Settings, clock
):
    """A partially populated hash must not produce a table with a tier missing entirely.

    A principal on a missing tier has no ceiling to look up, and 'no limit found' is
    indistinguishable from 'unlimited' at the point the decision is made.
    """
    gateway = FakeGateway(table={b"free": b"10|10|10|10"})
    registry = registry_for(settings, gateway, clock)

    snapshot = await registry.refresh()

    assert set(snapshot.tiers) == set(settings.tier_limits)
    assert snapshot.tiers["premium"] == settings.tier_limits["premium"]


async def test_a_malformed_row_falls_back_to_the_settings_default_and_logs_an_error(
    settings: Settings, clock, caplog
):
    """**A garbage row must not be able to make a tier unlimited.**

    `0|0|0|0` would read as "every gate off" in the decision script — an unmetered tier produced by
    a typo. The configured default is used instead, and the event is loud.
    """
    gateway = FakeGateway(table={**SEEDED_TABLE, b"free": b"0|0|0|0"})
    registry = registry_for(settings, gateway, clock)

    with caplog.at_level(logging.ERROR, logger="src.tiers"):
        snapshot = await registry.refresh()

    assert snapshot.tiers["free"] == settings.tier_limits["free"]
    assert snapshot.tiers["free"].daily_quota == 1000
    assert snapshot.tiers["free"].rate_limit_per_min > 0
    assert "free" in caplog.text
    assert any(record.levelno == logging.ERROR for record in caplog.records)
    # The other tiers are unaffected: the blast radius of one bad value is one tier.
    assert snapshot.tiers["premium"].rate_limit_per_min == 300


async def test_a_malformed_row_for_an_unknown_tier_is_dropped_loudly(
    settings: Settings, clock, caplog
):
    """There is no default to fall back to, and inventing numbers would be guessing at billing."""
    gateway = FakeGateway(table={**SEEDED_TABLE, b"platinum": b"not-a-tier"})
    registry = registry_for(settings, gateway, clock)

    with caplog.at_level(logging.ERROR, logger="src.tiers"):
        snapshot = await registry.refresh()

    assert "platinum" not in snapshot.tiers
    assert "platinum" in caplog.text


@pytest.mark.parametrize(
    "raw_name",
    [b"", b" ", b"   ", b"pre mium", b"free\n", b"free\t", b"fr\x00ee", b"free\x7f"],
    ids=[
        "empty",
        "one-space",
        "whitespace-only",
        "interior-space",
        "trailing-newline",
        "tab",
        "nul",
        "del",
    ],
)
async def test_a_row_with_an_unusable_tier_name_is_skipped_and_reported(
    settings: Settings, clock, caplog, raw_name
):
    """A tier name is a hot-path lookup key and an ARGV element the decision script indexes.

    Such a tier is inert — nothing can be assigned to a name a caller cannot type — so this is
    hygiene rather than a hole. But it would still be rendered into `argv_tail` and shipped to
    Redis, and Lua defending itself against `"free\\n"` is code nobody should have to write.

    Skipped and **reported**, never normalised: silently rewriting `"pre mium"` to `"premium"`
    would guess at which tier an operator meant and could quietly re-price a principal.
    """
    gateway = FakeGateway(table={**SEEDED_TABLE, raw_name: b"60|60|1000|25000"})
    registry = registry_for(settings, gateway, clock)

    with caplog.at_level(logging.ERROR, logger="src.tiers"):
        snapshot = await registry.refresh()

    assert set(snapshot.tiers) == set(settings.tier_limits)
    assert int(snapshot.argv_tail[0]) == 3
    assert len(snapshot.argv_tail) == 16
    assert "unusable tier name" in caplog.text
    # The legitimate rows in the same HASH are unaffected.
    assert snapshot.tiers["premium"].rate_limit_per_min == 300


async def test_a_well_formed_row_for_an_unknown_tier_is_adopted(settings: Settings, clock):
    """Tiers are runtime data — C10 can create one, and a restart must not lose it."""
    gateway = FakeGateway(table={**SEEDED_TABLE, b"platinum": b"5000|5000|1000000|25000000"})
    registry = registry_for(settings, gateway, clock)

    snapshot = await registry.refresh()

    assert snapshot.tiers["platinum"].rate_limit_per_min == 5000
    assert int(snapshot.argv_tail[0]) == 4


async def test_the_parser_does_not_depend_on_the_clients_decode_setting(
    settings: Settings, clock
):
    """`decode_responses=False` is the production setting, but the parser must not require it.

    A diagnostic client, a future double, or a redis-py default change would hand back `str`
    instead of `bytes`, and a parser that only understood one of the two would fall back to the
    configured defaults for every tier while logging that the live table was malformed — a silent
    reversion of every runtime change, caused by an encoding.
    """
    gateway = FakeGateway(table={"free": "11|12|13|14"}, version="5")  # type: ignore[arg-type]
    registry = registry_for(settings, gateway, clock)

    snapshot = await registry.refresh()

    assert snapshot.tiers["free"].rate_limit_per_min == 11
    assert snapshot.tiers["free"].monthly_quota == 14
    assert snapshot.version == 5


@pytest.mark.parametrize("raw_version", [None, b"not-a-number"], ids=["missing", "garbage"])
async def test_an_unreadable_version_keeps_the_previous_number(
    settings: Settings, clock, raw_version
):
    """C10 watches this number climb; a phantom rollback would read as a failed reload."""
    gateway = FakeGateway(table=SEEDED_TABLE, version=b"9")
    registry = registry_for(settings, gateway, clock)
    await registry.refresh()

    gateway.version = raw_version
    snapshot = await registry.refresh()

    assert snapshot.version == 9


async def test_an_outage_keeps_the_last_good_snapshot(settings: Settings, clock, caplog):
    """There is no version of 'we could not read the config' improved by forgetting the config."""
    gateway = FakeGateway(table={**SEEDED_TABLE, b"free": b"11|11|11|11"}, version=b"4")
    registry = registry_for(settings, gateway, clock)
    good = await registry.refresh()

    gateway.error = BackingStoreUnavailable("redis is down", op="tiers:read")
    with caplog.at_level(logging.WARNING, logger="src.tiers"):
        after = await registry.refresh()

    assert after is good
    assert after.tiers["free"].rate_limit_per_min == 11
    assert after.version == 4
    assert registry.refresh_failures == 1
    assert registry.refreshes == 1
    assert "serving the last known table" in caplog.text


def _refresh_warnings(caplog) -> list[str]:
    """Every "tier refresh failed" WARNING emitted so far, in order."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "src.tiers"
        and record.levelno == logging.WARNING
        and "tier refresh failed" in record.getMessage()
    ]


async def test_a_failing_refresh_backs_off_instead_of_retrying_on_every_request(
    settings: Settings, clock, caplog
):
    """**A failed refresh must not turn every subsequent request into a task and a log line.**

    A failure leaves the snapshot stale, and staleness is what schedules refreshes — so without a
    backoff the *permanent* staleness of an outage makes `snapshot()` allocate an `asyncio.Task`
    and write a synchronous line to stdout on **every single call**. At the project's 1000 rps
    target that is a thousand of each per second, during precisely the outage the fail-open path
    exists to ride out. The circuit breaker keeps the real Redis I/O at zero, so this is throughput
    and observability rather than correctness: the service gets slower and noisier exactly when it
    is already degraded, and `refresh_failures` stops counting *failures* and starts counting
    *requests* — destroying the one signal whose job is spotting a registry that has silently
    stopped refreshing.

    60 reads across two backoff windows: exactly two attempts, exactly two warnings.
    """
    gateway = FakeGateway(error=BackingStoreUnavailable("redis is down", op="tiers:read"))
    registry = registry_for(settings, gateway, clock)

    with caplog.at_level(logging.WARNING, logger="src.tiers"):
        for _ in range(30):
            registry.snapshot()
        first = registry._refresh_task
        assert first is not None
        await first

        assert registry.refresh_failures == 1
        assert len(_refresh_warnings(caplog)) == 1

        # Still inside the window: thirty more requests must cost nothing at all.
        for _ in range(30):
            registry.snapshot()
        assert registry._refresh_task is first
        assert registry.refresh_failures == 1
        assert len(_refresh_warnings(caplog)) == 1

        # Window elapsed: exactly one more attempt.
        clock.advance(registry._refresh_backoff_sec)
        registry.snapshot()
        second = registry._refresh_task
        assert second is not None and second is not first
        await second

    assert registry.refresh_failures == 2
    assert len(_refresh_warnings(caplog)) == 2
    assert "suppressing further attempts" in _refresh_warnings(caplog)[0]
    await registry.stop()


async def test_a_successful_refresh_clears_the_backoff_immediately(settings: Settings, clock):
    """Recovery must not be delayed by a window that was armed for a store which is now answering.

    Also pins that an explicitly awaited `refresh()` is never gated by the backoff: C10's
    `POST /admin/config/reload` is an operator action, not a per-request event, and refusing to try
    because a background attempt failed four seconds ago would make the reload button lie.
    """
    gateway = FakeGateway(
        table=SEEDED_TABLE, error=BackingStoreUnavailable("redis is down", op="tiers:read")
    )
    registry = registry_for(settings, gateway, clock)
    await registry.refresh()
    assert registry._refresh_backoff_until > clock.now

    gateway.error = None
    await registry.refresh()

    assert registry._refresh_backoff_until == 0.0
    assert registry.refreshes == 1

    # And scheduling works again on the very next TTL boundary rather than after the old window.
    clock.advance(settings.tier_cache_ttl_sec)
    registry.snapshot()
    task = registry._refresh_task
    assert task is not None
    await task

    assert registry.refreshes == 2


async def test_an_invalidate_during_an_in_flight_refresh_is_not_lost(
    settings: Settings, clock
):
    """**The race C10 hits on any admin write that overlaps a background refresh.**

    A refresh reads `config:tiers`; the operator's PUT lands while the reply is on the wire and
    calls `invalidate()`; the refresh then completes and installs a snapshot built from bytes that
    left Redis *before* the write. Clearing the stale flag there would erase the invalidation — the
    replica that served the operator's own change then shows the old number for a full TTL with
    nothing scheduled to correct it, which is the exact scenario `invalidate()` exists to prevent.

    Driven deterministically with a gate rather than a sleep: the refresh is held open at a chosen
    instant, so the test either passes or fails rather than flaking.
    """
    gateway = FakeGateway(table=SEEDED_TABLE, version=b"1")
    registry = registry_for(settings, gateway, clock)
    await registry.refresh()
    assert gateway.reads == 1

    gateway.gate = asyncio.Event()
    in_flight = asyncio.create_task(registry.refresh())
    while gateway.reads < 2:  # wait until the read is genuinely in flight
        await asyncio.sleep(0)

    # The admin PUT lands NOW: it writes to Redis and invalidates this replica's snapshot.
    gateway.table[b"free"] = b"10|60|1000|25000"
    gateway.version = b"2"
    registry.invalidate()

    gateway.gate.set()
    await in_flight

    # The in-flight read could not have seen the write, so it installed the pre-edit value...
    assert registry._snapshot.tiers["free"].rate_limit_per_min == 60
    # ...and the invalidation survived it, so another refresh is still owed.
    assert registry._stale is True

    registry.snapshot()
    follow_up = registry._refresh_task
    assert follow_up is not None
    await follow_up

    assert gateway.reads == 3
    assert registry.snapshot().tiers["free"].rate_limit_per_min == 10
    assert registry.snapshot().version == 2
    assert registry._stale is False


async def test_an_uncontended_refresh_clears_the_stale_flag(settings: Settings, clock):
    """The other side of the counter check: with no invalidation racing it, staleness is cleared.

    Without this, a registry that never cleared the flag would satisfy the race test above and
    still refresh on every request forever.
    """
    gateway = FakeGateway(table=SEEDED_TABLE)
    registry = registry_for(settings, gateway, clock)
    assert registry._stale is True

    await registry.refresh()

    assert registry._stale is False
    registry.snapshot()
    assert registry._refresh_task is None


async def test_with_no_good_snapshot_and_redis_down_the_settings_table_is_served(
    settings: Settings, clock, caplog
):
    """**The process must start and enforce SOMETHING rather than crash-loop.**

    The configured defaults are a legitimate source of truth — just not the authoritative one. A
    replica that refuses to boot because it cannot read its limits enforces nothing at all, on
    every request, for as long as the loop lasts.
    """
    gateway = FakeGateway(error=BackingStoreUnavailable("redis is down", op="tiers:read"))
    registry = registry_for(settings, gateway, clock)

    with caplog.at_level(logging.WARNING, logger="src.tiers"):
        await registry.start()

    snapshot = registry.snapshot()
    await registry.stop()

    assert snapshot.tiers == settings.tier_limits
    assert snapshot.version == 0
    assert snapshot.argv_tail == render_argv_tail(settings.tier_limits)
    assert registry.refreshes == 0


async def test_a_correctness_error_during_startup_does_not_crash_the_process(
    settings: Settings, clock, caplog
):
    """A WRONGTYPE on `config:tiers` is a bug, but a crash-looping replica meters nothing.

    Also pins that a failed *seed* does not skip the *refresh*: the two are attempted
    independently, because a replica that cannot write can still read the table another replica
    seeded.
    """
    gateway = FakeGateway(error=RuntimeError("WRONGTYPE"))
    registry = registry_for(settings, gateway, clock)

    with caplog.at_level(logging.ERROR, logger="src.tiers"):
        await registry.start()

    served = registry.snapshot()
    await registry.stop()

    assert served.tiers == settings.tier_limits
    assert "tier registry seeding failed" in caplog.text
    assert "initial tier refresh failed" in caplog.text


async def test_a_failing_background_refresh_is_logged_and_clears_its_in_flight_flag(
    settings: Settings, clock, caplog
):
    """A detached task that vanishes silently would leave the registry permanently un-refreshing.

    The `finally` clears the flag either way, so without the log the registry would look healthy
    while never reading Redis again.
    """
    gateway = FakeGateway(table=SEEDED_TABLE)
    registry = registry_for(settings, gateway, clock)
    await registry.refresh()

    gateway.error = RuntimeError("WRONGTYPE")
    clock.advance(settings.tier_cache_ttl_sec)
    registry.snapshot()

    task = registry._refresh_task
    assert task is not None
    with caplog.at_level(logging.ERROR, logger="src.tiers"):
        await task

    assert registry.refresh_failures == 1
    assert registry._refreshing is False
    assert "background tier refresh failed" in caplog.text
    # And the previous snapshot is still being served.
    assert registry.snapshot().tiers["free"].rate_limit_per_min == 60
    await registry.stop()


# --------------------------------------------------------------------------------------------
# seed()
# --------------------------------------------------------------------------------------------


async def test_seed_uses_hsetnx_and_bumps_the_version_when_it_writes(
    settings: Settings, clock
):
    gateway = FakeGateway(version=None)
    registry = registry_for(settings, gateway, clock)

    await registry.seed()

    assert {command for command, _, _ in gateway.writes} == {"hsetnx"}
    assert gateway.table[b"free"] == b"60|60|1000|25000"
    assert gateway.version == b"1"


async def test_seeding_twice_writes_nothing_the_second_time(settings: Settings, clock):
    """**The HSETNX contract.** With HSET, every replica restart silently reverts an operator."""
    gateway = FakeGateway(version=None)
    registry = registry_for(settings, gateway, clock)
    await registry.seed()

    gateway.writes.clear()
    await registry.seed()

    assert all(command == "hsetnx" for command, _, _ in gateway.writes)
    assert gateway.table[b"free"] == b"60|60|1000|25000"
    # Nothing changed, so the version must not move — a bump would invalidate every replica's
    # snapshot on every restart, for nothing.
    assert gateway.version == b"1"


async def test_reseed_overwrites_and_bumps_the_version(settings: Settings, clock):
    gateway = FakeGateway(table={b"free": b"1|1|1|1"}, version=b"3")
    registry = registry_for(settings, gateway, clock)

    await registry.seed(reseed=True)

    assert {command for command, _, _ in gateway.writes} == {"hset"}
    assert gateway.table[b"free"] == b"60|60|1000|25000"
    assert gateway.version == b"4"


# --------------------------------------------------------------------------------------------
# Lifecycle
# --------------------------------------------------------------------------------------------


async def test_start_seeds_then_takes_the_first_snapshot(settings: Settings, clock):
    gateway = FakeGateway(version=None)
    registry = registry_for(settings, gateway, clock)

    await registry.start()

    assert gateway.table[b"premium"] == b"300|300|50000|1250000"
    assert registry.snapshot().version == 1
    assert registry.refreshes == 1


async def test_stop_cancels_an_in_flight_refresh(settings: Settings, clock):
    """No orphaned tasks at shutdown, and the gateway's pool is not closed under a live borrower."""
    gateway = FakeGateway(table=SEEDED_TABLE)
    registry = registry_for(settings, gateway, clock)
    await registry.refresh()
    clock.advance(settings.tier_cache_ttl_sec)
    registry.snapshot()
    task = registry._refresh_task
    assert task is not None

    await registry.stop()

    assert task.cancelled() or task.done()
    assert registry._refresh_task is None
    assert registry._refreshing is False


async def test_stop_is_safe_with_nothing_in_flight(settings: Settings, clock):
    registry = registry_for(settings, FakeGateway(), clock)

    await registry.stop()
    await registry.stop()

    assert registry._refresh_task is None


async def test_stop_tolerates_an_already_finished_task(settings: Settings, clock):
    gateway = FakeGateway(table=SEEDED_TABLE)
    registry = registry_for(settings, gateway, clock)
    clock.advance(settings.tier_cache_ttl_sec)
    registry.snapshot()
    task = registry._refresh_task
    assert task is not None
    await task

    await registry.stop()

    assert task.done()


async def test_the_snapshot_object_is_replaced_never_mutated(settings: Settings, clock):
    """A concurrent reader must see the whole old table or the whole new one, never a half-swap."""
    gateway = FakeGateway(table=SEEDED_TABLE)
    registry = registry_for(settings, gateway, clock)

    first = await registry.refresh()
    gateway.table[b"free"] = b"7|7|7|7"
    second = await registry.refresh()

    assert first is not second
    assert first.tiers["free"].rate_limit_per_min == 60
    assert second.tiers["free"].rate_limit_per_min == 7
    with pytest.raises(dataclasses.FrozenInstanceError):
        first.version = 99  # type: ignore[misc]


async def test_the_published_tier_table_cannot_be_written_through(settings: Settings, clock):
    """**Immutability is enforced, not merely argued for.**

    `frozen=True` stops the *field* being rebound; it does nothing about the dict behind it. Any
    holder of a snapshot — C4's limiter, C10's admin handler — could otherwise write
    `snapshot().tiers["free"] = ...` and mutate the table every concurrent request is reading, from
    a call site that never went near this module. The read-only view makes that a TypeError at the
    responsible line instead of a limit that changed for reasons nobody can reconstruct.
    """
    registry = registry_for(settings, FakeGateway(table=SEEDED_TABLE), clock)
    snapshot = await registry.refresh()

    with pytest.raises(TypeError):
        snapshot.tiers["free"] = settings.tier_limits["enterprise"]  # type: ignore[index]
    with pytest.raises(TypeError):
        del snapshot.tiers["free"]  # type: ignore[attr-defined]

    assert snapshot.tiers["free"].rate_limit_per_min == 60
    # Still reads as an ordinary mapping everywhere else, so nothing downstream has to care.
    assert dict(snapshot.tiers) == registry.snapshot().tiers
    assert sorted(snapshot.tiers) == ["enterprise", "free", "premium"]
