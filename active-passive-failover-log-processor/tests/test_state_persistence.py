"""Tests for src/state_persistence.py — snapshot writes, loads, lifecycle."""

from __future__ import annotations

import asyncio
import logging

import pytest

from src.log_processor import LogProcessor
from src.models import NodeState, StateSnapshot, from_json, to_json
from src.redis_client import RedisClient
from src.state_persistence import StatePersister


# =========================================================================
# Helpers
# =========================================================================


class _StateBox:
    """Mutable holder for ``state_provider`` callable.

    Mirrors the pattern used in tests/test_heartbeat.py — gives the test
    body fine-grained control over what state the persister sees on each
    tick without resorting to monkeypatching.
    """

    def __init__(self, initial: NodeState) -> None:
        self.value: NodeState = initial

    def __call__(self) -> NodeState:
        return self.value


# =========================================================================
# snapshot_now()
# =========================================================================


async def test_snapshot_now_writes_when_primary(
    fake_redis_client: RedisClient,
) -> None:
    state = _StateBox(NodeState.PRIMARY)
    lp = LogProcessor()
    lp.ingest("a")
    lp.ingest("b")

    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=lp,
        state_provider=state,
        sync_interval=5.0,
    )

    written = await persister.snapshot_now()
    assert written is True
    assert persister.snapshots_written_total == 1

    # Round-trip the persisted blob and verify it carries the live counters.
    raw = await fake_redis_client.get_snapshot()
    assert raw is not None
    snap = from_json(StateSnapshot, raw)
    assert snap.version == 1
    assert snap.log_count == 2
    assert snap.last_log_id == 2
    assert snap.taken_at > 0
    assert snap.watermark > 0


async def test_snapshot_now_noop_when_standby(
    fake_redis_client: RedisClient,
) -> None:
    state = _StateBox(NodeState.STANDBY)
    lp = LogProcessor()
    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=lp,
        state_provider=state,
        sync_interval=5.0,
    )

    written = await persister.snapshot_now()
    assert written is False
    assert persister.snapshots_written_total == 0
    # Nothing landed in Redis.
    assert await fake_redis_client.get_snapshot() is None


@pytest.mark.parametrize(
    "skipped_state",
    [NodeState.INACTIVE, NodeState.ELECTION, NodeState.FAILED],
)
async def test_snapshot_now_noop_in_other_non_primary_states(
    fake_redis_client: RedisClient,
    skipped_state: NodeState,
) -> None:
    state = _StateBox(skipped_state)
    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=LogProcessor(),
        state_provider=state,
        sync_interval=5.0,
    )
    assert await persister.snapshot_now() is False
    assert persister.snapshots_written_total == 0
    assert await fake_redis_client.get_snapshot() is None


async def test_snapshot_counter_increments_per_call(
    fake_redis_client: RedisClient,
) -> None:
    state = _StateBox(NodeState.PRIMARY)
    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=LogProcessor(),
        state_provider=state,
        sync_interval=5.0,
    )
    for _ in range(4):
        assert await persister.snapshot_now() is True
    assert persister.snapshots_written_total == 4


# =========================================================================
# load_into()
# =========================================================================


async def test_load_into_returns_false_when_no_snapshot(
    fake_redis_client: RedisClient,
) -> None:
    state = _StateBox(NodeState.STANDBY)
    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=LogProcessor(),
        state_provider=state,
    )
    assert await persister.load_into(LogProcessor()) is False
    assert persister.snapshots_loaded_total == 0


async def test_load_into_seeds_next_id_and_seen_ids(
    fake_redis_client: RedisClient,
) -> None:
    """A snapshot with last_log_id=42 should seed _next_id=43 and
    backfill _seen_ids with {1..42}."""
    snap = StateSnapshot(
        version=1,
        log_count=42,
        last_log_id=42,
        watermark=1.0,
        taken_at=1.0,
    )
    await fake_redis_client.put_snapshot(to_json(snap))

    target = LogProcessor()
    state = _StateBox(NodeState.STANDBY)
    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=LogProcessor(),
        state_provider=state,
    )

    loaded = await persister.load_into(target)
    assert loaded is True
    assert persister.snapshots_loaded_total == 1
    assert target._next_id == 43
    # Every id in [1, 42] should now dedup.
    assert all(i in target._seen_ids for i in range(1, 43))
    # 0 is NOT seeded — we only backfill positive ids.
    assert 0 not in target._seen_ids
    # 43 is NOT seeded — that's the next allocator slot.
    assert 43 not in target._seen_ids


async def test_load_into_takes_max_of_current_and_snapshot_next_id(
    fake_redis_client: RedisClient,
) -> None:
    """Don't regress the allocator: if local _next_id is already higher
    than snap.last_log_id+1, keep the local value."""
    snap = StateSnapshot(
        version=1,
        log_count=3,
        last_log_id=3,
        watermark=1.0,
        taken_at=1.0,
    )
    await fake_redis_client.put_snapshot(to_json(snap))

    target = LogProcessor()
    # Manually advance the allocator past the snapshot's last_log_id+1.
    target._next_id = 100

    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=LogProcessor(),
        state_provider=_StateBox(NodeState.STANDBY),
    )
    assert await persister.load_into(target) is True
    # _next_id stays at 100 — we don't go BACKWARD.
    assert target._next_id == 100


async def test_load_into_returns_false_on_version_mismatch(
    fake_redis_client: RedisClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    snap = StateSnapshot(
        version=999,
        log_count=10,
        last_log_id=10,
        watermark=1.0,
        taken_at=1.0,
    )
    await fake_redis_client.put_snapshot(to_json(snap))

    target = LogProcessor()
    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=LogProcessor(),
        state_provider=_StateBox(NodeState.STANDBY),
        schema_version=1,
    )

    with caplog.at_level(logging.WARNING):
        loaded = await persister.load_into(target)
    assert loaded is False
    assert persister.snapshots_loaded_total == 0
    # Allocator was NOT seeded.
    assert target._next_id == 1
    # And a warning was logged.
    assert any(
        "schema version mismatch" in rec.message for rec in caplog.records
    )


async def test_load_into_skips_seen_ids_backfill_for_huge_last_log_id(
    fake_redis_client: RedisClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If snap.last_log_id is absurdly large, skip the _seen_ids backfill
    but still seed _next_id so allocation continues correctly."""
    huge_id = 2_000_000  # well above _SEEN_IDS_BACKFILL_LIMIT (1_000_000)
    snap = StateSnapshot(
        version=1,
        log_count=huge_id,
        last_log_id=huge_id,
        watermark=1.0,
        taken_at=1.0,
    )
    await fake_redis_client.put_snapshot(to_json(snap))

    target = LogProcessor()
    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=LogProcessor(),
        state_provider=_StateBox(NodeState.STANDBY),
    )

    with caplog.at_level(logging.WARNING):
        loaded = await persister.load_into(target)
    assert loaded is True
    assert persister.snapshots_loaded_total == 1
    # _next_id was seeded.
    assert target._next_id == huge_id + 1
    # _seen_ids was NOT backfilled (would be ~2M ints).
    assert len(target._seen_ids) == 0
    # And a warning was logged.
    assert any(
        "exceeds backfill limit" in rec.message for rec in caplog.records
    )


async def test_load_into_with_zero_last_log_id_is_a_noop_seed(
    fake_redis_client: RedisClient,
) -> None:
    """An empty-counter snapshot (last_log_id=0) loads but seeds nothing."""
    snap = StateSnapshot(
        version=1,
        log_count=0,
        last_log_id=0,
        watermark=1.0,
        taken_at=1.0,
    )
    await fake_redis_client.put_snapshot(to_json(snap))

    target = LogProcessor()
    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=LogProcessor(),
        state_provider=_StateBox(NodeState.STANDBY),
    )

    assert await persister.load_into(target) is True
    # _next_id stays at 1 (already at last_log_id + 1 = 1).
    assert target._next_id == 1
    # No ids backfilled (range(1, 1) is empty).
    assert len(target._seen_ids) == 0


# =========================================================================
# Lifecycle: start/stop, periodic loop
# =========================================================================


async def test_start_writes_at_least_one_snapshot_when_primary(
    fake_redis_client: RedisClient,
) -> None:
    state = _StateBox(NodeState.PRIMARY)
    lp = LogProcessor()
    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=lp,
        state_provider=state,
        sync_interval=0.05,  # tight for the test
    )

    await persister.start()
    try:
        # Wait up to ~1s for at least 1-2 snapshots to land.
        for _ in range(40):
            if persister.snapshots_written_total >= 1:
                break
            await asyncio.sleep(0.025)
        assert persister.snapshots_written_total >= 1
        assert await fake_redis_client.get_snapshot() is not None
    finally:
        await persister.stop()


async def test_start_idle_when_standby(
    fake_redis_client: RedisClient,
) -> None:
    state = _StateBox(NodeState.STANDBY)
    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=LogProcessor(),
        state_provider=state,
        sync_interval=0.05,
    )

    await persister.start()
    try:
        # Give the loop several ticks to confirm it's not writing.
        await asyncio.sleep(0.2)
        assert persister.snapshots_written_total == 0
        assert await fake_redis_client.get_snapshot() is None
    finally:
        await persister.stop()


async def test_stop_is_idempotent(
    fake_redis_client: RedisClient,
) -> None:
    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=LogProcessor(),
        state_provider=_StateBox(NodeState.STANDBY),
        sync_interval=0.05,
    )
    await persister.start()
    await persister.stop()
    # Second stop must not raise.
    await persister.stop()
    assert persister._task is None


async def test_double_start_is_idempotent(
    fake_redis_client: RedisClient,
) -> None:
    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=LogProcessor(),
        state_provider=_StateBox(NodeState.STANDBY),
        sync_interval=0.05,
    )
    await persister.start()
    first_task = persister._task
    # Calling start() again must NOT spawn a second task.
    await persister.start()
    second_task = persister._task
    assert first_task is second_task
    await persister.stop()


async def test_loop_resumes_when_state_flips_to_primary(
    fake_redis_client: RedisClient,
) -> None:
    """STANDBY → PRIMARY mid-flight should let the loop resume writing
    without restart (mirrors the heartbeat-emitter idle pattern)."""
    state = _StateBox(NodeState.STANDBY)
    persister = StatePersister(
        redis_client=fake_redis_client,
        log_processor=LogProcessor(),
        state_provider=state,
        sync_interval=0.05,
    )
    await persister.start()
    try:
        # Confirm it stays quiet.
        await asyncio.sleep(0.15)
        assert persister.snapshots_written_total == 0

        # Promote.
        state.value = NodeState.PRIMARY
        for _ in range(40):
            if persister.snapshots_written_total >= 1:
                break
            await asyncio.sleep(0.025)
        assert persister.snapshots_written_total >= 1
    finally:
        await persister.stop()


# =========================================================================
# Counters round-trip
# =========================================================================


async def test_counters_roundtrip_through_redis(
    fake_redis_client: RedisClient,
) -> None:
    """Persister A writes; persister B reads — the counters propagate
    through Redis even though A's instance counter doesn't bleed into B."""
    lp_writer = LogProcessor()
    lp_writer.ingest("x")
    lp_writer.ingest("y")
    lp_writer.ingest("z")

    writer = StatePersister(
        redis_client=fake_redis_client,
        log_processor=lp_writer,
        state_provider=_StateBox(NodeState.PRIMARY),
    )
    await writer.snapshot_now()
    assert writer.snapshots_written_total == 1

    target = LogProcessor()
    reader = StatePersister(
        redis_client=fake_redis_client,
        log_processor=LogProcessor(),
        state_provider=_StateBox(NodeState.STANDBY),
    )
    assert await reader.load_into(target) is True
    assert reader.snapshots_loaded_total == 1
    # Counters travelled across the snapshot.
    assert target._next_id == 4  # last_log_id was 3
    # Writer counter stays unaffected.
    assert writer.snapshots_loaded_total == 0
