"""Tests for the consumer module — LogConsumer and ConsumerManager."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest

from src.config import Config
from src.consumer import ConsumerManager, LogConsumer
from src.metrics import MetricsAggregator
from src.processor import LogProcessor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_LOG = (
    '192.168.1.1 - - [10/Mar/2026:13:55:36 +0000] '
    '"GET /api/users HTTP/1.1" 200 1234 "-" "curl/7.68" 45.2'
)


def _make_consumer(
    fake_redis,
    config: Config,
    consumer_id: str = "test-consumer-0",
    worker_index: int = 0,
) -> LogConsumer:
    """Build a LogConsumer wired to fakeredis."""
    processor = LogProcessor()
    metrics = MetricsAggregator(window_sec=config.metrics_window_sec)
    return LogConsumer(
        consumer_id, fake_redis, config, processor, metrics,
        worker_index=worker_index,
    )


# ---------------------------------------------------------------------------
# Tests — LogConsumer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_group_creates_group(fake_redis, config):
    """Consumer creates the consumer group on a fresh Redis instance."""
    consumer = _make_consumer(fake_redis, config)
    await consumer._ensure_group()

    # Verify group exists by querying XINFO GROUPS
    groups = await fake_redis.xinfo_groups(config.stream_key)
    group_names = [g["name"] for g in groups]
    assert config.consumer_group in group_names


@pytest.mark.asyncio
async def test_ensure_group_busygroup(fake_redis, config):
    """Calling _ensure_group twice does not raise (BUSYGROUP is handled)."""
    consumer = _make_consumer(fake_redis, config)
    await consumer._ensure_group()
    # Second call should silently succeed
    await consumer._ensure_group()


@pytest.mark.asyncio
async def test_consume_and_ack(fake_redis, config):
    """Add a message to the stream, consumer processes and ACKs it."""
    # Seed the stream with a message before the consumer starts
    await fake_redis.xadd(config.stream_key, {"log": SAMPLE_LOG})

    consumer = _make_consumer(fake_redis, config)

    # Run the consumer for a short time so it can pick up the message
    task = asyncio.create_task(consumer.start())
    try:
        await asyncio.wait_for(asyncio.sleep(0.5), timeout=3.0)
    except asyncio.TimeoutError:
        pass
    await consumer.stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert consumer.processed_count >= 1
    assert consumer.last_active is not None

    # Verify there are no pending messages (i.e. the message was ACKed)
    pending = await fake_redis.xpending(config.stream_key, config.consumer_group)
    assert pending["pending"] == 0


@pytest.mark.asyncio
async def test_stop_consumer(fake_redis, config):
    """Consumer stops gracefully when stop_event is set."""
    consumer = _make_consumer(fake_redis, config)

    task = asyncio.create_task(consumer.start())
    # Give it a moment to enter the consume loop
    await asyncio.sleep(0.2)
    await consumer.stop()

    # The task should finish on its own or be cancellable
    task.cancel()
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except (asyncio.CancelledError, asyncio.TimeoutError):
        pass

    assert consumer._stop_event.is_set()
    assert consumer._running is False


@pytest.mark.asyncio
async def test_consumer_stats(fake_redis, config):
    """processed_count increments and success_rate is computed correctly."""
    # Seed the stream with two messages
    await fake_redis.xadd(config.stream_key, {"log": SAMPLE_LOG})
    await fake_redis.xadd(config.stream_key, {"log": SAMPLE_LOG})

    consumer = _make_consumer(fake_redis, config)

    task = asyncio.create_task(consumer.start())
    try:
        await asyncio.wait_for(asyncio.sleep(0.5), timeout=3.0)
    except asyncio.TimeoutError:
        pass
    await consumer.stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    stats = consumer.get_stats()
    assert stats.processed_count >= 2
    assert stats.error_count == 0
    assert stats.success_rate == 1.0
    assert stats.consumer_id == "test-consumer-0"


@pytest.mark.asyncio
async def test_pending_recovery(fake_redis, config):
    """_recover_pending processes and ACKs messages returned with ID '0'.

    fakeredis does not faithfully reproduce xreadgroup with a '0' start ID
    (it returns an empty list instead of pending messages). We work around
    this by patching xreadgroup to return a pending message for the '0'
    call while keeping the rest of the Redis interactions real.
    """
    # Step 1: Create group and seed a message
    await fake_redis.xgroup_create(
        config.stream_key, config.consumer_group, id="0", mkstream=True
    )
    msg_id = await fake_redis.xadd(config.stream_key, {"log": SAMPLE_LOG})

    # Step 2: Claim the message (without ACK) so it is pending
    claimed = await fake_redis.xreadgroup(
        config.consumer_group,
        "claimer-0",
        {config.stream_key: ">"},
        count=10,
    )
    assert claimed  # Sanity check

    pending_before = await fake_redis.xpending(config.stream_key, config.consumer_group)
    assert pending_before["pending"] == 1

    # Step 3: Build the consumer and patch xreadgroup so that the '0' ID
    # call returns the pending message (simulating real Redis behaviour).
    consumer = _make_consumer(fake_redis, config, consumer_id="claimer-0")

    original_xreadgroup = fake_redis.xreadgroup

    async def _patched_xreadgroup(group, consumer_name, streams, **kwargs):
        """Return pending data for the '0' read, delegate otherwise."""
        stream_id = list(streams.values())[0]
        if stream_id == "0":
            return [[config.stream_key, [(msg_id, {"log": SAMPLE_LOG})]]]
        return await original_xreadgroup(group, consumer_name, streams, **kwargs)

    fake_redis.xreadgroup = _patched_xreadgroup

    # Step 4: Start consumer — _recover_pending should process the pending msg
    task = asyncio.create_task(consumer.start())
    try:
        await asyncio.wait_for(asyncio.sleep(0.5), timeout=3.0)
    except asyncio.TimeoutError:
        pass
    await consumer.stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # The pending message should have been processed and ACKed
    assert consumer.processed_count >= 1
    pending_after = await fake_redis.xpending(config.stream_key, config.consumer_group)
    assert pending_after["pending"] == 0


# ---------------------------------------------------------------------------
# Tests — ConsumerManager
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_manager_starts_n_consumers(fake_redis, config):
    """Manager creates the correct number of consumers."""
    processor = LogProcessor()
    metrics = MetricsAggregator(window_sec=config.metrics_window_sec)
    manager = ConsumerManager(config, processor, metrics)

    await manager.start(fake_redis)
    # Give consumers a moment to spin up
    await asyncio.sleep(0.3)

    assert len(manager.consumers) == config.num_workers
    assert len(manager._tasks) == config.num_workers

    # Each consumer should have a unique ID
    ids = {c.consumer_id for c in manager.consumers}
    assert len(ids) == config.num_workers

    # Each consumer should have a distinct worker_index
    indices = {c._worker_index for c in manager.consumers}
    assert indices == set(range(config.num_workers))

    # All stats should be returned
    stats = manager.get_consumer_stats()
    assert len(stats) == config.num_workers

    await manager.stop()


# ---------------------------------------------------------------------------
# Tests — Idempotency
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_idempotency_skips_duplicate(fake_redis, config):
    """Processing the same msg_id twice: second call is skipped (duplicate)."""
    consumer = _make_consumer(fake_redis, config)
    await consumer._ensure_group()

    msg_id = await fake_redis.xadd(config.stream_key, {"log": SAMPLE_LOG})

    # First processing — should succeed
    await consumer._process_message(msg_id, {"log": SAMPLE_LOG})
    assert consumer.processed_count == 1

    # Second processing of the same msg_id — should be skipped
    await consumer._process_message(msg_id, {"log": SAMPLE_LOG})
    assert consumer.processed_count == 1  # Still 1, not 2

    # Verify the idempotency key exists in Redis
    idempotency_val = await fake_redis.get(f"processed:{msg_id}")
    assert idempotency_val == "1"


# ---------------------------------------------------------------------------
# Tests — Ordering
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ordering_routes_correctly(fake_redis, config):
    """With ordering enabled, consumer only processes messages whose
    ordering_key hashes to its worker_index."""
    # Enable ordering for this test
    config.enable_ordering = True
    config.num_workers = 2

    consumer_0 = _make_consumer(fake_redis, config, consumer_id="worker-0", worker_index=0)
    consumer_1 = _make_consumer(fake_redis, config, consumer_id="worker-1", worker_index=1)

    await consumer_0._ensure_group()

    # Pick an ordering_key and figure out which worker it routes to
    ordering_key = "user-42"
    target_worker = hash(ordering_key) % config.num_workers

    msg_id = await fake_redis.xadd(
        config.stream_key, {"log": SAMPLE_LOG, "ordering_key": ordering_key}
    )

    # Process on both consumers
    await consumer_0._process_message(msg_id, {"log": SAMPLE_LOG, "ordering_key": ordering_key})

    # The idempotency key was set by consumer_0 if it was the target, or deleted if not.
    # Reset idempotency for the second consumer attempt.
    await fake_redis.delete(f"processed:{msg_id}")

    await consumer_1._process_message(msg_id, {"log": SAMPLE_LOG, "ordering_key": ordering_key})

    # Only the target worker should have processed the message
    if target_worker == 0:
        assert consumer_0.processed_count == 1
        assert consumer_1.processed_count == 0
    else:
        assert consumer_0.processed_count == 0
        assert consumer_1.processed_count == 1


# ---------------------------------------------------------------------------
# Tests — XCLAIM abandoned messages
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_claim_abandoned(fake_redis, config):
    """_claim_abandoned uses XPENDING + XCLAIM to reclaim idle messages.

    fakeredis may not fully support xpending_range/xclaim, so we mock
    the Redis methods to verify the consumer logic.
    """
    consumer = _make_consumer(fake_redis, config)
    await consumer._ensure_group()

    msg_id = "1234567890-0"
    msg_data = {"log": SAMPLE_LOG}

    # Mock xpending_range to return one entry that exceeds claim_idle_ms
    fake_redis.xpending_range = AsyncMock(return_value=[
        {
            "message_id": msg_id,
            "consumer": "dead-consumer",
            "time_since_delivered": config.claim_idle_ms + 1000,
            "times_delivered": 1,
        }
    ])

    # Mock xclaim to return the claimed message
    fake_redis.xclaim = AsyncMock(return_value=[
        (msg_id, msg_data),
    ])

    # Seed the stream and add the idempotency-friendly msg_id
    await fake_redis.xadd(config.stream_key, msg_data)

    await consumer._claim_abandoned()

    # Verify xpending_range was called correctly
    fake_redis.xpending_range.assert_awaited_once_with(
        config.stream_key,
        config.consumer_group,
        min="-",
        max="+",
        count=config.batch_size,
    )

    # Verify xclaim was called for the idle message
    fake_redis.xclaim.assert_awaited_once_with(
        config.stream_key,
        config.consumer_group,
        consumer.consumer_id,
        min_idle_time=config.claim_idle_ms,
        message_ids=[msg_id],
    )

    # The claimed message should have been processed
    assert consumer.processed_count == 1


@pytest.mark.asyncio
async def test_claim_abandoned_skips_non_idle(fake_redis, config):
    """Messages with idle time below claim_idle_ms are not claimed."""
    consumer = _make_consumer(fake_redis, config)
    await consumer._ensure_group()

    # Mock xpending_range with entry below threshold
    fake_redis.xpending_range = AsyncMock(return_value=[
        {
            "message_id": "999-0",
            "consumer": "other-consumer",
            "time_since_delivered": config.claim_idle_ms - 1000,  # Below threshold
            "times_delivered": 1,
        }
    ])
    fake_redis.xclaim = AsyncMock()

    await consumer._claim_abandoned()

    # xclaim should NOT have been called
    fake_redis.xclaim.assert_not_awaited()
    assert consumer.processed_count == 0
