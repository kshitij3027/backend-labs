from __future__ import annotations

import asyncio

import pytest

from src.metrics.collector import MetricsCollector
from src.metrics.ring_buffer import RingBuffer
from src.metrics.sample import StageEvent
from src.resource_sampler.sampler import ResourceSnapshot


def _make_event(stage: str = "parse", duration_ns: int = 2_500_000) -> StageEvent:
    return StageEvent(
        stage=stage,
        started_ns=1_000_000_000,
        duration_ns=duration_ns,
        cpu_delta_pct=0.0,
        rss_delta_kb=0,
        record_count=1,
    )


async def test_submit_and_drain_appends_sample() -> None:
    buffer = RingBuffer(maxlen=10)
    collector = MetricsCollector(buffer=buffer, batch_size=4)
    collector.submit(_make_event(duration_ns=3_000_000))

    task = asyncio.create_task(collector.drain_loop())
    # Allow the loop to pick up the event.
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    samples = buffer.snapshot()
    assert len(samples) == 1
    assert samples[0].stage == "parse"
    assert samples[0].latency_ms == pytest.approx(3.0)


async def test_full_queue_increments_dropped() -> None:
    buffer = RingBuffer(maxlen=100)
    collector = MetricsCollector(buffer=buffer, batch_size=2)
    # Capacity is batch_size * 8 == 16. Push more than capacity without
    # ever running the drain loop so nothing is ever consumed.
    for _ in range(200):
        collector.submit(_make_event())
    assert collector.metrics_dropped() > 0


async def test_batch_flush() -> None:
    buffer = RingBuffer(maxlen=50)
    collector = MetricsCollector(buffer=buffer, batch_size=3)
    for _ in range(5):
        collector.submit(_make_event())

    task = asyncio.create_task(collector.drain_loop())
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(buffer.snapshot()) == 5


async def test_cancellation_drains_remaining() -> None:
    buffer = RingBuffer(maxlen=50)
    collector = MetricsCollector(buffer=buffer, batch_size=4)
    for _ in range(3):
        collector.submit(_make_event())

    task = asyncio.create_task(collector.drain_loop())
    # Yield once so the task body starts (asyncio does not run a freshly-created
    # task until the event loop ticks). Then cancel — the cancellation handler
    # must drain whatever is left in the queue.
    await asyncio.sleep(0)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert len(buffer.snapshot()) == 3


async def test_joined_sample_carries_resource_snapshot() -> None:
    buffer = RingBuffer(maxlen=10)

    def lookup(stage: str) -> ResourceSnapshot:
        return ResourceSnapshot(
            stage=stage,
            ts=0.0,
            cpu_pct=42.0,
            mem_mb=128.0,
            io_read_bytes=1024,
            io_write_bytes=2048,
            queue_depth=7,
        )

    collector = MetricsCollector(buffer=buffer, batch_size=2, resource_lookup=lookup)
    collector.submit(_make_event())

    task = asyncio.create_task(collector.drain_loop())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    samples = buffer.snapshot()
    assert len(samples) == 1
    sample = samples[0]
    assert sample.cpu_pct == 42.0
    assert sample.mem_mb == 128.0
    assert sample.io_read_bytes == 1024
    assert sample.io_write_bytes == 2048
    assert sample.queue_depth == 7


async def test_no_lookup_uses_zeros() -> None:
    buffer = RingBuffer(maxlen=10)
    collector = MetricsCollector(buffer=buffer, batch_size=2)
    collector.submit(_make_event())

    task = asyncio.create_task(collector.drain_loop())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    samples = buffer.snapshot()
    assert len(samples) == 1
    sample = samples[0]
    assert sample.cpu_pct == 0.0
    assert sample.mem_mb == 0.0
    assert sample.io_read_bytes == 0
    assert sample.io_write_bytes == 0
    assert sample.queue_depth == 0
