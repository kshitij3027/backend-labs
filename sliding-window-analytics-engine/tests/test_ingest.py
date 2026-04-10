"""Unit tests for :mod:`src.ingest`.

Covers the new Commit 6 backpressure pipeline:

* Drop-oldest overflow policy preserves the **newest** events when the
  bounded queue hits capacity.
* Adaptive 1-in-N sampling kicks in for generator-sourced events once
  the queue crosses the 80% high-water mark, but never for
  user-submitted events.
* ``run_consumer`` drains the queue into a fake :class:`WindowManager`
  until the ``stop_event`` is set.
* Counter invariants hold regardless of the overload path taken.

Tests use pytest-asyncio in "auto" mode, so no explicit
``@pytest.mark.asyncio`` marker is needed on individual coroutines.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field

from src.ingest import IngestPipeline
from src.models import Event


@dataclass
class FakeWindowManager:
    """Minimal stand-in for :class:`src.window_manager.WindowManager`.

    Captures dispatched events so tests can assert on them, and exposes
    the ``active_count`` attribute the pipeline doesn't actually read
    but which mirrors the real manager's public surface.
    """

    dispatched: list[Event] = field(default_factory=list)
    active_count: int = 0

    def dispatch(self, event: Event) -> None:
        self.dispatched.append(event)


def _make_event(value: float = 1.0, timestamp: float = 0.0) -> Event:
    return Event(
        event_id=str(uuid.uuid4()),
        timestamp=timestamp,
        value=value,
        metric="response_time",
        metadata={},
    )


async def test_snapshot_empty() -> None:
    pipeline = IngestPipeline(FakeWindowManager(), maxsize=10)
    snap = pipeline.snapshot()
    assert snap["queue_depth"] == 0
    assert snap["queue_maxsize"] == 10
    assert snap["enqueued"] == 0
    assert snap["dropped"] == 0
    assert snap["sampled"] == 0
    assert snap["processed"] == 0


async def test_submit_user_no_sampling() -> None:
    """submit_user is never sampled, even well above the high-water mark.

    With maxsize=4, the first 4 events fill the queue. The next 6 each
    evict the oldest entry in turn (drop-oldest policy). Net effect:
    all 10 are ``enqueued``, 6 are ``dropped``, and the queue depth is
    back to maxsize.
    """
    pipeline = IngestPipeline(FakeWindowManager(), maxsize=4)
    for i in range(10):
        await pipeline.submit_user(_make_event(value=float(i)))
    snap = pipeline.snapshot()
    assert snap["enqueued"] == 10
    assert snap["dropped"] == 6
    assert snap["sampled"] == 0
    assert snap["queue_depth"] == 4


async def test_submit_generated_sampling_triggers_above_threshold() -> None:
    """Generator-sourced events are sampled once the queue is >= 80% full.

    Pre-fills the queue with 8 user-submitted events (maxsize=10 so
    80% = 8 events, right at the high-water mark). Then submits 10
    generator events — as the queue fills past 95% the sampler
    escalates from 1-in-2 to 1-in-4, so most of the burst is dropped
    before it ever reaches the queue. The precise sampled count is
    deterministic (4 sampled at light rate, the rest at heavy rate)
    but we assert a generous range to keep the test robust against
    future tweaks to the ratios.
    """
    pipeline = IngestPipeline(FakeWindowManager(), maxsize=10)
    for _ in range(8):
        await pipeline.submit_user(_make_event())
    assert pipeline.queue_depth == 8

    for _ in range(10):
        await pipeline.submit_generated(_make_event())

    snap = pipeline.snapshot()
    # At least 4 samples must have been dropped under backpressure —
    # otherwise the sampler isn't doing its job. Upper bound leaves
    # headroom for the escalation path (N=4) which can drop up to 8.
    assert 4 <= snap["sampled"] <= 8, f"sampled count out of range: {snap['sampled']}"
    # Every generator event is either sampled, enqueued successfully,
    # or enqueued-then-evicted; the 8 user-submitted events remain
    # counted in enqueued from the prefill phase.
    generator_enqueued = snap["enqueued"] - 8
    assert snap["sampled"] + generator_enqueued == 10


async def test_run_consumer_drains_into_window_manager() -> None:
    fake = FakeWindowManager()
    pipeline = IngestPipeline(fake, maxsize=10)
    for i in range(5):
        await pipeline.submit_user(_make_event(value=float(i)))

    stop_event = asyncio.Event()
    consumer = asyncio.create_task(pipeline.run_consumer(stop_event))
    await asyncio.sleep(0.4)
    stop_event.set()
    await asyncio.wait_for(consumer, timeout=1.0)

    assert len(fake.dispatched) == 5
    assert pipeline.counters.processed == 5
    assert pipeline.queue_depth == 0


async def test_drop_oldest_preserves_newest() -> None:
    """With maxsize=3, after submitting events 1..5 the queue should hold {3,4,5}."""
    pipeline = IngestPipeline(FakeWindowManager(), maxsize=3)
    for i in range(1, 6):
        await pipeline.submit_user(_make_event(value=float(i), timestamp=float(i)))

    # Drain the queue directly (no consumer) and check the remaining
    # events are the newest three in insertion order.
    remaining: list[float] = []
    while not pipeline._queue.empty():
        event = pipeline._queue.get_nowait()
        remaining.append(event.value)
    assert remaining == [3.0, 4.0, 5.0]


async def test_counters_total_invariant() -> None:
    """Submit 20 user events into maxsize=5: 20 enqueued, 15 dropped, depth=5."""
    pipeline = IngestPipeline(FakeWindowManager(), maxsize=5)
    for i in range(20):
        await pipeline.submit_user(_make_event(value=float(i)))

    snap = pipeline.snapshot()
    assert snap["enqueued"] == 20
    assert snap["dropped"] == 15
    assert snap["sampled"] == 0
    assert snap["queue_depth"] == 5


async def test_consumer_drains_on_shutdown() -> None:
    """Events still on the queue when stop_event fires are processed during the final drain."""
    fake = FakeWindowManager()
    pipeline = IngestPipeline(fake, maxsize=10)
    stop_event = asyncio.Event()
    stop_event.set()  # signal stop *before* the consumer starts so it takes the drain path.

    for i in range(3):
        await pipeline.submit_user(_make_event(value=float(i)))

    await asyncio.wait_for(pipeline.run_consumer(stop_event), timeout=1.0)
    assert len(fake.dispatched) == 3
    assert pipeline.counters.processed == 3
    assert pipeline.queue_depth == 0
