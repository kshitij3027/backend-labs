import asyncio

import pytest

from src.config import Settings
from src.queues import PriorityQueues
from src.state import Priority


def _settings():
    return Settings()


@pytest.mark.asyncio
async def test_strict_priority_drain_order():
    q = PriorityQueues(_settings())
    q.put_nowait(Priority.LOW, "low1", 1.0)
    q.put_nowait(Priority.NORMAL, "norm1", 2.0)
    q.put_nowait(Priority.HIGH, "high1", 3.0)
    q.put_nowait(Priority.CRITICAL, "crit1", 4.0)
    order = []
    for _ in range(4):
        p, _item, _enq = await q.get_next()
        order.append(p)
    assert order == [Priority.CRITICAL, Priority.HIGH, Priority.NORMAL, Priority.LOW]


@pytest.mark.asyncio
async def test_cap_honored_raises_queuefull():
    s = _settings()
    q = PriorityQueues(s)
    for i in range(s.critical_queue_max):
        q.put_nowait(Priority.CRITICAL, i, float(i))
    with pytest.raises(asyncio.QueueFull):
        q.put_nowait(Priority.CRITICAL, "overflow", 9999.0)


@pytest.mark.asyncio
async def test_get_next_blocks_until_put():
    q = PriorityQueues(_settings())

    async def producer():
        await asyncio.sleep(0.05)
        q.put_nowait(Priority.LOW, "x", 1.0)

    async def consumer():
        return await q.get_next()

    prod_task = asyncio.create_task(producer())
    p, item, _ = await asyncio.wait_for(consumer(), timeout=1.0)
    await prod_task
    assert p == Priority.LOW
    assert item == "x"


@pytest.mark.asyncio
async def test_total_depth_ratio_and_oldest():
    q = PriorityQueues(_settings())
    q.put_nowait(Priority.LOW, "x", 100.0)
    q.put_nowait(Priority.HIGH, "y", 50.0)
    q.put_nowait(Priority.CRITICAL, "z", 200.0)
    assert q.total_qsize() == 3
    assert q.total_depth_ratio() == pytest.approx(3 / Settings().max_queue_size)
    assert q.oldest_enqueued_at() == 50.0


@pytest.mark.asyncio
async def test_bump_promotes_aged_items():
    s = _settings()
    q = PriorityQueues(s)
    # LOW item at t=0; default anti_starvation_age_seconds is 30s.
    q.put_nowait(Priority.LOW, "stale", 0.0)
    bumped = q._do_bump_pass(now_fn=lambda: 31.0)
    assert bumped == 1
    assert q.qsize(Priority.LOW) == 0
    assert q.qsize(Priority.NORMAL) == 1
