import asyncio
import pytest

from src.breaker import CircuitBreaker
from src.config import Settings
from src.processor import WorkerPool
from src.queues import PriorityQueues
from src.state import Priority


@pytest.mark.asyncio
async def test_workers_drain_in_strict_priority_order():
    s = Settings()
    object.__setattr__(s, "worker_count", 1)
    object.__setattr__(s, "processing_latency_seconds", 0.001)
    q = PriorityQueues(s)
    cb = CircuitBreaker()
    pool = WorkerPool(q, cb, s)
    q.put_nowait(Priority.LOW, "L", 1.0)
    q.put_nowait(Priority.NORMAL, "N", 2.0)
    q.put_nowait(Priority.HIGH, "H", 3.0)
    q.put_nowait(Priority.CRITICAL, "C", 4.0)
    pool.start()
    await asyncio.sleep(0.15)
    await pool.stop()
    assert pool.processed_count == 4


@pytest.mark.asyncio
async def test_processing_lag_reflects_oldest_enqueued():
    s = Settings()
    object.__setattr__(s, "worker_count", 1)
    object.__setattr__(s, "processing_latency_seconds", 0.05)
    q = PriorityQueues(s)
    cb = CircuitBreaker()
    pool = WorkerPool(q, cb, s, clock=lambda: 10.0)
    q.put_nowait(Priority.CRITICAL, "x", 0.0)
    pool.start()
    await asyncio.sleep(0.2)
    await pool.stop()
    assert pool.processing_lag >= 10.0
