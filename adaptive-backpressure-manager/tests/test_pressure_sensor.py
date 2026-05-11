import asyncio
import pytest

from src.aimd import AIMDLimiter
from src.config import Settings
from src.metrics_core import PressureFuser
from src.pressure_sensor import PressureSensor
from src.queues import PriorityQueues
from src.state import PressureLevel, Priority
from src.state_machine import BackpressureManager
from src.upstream_breaker import UpstreamBreaker


def _components(settings):
    queues = PriorityQueues(settings)
    fuser = PressureFuser(alpha=settings.ewma_alpha, history_size=settings.pressure_history_size)
    manager = BackpressureManager(settings)
    aimd = AIMDLimiter(initial_limit=100, beta=settings.aimd_beta, additive=1, ai_period_ticks=3)
    upstream = UpstreamBreaker(settings)
    return queues, fuser, manager, aimd, upstream


@pytest.mark.asyncio
async def test_sensor_produces_samples_in_one_second_real_time():
    s = Settings()
    object.__setattr__(s, "sampling_interval", 0.1)
    q, f, m, a, u = _components(s)
    sensor = PressureSensor(q, f, m, a, u, s)
    sensor.start()
    await asyncio.sleep(0.45)
    await sensor.stop()
    assert sensor.sample_count >= 3


@pytest.mark.asyncio
async def test_sensor_triggers_aimd_on_overload_entry():
    s = Settings()
    object.__setattr__(s, "sampling_interval", 0.05)
    q, f, m, a, u = _components(s)
    for i in range(int(s.max_queue_size * 0.9)):
        try:
            q.put_nowait(Priority.NORMAL, i, float(i))
        except asyncio.QueueFull:
            break
    sensor = PressureSensor(q, f, m, a, u, s)
    sensor.start()
    await asyncio.sleep(s.min_dwell_seconds + 0.5)
    await sensor.stop()
    assert m.level == PressureLevel.OVERLOAD or a.limit < 100
