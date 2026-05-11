import asyncio
import pytest

from src.admission import Admission
from src.aimd import AIMDLimiter
from src.config import Settings
from src.load_tester import InternalLoadTester, LoadPhase
from src.queues import PriorityQueues
from src.state_machine import BackpressureManager
from src.upstream_breaker import UpstreamBreaker


def _components():
    s = Settings()
    queues = PriorityQueues(s)
    manager = BackpressureManager(s)
    aimd = AIMDLimiter(initial_limit=100_000, beta=s.aimd_beta)
    upstream = UpstreamBreaker(s)
    admission = Admission(s, aimd, upstream)
    return admission, queues, manager, s


@pytest.mark.asyncio
async def test_smoke_profile_emits_within_tolerance():
    admission, queues, manager, s = _components()
    lt = InternalLoadTester(admission, queues, manager, s, rng_seed=42)
    await lt.start(profile="smoke", rps=100, duration_seconds=2)
    await asyncio.sleep(2.3)
    await lt.stop()
    st = lt.status()
    assert 12 <= st.emitted <= 35
    assert st.accepted >= int(st.emitted * 0.8)


@pytest.mark.asyncio
async def test_stop_is_idempotent():
    admission, queues, manager, s = _components()
    lt = InternalLoadTester(admission, queues, manager, s, rng_seed=42)
    await lt.start(profile="smoke", rps=50, duration_seconds=10)
    await asyncio.sleep(0.2)
    s1 = await lt.stop()
    s2 = await lt.stop()
    assert s1.state == "idle"
    assert s2.state == "idle"


@pytest.mark.asyncio
async def test_full_profile_has_expected_phase_sequence():
    """We don't run the full 60s; we just verify the schedule is built correctly."""
    admission, queues, manager, s = _components()
    lt = InternalLoadTester(admission, queues, manager, s, rng_seed=42)
    schedule = lt._build_schedule("full", baseline_rps=200, duration_seconds=60, spike_multiplier=10.0)
    phases = [p.value for p, _, _ in schedule]
    assert phases == ["smoke", "ramp", "spike", "soak", "recovery"]
    assert sum(d for _, d, _ in schedule) == 60
    spike_rps = [r for p, _, r in schedule if p == LoadPhase.SPIKE][0]
    assert spike_rps == 2000
