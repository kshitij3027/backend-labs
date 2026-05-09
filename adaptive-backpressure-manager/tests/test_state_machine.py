import pytest

from src.config import Settings
from src.state import PressureLevel
from src.state_machine import BackpressureManager


class _FakeClock:
    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


@pytest.fixture
def settings():
    return Settings()


@pytest.fixture
def clock():
    return _FakeClock()


def _new(settings, clock):
    return BackpressureManager(settings=settings, clock=clock)


def test_initial_state_is_normal(settings, clock):
    m = _new(settings, clock)
    assert m.level == PressureLevel.NORMAL


def test_normal_to_pressure_after_threshold_and_dwell(settings, clock):
    m = _new(settings, clock)
    clock.advance(settings.min_dwell_seconds + 0.1)
    assert m.tick(0.75) == PressureLevel.PRESSURE


def test_no_transition_within_dwell(settings, clock):
    m = _new(settings, clock)
    transitions = []
    for _ in range(10):
        prev = m.level
        m.tick(0.75)
        m.tick(0.65)
        if m.level != prev:
            transitions.append(m.level)
        clock.advance(0.1)
    assert transitions == []


def test_overload_path_through_recovery(settings, clock):
    m = _new(settings, clock)
    clock.advance(settings.min_dwell_seconds + 0.1)
    m.tick(0.75)
    clock.advance(settings.min_dwell_seconds + 0.1)
    m.tick(0.9)
    assert m.level == PressureLevel.OVERLOAD
    clock.advance(settings.min_dwell_seconds + 0.1)
    m.tick(0.7)
    assert m.level == PressureLevel.RECOVERY


def test_recovery_does_not_exit_above_threshold(settings, clock):
    m = _new(settings, clock)
    clock.advance(settings.min_dwell_seconds + 0.1); m.tick(0.75)
    clock.advance(settings.min_dwell_seconds + 0.1); m.tick(0.9)
    clock.advance(settings.min_dwell_seconds + 0.1); m.tick(0.7)
    assert m.level == PressureLevel.RECOVERY
    clock.advance(settings.min_dwell_seconds + 0.1)
    # 0.5 is above down_recovery_to_normal=0.45 → stay in RECOVERY
    m.tick(0.5)
    assert m.level == PressureLevel.RECOVERY


def test_recovery_exits_to_normal_below_threshold(settings, clock):
    m = _new(settings, clock)
    clock.advance(settings.min_dwell_seconds + 0.1); m.tick(0.75)
    clock.advance(settings.min_dwell_seconds + 0.1); m.tick(0.9)
    clock.advance(settings.min_dwell_seconds + 0.1); m.tick(0.7)
    clock.advance(settings.min_dwell_seconds + 0.1)
    m.tick(0.4)
    assert m.level == PressureLevel.NORMAL


def test_recovery_re_overload(settings, clock):
    """RECOVERY can re-escalate directly to OVERLOAD if pressure spikes again."""
    m = _new(settings, clock)
    clock.advance(settings.min_dwell_seconds + 0.1); m.tick(0.75)
    clock.advance(settings.min_dwell_seconds + 0.1); m.tick(0.9)
    clock.advance(settings.min_dwell_seconds + 0.1); m.tick(0.7)
    assert m.level == PressureLevel.RECOVERY
    clock.advance(settings.min_dwell_seconds + 0.1)
    m.tick(0.95)
    assert m.level == PressureLevel.OVERLOAD


def test_pressure_drops_directly_to_normal(settings, clock):
    """PRESSURE → NORMAL on low score (NOT through RECOVERY; that path is only post-OVERLOAD)."""
    m = _new(settings, clock)
    clock.advance(settings.min_dwell_seconds + 0.1); m.tick(0.75)
    assert m.level == PressureLevel.PRESSURE
    clock.advance(settings.min_dwell_seconds + 0.1); m.tick(0.4)
    assert m.level == PressureLevel.NORMAL
