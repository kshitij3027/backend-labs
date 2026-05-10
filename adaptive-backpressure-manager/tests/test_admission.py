import pytest

from src.admission import Admission, AdmissionVerdict
from src.aimd import AIMDLimiter
from src.config import Settings
from src.state import PressureLevel, Priority
from src.upstream_breaker import UpstreamBreaker


def _new(initial_limit: int = 10_000) -> Admission:
    s = Settings()
    aimd = AIMDLimiter(initial_limit=initial_limit, beta=0.7, additive=1, ai_period_ticks=3)
    ub = UpstreamBreaker(s)
    return Admission(s, aimd, ub)


@pytest.mark.parametrize(
    "level,priority,expected",
    [
        # NORMAL: everyone accepted
        (PressureLevel.NORMAL, Priority.CRITICAL, AdmissionVerdict.ACCEPT),
        (PressureLevel.NORMAL, Priority.HIGH, AdmissionVerdict.ACCEPT),
        (PressureLevel.NORMAL, Priority.NORMAL, AdmissionVerdict.ACCEPT),
        (PressureLevel.NORMAL, Priority.LOW, AdmissionVerdict.ACCEPT),
        # PRESSURE: LOW dropped, others accepted
        (PressureLevel.PRESSURE, Priority.CRITICAL, AdmissionVerdict.ACCEPT),
        (PressureLevel.PRESSURE, Priority.HIGH, AdmissionVerdict.ACCEPT),
        (PressureLevel.PRESSURE, Priority.NORMAL, AdmissionVerdict.ACCEPT),
        (PressureLevel.PRESSURE, Priority.LOW, AdmissionVerdict.DROP_SILENT),
        # OVERLOAD: CRITICAL accepted, HIGH 503, NORMAL/LOW dropped
        (PressureLevel.OVERLOAD, Priority.CRITICAL, AdmissionVerdict.ACCEPT),
        (PressureLevel.OVERLOAD, Priority.HIGH, AdmissionVerdict.REJECT_503),
        (PressureLevel.OVERLOAD, Priority.NORMAL, AdmissionVerdict.DROP_SILENT),
        (PressureLevel.OVERLOAD, Priority.LOW, AdmissionVerdict.DROP_SILENT),
        # RECOVERY: CRITICAL accepted, LOW dropped, others may be 429 but with large AIMD limit -> ACCEPT
        (PressureLevel.RECOVERY, Priority.CRITICAL, AdmissionVerdict.ACCEPT),
        (PressureLevel.RECOVERY, Priority.HIGH, AdmissionVerdict.ACCEPT),
        (PressureLevel.RECOVERY, Priority.NORMAL, AdmissionVerdict.ACCEPT),
        (PressureLevel.RECOVERY, Priority.LOW, AdmissionVerdict.DROP_SILENT),
    ],
)
def test_admission_matrix(level, priority, expected):
    a = _new()
    assert a.decide(priority, level) == expected


def test_aimd_exhaustion_throttles_429_in_recovery():
    a = _new(initial_limit=2)  # tiny AIMD limit to force exhaustion
    # CRITICAL is exempt and is the only path that bypasses AIMD; HIGH/NORMAL go through.
    # Drain the AIMD budget with two HIGH calls in RECOVERY.
    assert a.decide(Priority.HIGH, PressureLevel.RECOVERY) == AdmissionVerdict.ACCEPT
    assert a.decide(Priority.HIGH, PressureLevel.RECOVERY) == AdmissionVerdict.ACCEPT
    # Third HIGH in RECOVERY should 429 (throttle_on_full=True).
    assert a.decide(Priority.HIGH, PressureLevel.RECOVERY) == AdmissionVerdict.THROTTLE_429


def test_upstream_breaker_forces_503_for_non_critical():
    a = _new()
    # Trip the breaker.
    for _ in range(3):
        a._upstream.observe(0.99)
    assert a._upstream.is_open
    assert a.decide(Priority.HIGH, PressureLevel.NORMAL) == AdmissionVerdict.REJECT_503
    assert a.decide(Priority.NORMAL, PressureLevel.NORMAL) == AdmissionVerdict.REJECT_503
    assert a.decide(Priority.LOW, PressureLevel.NORMAL) == AdmissionVerdict.REJECT_503
    # CRITICAL still admitted.
    assert a.decide(Priority.CRITICAL, PressureLevel.NORMAL) == AdmissionVerdict.ACCEPT


def test_upstream_breaker_resets_below_threshold():
    a = _new()
    for _ in range(3):
        a._upstream.observe(0.99)
    assert a._upstream.is_open
    a._upstream.observe(0.5)
    assert not a._upstream.is_open
    assert a.decide(Priority.HIGH, PressureLevel.NORMAL) == AdmissionVerdict.ACCEPT


def test_counters_increment_correctly():
    a = _new()
    a.decide(Priority.CRITICAL, PressureLevel.NORMAL)
    a.decide(Priority.LOW, PressureLevel.PRESSURE)
    a.decide(Priority.HIGH, PressureLevel.OVERLOAD)
    c = a.counters
    assert c["accepted"] == 1
    assert c["dropped"] == 1
    assert c["rejected"] == 1
