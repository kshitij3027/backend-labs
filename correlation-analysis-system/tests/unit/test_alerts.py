"""Unit tests for the AlertManager (rules, cooldowns, bounded history).

Rules run in order and the first match per correlation wins: an error_cascade
at strength >= 0.6 pages as critical; a PatternLearner-flagged anomaly
(details["anomaly"], C7) warns regardless of absolute strength/confidence; any
type at strength >= 0.8 AND confidence >= 0.6 warns; anything weaker stays
silent. A per-(rule, type, source-pair) cooldown (60 s) keeps a persistent
condition to one alert per minute instead of one per detection cycle.
"""

import pytest

from src.aggregation import MetricAggregator
from src.alerts import CASCADE_STRENGTH_THRESHOLD, RECENT_MAX, AlertManager
from src.config import Settings
from src.engine import CorrelationEngine
from src.models import Correlation, CorrelationType, EventRef, SourceType

EPOCH = 1000.0


def ref(source: SourceType, ts: float) -> EventRef:
    return EventRef(
        id=f"{source.value}-{ts}",
        source=source,
        service=source.value,
        message=f"{source.value} event",
        timestamp=ts,
    )


def fab_corr(
    ctype: CorrelationType = CorrelationType.SESSION,
    strength: float = 0.9,
    confidence: float = 0.95,
    source_a: SourceType = SourceType.WEB,
    source_b: SourceType = SourceType.DATABASE,
    details: dict | None = None,
) -> Correlation:
    """A fabricated correlation (defaults trip the strong-correlation rule)."""
    return Correlation(
        id="corr-fab",
        detected_at=EPOCH,
        correlation_type=ctype,
        event_a=ref(source_a, EPOCH - 3.0),
        event_b=ref(source_b, EPOCH),
        strength=strength,
        confidence=confidence,
        details=details or {},
    )


def make_manager() -> AlertManager:
    return AlertManager(Settings(_env_file=None))


def test_cascade_above_threshold_fires_critical():
    manager = make_manager()
    corr = fab_corr(
        CorrelationType.CASCADE,
        strength=0.7,
        confidence=0.5,  # below the generic confidence gate — rule 1 ignores it
        source_a=SourceType.DATABASE,
        source_b=SourceType.WEB,
        details={"distinct_services": 3, "span_seconds": 2.5},
    )
    assert corr.strength >= CASCADE_STRENGTH_THRESHOLD  # test premise

    fired = manager.evaluate([corr], now=EPOCH)

    assert len(fired) == 1
    alert = fired[0]
    assert alert.severity == "critical"
    assert alert.title == "Error cascade detected"
    assert "database" in alert.message and "web" in alert.message
    assert "3 services" in alert.message
    assert alert.correlation_type is CorrelationType.CASCADE
    assert alert.strength == 0.7
    assert alert.confidence == 0.5
    assert alert.created_at == EPOCH
    assert list(manager.alerts) == [alert]


def test_strong_correlation_fires_warning():
    fired = make_manager().evaluate([fab_corr(strength=0.9, confidence=0.95)], now=EPOCH)

    assert len(fired) == 1
    alert = fired[0]
    assert alert.severity == "warning"
    assert alert.title == "Strong correlation detected"
    assert "session_based" in alert.message
    assert "web" in alert.message and "database" in alert.message


def test_strong_cascade_matches_cascade_rule_first():
    # Trips BOTH rules — first match wins, so exactly one critical alert fires.
    corr = fab_corr(
        CorrelationType.CASCADE,
        strength=0.9,
        confidence=0.95,
        source_a=SourceType.DATABASE,
        source_b=SourceType.WEB,
    )
    fired = make_manager().evaluate([corr], now=EPOCH)
    assert len(fired) == 1
    assert fired[0].severity == "critical"


def test_anomaly_flag_fires_warning_even_when_weak():
    # C7: the PatternLearner sets details["anomaly"] when a well-known pattern
    # deviates >2 sigma from its learned baseline — the deviation itself alerts,
    # even though 0.5/0.5 would fail both the cascade and the strong-rule gates.
    manager = make_manager()
    corr = fab_corr(strength=0.5, confidence=0.5, details={"anomaly": True})

    fired = manager.evaluate([corr], now=EPOCH)

    assert len(fired) == 1
    alert = fired[0]
    assert alert.severity == "warning"
    assert alert.title == "Anomalous correlation pattern"
    assert "session_based" in alert.message
    assert "web" in alert.message and "database" in alert.message
    assert "0.50" in alert.message  # strength cited against the learned baseline
    assert alert.strength == 0.5
    assert alert.confidence == 0.5


def test_anomaly_rule_respects_cooldown():
    manager = make_manager()  # alert_cooldown_seconds = 60
    corr = fab_corr(strength=0.5, confidence=0.5, details={"anomaly": True})
    assert len(manager.evaluate([corr], now=EPOCH)) == 1
    # Same anomalous condition again within the cooldown: suppressed.
    assert manager.evaluate([corr], now=EPOCH + 30.0) == []
    assert len(manager.alerts) == 1
    # 61 s after the first firing the cooldown has expired: fires again.
    assert len(manager.evaluate([corr], now=EPOCH + 61.0)) == 1
    assert len(manager.alerts) == 2


def test_weak_correlation_stays_silent():
    manager = make_manager()
    assert manager.evaluate([fab_corr(strength=0.5, confidence=0.5)], now=EPOCH) == []
    assert len(manager.alerts) == 0


def test_high_strength_low_confidence_stays_silent():
    # The generic rule requires strength AND confidence — 0.55 < 0.6 fails it.
    assert make_manager().evaluate([fab_corr(strength=0.85, confidence=0.55)], now=EPOCH) == []


def test_cooldown_suppresses_then_reopens():
    manager = make_manager()  # alert_cooldown_seconds = 60
    corr = fab_corr()
    assert len(manager.evaluate([corr], now=EPOCH)) == 1
    # Same condition again within the cooldown: suppressed, still 1 alert total.
    assert manager.evaluate([corr], now=EPOCH + 30.0) == []
    assert len(manager.alerts) == 1
    # 61 s after the first firing the cooldown has expired: fires again.
    assert len(manager.evaluate([corr], now=EPOCH + 61.0)) == 1
    assert len(manager.alerts) == 2


def test_cooldown_is_per_source_pair():
    manager = make_manager()
    a = fab_corr(source_a=SourceType.WEB, source_b=SourceType.DATABASE)
    b = fab_corr(source_a=SourceType.WEB, source_b=SourceType.PAYMENT)
    assert len(manager.evaluate([a, b], now=EPOCH)) == 2


def test_alert_history_bounded_at_200():
    manager = make_manager()
    corr = fab_corr()
    for i in range(RECENT_MAX + 30):
        # Spaced past the cooldown so every evaluation fires exactly once.
        assert len(manager.evaluate([corr], now=EPOCH + i * 61.0)) == 1
    assert len(manager.alerts) == RECENT_MAX


def test_recent_returns_newest_first():
    manager = make_manager()
    corr = fab_corr()
    for i in range(5):
        manager.evaluate([corr], now=EPOCH + i * 61.0)

    newest_two = manager.recent(limit=2)
    assert len(newest_two) == 2
    assert newest_two[0].created_at == EPOCH + 4 * 61.0
    assert newest_two[1].created_at == EPOCH + 3 * 61.0
    assert len(manager.recent()) == 5  # default limit caps, never pads


class _StubDetector:
    """A detector that returns a canned batch (drives the real engine path)."""

    name = "stub"

    def __init__(self, out: list[Correlation]) -> None:
        self._out = out

    def detect(self, ctx) -> list[Correlation]:
        return self._out


def test_engine_hands_findings_to_alert_manager():
    # The C5 hook: detect() must feed its findings to the wired AlertManager.
    settings = Settings(_env_file=None)
    manager = AlertManager(settings)
    engine = CorrelationEngine(settings, MetricAggregator(), store=None, alerts=manager)
    engine.detectors = [_StubDetector([fab_corr(strength=0.9, confidence=0.95)])]

    engine.detect([], [], now=EPOCH)

    assert len(manager.alerts) == 1
    assert manager.alerts[0].severity == "warning"
    assert manager.alerts[0].created_at == EPOCH
