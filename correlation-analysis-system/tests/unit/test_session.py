"""Unit tests for the SessionDetector (journey grouping, quiescence, dedupe).

A journey (events sharing a ``correlation_id``) emits exactly one session_based
correlation, and only once it has gone quiet (newest hop >= 2.5 s old). Scores
follow coverage = distinct_sources/5: strength = coverage, confidence =
0.7 + 0.3 * coverage.
"""

import pytest

from src.config import Settings
from src.engine.base import DetectionContext
from src.engine.session import SessionDetector
from src.models import CorrelationType, LogEvent, SourceType


def mk_event(source: SourceType, ts: float, **kw) -> LogEvent:
    """A hand-built LogEvent with sensible defaults for the non-essential fields."""
    return LogEvent(
        id=kw.pop("id", f"{source.value}-{ts}"),
        timestamp=ts,
        source=source,
        service=kw.pop("service", source.value),
        level=kw.pop("level", "INFO"),
        message=kw.pop("message", f"{source.value} event at {ts}"),
        **kw,
    )


def make_ctx(window, now) -> DetectionContext:
    """Session detection only reads window_events; new_events stays empty."""
    return DetectionContext(
        now=now, new_events=[], window_events=list(window), aggregator=None
    )


def make_detector() -> SessionDetector:
    return SessionDetector(Settings(_env_file=None))


def journey_events() -> list[LogEvent]:
    """A 3-source journey spanning 1.0 s under one correlation_id/user_id."""
    return [
        mk_event(SourceType.WEB, 1000.0, correlation_id="corr_abc", user_id="user_42"),
        mk_event(
            SourceType.API_SERVICE, 1000.4, correlation_id="corr_abc", user_id="user_42"
        ),
        mk_event(
            SourceType.DATABASE, 1001.0, correlation_id="corr_abc", user_id="user_42"
        ),
    ]


def test_quiescent_multi_source_journey_emits_once_with_exact_scores():
    detector = make_detector()

    # now=1004.0 -> newest hop is 3.0 s old >= 2.5 s quiescence -> emittable.
    found = detector.detect(make_ctx(journey_events(), now=1004.0))

    assert len(found) == 1
    corr = found[0]
    assert corr.correlation_type is CorrelationType.SESSION
    assert corr.detected_at == 1004.0
    # coverage = 3 distinct sources / 5 -> strength 0.6, confidence 0.88.
    assert corr.strength == pytest.approx(0.6)
    assert corr.confidence == pytest.approx(0.88)
    assert corr.details["hops"] == ["web", "api_service", "database"]
    assert corr.details["distinct_sources"] == 3
    assert corr.details["span_seconds"] == pytest.approx(1.0)
    assert corr.details["user_id"] == "user_42"
    # event_a = first hop in time, event_b = last hop.
    assert corr.event_a.source is SourceType.WEB
    assert corr.event_a.timestamp == 1000.0
    assert corr.event_b.source is SourceType.DATABASE
    assert corr.event_b.timestamp == 1001.0


def test_journey_not_yet_quiescent_is_withheld():
    # now=1001.5 -> newest hop only 0.5 s old < 2.5 s -> the journey may still
    # be producing hops, so nothing emits yet.
    assert make_detector().detect(make_ctx(journey_events(), now=1001.5)) == []


def test_single_source_journey_ignored():
    events = [
        mk_event(SourceType.WEB, 1000.0, correlation_id="corr_solo"),
        mk_event(SourceType.WEB, 1000.5, correlation_id="corr_solo"),
    ]
    assert make_detector().detect(make_ctx(events, now=1004.0)) == []


def test_same_journey_deduped_on_later_ticks():
    detector = make_detector()
    events = journey_events()
    assert len(detector.detect(make_ctx(events, now=1004.0))) == 1
    # The journey is still in the window next tick — but it already emitted.
    assert detector.detect(make_ctx(events, now=1006.0)) == []


def test_withheld_journey_still_emits_after_quiescence():
    # The not-yet-quiescent skip must NOT record the journey as seen.
    detector = make_detector()
    events = journey_events()
    assert detector.detect(make_ctx(events, now=1001.5)) == []
    assert len(detector.detect(make_ctx(events, now=1004.0))) == 1


def test_events_without_correlation_id_ignored():
    events = [mk_event(SourceType.WEB, 1000.0), mk_event(SourceType.DATABASE, 1001.0)]
    assert make_detector().detect(make_ctx(events, now=1004.0)) == []
