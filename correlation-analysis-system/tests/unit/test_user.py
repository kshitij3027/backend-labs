"""Unit tests for the UserDetector (cross-journey activity of one user).

A user qualifies once the window holds >= 2 distinct journeys (non-None
correlation_ids) across >= 2 distinct sources for that user. Scores follow
coverage = distinct_sources/5: strength = coverage, confidence =
0.5 + 0.3 * coverage. Emissions are anchored to fresh activity: a user whose
newest event is more than FRESHNESS_SECONDS old is skipped without being
marked seen.
"""

import pytest

from src.config import Settings
from src.engine.base import DetectionContext
from src.engine.user import MAX_EMISSIONS_PER_TICK, UserDetector
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
    """User detection only reads window_events; new_events stays empty."""
    return DetectionContext(
        now=now, new_events=[], window_events=list(window), aggregator=None
    )


def make_detector() -> UserDetector:
    return UserDetector(Settings(_env_file=None))


def cross_journey_events() -> list[LogEvent]:
    """user_7 across two journeys (corr_a, corr_b) touching 4 distinct sources."""
    return [
        mk_event(SourceType.WEB, 1000.0, correlation_id="corr_a", user_id="user_7"),
        mk_event(
            SourceType.API_SERVICE, 1000.5, correlation_id="corr_a", user_id="user_7"
        ),
        mk_event(SourceType.PAYMENT, 1010.0, correlation_id="corr_b", user_id="user_7"),
        mk_event(SourceType.DATABASE, 1011.0, correlation_id="corr_b", user_id="user_7"),
    ]


def test_user_across_two_journeys_emits_with_exact_scores():
    found = make_detector().detect(make_ctx(cross_journey_events(), now=1012.0))

    assert len(found) == 1
    corr = found[0]
    assert corr.correlation_type is CorrelationType.USER
    assert corr.detected_at == 1012.0
    # event_a = the user's first event in time, event_b = the last.
    assert corr.event_a.source is SourceType.WEB
    assert corr.event_a.timestamp == 1000.0
    assert corr.event_b.source is SourceType.DATABASE
    assert corr.event_b.timestamp == 1011.0
    # coverage = 4 distinct sources / 5 -> strength 0.8, confidence 0.74.
    assert corr.strength == pytest.approx(0.8)
    assert corr.confidence == pytest.approx(0.74)
    assert corr.details["user_id"] == "user_7"
    assert corr.details["distinct_sources"] == 4
    assert corr.details["journey_count"] == 2
    assert corr.details["span_seconds"] == pytest.approx(11.0)


def test_single_journey_user_ignored():
    events = [
        mk_event(SourceType.WEB, 1000.0, correlation_id="corr_a", user_id="user_1"),
        mk_event(SourceType.DATABASE, 1001.0, correlation_id="corr_a", user_id="user_1"),
    ]
    assert make_detector().detect(make_ctx(events, now=1004.0)) == []


def test_single_source_user_ignored():
    events = [
        mk_event(SourceType.WEB, 1000.0, correlation_id="corr_a", user_id="user_1"),
        mk_event(SourceType.WEB, 1005.0, correlation_id="corr_b", user_id="user_1"),
    ]
    assert make_detector().detect(make_ctx(events, now=1006.0)) == []


def test_events_without_user_id_ignored():
    events = [
        mk_event(SourceType.WEB, 1000.0, correlation_id="corr_a"),
        mk_event(SourceType.DATABASE, 1001.0, correlation_id="corr_b"),
    ]
    assert make_detector().detect(make_ctx(events, now=1004.0)) == []


def test_same_user_deduped_on_later_ticks():
    detector = make_detector()  # dedup_ttl_seconds = 30
    events = cross_journey_events()
    assert len(detector.detect(make_ctx(events, now=1012.0))) == 1
    # The user's events are still in the window next tick — already emitted.
    assert detector.detect(make_ctx(events, now=1014.0)) == []


def test_gated_user_still_emits_once_second_journey_appears():
    # The single-journey skip must NOT record the user as seen.
    detector = make_detector()
    events = cross_journey_events()
    assert detector.detect(make_ctx(events[:2], now=1002.0)) == []
    assert len(detector.detect(make_ctx(events, now=1012.0))) == 1


def test_stale_user_activity_not_emitted():
    # Freshness guard: the user qualifies (2 journeys, 4 sources) but their
    # newest event is 10 s old — emitting now would report ~10 s of detection
    # latency, so the detector must stay quiet AND must not mark the user as
    # seen (a wrongly recorded TTL would suppress the fresh emission below).
    detector = make_detector()
    events = cross_journey_events()  # newest event at t=1011.0
    assert detector.detect(make_ctx(events, now=1021.0)) == []
    # Fresh activity arrives for the same user: the emission goes through and
    # is anchored to the new event, not the stale window remnants.
    fresh = events + [
        mk_event(
            SourceType.INVENTORY, 1024.0, correlation_id="corr_c", user_id="user_7"
        )
    ]
    found = detector.detect(make_ctx(fresh, now=1025.0))
    assert len(found) == 1
    assert found[0].details["user_id"] == "user_7"
    assert found[0].event_b.timestamp == 1024.0  # event_b = the fresh event


def test_emissions_capped_per_tick():
    detector = make_detector()
    # Timestamps are packed within ~3.5 s of `now` so every user clears the
    # freshness guard and the per-tick cap is the only limiting factor.
    events: list[LogEvent] = []
    for i in range(MAX_EMISSIONS_PER_TICK + 5):
        user = f"user_{i}"
        events.append(
            mk_event(
                SourceType.WEB, 1000.0 + i * 0.1, correlation_id=f"corr_{i}a", user_id=user
            )
        )
        events.append(
            mk_event(
                SourceType.DATABASE, 1000.5 + i * 0.1, correlation_id=f"corr_{i}b", user_id=user
            )
        )

    found = detector.detect(make_ctx(events, now=1004.0))

    assert len(found) == MAX_EMISSIONS_PER_TICK
    # One emission per distinct user, never two for the same one.
    assert len({c.details["user_id"] for c in found}) == MAX_EMISSIONS_PER_TICK
