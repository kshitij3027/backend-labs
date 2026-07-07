"""Unit tests for the TemporalDetector (spec area: temporal correlation detection).

All events are hand-built with explicit timestamps and every detect() call gets
an explicit simulated ``now``, so scores are asserted against the exact formulas
(strength = 1 - dt/window, confidence = clamp(support/10, 0.1, 0.9)).
"""

import pytest

from src.config import Settings
from src.engine.base import DetectionContext
from src.engine.temporal import MAX_EMISSIONS_PER_TICK, TemporalDetector
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


def make_ctx(new, now, window=None) -> DetectionContext:
    """A detection context where the window defaults to exactly the new events."""
    return DetectionContext(
        now=now,
        new_events=list(new),
        window_events=list(new) if window is None else list(window),
        aggregator=None,
    )


def make_detector(**overrides) -> TemporalDetector:
    return TemporalDetector(Settings(_env_file=None, **overrides))


def test_cross_source_pair_two_seconds_apart_emits_one_correlation():
    detector = make_detector()
    web = mk_event(SourceType.WEB, 1000.0)
    db = mk_event(SourceType.DATABASE, 1002.0)

    found = detector.detect(make_ctx([web, db], now=1002.5))

    assert len(found) == 1
    corr = found[0]
    assert corr.correlation_type is CorrelationType.TEMPORAL
    assert corr.strength == pytest.approx(1 - 2 / 30, abs=1e-6)
    assert corr.detected_at == 1002.5
    # event_a is the earlier event of the pair.
    assert corr.event_a.timestamp < corr.event_b.timestamp
    assert corr.event_a.source is SourceType.WEB
    assert corr.event_b.source is SourceType.DATABASE
    assert corr.details["dt_seconds"] == pytest.approx(2.0)
    # Both new events produced one candidate each -> support 2 -> confidence 0.2.
    assert corr.details["support"] == 2
    assert corr.confidence == pytest.approx(0.2)


def test_same_source_pair_never_emitted():
    detector = make_detector()
    events = [mk_event(SourceType.WEB, 1000.0), mk_event(SourceType.WEB, 1001.0)]
    assert detector.detect(make_ctx(events, now=1001.5)) == []


def test_pair_beyond_window_not_emitted():
    detector = make_detector()  # window_seconds = 30
    events = [mk_event(SourceType.WEB, 1000.0), mk_event(SourceType.DATABASE, 1031.0)]
    assert detector.detect(make_ctx(events, now=1031.5)) == []


def test_dedupe_suppresses_same_pair_within_ttl():
    detector = make_detector()  # dedup_ttl_seconds = 30
    web1 = mk_event(SourceType.WEB, 1000.0)
    db1 = mk_event(SourceType.DATABASE, 1002.0)
    assert len(detector.detect(make_ctx([web1, db1], now=1002.5))) == 1

    # Next tick, 2 s later (well inside the 30 s TTL): fresh events from the
    # same source pair must be suppressed by the dedupe cache.
    web2 = mk_event(SourceType.WEB, 1003.0)
    db2 = mk_event(SourceType.DATABASE, 1004.0)
    second = detector.detect(
        make_ctx([web2, db2], now=1004.5, window=[web1, db1, web2, db2])
    )
    assert second == []


def test_emissions_capped_and_unique_per_source_pair():
    detector = make_detector()
    # One event per source in a tight cluster: every cross-source pair is a
    # candidate (5 sources -> 10 unordered pairs, comfortably under the cap).
    events = [mk_event(source, 1000.0 + 0.1 * i) for i, source in enumerate(SourceType)]

    found = detector.detect(make_ctx(events, now=1001.0))

    assert 0 < len(found) <= MAX_EMISSIONS_PER_TICK
    pairs = {frozenset((c.event_a.source, c.event_b.source)) for c in found}
    assert len(pairs) == len(found)  # at most one emission per source pair
    assert all(c.event_a.source is not c.event_b.source for c in found)
