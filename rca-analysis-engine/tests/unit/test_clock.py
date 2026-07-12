"""Unit tests for the ClockSkewCorrector (C8, feature area E).

Pin the clock-correction contract on small, controlled batches:

* **ε-tolerance** — two near-simultaneous events across independent services are treated
  as concurrent, so the sub-ε timestamp difference does NOT force an order: the original
  input order is preserved rather than re-sorted to raw-timestamp order;
* **dependency happens-before** — when a downstream event is stamped slightly *before* its
  upstream cause within ε (a clock-skew inversion), the corrected order pulls the upstream
  ahead;
* **real gaps trusted** — events more than ε apart keep their true chronological order and
  their exact instants;
* **conservation** — correction is a pure permutation: same event multiset out as in, no
  drops or duplicates;
* the optional correlation-id parser and per-service offset map behave; and
* **integration** — ``RCAAnalyzer.analyze`` (which runs correction first) still ranks the
  true root cause #1 on a skew-inverted cascade that would otherwise mis-order the graph.
"""

from datetime import datetime, timedelta, timezone

import pytest

from src.analysis import RCAAnalyzer
from src.analysis.clock import ClockSkewCorrector, _parse_correlation_id
from src.analysis.timeline import _parse_timestamp
from src.config import Settings
from src.generators import generate_incident
from src.models import LogEvent, LogLevel
from src.service_map import ServiceDependencyMap

#: Fixed anchor so controlled offsets map to reproducible ISO timestamps.
_BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _ts(offset_seconds: float) -> str:
    """An ISO-8601 timestamp ``offset_seconds`` after the fixed base time."""
    return (_BASE + timedelta(seconds=offset_seconds)).isoformat()


def _event(offset, service, level=LogLevel.ERROR, message="boom", event_id=None) -> LogEvent:
    return LogEvent(
        timestamp=_ts(offset),
        service=service,
        level=level,
        message=message,
        event_id=event_id,
    )


@pytest.fixture()
def settings():
    # Hermetic defaults (clock_skew_epsilon == 2.0), independent of any ambient .env.
    return Settings(_env_file=None)


@pytest.fixture()
def corrector(settings):
    return ClockSkewCorrector(settings, ServiceDependencyMap.from_settings(settings))


# --- ε-tolerance: concurrent events keep input order -----------------------------


def test_concurrent_independent_events_preserve_input_order(corrector):
    # 20ms apart across independent (no-dependency) services => concurrent. Input order has
    # the LATER-timestamped event first; a naive timestamp sort would swap them, but the
    # sub-ε difference must NOT force a reorder, so input order is preserved.
    events = [
        _event(0.02, "redis", event_id="a"),  # later timestamp, first in input
        _event(0.00, "file-storage", event_id="b"),  # earlier timestamp, second in input
    ]

    out = corrector.correct(events)

    assert [e.event_id for e in out] == ["a", "b"]


# --- Dependency-informed happens-before ------------------------------------------


def test_dependency_reorders_skew_inverted_pair(corrector):
    # 'auth' (downstream) is stamped 10ms BEFORE 'api-gateway' (its upstream) — a clock-skew
    # inversion within ε. The dependency happens-before overrides the sub-ε noise, pulling
    # the upstream cause ahead of the effect.
    events = [
        _event(0.00, "auth", event_id="auth"),  # downstream, earlier ts (fast clock)
        _event(0.01, "api-gateway", level=LogLevel.CRITICAL, event_id="gw"),  # upstream
    ]

    out = corrector.correct(events)

    assert [e.event_id for e in out] == ["gw", "auth"]


def test_dependency_within_epsilon_but_already_ordered_is_stable(corrector):
    # Upstream already first and within ε => order is already causal, correction is a no-op.
    events = [
        _event(0.00, "api-gateway", level=LogLevel.CRITICAL, event_id="gw"),
        _event(0.01, "auth", event_id="auth"),
    ]

    out = corrector.correct(events)

    assert [e.event_id for e in out] == ["gw", "auth"]


# --- Real (> ε) gaps are trusted -------------------------------------------------


def test_far_apart_events_keep_true_timestamp_order(corrector):
    # 5s apart (> ε) => a real gap. Input order is REVERSED vs. time; correction must trust
    # the timestamps (chronological), not the input order, across a real gap.
    events = [
        _event(5.0, "auth", event_id="auth"),  # later ts, first in input
        _event(0.0, "database", event_id="db"),  # earlier ts, second in input
    ]

    out = corrector.correct(events)

    assert [e.event_id for e in out] == ["db", "auth"]
    # Far-apart events keep their exact original instants (only the concurrency band nudges).
    assert _parse_timestamp(out[0].timestamp) == _BASE
    assert _parse_timestamp(out[1].timestamp) == _BASE + timedelta(seconds=5)


# --- Conservation: pure permutation ----------------------------------------------


def test_correction_preserves_event_multiset(corrector):
    events = generate_incident(seed=3).events

    out = corrector.correct(events)

    assert len(out) == len(events)

    def identity(event: LogEvent):
        # Timestamps may be corrected within ε, so exclude them from the identity key.
        return (event.event_id, event.service, event.level.value, event.message)

    assert sorted(map(identity, out)) == sorted(map(identity, events))


def test_correction_does_not_mutate_inputs(corrector):
    events = [
        _event(0.00, "auth", event_id="auth"),
        _event(0.01, "api-gateway", level=LogLevel.CRITICAL, event_id="gw"),
    ]
    before = [e.timestamp for e in events]

    corrector.correct(events)

    # The input list and its events are untouched (correction returns fresh copies).
    assert [e.timestamp for e in events] == before


def test_empty_and_single_event(corrector):
    assert corrector.correct([]) == []

    single = corrector.correct([_event(1.0, "database", event_id="solo")])
    assert [e.event_id for e in single] == ["solo"]
    assert _parse_timestamp(single[0].timestamp) == _BASE + timedelta(seconds=1)


def test_unparseable_timestamp_is_deferred_not_raised(corrector):
    # Correction never raises on a bad timestamp — it returns the batch untouched so the
    # timeline stage raises the single canonical ValueError (mapped to HTTP 422).
    bad = [LogEvent(timestamp="not-a-date", service="database", level=LogLevel.ERROR, message="x")]

    out = corrector.correct(bad)

    assert len(out) == 1
    assert out[0].timestamp == "not-a-date"


# --- Correlation-id parsing ------------------------------------------------------


@pytest.mark.parametrize(
    "message,expected",
    [
        ("connection refused request_id=abc-123", "abc-123"),
        ("trace=XY_9.z downstream failed", "xy_9.z"),
        ("correlation_id: REQ42 timed out", "req42"),
        ("txn-id=7f3a", "7f3a"),
        ("no id token here", None),
        ("", None),
    ],
)
def test_parse_correlation_id(message, expected):
    assert _parse_correlation_id(message) == expected


# --- Per-service offset map ------------------------------------------------------


def test_per_service_offset_aligns_a_fast_clock(settings):
    # 'auth' runs 5s fast: its raw t=3.0 really happened at t=-2, before the database event
    # at t=0. The offset pulls it back so the corrected order reflects the aligned clock.
    corrector = ClockSkewCorrector(
        settings, ServiceDependencyMap.from_settings(settings), offsets={"auth": 5.0}
    )
    events = [
        _event(3.0, "auth", event_id="auth"),  # fast clock: really earlier
        _event(0.0, "database", event_id="db"),
    ]

    out = corrector.correct(events)

    assert [e.event_id for e in out] == ["auth", "db"]
    # The alignment is baked into the emitted timestamp so it survives downstream re-sorts.
    assert _parse_timestamp(out[0].timestamp) < _parse_timestamp(out[1].timestamp)


# --- Integration: analyze() ranks the true root on a skewed scenario -------------


def test_analyze_ranks_true_root_on_skew_inverted_cascade():
    # A cascade where clock skew stamps the downstream effects slightly BEFORE the
    # api-gateway root (all within ε). Without correction the graph builder would sort the
    # effects ahead of the cause and api-gateway would have out-degree 0; clock correction
    # (run first inside analyze) restores the causal order so api-gateway ranks #1.
    events = [
        _event(10.00, "api-gateway", level=LogLevel.CRITICAL, event_id="gw"),
        _event(9.99, "auth", event_id="auth"),  # skew: 10ms early
        _event(9.98, "user", event_id="user"),  # skew: 20ms early
        _event(10.02, "database", event_id="db"),
    ]

    report = RCAAnalyzer(Settings(_env_file=None)).analyze(events)

    assert report.root_causes[0].event_id == "gw"
    assert report.root_causes[0].confidence == pytest.approx(1.0)
    # api-gateway is a genuine causal source after correction (its restored out-edges).
    sources = {edge["source"] for edge in report.causal_graph["edges"]}
    assert "gw" in sources
