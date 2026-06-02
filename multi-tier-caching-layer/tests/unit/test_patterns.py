"""Unit tests for the heuristic query-pattern engine (src/patterns.py).

Recency depends on wall-clock time, so every test injects a mutable fake clock
(``FakeClock``) and advances it explicitly — no ``time.sleep``, fully
deterministic. Monotonicity tests isolate one scoring factor at a time by
holding the other two equal across the two keys being compared.
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.patterns import PatternEngine, QueryObservation


class FakeClock:
    """A tiny mutable clock. ``engine = PatternEngine(timer=clock)``."""

    def __init__(self, start: float = 0.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# A fixed epoch with a known UTC hour/day for the recording tests.
# 2024-01-01 is a Monday (weekday() == 0). 13:00:00 UTC -> hour 13.
KNOWN_TS = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc).timestamp()


# --------------------------------------------------------------------------
# Recording / dataclass / analysis
# --------------------------------------------------------------------------


def test_record_query_derives_hour_and_day_from_ts() -> None:
    """A known ts yields the expected UTC hour-of-day and day-of-week."""
    engine = PatternEngine()
    engine.record_query("k1", "q1", "team-a", 50.0, ts=KNOWN_TS)

    # Pull the single observation back out of the window for inspection.
    obs = engine._observations[-1]
    assert isinstance(obs, QueryObservation)
    assert obs.hour_of_day == 13
    assert obs.day_of_week == 0  # Monday
    assert obs.key == "k1"
    assert obs.query == "q1"
    assert obs.source == "team-a"
    assert obs.response_time_ms == 50.0
    assert obs.ts == KNOWN_TS


def test_query_observation_is_frozen() -> None:
    """QueryObservation is immutable (frozen dataclass)."""
    import dataclasses

    import pytest

    obs = QueryObservation(
        key="k", query="q", source=None, response_time_ms=1.0,
        ts=0.0, hour_of_day=0, day_of_week=0,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        obs.key = "other"  # type: ignore[misc]


def test_record_query_ts_defaults_to_injected_timer() -> None:
    """With no explicit ts, the engine stamps the injected clock's value."""
    clock = FakeClock(start=KNOWN_TS)
    engine = PatternEngine(timer=clock)
    engine.record_query("k1", "q1", None, 10.0)
    obs = engine._observations[-1]
    assert obs.ts == KNOWN_TS
    assert obs.hour_of_day == 13


def test_analyze_histograms_reflect_recorded_observations() -> None:
    """hour/day histograms count exactly the recorded observations."""
    engine = PatternEngine()
    # Three queries at hour 13 / Monday, two at a different hour/day.
    other_ts = datetime(2024, 1, 3, 9, 0, 0, tzinfo=timezone.utc).timestamp()
    other_dow = datetime(2024, 1, 3, 9, 0, 0, tzinfo=timezone.utc).weekday()  # Wed == 2
    for _ in range(3):
        engine.record_query("a", "qa", "team-a", 20.0, ts=KNOWN_TS)
    for _ in range(2):
        engine.record_query("b", "qb", "team-b", 20.0, ts=other_ts)

    analysis = engine.analyze()

    # Full zero-filled histograms: all 24 hours and 7 days present.
    assert len(analysis["hour_of_day"]) == 24
    assert len(analysis["day_of_week"]) == 7
    assert analysis["hour_of_day"][13] == 3
    assert analysis["hour_of_day"][9] == 2
    assert analysis["day_of_week"][0] == 3  # Monday
    assert analysis["day_of_week"][other_dow] == 2  # Wednesday
    # Untouched buckets stay zero.
    assert analysis["hour_of_day"][0] == 0
    assert analysis["total_observations"] == 5


def test_analyze_per_source_counts_correct() -> None:
    """per_source tallies observations per source; None -> 'unknown'."""
    engine = PatternEngine()
    engine.record_query("a", "qa", "team-a", 20.0, ts=KNOWN_TS)
    engine.record_query("b", "qb", "team-a", 20.0, ts=KNOWN_TS)
    engine.record_query("c", "qc", "team-b", 20.0, ts=KNOWN_TS)
    engine.record_query("d", "qd", None, 20.0, ts=KNOWN_TS)

    per_source = engine.analyze()["per_source"]
    assert per_source["team-a"] == 2
    assert per_source["team-b"] == 1
    assert per_source["unknown"] == 1


def test_cost_ema_seeds_then_averages() -> None:
    """First sight seeds cost_ema_ms with the raw value; later sights blend it."""
    clock = FakeClock(start=KNOWN_TS)
    engine = PatternEngine(timer=clock)
    engine.record_query("k", "q", None, 100.0)
    assert engine._aggregates["k"]["cost_ema_ms"] == 100.0
    engine.record_query("k", "q", None, 200.0)
    # EMA alpha=0.3 -> 0.3*200 + 0.7*100 = 130.0
    assert abs(engine._aggregates["k"]["cost_ema_ms"] - 130.0) < 1e-9
    assert engine._aggregates["k"]["count"] == 2


# --------------------------------------------------------------------------
# Scoring monotonicity (isolate one factor, hold the other two equal)
# --------------------------------------------------------------------------


def _score_for(engine: PatternEngine, key: str) -> float:
    """Helper: score a single tracked key at the current clock time."""
    return engine._score(engine._aggregates[key], engine._timer())


def test_monotonic_in_frequency() -> None:
    """More records (same recency, same cost) -> strictly higher score."""
    clock = FakeClock(start=KNOWN_TS)
    engine = PatternEngine(timer=clock)
    # A seen 5x, B seen 2x; identical cost, identical last_seen (same clock).
    for _ in range(5):
        engine.record_query("A", "qa", None, 100.0)
    for _ in range(2):
        engine.record_query("B", "qb", None, 100.0)

    # last_seen is identical (clock never advanced); cost_ema identical (same value).
    assert engine._aggregates["A"]["last_seen"] == engine._aggregates["B"]["last_seen"]
    assert engine._aggregates["A"]["cost_ema_ms"] == engine._aggregates["B"]["cost_ema_ms"]
    assert _score_for(engine, "A") > _score_for(engine, "B")


def test_monotonic_in_recency() -> None:
    """Equal count + cost, but the more recently seen key scores higher."""
    clock = FakeClock(start=KNOWN_TS)
    engine = PatternEngine(timer=clock)
    # B recorded first (older), then clock advances, then A recorded (newer).
    engine.record_query("B", "qb", None, 100.0)
    clock.advance(1800.0)  # half of the default 3600s half-life
    engine.record_query("A", "qa", None, 100.0)

    # Same count (1 each) and same cost; only last_seen differs.
    assert engine._aggregates["A"]["count"] == engine._aggregates["B"]["count"]
    assert engine._aggregates["A"]["cost_ema_ms"] == engine._aggregates["B"]["cost_ema_ms"]
    assert engine._aggregates["A"]["last_seen"] > engine._aggregates["B"]["last_seen"]
    assert _score_for(engine, "A") > _score_for(engine, "B")


def test_monotonic_in_cost() -> None:
    """Equal count + recency, but the more expensive key scores higher."""
    clock = FakeClock(start=KNOWN_TS)
    engine = PatternEngine(timer=clock)
    # Both recorded once at the same instant; A is more expensive than B.
    engine.record_query("A", "qa", None, 500.0)
    engine.record_query("B", "qb", None, 50.0)

    assert engine._aggregates["A"]["count"] == engine._aggregates["B"]["count"]
    assert engine._aggregates["A"]["last_seen"] == engine._aggregates["B"]["last_seen"]
    assert engine._aggregates["A"]["cost_ema_ms"] > engine._aggregates["B"]["cost_ema_ms"]
    assert _score_for(engine, "A") > _score_for(engine, "B")


def test_recency_in_unit_interval() -> None:
    """recency is 1.0 when just seen and decays toward (but stays above) 0."""
    clock = FakeClock(start=KNOWN_TS)
    engine = PatternEngine(timer=clock, recency_half_life_seconds=3600.0)
    engine.record_query("k", "q", None, 10.0)
    # Just seen -> recency 1.0.
    assert abs(engine._recency(engine._aggregates["k"]["last_seen"], clock()) - 1.0) < 1e-9
    # After one half-life -> 0.5.
    clock.advance(3600.0)
    assert abs(engine._recency(engine._aggregates["k"]["last_seen"], clock()) - 0.5) < 1e-9
    # Always strictly positive.
    clock.advance(3600.0 * 100)
    assert engine._recency(engine._aggregates["k"]["last_seen"], clock()) > 0.0


# --------------------------------------------------------------------------
# Recommendations / hot_keys ranking + truncation
# --------------------------------------------------------------------------


def test_recommendations_truncate_and_sorted_desc() -> None:
    """recommendations(top_n=2) returns exactly 2 items, sorted by score desc."""
    clock = FakeClock(start=KNOWN_TS)
    engine = PatternEngine(timer=clock)
    # Three keys with strictly increasing frequency (same recency + cost) so the
    # score ordering is unambiguous: C > B > A.
    for _ in range(2):
        engine.record_query("A", "qa", None, 100.0)
    for _ in range(4):
        engine.record_query("B", "qb", None, 100.0)
    for _ in range(6):
        engine.record_query("C", "qc", None, 100.0)

    recs = engine.recommendations(top_n=2)
    assert len(recs) == 2
    scores = [r["score"] for r in recs]
    assert scores == sorted(scores, reverse=True)
    # Top two are the two highest-frequency keys.
    assert [r["key"] for r in recs] == ["C", "B"]
    # Each rec carries the documented shape.
    assert set(recs[0].keys()) == {"key", "query", "source", "score", "count", "reason"}
    assert "freq=" in recs[0]["reason"]


def test_frequent_recent_expensive_outranks_rare_old_cheap() -> None:
    """A hot/recent/expensive key beats a rare/old/cheap one."""
    clock = FakeClock(start=KNOWN_TS)
    engine = PatternEngine(timer=clock)

    # RARE: seen once, long ago, cheap.
    engine.record_query("rare", "q_rare", "team-x", 5.0)
    clock.advance(7200.0)  # two half-lives in the past once we add HOT

    # HOT: seen many times, just now, expensive.
    for _ in range(20):
        engine.record_query("hot", "q_hot", "team-y", 400.0)

    recs = engine.recommendations(top_n=2)
    assert recs[0]["key"] == "hot"
    assert recs[1]["key"] == "rare"
    assert recs[0]["score"] > recs[1]["score"]


def test_hot_keys_shape_and_ranking() -> None:
    """hot_keys returns the leaner shape and the same descending ranking."""
    clock = FakeClock(start=KNOWN_TS)
    engine = PatternEngine(timer=clock)
    for _ in range(3):
        engine.record_query("low", "q_low", None, 100.0)
    for _ in range(9):
        engine.record_query("high", "q_high", None, 100.0)

    hot = engine.hot_keys(top_n=5)
    assert [h["key"] for h in hot] == ["high", "low"]
    assert set(hot[0].keys()) == {"key", "query", "score", "count"}
    assert hot[0]["count"] == 9
    assert hot[0]["score"] > hot[1]["score"]


def test_recommendations_empty_when_no_traffic() -> None:
    """No recorded queries -> empty recommendations and hot_keys."""
    engine = PatternEngine()
    assert engine.recommendations() == []
    assert engine.hot_keys() == []


def test_aggregate_eviction_keeps_most_recent() -> None:
    """At capacity, the least-recently-seen aggregate is evicted on a new key."""
    clock = FakeClock(start=KNOWN_TS)
    engine = PatternEngine(history_size=2, timer=clock)
    engine.record_query("old", "q_old", None, 10.0)
    clock.advance(10.0)
    engine.record_query("mid", "q_mid", None, 10.0)
    clock.advance(10.0)
    # Adding a third key should evict "old" (oldest last_seen).
    engine.record_query("new", "q_new", None, 10.0)

    keys = {r["key"] for r in engine.hot_keys()}
    assert keys == {"mid", "new"}
    assert "old" not in keys
