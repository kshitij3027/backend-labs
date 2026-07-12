"""Unit tests for the AnomalyAmplifier (C7, feature area D).

Pin the base-rate-correction contract: after a baseline learns that a routine
``(database, INFO)`` type is ubiquitous while a ``(payment, CRITICAL)`` type is rare, the
rare event must score HIGHER than the common one (no false positive from routine noise);
the Ochiai lift must suppress a type that fired in every historical window; an
empty-history amplifier must degrade gracefully (surprise/severity fallback) without
crashing; and every score must land in ``[0, 1]``. The amplifier is pure/deterministic
given its accumulated baseline, so identical input yields identical scores.
"""

import pytest

from src.analysis.anomaly import AnomalyAmplifier
from src.config import get_settings
from src.generators import generate_incident
from src.models import LogEvent, LogLevel


def _event(service: str, level: LogLevel, i: int = 0, event_id: str | None = None) -> LogEvent:
    return LogEvent(
        timestamp="2026-01-01T00:00:00+00:00",
        service=service,
        level=level,
        message=f"{service} {level.value}",
        event_id=event_id or f"{service}-{level.value}-{i}",
    )


@pytest.fixture()
def amp():
    return AnomalyAmplifier(get_settings())


# --- Base-rate correction (the KEY property) -------------------------------------


def test_rare_type_scores_higher_than_common_after_baseline(amp):
    # Baseline: ten incidents dominated by routine (database, INFO); a (payment,
    # CRITICAL) type is never seen historically.
    for n in range(10):
        baseline = [
            _event("database", LogLevel.INFO, i, event_id=f"b{n}-{i}") for i in range(5)
        ]
        amp.observe(baseline)

    # Score an incident carrying both a routine event and a rare, severe one.
    incident = [
        _event("database", LogLevel.INFO, event_id="cur-db"),
        _event("payment", LogLevel.CRITICAL, event_id="cur-pay"),
    ]
    scores = amp.score(incident)

    assert 0.0 <= scores["cur-db"] <= 1.0
    assert 0.0 <= scores["cur-pay"] <= 1.0
    # Base-rate correction: the rare CRITICAL outranks the ubiquitous routine INFO, so the
    # common event is not amplified into a false-positive root cause.
    assert scores["cur-pay"] > scores["cur-db"]


def test_ochiai_suppresses_type_present_in_every_window(amp):
    # X fires in every historical incident; Y in exactly one; Z has never been seen.
    for n in range(8):
        amp.observe([_event("redis", LogLevel.WARNING, event_id=f"x{n}")])
    amp.observe([_event("auth", LogLevel.ERROR, event_id="y0")])

    ubiquitous = amp._ochiai(("redis", "WARNING"))
    rare = amp._ochiai(("auth", "ERROR"))
    unseen = amp._ochiai(("payment", "CRITICAL"))

    # More historical windows containing the type => stronger suppression (lower lift).
    assert ubiquitous < rare < unseen
    # A never-before-seen type gets the maximal Ochiai lift.
    assert unseen == pytest.approx(1.0)


def test_ubiquitous_type_scores_low_end_to_end(amp):
    # A type present in every window should be suppressed in the final score too, well
    # below a rarely-seen peer of the same severity.
    for n in range(6):
        amp.observe(
            [_event("redis", LogLevel.WARNING, i, event_id=f"w{n}-{i}") for i in range(4)]
        )
    amp.observe([_event("auth", LogLevel.WARNING, event_id="seed-auth")])

    scores = amp.score(
        [
            _event("redis", LogLevel.WARNING, event_id="cur-redis"),
            _event("auth", LogLevel.WARNING, event_id="cur-auth"),
        ]
    )
    # Same severity, so the gap is pure base rate: the ubiquitous type scores lower.
    assert scores["cur-redis"] < scores["cur-auth"]


# --- Empty-history fallback ------------------------------------------------------


def test_empty_history_graceful_fallback(amp):
    # No observe() calls => empty baseline. score() must still return valid [0, 1] scores
    # via the surprise/severity fallback, never a crash or a divide-by-zero.
    incident = [_event("api-gateway", LogLevel.CRITICAL, event_id="crit")]
    incident += [_event("database", LogLevel.INFO, i, event_id=f"info{i}") for i in range(3)]

    scores = amp.score(incident)

    assert set(scores) == {"crit", "info0", "info1", "info2"}
    assert all(0.0 <= s <= 1.0 for s in scores.values())
    # Fallback leans on within-incident rarity + severity: the lone CRITICAL is more
    # anomalous than the repeated routine INFO.
    assert scores["crit"] > scores["info0"]


# --- Range + determinism ---------------------------------------------------------


def test_all_scores_within_unit_interval_on_generated_incident(amp):
    scores = amp.score(generate_incident(seed=1).events)

    assert scores  # a realistic cascade produces per-event scores
    assert all(0.0 <= s <= 1.0 for s in scores.values())


def test_score_is_deterministic(amp):
    events = generate_incident(seed=2).events
    # Pure read (no observe between calls) => byte-identical results.
    assert amp.score(events) == amp.score(events)


def test_empty_events_returns_empty(amp):
    assert amp.score([]) == {}


def test_observe_ignores_empty_batch(amp):
    amp.observe([])
    # An empty batch is not counted as an incident, so history stays empty and scoring
    # still takes the fallback path.
    assert amp._incident_count == 0
    scores = amp.score([_event("payment", LogLevel.CRITICAL, event_id="p0")])
    assert 0.0 <= scores["p0"] <= 1.0
