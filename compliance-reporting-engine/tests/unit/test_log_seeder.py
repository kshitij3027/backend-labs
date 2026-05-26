"""Unit tests for :mod:`src.logs.seeder`.

These guard four properties the rest of the pipeline relies on:

  * **Determinism** — same seed must yield identical output. The
    aggregator's fixtures and the demo screenshots all depend on this.
  * **Tag containment** — every event's ``framework_tags`` must be a
    subset of what was requested, otherwise the repository's
    framework-membership filter would silently miss events.
  * **Window containment** — every ``timestamp`` lies within the
    requested window. The aggregator filters by ``BETWEEN``, so any
    out-of-window event would never surface anyway, but it'd skew
    seed-time counts.
  * **Payload sanity** — payload is a non-empty dict so JSON / XML /
    CSV / PDF exporters never have to special-case empty payloads.
  * **Event-type per-framework consistency** — an event's
    ``event_type`` must belong to the allowlist of at least one of its
    tagged frameworks, so the framework classifiers can resolve it.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.logs.seeder import FRAMEWORK_EVENT_TYPES, generate_synthetic_logs


def test_deterministic_same_seed() -> None:
    """Two calls with identical args produce identical event lists."""
    frameworks = ["SOX", "HIPAA", "PCI_DSS", "GDPR"]
    period_end = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    period_start = period_end - timedelta(days=30)

    first = generate_synthetic_logs(
        100,
        frameworks=frameworks,
        seed=123,
        period_start=period_start,
        period_end=period_end,
    )
    second = generate_synthetic_logs(
        100,
        frameworks=frameworks,
        seed=123,
        period_start=period_start,
        period_end=period_end,
    )

    assert len(first) == len(second) == 100
    for a, b in zip(first, second):
        assert a == b


def test_framework_tags_subset_of_input() -> None:
    """Every event's framework_tags is a subset of the requested frameworks."""
    frameworks = ["SOX", "HIPAA"]
    period_end = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    period_start = period_end - timedelta(days=10)

    events = generate_synthetic_logs(
        200,
        frameworks=frameworks,
        seed=7,
        period_start=period_start,
        period_end=period_end,
    )

    requested = set(frameworks)
    for event in events:
        tags = set(event["framework_tags"])
        assert tags, "tags must not be empty"
        assert tags.issubset(requested), f"tags {tags} not subset of {requested}"


def test_timestamps_within_window() -> None:
    """Every timestamp is within ``[period_start, period_end]`` and tz-aware UTC."""
    frameworks = ["SOX", "HIPAA", "PCI_DSS", "GDPR"]
    period_end = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    period_start = period_end - timedelta(days=30)

    events = generate_synthetic_logs(
        150,
        frameworks=frameworks,
        seed=99,
        period_start=period_start,
        period_end=period_end,
    )

    for event in events:
        ts = event["timestamp"]
        assert ts.tzinfo is not None
        assert period_start <= ts <= period_end


def test_payload_non_empty() -> None:
    """Every event has a non-empty dict payload."""
    frameworks = ["SOX", "HIPAA", "PCI_DSS", "GDPR"]
    events = generate_synthetic_logs(75, frameworks=frameworks, seed=1)

    for event in events:
        payload = event["payload"]
        assert isinstance(payload, dict)
        assert len(payload) > 0


def test_event_type_matches_framework() -> None:
    """Each event_type must be in the allowlist of at least one tagged framework."""
    frameworks = ["SOX", "HIPAA", "PCI_DSS", "GDPR"]
    events = generate_synthetic_logs(250, frameworks=frameworks, seed=2026)

    for event in events:
        event_type = event["event_type"]
        tags = event["framework_tags"]
        allowed = {
            evt
            for fw in tags
            for evt in FRAMEWORK_EVENT_TYPES.get(fw, [])
        }
        assert event_type in allowed, (
            f"event_type {event_type!r} not in allowlist of tagged frameworks {tags}"
        )
