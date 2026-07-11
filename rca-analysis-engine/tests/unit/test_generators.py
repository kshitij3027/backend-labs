"""Unit tests for the synthetic incident / event generators (C2).

Cover determinism, the injected root cause's map-consistency, downstream events
landing inside the temporal window, and bulk-event uniqueness for load testing.
"""

from dateutil.parser import isoparse

from src.config import get_settings
from src.generators import Scenario, generate_events, generate_incident
from src.models import LogEvent, LogLevel
from src.service_map import ServiceDependencyMap


def _fields(scenario):
    """The content tuple of every event in a scenario, for equality comparison."""
    return [
        (e.timestamp, e.service, e.level, e.message, e.event_id)
        for e in scenario.events
    ]


def test_generate_incident_is_deterministic():
    a = generate_incident(seed=1)
    b = generate_incident(seed=1)

    assert isinstance(a, Scenario)
    assert a.root_cause_event_id == b.root_cause_event_id
    assert a.root_cause_service == b.root_cause_service
    # Byte-identical event streams for the same seed.
    assert _fields(a) == _fields(b)


def test_generate_incident_seed_changes_jitter():
    a = generate_incident(seed=1)
    b = generate_incident(seed=2)
    # Fixed structure (same services/levels/order) but the seeded jitter shifts the
    # non-root timestamps, so the two streams are not identical.
    assert [e.service for e in a.events] == [e.service for e in b.events]
    assert [e.level for e in a.events] == [e.level for e in b.events]
    assert [e.timestamp for e in a.events] != [e.timestamp for e in b.events]


def test_root_cause_is_an_upstream_service_in_the_map():
    scenario = generate_incident(seed=1)
    smap = ServiceDependencyMap.from_settings(get_settings())

    assert scenario.root_cause_service in smap.all_services()
    # An upstream service: it has at least one declared direct downstream dependent.
    assert smap.downstream_of(scenario.root_cause_service)

    # The reported id points at an actual event, and that event is an early CRITICAL.
    root = next(e for e in scenario.events if e.event_id == scenario.root_cause_event_id)
    assert root.level == LogLevel.CRITICAL
    assert root is scenario.events[0]


def test_downstream_events_fall_within_temporal_window():
    settings = get_settings()
    scenario = generate_incident(seed=1)
    root = next(e for e in scenario.events if e.event_id == scenario.root_cause_event_id)
    root_dt = isoparse(root.timestamp)

    for event in scenario.events:
        delta = (isoparse(event.timestamp) - root_dt).total_seconds()
        # Root is the earliest event; everything else falls inside the window.
        assert 0 <= delta <= settings.temporal_window


def test_incident_event_ids_are_unique():
    scenario = generate_incident(seed=7)
    ids = [e.event_id for e in scenario.events]
    assert len(set(ids)) == len(ids)
    assert scenario.root_cause_event_id == "evt-7-000"


def test_generate_events_count_and_unique_ids():
    events = generate_events(1000, seed=2)
    assert len(events) == 1000
    assert all(isinstance(e, LogEvent) for e in events)
    assert len({e.event_id for e in events}) == 1000


def test_generate_events_is_deterministic():
    a = generate_events(50, seed=3)
    b = generate_events(50, seed=3)
    assert [(e.service, e.level, e.timestamp, e.event_id) for e in a] == [
        (e.service, e.level, e.timestamp, e.event_id) for e in b
    ]


def test_generate_events_empty_for_non_positive_count():
    assert generate_events(0, seed=1) == []
    assert generate_events(-5, seed=1) == []
