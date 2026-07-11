"""Unit tests for the TimelineReconstructor (C2).

Cover chronological sorting, the ``T+M:SS`` relative-time format, contiguous
sequence ids, surrounding-event context, empty/single-event edges, unparseable
timestamps, and the event-id preservation / deterministic-derivation contract.
"""

import pytest

from src.analysis.timeline import TimelineReconstructor
from src.config import get_settings
from src.models import LogEvent, LogLevel


@pytest.fixture()
def reconstructor():
    return TimelineReconstructor(get_settings())


def _event(ts, service="api-gateway", level=LogLevel.ERROR, message="boom", event_id=None):
    return LogEvent(
        timestamp=ts, service=service, level=level, message=message, event_id=event_id
    )


def test_out_of_order_events_sort_ascending(reconstructor):
    # Deliberately unsorted input: +10s, +0s, +125s, +5s.
    events = [
        _event("2026-01-01T00:00:10Z", message="ten"),
        _event("2026-01-01T00:00:00Z", message="zero"),
        _event("2026-01-01T00:02:05Z", message="one-twentyfive"),
        _event("2026-01-01T00:00:05Z", message="five"),
    ]
    timeline = reconstructor.reconstruct(events)

    assert [e.message for e in timeline] == ["zero", "five", "ten", "one-twentyfive"]
    assert timeline[0].timestamp == "2026-01-01T00:00:00Z"


def test_first_entry_relative_time_is_zero(reconstructor):
    timeline = reconstructor.reconstruct([_event("2026-01-01T00:00:00Z")])
    assert timeline[0].relative_time == "T+0:00"


def test_relative_time_formats_minutes_and_padded_seconds(reconstructor):
    # 125s after start -> 2 minutes, 05 seconds -> "T+2:05".
    events = [
        _event("2026-01-01T00:00:00Z", message="start"),
        _event("2026-01-01T00:02:05Z", message="later"),
    ]
    timeline = reconstructor.reconstruct(events)
    assert timeline[0].relative_time == "T+0:00"
    assert timeline[1].relative_time == "T+2:05"


def test_relative_time_minutes_unbounded(reconstructor):
    # Over an hour out: minutes exceed 59 (not wrapped), seconds still zero-padded.
    events = [
        _event("2026-01-01T00:00:00Z"),
        _event("2026-01-01T01:01:01Z"),  # 3661s -> 61:01
    ]
    timeline = reconstructor.reconstruct(events)
    assert timeline[1].relative_time == "T+61:01"


def test_sequence_ids_are_contiguous_one_based(reconstructor):
    events = [_event(f"2026-01-01T00:00:{s:02d}Z") for s in (3, 1, 2, 0)]
    timeline = reconstructor.reconstruct(events)
    assert [e.sequence_id for e in timeline] == [1, 2, 3, 4]


def test_context_preceding_and_following_neighbours(reconstructor):
    events = [
        _event("2026-01-01T00:00:00Z", message="a"),
        _event("2026-01-01T00:00:01Z", message="b"),
        _event("2026-01-01T00:00:02Z", message="c"),
    ]
    timeline = reconstructor.reconstruct(events)

    # Ends have no neighbour on the missing side.
    assert timeline[0].context["preceding_event_id"] is None
    assert timeline[0].context["following_event_id"] == timeline[1].event_id
    assert timeline[1].context["preceding_event_id"] == timeline[0].event_id
    assert timeline[1].context["following_event_id"] == timeline[2].event_id
    assert timeline[2].context["following_event_id"] is None
    # Position / total are 1-based and count all events.
    assert timeline[1].context["position"] == 2
    assert timeline[1].context["total"] == 3


def test_context_prior_same_service(reconstructor):
    events = [
        _event("2026-01-01T00:00:00Z", service="auth"),
        _event("2026-01-01T00:00:01Z", service="user"),
        _event("2026-01-01T00:00:02Z", service="auth"),
    ]
    timeline = reconstructor.reconstruct(events)

    assert timeline[0].context["prior_same_service_event_id"] is None  # first auth
    assert timeline[1].context["prior_same_service_event_id"] is None  # first user
    # Third event is auth again -> points back at the first auth entry.
    assert timeline[2].context["prior_same_service_event_id"] == timeline[0].event_id


def test_single_event_edge_case(reconstructor):
    timeline = reconstructor.reconstruct([_event("2026-01-01T00:00:00Z")])
    assert len(timeline) == 1
    entry = timeline[0]
    assert entry.sequence_id == 1
    assert entry.relative_time == "T+0:00"
    assert entry.context == {
        "preceding_event_id": None,
        "following_event_id": None,
        "prior_same_service_event_id": None,
        "position": 1,
        "total": 1,
    }


def test_empty_list_edge_case(reconstructor):
    assert reconstructor.reconstruct([]) == []


def test_unparseable_timestamp_raises_value_error(reconstructor):
    with pytest.raises(ValueError):
        reconstructor.reconstruct([_event("not-a-timestamp")])


def test_existing_event_id_preserved(reconstructor):
    event = _event("2026-01-01T00:00:00Z", event_id="client-supplied-123")
    timeline = reconstructor.reconstruct([event])
    assert timeline[0].event_id == "client-supplied-123"
    # Source LogEvent is untouched when it already carried an id.
    assert event.event_id == "client-supplied-123"


def test_missing_event_id_is_backfilled_and_deterministic(reconstructor):
    def make_batch():
        return [
            _event("2026-01-01T00:00:00Z", service="auth", message="x"),
            _event("2026-01-01T00:00:01Z", service="user", message="y"),
        ]

    first = make_batch()
    assert all(e.event_id is None for e in first)

    timeline_a = reconstructor.reconstruct(first)
    # Back-filled onto the source events for downstream keying.
    assert all(e.event_id is not None for e in first)
    assert [e.event_id for e in first] == [entry.event_id for entry in timeline_a]

    # Same input (fresh objects) -> identical, stable, derived ids.
    timeline_b = reconstructor.reconstruct(make_batch())
    ids_a = [entry.event_id for entry in timeline_a]
    ids_b = [entry.event_id for entry in timeline_b]
    assert ids_a == ids_b
    assert all(i.startswith("evt-") for i in ids_a)
    assert len(set(ids_a)) == 2  # unique per event


def test_naive_and_aware_timestamps_sort_together(reconstructor):
    # A naive timestamp (assumed UTC) and an aware one must be mutually comparable.
    events = [
        _event("2026-01-01T00:00:10+00:00", message="aware-later"),
        _event("2026-01-01T00:00:00", message="naive-earlier"),
    ]
    timeline = reconstructor.reconstruct(events)
    assert [e.message for e in timeline] == ["naive-earlier", "aware-later"]
