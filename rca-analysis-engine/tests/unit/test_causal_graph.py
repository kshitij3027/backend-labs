"""Unit tests for the CausalGraphBuilder (C3).

Cover node construction (one per event, keyed by ``event_id``), the three-axis edge
admissibility rule (temporal window, service-dependency direction, severity), the
additive/clamped causal-strength formula (dependency bonus, error-propagation bonus,
temporal-gap penalty), the window/direction/severity exclusions, the sorted
temporal-sweep's near-linear performance on 1000 events, JSON-safe serialization,
and the RCAAnalyzer integration that folds the serialized graph into the report.
"""

import json
import time
from datetime import datetime, timedelta, timezone

import pytest

from src.analysis import RCAAnalyzer
from src.analysis.causal_graph import CausalGraphBuilder
from src.config import get_settings
from src.generators import generate_events, generate_incident
from src.models import LogEvent, LogLevel
from src.service_map import ServiceDependencyMap

#: Fixed anchor so controlled offsets map to reproducible ISO timestamps.
_BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _ts(offset_seconds: float) -> str:
    """An ISO-8601 timestamp ``offset_seconds`` after the fixed base time."""
    return (_BASE + timedelta(seconds=offset_seconds)).isoformat()


def _event(offset, service, level, message="boom", event_id=None) -> LogEvent:
    return LogEvent(
        timestamp=_ts(offset),
        service=service,
        level=level,
        message=message,
        event_id=event_id,
    )


@pytest.fixture()
def settings():
    return get_settings()


@pytest.fixture()
def builder(settings):
    return CausalGraphBuilder(settings, ServiceDependencyMap.from_settings(settings))


# --- Nodes -----------------------------------------------------------------------


def test_one_node_per_event_keyed_by_event_id(builder):
    events = [
        _event(0, "auth", LogLevel.ERROR, event_id="e0"),
        _event(10, "database", LogLevel.ERROR, event_id="e1"),
        _event(20, "redis", LogLevel.WARNING, message="slow", event_id="e2"),
    ]
    graph = builder.build(events)

    # One node per event, keyed by the supplied event_id.
    assert graph.number_of_nodes() == len(events)
    assert set(graph.nodes) == {"e0", "e1", "e2"}
    # Node attributes carry service / level (enum's string value) / message.
    assert graph.nodes["e0"]["service"] == "auth"
    assert graph.nodes["e0"]["level"] == "ERROR"
    assert graph.nodes["e2"]["level"] == "WARNING"
    assert graph.nodes["e2"]["message"] == "slow"
    assert graph.nodes["e0"]["timestamp"] == _ts(0)


def test_missing_event_id_falls_back_to_deterministic_scheme(builder):
    # No ids supplied -> build() must still key every node (timeline's fallback scheme).
    events = [
        _event(0, "auth", LogLevel.ERROR),
        _event(10, "database", LogLevel.ERROR),
    ]
    graph = builder.build(events)
    assert graph.number_of_nodes() == 2
    assert all(node_id.startswith("evt-") for node_id in graph.nodes)


# --- Edge strength formula -------------------------------------------------------


def test_dependent_error_to_error_in_window_scores_one(builder):
    # auth -> database is a declared dependency; both ERROR; 10s gap (< 60s threshold).
    # strength = BASE(0.5) + DEP(0.3) + ERR_PROP(0.2) = 1.0.
    events = [
        _event(0, "auth", LogLevel.ERROR, event_id="u"),
        _event(10, "database", LogLevel.ERROR, event_id="v"),
    ]
    graph = builder.build(events)

    assert graph.has_edge("u", "v")
    assert graph["u"]["v"]["strength"] == pytest.approx(1.0)
    assert graph["u"]["v"]["weight"] == pytest.approx(1.0)
    # No reverse edge: database is not upstream of auth.
    assert not graph.has_edge("v", "u")


def test_same_service_error_to_error_has_no_dependency_bonus(builder):
    # Same service is admissible (self-propagation) but earns no +0.3 dependency bonus.
    # strength = BASE(0.5) + ERR_PROP(0.2) = 0.7.
    events = [
        _event(0, "auth", LogLevel.ERROR, event_id="u"),
        _event(10, "auth", LogLevel.ERROR, event_id="v"),
    ]
    graph = builder.build(events)

    assert graph.has_edge("u", "v")
    assert graph["u"]["v"]["strength"] == pytest.approx(0.7)


def test_temporal_gap_penalty_applies_past_threshold(builder):
    # Dependent ERROR->ERROR but 90s apart (> 60s threshold) -> -0.1 penalty.
    # strength = 0.5 + 0.3 + 0.2 - 0.1 = 0.9.
    events = [
        _event(0, "auth", LogLevel.ERROR, event_id="u"),
        _event(90, "database", LogLevel.ERROR, event_id="v"),
    ]
    graph = builder.build(events)

    assert graph.has_edge("u", "v")
    assert graph["u"]["v"]["strength"] == pytest.approx(0.9)


def test_non_error_pair_gets_base_plus_dependency_only(builder):
    # api-gateway CRITICAL -> auth WARNING: dependency, both causal, but not ERROR->ERROR
    # and within the gap threshold -> strength = BASE(0.5) + DEP(0.3) = 0.8.
    events = [
        _event(0, "api-gateway", LogLevel.CRITICAL, event_id="u"),
        _event(5, "auth", LogLevel.WARNING, event_id="v"),
    ]
    graph = builder.build(events)

    assert graph["u"]["v"]["strength"] == pytest.approx(0.8)


# --- Admissibility exclusions ----------------------------------------------------


def test_no_edge_outside_temporal_window(builder):
    # 301s apart (> 300s window) -> the sweep prunes it, no edge at all.
    events = [
        _event(0, "auth", LogLevel.ERROR, event_id="u"),
        _event(301, "database", LogLevel.ERROR, event_id="v"),
    ]
    graph = builder.build(events)

    assert not graph.has_edge("u", "v")
    assert graph.number_of_edges() == 0


def test_temporal_window_boundary_is_inclusive(builder):
    # Exactly 300s apart is still admissible (0 <= dt <= window); gap penalty applies.
    events = [
        _event(0, "auth", LogLevel.ERROR, event_id="u"),
        _event(300, "database", LogLevel.ERROR, event_id="v"),
    ]
    graph = builder.build(events)

    assert graph.has_edge("u", "v")
    assert graph["u"]["v"]["strength"] == pytest.approx(0.9)  # 1.0 - gap penalty


def test_no_edge_against_dependency_direction(builder):
    # database -> api-gateway is neither a declared dependency nor same-service.
    events = [
        _event(0, "database", LogLevel.ERROR, event_id="u"),
        _event(10, "api-gateway", LogLevel.ERROR, event_id="v"),
    ]
    graph = builder.build(events)

    assert not graph.has_edge("u", "v")
    assert graph.number_of_edges() == 0


def test_info_endpoints_never_produce_edges(builder):
    # api-gateway -> auth is a dependency and in-window, but an INFO endpoint (source
    # or sink) is never causal, so none of these pairs yield an edge.
    events = [
        _event(0, "api-gateway", LogLevel.INFO, event_id="a"),  # INFO source
        _event(5, "auth", LogLevel.INFO, event_id="b"),  # INFO sink + source
        _event(10, "auth", LogLevel.ERROR, event_id="c"),
    ]
    graph = builder.build(events)

    assert graph.number_of_edges() == 0


def test_all_strengths_within_clamp_range(builder, settings):
    # On a realistic generated cascade, every edge strength stays within [0.1, 1.0].
    graph = builder.build(generate_incident(seed=5).events)

    assert graph.number_of_edges() > 0
    for _u, _v, data in graph.edges(data=True):
        assert (
            settings.causal_strength_min
            <= data["strength"]
            <= settings.causal_strength_max
        )


# --- Temporal-sweep performance --------------------------------------------------


def test_temporal_sweep_scales_to_1000_events(builder):
    events = generate_events(1000, seed=3)

    start = time.perf_counter()
    graph = builder.build(events)
    elapsed = time.perf_counter() - start

    # One node per event proves nothing was dropped by the sweep.
    assert graph.number_of_nodes() == 1000
    # The sorted two-pointer sweep keeps the build near-linear; an O(n^2) double loop
    # would be dramatically slower. Threshold kept lenient to avoid CI flakiness.
    assert elapsed < 2.0


# --- Serialization ---------------------------------------------------------------


def test_to_serializable_is_json_safe(builder):
    events = [
        _event(0, "auth", LogLevel.ERROR, message="down", event_id="u"),
        _event(10, "database", LogLevel.ERROR, event_id="v"),
    ]
    graph = builder.build(events)
    payload = builder.to_serializable(graph)

    assert set(payload) == {"nodes", "edges"}
    node = next(n for n in payload["nodes"] if n["id"] == "u")
    assert node == {
        "id": "u",
        "service": "auth",
        "level": "ERROR",
        "message": "down",
        "timestamp": _ts(0),
    }
    # The internal parsed datetime must not leak into the serialized payload.
    assert "dt" not in node

    edge = payload["edges"][0]
    assert set(edge) == {"source", "target", "strength"}
    assert edge["source"] == "u" and edge["target"] == "v"

    # The whole payload round-trips through JSON (no datetimes / enums leak through).
    json.dumps(payload)


# --- RCAAnalyzer integration -----------------------------------------------------


def test_analyze_serializes_causal_graph_into_report():
    scenario = generate_incident(seed=1)
    report = RCAAnalyzer(get_settings()).analyze(scenario.events)

    graph_json = report.causal_graph
    assert graph_json["nodes"], "nodes should be populated"
    assert graph_json["edges"], "edges should be populated"
    # One serialized node per input event.
    assert len(graph_json["nodes"]) == len(scenario.events)

    # The injected api-gateway CRITICAL root propagates to its ERROR downstreams, so it
    # is the source of at least one causal edge.
    sources = {e["source"] for e in graph_json["edges"]}
    assert scenario.root_cause_event_id in sources
