"""Unit tests for the PostMortemReporter (C9, feature area F).

Exercise the reporter against hand-built ``networkx.DiGraph``s (and one real analyzed
report) so the assertions pin the reporter's own contract:

* **recovery points** sit on the max-impact propagation path — the interior node gating
  the largest downstream subtree, never a leaf and never the root cause itself — ranked by
  gated-subtree size descending;
* **event classification** partitions every event into exactly one
  :class:`~src.models.EventClass`: a graph source -> ``PRIMARY_TRIGGER``, an interior node
  -> ``PROPAGATION_PATH``, a leaf / isolated / off-graph node -> ``CONTRIBUTING_FACTOR``;
* **to_markdown** returns non-empty, well-formed markdown carrying the incident id and the
  "Root Cause" / "Recovery" headings;
* **build** works from a *serialized* ``report.causal_graph`` with ``graph=None`` (a stored
  report), rebuilding the graph and producing all three artifacts.
"""

import networkx as nx
import pytest

from src.analysis import RCAAnalyzer
from src.analysis.report import PostMortemReporter
from src.config import get_settings
from src.generators import generate_incident
from src.models import EventClass, LogEvent, LogLevel, RootCause


@pytest.fixture()
def reporter():
    return PostMortemReporter(get_settings())


def _add_node(graph: nx.DiGraph, node_id: str, service: str, level=LogLevel.ERROR) -> None:
    """Insert a node with the attrs the reporter reads (``service`` / ``level`` / ...)."""
    graph.add_node(
        node_id,
        service=service,
        level=level.value,
        message=f"{service} boom",
        timestamp="2026-01-01T00:00:00+00:00",
    )


def _event(event_id: str, service: str, level=LogLevel.ERROR) -> LogEvent:
    return LogEvent(
        timestamp="2026-01-01T00:00:00+00:00",
        service=service,
        level=level,
        message="m",
        event_id=event_id,
    )


def _root(event_id: str, service: str = "api-gateway") -> RootCause:
    return RootCause(
        event_id=event_id,
        service=service,
        level=LogLevel.CRITICAL,
        message="root",
        confidence=1.0,
        raw_confidence=1.0,
        timestamp="2026-01-01T00:00:00+00:00",
    )


# --- Recovery points -------------------------------------------------------------


def test_recovery_point_is_interior_choke_gating_largest_subtree(reporter):
    # r -> m -> {x, y, z}: m is the sole interior choke point; intervening there truncates
    # the entire downstream fan-out, so m (not the root, not a leaf) is the top recovery point.
    graph = nx.DiGraph()
    for node_id, service in [
        ("r", "api-gateway"),
        ("m", "auth"),
        ("x", "database"),
        ("y", "redis"),
        ("z", "file-storage"),
    ]:
        _add_node(graph, node_id, service)
    graph.add_edge("r", "m")
    for leaf in ("x", "y", "z"):
        graph.add_edge("m", leaf)

    points = reporter.recovery_points(graph, [_root("r")])

    assert points, "a choke point should be identified"
    top = points[0]
    assert top["event_id"] == "m"
    assert top["gated_subtree_size"] == 3
    assert top["service"] == "auth"
    assert set(top) == {"event_id", "service", "gated_subtree_size", "rationale"}
    # Leaves (0 descendants) and the root itself are never recovery points.
    ids = {p["event_id"] for p in points}
    assert ids == {"m"}
    assert "r" not in ids


def test_recovery_points_ranked_by_gated_subtree_on_chain(reporter):
    graph = nx.DiGraph()
    ids = ["r", "a", "b", "c", "d"]
    services = ["api-gateway", "auth", "user", "payment", "database"]
    for node_id, service in zip(ids, services):
        _add_node(graph, node_id, service)
    for upstream, downstream in zip(ids, ids[1:]):
        graph.add_edge(upstream, downstream)

    points = reporter.recovery_points(graph, [_root("r")])

    # Descendants of r on the path: a(3), b(2), c(1); d is a leaf (0) -> excluded.
    assert [p["event_id"] for p in points] == ["a", "b", "c"]
    assert [p["gated_subtree_size"] for p in points] == [3, 2, 1]
    sizes = [p["gated_subtree_size"] for p in points]
    assert sizes == sorted(sizes, reverse=True)


def test_recovery_points_empty_for_degenerate_inputs(reporter):
    assert reporter.recovery_points(nx.DiGraph(), [_root("r")]) == []
    graph = nx.DiGraph()
    _add_node(graph, "r", "api-gateway")
    assert reporter.recovery_points(graph, []) == []


# --- Event classification --------------------------------------------------------


def test_classify_partitions_chain(reporter):
    graph = nx.DiGraph()
    ids = ["r", "a", "b", "c", "d"]
    services = ["api-gateway", "auth", "user", "payment", "database"]
    for node_id, service in zip(ids, services):
        _add_node(graph, node_id, service)
    for upstream, downstream in zip(ids, ids[1:]):
        graph.add_edge(upstream, downstream)
    events = [_event(node_id, service) for node_id, service in zip(ids, services)]

    classes = reporter.classify_events(events, graph, [_root("r")])

    assert classes["r"] == EventClass.PRIMARY_TRIGGER.value  # source (in 0, out > 0)
    assert classes["a"] == EventClass.PROPAGATION_PATH.value  # interior
    assert classes["b"] == EventClass.PROPAGATION_PATH.value
    assert classes["c"] == EventClass.PROPAGATION_PATH.value
    assert classes["d"] == EventClass.CONTRIBUTING_FACTOR.value  # leaf (out 0)
    # Every event classified exactly once into a valid class.
    assert set(classes) == set(ids)
    assert len(classes) == len(events)
    assert set(classes.values()) <= {cls.value for cls in EventClass}


def test_classify_partitions_diamond_with_isolated_node(reporter):
    # Diamond r -> a, r -> b, a -> c, b -> c, plus an isolated INFO node 'n'.
    graph = nx.DiGraph()
    _add_node(graph, "r", "api-gateway", LogLevel.CRITICAL)
    _add_node(graph, "a", "auth", LogLevel.ERROR)
    _add_node(graph, "b", "user", LogLevel.ERROR)
    _add_node(graph, "c", "database", LogLevel.ERROR)
    _add_node(graph, "n", "redis", LogLevel.INFO)
    graph.add_edge("r", "a")
    graph.add_edge("r", "b")
    graph.add_edge("a", "c")
    graph.add_edge("b", "c")
    events = [
        _event("r", "api-gateway", LogLevel.CRITICAL),
        _event("a", "auth"),
        _event("b", "user"),
        _event("c", "database"),
        _event("n", "redis", LogLevel.INFO),
    ]

    classes = reporter.classify_events(events, graph, [_root("r")])

    assert classes["r"] == EventClass.PRIMARY_TRIGGER.value
    assert classes["a"] == EventClass.PROPAGATION_PATH.value
    assert classes["b"] == EventClass.PROPAGATION_PATH.value
    assert classes["c"] == EventClass.CONTRIBUTING_FACTOR.value  # sink (out 0)
    assert classes["n"] == EventClass.CONTRIBUTING_FACTOR.value  # isolated (in 0, out 0)
    assert set(classes) == {"r", "a", "b", "c", "n"}


def test_classify_event_absent_from_graph_is_contributing(reporter):
    graph = nx.DiGraph()
    _add_node(graph, "r", "api-gateway")
    _add_node(graph, "a", "auth")
    graph.add_edge("r", "a")
    # 'ghost' formed no causal edge and has no node in the graph.
    events = [
        _event("r", "api-gateway"),
        _event("a", "auth"),
        _event("ghost", "redis", LogLevel.INFO),
    ]

    classes = reporter.classify_events(events, graph, [_root("r")])

    assert classes["ghost"] == EventClass.CONTRIBUTING_FACTOR.value
    assert set(classes) == {"r", "a", "ghost"}


# --- Markdown + build ------------------------------------------------------------


def test_to_markdown_is_nonempty_with_key_sections(reporter):
    report = RCAAnalyzer(get_settings()).analyze(generate_incident(seed=1).events)

    markdown = reporter.to_markdown(report)

    assert isinstance(markdown, str) and markdown.strip()
    assert report.incident_id in markdown
    assert "# Post-Mortem" in markdown
    assert "Root Cause" in markdown  # the "## Root Causes" heading
    assert "Recovery" in markdown  # the "## Recovery Points" heading
    assert "## Event Classification" in markdown


def test_build_from_serialized_graph_with_graph_none(reporter):
    report = RCAAnalyzer(get_settings()).analyze(generate_incident(seed=2).events)

    # Simulate a stored report: no live graph -> build rebuilds it from causal_graph.
    built = reporter.build(report, graph=None)

    assert set(built) == {"markdown", "recovery_points", "classifications"}
    assert built["markdown"].strip()
    assert report.incident_id in built["markdown"]
    # Classifications cover every event exactly once.
    assert set(built["classifications"]) == {event.event_id for event in report.events}
    # A multi-level cascade always has an interior choke point.
    assert built["recovery_points"]
    for point in built["recovery_points"]:
        assert set(point) == {"event_id", "service", "gated_subtree_size", "rationale"}
        assert point["gated_subtree_size"] >= 1


def test_build_matches_live_graph_recovery_points(reporter):
    # Rebuilding from the serialized graph yields the same recovery points as the live one.
    analyzer = RCAAnalyzer(get_settings())
    report = analyzer.analyze(generate_incident(seed=3).events)

    built = reporter.build(report, graph=None)
    from_analyze = report.recovery_points

    assert [p["event_id"] for p in built["recovery_points"]] == [
        p["event_id"] for p in from_analyze
    ]
