"""Unit tests for the ImpactAnalyzer (C5).

Exercise the impact stage in isolation against hand-built ``networkx.DiGraph``s and
minimal :class:`~src.models.RootCause` lists, so the assertions pin the analyzer's
own contract (never the upstream causal-graph builder). Coverage:

* **blast radius** equals the reachable-set size (``len(nx.descendants(...))``) of the
  top root cause on a chain graph;
* **affected services** is the sorted, distinct set of the descendant cone plus the
  root's own service (duplicates collapsed, unreachable services excluded);
* **weighted impact** sums ``severity_weight(level) * strongest-path product`` over the
  descendant cone — it decreases as edge weights shrink or a descendant's path grows,
  uses the *strongest* (max-product) path, and falls back ``weight -> strength -> 1.0``;
* **degenerate inputs** — empty graph and/or no root causes — yield a zeroed
  :class:`~src.models.ImpactAnalysis` that still carries ``total_events`` and the stable
  zero-``details`` key set, never a crash.

Expected numbers are derived from the injected ``Settings`` severity knobs
(``score_critical`` / ``score_error`` / ``score_warning``) rather than hardcoded, so the
tests validate the formula, not a particular config.
"""

import networkx as nx
import pytest

from src.analysis.impact import ImpactAnalyzer
from src.config import get_settings
from src.models import ImpactAnalysis, LogEvent, LogLevel, RootCause


@pytest.fixture()
def settings():
    return get_settings()


@pytest.fixture()
def analyzer(settings):
    return ImpactAnalyzer(settings)


def _add_node(graph: nx.DiGraph, node_id: str, service: str, level: LogLevel) -> None:
    """Insert a node with only the attrs the ImpactAnalyzer reads: ``service``/``level``."""
    graph.add_node(node_id, service=service, level=level.value)


def _root(event_id: str, service: str, level: LogLevel) -> RootCause:
    """A minimal ranked root cause (only ``event_id`` drives the impact walk)."""
    return RootCause(
        event_id=event_id,
        service=service,
        level=level,
        message="root",
        timestamp="2026-01-01T00:00:00+00:00",
    )


def _n_events(n: int) -> list[LogEvent]:
    """``n`` throwaway events — the analyzer uses them only for ``total_events``."""
    return [
        LogEvent(
            timestamp="2026-01-01T00:00:00+00:00",
            service="svc",
            level=LogLevel.INFO,
            message="x",
            event_id=f"e{i}",
        )
        for i in range(n)
    ]


# --- Blast radius ----------------------------------------------------------------


def test_blast_radius_equals_reachable_set_size_on_chain(analyzer):
    graph = nx.DiGraph()
    ids = ["n0", "n1", "n2", "n3", "n4"]
    services = ["api-gateway", "auth", "user", "payment", "database"]
    for node_id, service in zip(ids, services):
        _add_node(graph, node_id, service, LogLevel.ERROR)
    for upstream, downstream in zip(ids, ids[1:]):
        graph.add_edge(upstream, downstream, weight=1.0)

    result = analyzer.analyze(
        _n_events(len(ids)), graph, [_root("n0", "api-gateway", LogLevel.ERROR)]
    )

    # Blast radius is exactly the size of the root's downstream cone.
    assert result.blast_radius == len(nx.descendants(graph, "n0")) == 4
    # ...and the report exposes those reachable ids, sorted.
    assert result.details["reachable_event_ids"] == ["n1", "n2", "n3", "n4"]


def test_details_summarize_primary_root_cause(analyzer):
    graph = nx.DiGraph()
    _add_node(graph, "r", "api-gateway", LogLevel.CRITICAL)
    _add_node(graph, "a", "auth", LogLevel.ERROR)
    _add_node(graph, "b", "database", LogLevel.ERROR)
    graph.add_edge("r", "a", weight=1.0)
    graph.add_edge("a", "b", weight=1.0)

    result = analyzer.analyze(
        _n_events(3), graph, [_root("r", "api-gateway", LogLevel.CRITICAL)]
    )

    details = result.details
    assert details["primary_root_cause_event_id"] == "r"
    assert details["reachable_event_ids"] == ["a", "b"]
    assert details["affected_service_count"] == len(result.affected_services)
    # A single ranked cause => exactly one per-root-cause summary, mirroring the report.
    assert len(details["per_root_cause"]) == 1
    entry = details["per_root_cause"][0]
    assert entry["event_id"] == "r"
    assert entry["blast_radius"] == 2
    assert set(entry["affected_services"]) == {"api-gateway", "auth", "database"}


def test_per_root_cause_is_capped_at_top_k(analyzer):
    # Four ranked causes, but details.per_root_cause summarizes at most the top 3.
    graph = nx.DiGraph()
    for i in range(4):
        _add_node(graph, f"r{i}", "api-gateway", LogLevel.ERROR)
    _add_node(graph, "d", "auth", LogLevel.ERROR)
    graph.add_edge("r0", "d", weight=1.0)  # primary (rank 0) gets a downstream node
    root_causes = [_root(f"r{i}", "api-gateway", LogLevel.ERROR) for i in range(4)]

    result = analyzer.analyze(_n_events(5), graph, root_causes)

    assert len(result.details["per_root_cause"]) == 3
    assert result.details["primary_root_cause_event_id"] == "r0"
    assert result.blast_radius == 1  # r0 -> d


# --- Affected services -----------------------------------------------------------


def test_affected_services_is_distinct_reachable_set_plus_root(analyzer):
    graph = nx.DiGraph()
    _add_node(graph, "r", "api-gateway", LogLevel.CRITICAL)
    _add_node(graph, "a", "auth", LogLevel.ERROR)
    _add_node(graph, "b", "auth", LogLevel.ERROR)  # duplicate service, collapses to one
    _add_node(graph, "c", "database", LogLevel.ERROR)
    _add_node(graph, "z", "redis", LogLevel.ERROR)  # NOT reachable from r
    graph.add_edge("r", "a", weight=1.0)
    graph.add_edge("a", "b", weight=1.0)
    graph.add_edge("b", "c", weight=1.0)

    result = analyzer.analyze(
        _n_events(5), graph, [_root("r", "api-gateway", LogLevel.CRITICAL)]
    )

    # Sorted, distinct: root's own service + the reachable cone's services, deduped.
    assert result.affected_services == ["api-gateway", "auth", "database"]
    assert "redis" not in result.affected_services  # unreachable service excluded
    assert result.blast_radius == 3  # a, b, c


# --- Weighted impact -------------------------------------------------------------


def test_weighted_impact_sums_descendant_severity_weights(analyzer, settings):
    graph = nx.DiGraph()
    _add_node(graph, "r", "api-gateway", LogLevel.CRITICAL)
    _add_node(graph, "a", "auth", LogLevel.WARNING)
    _add_node(graph, "b", "user", LogLevel.CRITICAL)
    _add_node(graph, "c", "database", LogLevel.ERROR)
    _add_node(graph, "n", "redis", LogLevel.INFO)  # zero severity weight
    for downstream in ("a", "b", "c", "n"):
        graph.add_edge("r", downstream, weight=1.0)  # products all 1.0

    result = analyzer.analyze(
        _n_events(5), graph, [_root("r", "api-gateway", LogLevel.CRITICAL)]
    )

    # Sum over descendants of severity_weight(level) * 1.0; INFO contributes nothing.
    expected = settings.score_warning + settings.score_critical + settings.score_error
    assert result.details["weighted_impact"] == pytest.approx(expected)
    # Blast radius / affected services span ALL descendants (severity-agnostic).
    assert result.blast_radius == 4  # a, b, c, n
    assert set(result.affected_services) == {
        "api-gateway",
        "auth",
        "user",
        "database",
        "redis",
    }


def test_weighted_impact_decreases_as_edge_weight_shrinks(analyzer, settings):
    def weighted(weight: float) -> float:
        graph = nx.DiGraph()
        _add_node(graph, "r", "api-gateway", LogLevel.CRITICAL)
        _add_node(graph, "d", "auth", LogLevel.ERROR)
        graph.add_edge("r", "d", weight=weight)
        result = analyzer.analyze(
            _n_events(2), graph, [_root("r", "api-gateway", LogLevel.CRITICAL)]
        )
        return result.details["weighted_impact"]

    assert weighted(1.0) > weighted(0.5) > weighted(0.2)
    # Exact: one ERROR descendant weighted by the single-edge product.
    assert weighted(1.0) == pytest.approx(settings.score_error * 1.0)
    assert weighted(0.5) == pytest.approx(settings.score_error * 0.5)


def test_weighted_impact_shrinks_as_path_to_a_descendant_grows(analyzer, settings):
    root_causes = [_root("r", "api-gateway", LogLevel.CRITICAL)]

    # One hop: r -> d, d the sole severity-bearing descendant.
    near = nx.DiGraph()
    _add_node(near, "r", "api-gateway", LogLevel.CRITICAL)
    _add_node(near, "d", "auth", LogLevel.ERROR)
    near.add_edge("r", "d", weight=0.5)
    near_impact = analyzer.analyze(_n_events(2), near, root_causes).details[
        "weighted_impact"
    ]

    # Two hops: r -> m -> d; m is INFO (zero weight) so only d contributes, now via a
    # longer path => smaller best-path product => smaller weighted impact.
    far = nx.DiGraph()
    _add_node(far, "r", "api-gateway", LogLevel.CRITICAL)
    _add_node(far, "m", "auth", LogLevel.INFO)
    _add_node(far, "d", "user", LogLevel.ERROR)
    far.add_edge("r", "m", weight=0.5)
    far.add_edge("m", "d", weight=0.5)
    far_impact = analyzer.analyze(_n_events(3), far, root_causes).details[
        "weighted_impact"
    ]

    assert near_impact > far_impact > 0.0
    assert near_impact == pytest.approx(settings.score_error * 0.5)
    assert far_impact == pytest.approx(settings.score_error * 0.25)  # 0.5 * 0.5


def test_weighted_impact_uses_strongest_path_product(analyzer, settings):
    # Two paths reach d: a weak direct edge (0.2) and a strong two-hop (0.9*0.9=0.81).
    # The analyzer must credit d with the STRONGER path's product.
    graph = nx.DiGraph()
    _add_node(graph, "r", "api-gateway", LogLevel.CRITICAL)
    _add_node(graph, "a", "auth", LogLevel.INFO)  # intermediate, zero severity weight
    _add_node(graph, "d", "database", LogLevel.ERROR)
    graph.add_edge("r", "d", weight=0.2)
    graph.add_edge("r", "a", weight=0.9)
    graph.add_edge("a", "d", weight=0.9)

    result = analyzer.analyze(
        _n_events(3), graph, [_root("r", "api-gateway", LogLevel.CRITICAL)]
    )

    assert result.details["weighted_impact"] == pytest.approx(settings.score_error * 0.81)


def test_edge_weight_falls_back_to_strength_then_unit(analyzer, settings):
    # An edge with only ``strength`` (no ``weight``) uses strength; a bare edge -> 1.0.
    strength_only = nx.DiGraph()
    _add_node(strength_only, "r", "api-gateway", LogLevel.CRITICAL)
    _add_node(strength_only, "d", "auth", LogLevel.ERROR)
    strength_only.add_edge("r", "d", strength=0.5)  # no explicit weight
    result_strength = analyzer.analyze(
        _n_events(2), strength_only, [_root("r", "api-gateway", LogLevel.CRITICAL)]
    )
    assert result_strength.details["weighted_impact"] == pytest.approx(
        settings.score_error * 0.5
    )

    bare = nx.DiGraph()
    _add_node(bare, "r", "api-gateway", LogLevel.CRITICAL)
    _add_node(bare, "d", "auth", LogLevel.ERROR)
    bare.add_edge("r", "d")  # neither weight nor strength -> defaults to 1.0
    result_bare = analyzer.analyze(
        _n_events(2), bare, [_root("r", "api-gateway", LogLevel.CRITICAL)]
    )
    assert result_bare.details["weighted_impact"] == pytest.approx(settings.score_error)


# --- Degenerate inputs (never crash) ---------------------------------------------


def test_empty_graph_with_root_causes_is_zeroed(analyzer):
    result = analyzer.analyze(
        _n_events(4), nx.DiGraph(), [_root("r", "api-gateway", LogLevel.CRITICAL)]
    )

    assert isinstance(result, ImpactAnalysis)
    assert result.blast_radius == 0
    assert result.affected_services == []
    assert result.total_events == 4  # preserved even on the zero path
    assert result.details == {
        "primary_root_cause_event_id": None,
        "weighted_impact": 0.0,
        "reachable_event_ids": [],
        "per_root_cause": [],
        "affected_service_count": 0,
    }


def test_no_root_causes_is_zeroed_but_keeps_total_events(analyzer):
    graph = nx.DiGraph()
    _add_node(graph, "r", "api-gateway", LogLevel.CRITICAL)
    _add_node(graph, "d", "auth", LogLevel.ERROR)
    graph.add_edge("r", "d", weight=1.0)

    result = analyzer.analyze(_n_events(3), graph, [])

    assert result.blast_radius == 0
    assert result.affected_services == []
    assert result.total_events == 3
    assert result.details["per_root_cause"] == []
    assert result.details["primary_root_cause_event_id"] is None


def test_empty_events_and_graph_is_fully_zeroed(analyzer):
    result = analyzer.analyze([], nx.DiGraph(), [])

    assert result.blast_radius == 0
    assert result.affected_services == []
    assert result.total_events == 0
    assert result.details["weighted_impact"] == 0.0
    assert result.details["reachable_event_ids"] == []
