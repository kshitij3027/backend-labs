"""Unit tests for the MultiHypothesisTracker (C7, feature area A).

Pin the multi-hypothesis contract on small hand-built graphs: personalized PageRank on
the *reversed* graph pushes mass back onto the causal source (even when the anomaly peaks
on a downstream symptom); the returned confidences are INDEPENDENT (two symmetric sources
each score ~1.0, so the set does not sum to 1); the lifecycle states are assigned per the
configured thresholds with sub-threshold hypotheses pruned/dropped; a ``Settings`` override
of ``max_hypotheses`` / thresholds is respected; the power iteration converges well short
of ``pagerank_max_iter`` on a trivial graph; and empty / single-node graphs degrade
sensibly.
"""

import networkx as nx
import pytest

from src.analysis.hypotheses import MultiHypothesisTracker
from src.config import Settings, get_settings
from src.models import Hypothesis, HypothesisState


def _graph(edges, nodes=None) -> nx.DiGraph:
    """Build a DiGraph with ``(u, v, strength)`` edges (weight == strength) + bare nodes."""
    graph = nx.DiGraph()
    for node_id in nodes or []:
        graph.add_node(node_id, service="svc", level="ERROR")
    for source, target, strength in edges:
        for node_id in (source, target):
            if node_id not in graph:
                graph.add_node(node_id, service="svc", level="ERROR")
        graph.add_edge(source, target, weight=strength, strength=strength)
    return graph


@pytest.fixture()
def tracker():
    return MultiHypothesisTracker(get_settings())


# --- Basic shape -----------------------------------------------------------------


def test_rank_returns_valid_hypothesis_objects(tracker):
    graph = _graph([("root", "a", 1.0), ("root", "b", 1.0)])
    hyps = tracker.rank([], graph, {"root": 0.8, "a": 0.5, "b": 0.5})

    assert len(hyps) >= 2
    for hyp in hyps:
        assert isinstance(hyp, Hypothesis)
        assert hyp.hypothesis_id.startswith("hyp-")
        assert 0.0 <= hyp.confidence <= 1.0
        assert hyp.state in (HypothesisState.CONFIRMED, HypothesisState.TENTATIVE)
    # Returned sorted by confidence descending.
    confidences = [hyp.confidence for hyp in hyps]
    assert confidences == sorted(confidences, reverse=True)
    # Hypothesis ids are unique.
    assert len({hyp.hypothesis_id for hyp in hyps}) == len(hyps)


# --- Personalized PageRank on the reversed graph ---------------------------------


def test_ppr_concentrates_mass_on_causal_source(tracker):
    # Diamond: root -> {m1, m2} -> leaf. The anomaly PEAKS on the downstream leaf, yet the
    # reversed-graph walk pushes mass back onto the causal source 'root'.
    graph = _graph(
        [("root", "m1", 1.0), ("root", "m2", 1.0), ("m1", "leaf", 1.0), ("m2", "leaf", 1.0)]
    )
    anomaly = {"root": 0.3, "m1": 0.4, "m2": 0.4, "leaf": 1.0}

    hyps = tracker.rank([], graph, anomaly)

    assert len(hyps) >= 2
    # PPR surfaces the hidden upstream cause as the top hypothesis.
    assert hyps[0].root_cause_event_id == "root"


def test_confidences_are_independent_not_normalized(tracker):
    # Two disjoint, symmetric sources: a -> b and c -> d, all equally anomalous.
    graph = _graph([("a", "b", 1.0), ("c", "d", 1.0)])
    anomaly = {"a": 1.0, "b": 1.0, "c": 1.0, "d": 1.0}

    hyps = tracker.rank([], graph, anomaly)
    by_id = {hyp.root_cause_event_id: hyp for hyp in hyps}

    assert "a" in by_id and "c" in by_id
    # Symmetric sources get equal confidence...
    assert by_id["a"].confidence == pytest.approx(by_id["c"].confidence)
    # ...and both ~1.0, so the set sums to well over 1 — these are independent posteriors,
    # NOT a normalized-to-1 distribution.
    assert sum(hyp.confidence for hyp in hyps) > 1.0


# --- Lifecycle states ------------------------------------------------------------


def test_states_assigned_and_low_confidence_pruned(tracker):
    # root -> {a, b}; plus an isolated zero-anomaly noise node.
    graph = _graph([("root", "a", 1.0), ("root", "b", 1.0)], nodes=["iso"])
    anomaly = {"root": 0.9, "a": 0.5, "b": 0.5, "iso": 0.0}

    hyps = tracker.rank([], graph, anomaly)
    by_id = {hyp.root_cause_event_id: hyp for hyp in hyps}

    # High PageRank mass + high anomaly => CONFIRMED.
    assert by_id["root"].state == HypothesisState.CONFIRMED
    # Lower-mass symptom => survives but only TENTATIVE.
    assert by_id["a"].state == HypothesisState.TENTATIVE
    # Isolated zero-anomaly node => confidence below the prune threshold => dropped.
    assert "iso" not in by_id
    # No PRUNED hypothesis ever leaks into the returned set (pruned == dropped).
    assert all(
        hyp.state in (HypothesisState.CONFIRMED, HypothesisState.TENTATIVE) for hyp in hyps
    )


def test_settings_override_caps_count_and_thresholds():
    settings = Settings(
        _env_file=None,
        max_hypotheses=2,
        hypothesis_confirm_threshold=0.99,
        hypothesis_prune_threshold=0.05,
    )
    tracker = MultiHypothesisTracker(settings)
    graph = _graph(
        [("root", "m1", 1.0), ("root", "m2", 1.0), ("m1", "leaf", 1.0), ("m2", "leaf", 1.0)]
    )
    anomaly = {"root": 0.9, "m1": 0.4, "m2": 0.4, "leaf": 1.0}

    hyps = tracker.rank([], graph, anomaly)

    # max_hypotheses caps the returned set...
    assert len(hyps) <= 2
    # ...and a near-1.0 confirm threshold keeps even the strong source out of CONFIRMED.
    assert all(hyp.state != HypothesisState.CONFIRMED for hyp in hyps)


# --- Convergence -----------------------------------------------------------------


def test_power_iteration_converges_on_trivial_graph(tracker):
    graph = _graph([("a", "b", 1.0)])

    nodes, pi, iterations = tracker.random_walk_with_restart(graph, {"a": 1.0, "b": 1.0})

    # Converged before hitting the cap (the restart makes the iteration a contraction).
    assert iterations < tracker.settings.pagerank_max_iter
    # Stays a probability vector.
    assert float(pi.sum()) == pytest.approx(1.0, abs=1e-6)
    # Reversed-graph RWR pushes mass onto the source 'a'.
    assert pi[nodes.index("a")] > pi[nodes.index("b")]


# --- Degenerate graphs -----------------------------------------------------------


def test_empty_graph_returns_no_hypotheses(tracker):
    assert tracker.rank([], nx.DiGraph(), {}) == []
    nodes, pi, iterations = tracker.random_walk_with_restart(nx.DiGraph(), {})
    assert nodes == []
    assert pi.size == 0
    assert iterations == 0


def test_single_node_graph_yields_one_hypothesis(tracker):
    graph = nx.DiGraph()
    graph.add_node("solo", service="api-gateway", level="CRITICAL")

    hyps = tracker.rank([], graph, {"solo": 0.8})

    assert len(hyps) == 1
    assert hyps[0].root_cause_event_id == "solo"


def test_zero_anomaly_scores_fall_back_to_uniform_restart(tracker):
    # All-zero anomaly => uniform restart vector, still a valid ranking (no NaNs / crash).
    graph = _graph([("root", "a", 1.0), ("root", "b", 1.0)])

    hyps = tracker.rank([], graph, {"root": 0.0, "a": 0.0, "b": 0.0})

    assert hyps  # at least the top node survives
    assert all(0.0 <= hyp.confidence <= 1.0 for hyp in hyps)
    # With uniform seeding, the reversed walk still concentrates on the structural source.
    assert hyps[0].root_cause_event_id == "root"
