"""Unit tests for the RootCauseIdentifier + ConfidenceScorer (C4).

Cover the candidate rule (union of causal sources and severe events), the three-term
confidence formula (severity + temporal position + normalized out-degree centrality),
its clamping to ``[0, 1]``, temporal monotonicity, centrality normalization against the
busiest source, descending ranking with deterministic tie-breaks, the zero-span
single-event edge case, and the Req success criterion: the injected ground-truth root
of a generated cascade lands in the top-3 (in fact at rank #1) after
``RCAAnalyzer.analyze``.
"""

from datetime import datetime, timedelta, timezone

import networkx as nx
import pytest

from src.analysis import RCAAnalyzer
from src.analysis.causal_graph import CausalGraphBuilder
from src.analysis.root_cause import ConfidenceScorer, RootCauseIdentifier
from src.config import get_settings
from src.generators import generate_incident
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


def _add_node(graph, node_id, service, level, offset, message="boom") -> None:
    """Insert a node carrying the same attrs the CausalGraphBuilder emits (incl. ``dt``)."""
    dt = _BASE + timedelta(seconds=offset)
    graph.add_node(
        node_id,
        service=service,
        level=level.value,
        message=message,
        timestamp=dt.isoformat(),
        dt=dt,
    )


@pytest.fixture()
def settings():
    return get_settings()


@pytest.fixture()
def builder(settings):
    return CausalGraphBuilder(settings, ServiceDependencyMap.from_settings(settings))


@pytest.fixture()
def scorer(settings):
    return ConfidenceScorer(settings)


# --- Candidate identification ----------------------------------------------------


def test_identify_unions_sources_and_severe_events(builder, settings):
    identifier = RootCauseIdentifier(settings)
    events = [
        _event(0, "api-gateway", LogLevel.WARNING, event_id="src"),  # source (-> mid)
        _event(10, "auth", LogLevel.WARNING, event_id="mid"),  # source (-> leaf)
        _event(20, "database", LogLevel.ERROR, event_id="leaf"),  # severe leaf, out 0
        _event(25, "redis", LogLevel.INFO, event_id="noise"),  # INFO, isolated
        _event(30, "file-storage", LogLevel.WARNING, event_id="lonely"),  # WARN, isolated
    ]
    graph = builder.build(events)
    candidates = set(identifier.identify(events, graph))

    # Union of {out-degree > 0 sources} and {ERROR/CRITICAL events}.
    assert candidates == {"src", "mid", "leaf"}
    # Isolated non-severe events (INFO / WARNING with no out-edge) are never candidates.
    assert "noise" not in candidates and "lonely" not in candidates


# --- Ranking / confidence formula ------------------------------------------------


def test_earliest_critical_source_ranks_first_with_confidence_one(builder, scorer):
    # api-gateway CRITICAL at t0 fans out to its three ERROR downstreams -> it is the
    # earliest event, the sole CRITICAL, and the max-out-degree source.
    events = [
        _event(0, "api-gateway", LogLevel.CRITICAL, event_id="root"),
        _event(10, "auth", LogLevel.ERROR, event_id="a"),
        _event(20, "user", LogLevel.ERROR, event_id="b"),
        _event(30, "payment", LogLevel.ERROR, event_id="c"),
    ]
    graph = builder.build(events)
    ranked = scorer.rank(events, graph)

    assert ranked[0].event_id == "root"
    assert ranked[0].level == LogLevel.CRITICAL
    # severity 0.6 + temporal 0.3 (earliest) + centrality 0.2 (max out-degree) = 1.1 -> 1.0.
    assert ranked[0].confidence == pytest.approx(1.0)
    # Full descending order (root 1.0 > a 0.6 > b 0.5 > c 0.4); no ties.
    assert [rc.event_id for rc in ranked] == ["root", "a", "b", "c"]


def test_temporal_position_is_monotonic_earlier_scores_higher(scorer):
    # Two candidates identical except for time (same severity, both isolated) so the
    # only differing term is temporal position.
    graph = nx.DiGraph()
    _add_node(graph, "early", "auth", LogLevel.ERROR, offset=0)
    _add_node(graph, "late", "auth", LogLevel.ERROR, offset=100)
    start, end = _BASE, _BASE + timedelta(seconds=100)

    early = scorer.score("early", graph, start, end, max_out_degree=0)
    late = scorer.score("late", graph, start, end, max_out_degree=0)

    assert early > late
    # early: 0.4 sev + 0.3*1.0 temporal + 0 centrality; late: 0.4 + 0.3*0 + 0.
    assert early == pytest.approx(0.7)
    assert late == pytest.approx(0.4)


def test_centrality_normalized_against_max_out_degree(scorer, settings):
    # Two candidates at the SAME timestamp and severity so the only differing term is
    # out-degree centrality: a hub (max out-degree) vs an isolated node.
    graph = nx.DiGraph()
    _add_node(graph, "hub", "api-gateway", LogLevel.WARNING, offset=0)
    _add_node(graph, "iso", "database", LogLevel.WARNING, offset=0)
    _add_node(graph, "t1", "auth", LogLevel.WARNING, offset=10)
    _add_node(graph, "t2", "user", LogLevel.WARNING, offset=20)
    graph.add_edge("hub", "t1", weight=0.8, strength=0.8)
    graph.add_edge("hub", "t2", weight=0.8, strength=0.8)
    start, end = _BASE, _BASE + timedelta(seconds=20)
    max_out = 2

    hub = scorer.score("hub", graph, start, end, max_out)
    iso = scorer.score("iso", graph, start, end, max_out)

    # Same severity + same temporal position => the gap is exactly the centrality term:
    # the max-out-degree node earns the full CENTRALITY_SCORE_WEIGHT, the isolated node 0.
    assert hub - iso == pytest.approx(settings.centrality_score_weight)
    assert hub == pytest.approx(0.7)  # 0.2 sev + 0.3 temporal + 0.2 centrality
    assert iso == pytest.approx(0.5)  # 0.2 sev + 0.3 temporal + 0.0 centrality


def test_confidence_is_clamped_into_unit_interval(scorer):
    # CRITICAL (0.6) + earliest (temporal 0.3) + max out-degree (centrality 0.2) = 1.1.
    graph = nx.DiGraph()
    _add_node(graph, "hot", "api-gateway", LogLevel.CRITICAL, offset=0)
    _add_node(graph, "t1", "auth", LogLevel.ERROR, offset=10)
    graph.add_edge("hot", "t1", weight=1.0, strength=1.0)
    start, end = _BASE, _BASE + timedelta(seconds=10)

    conf = scorer.score("hot", graph, start, end, max_out_degree=1)

    # Pre-clamp 1.1 -> clamped to exactly 1.0, and always within [0, 1].
    assert conf == pytest.approx(1.0)
    assert 0.0 <= conf <= 1.0


def test_all_confidences_within_unit_interval(builder, scorer):
    events = generate_incident(seed=2).events
    graph = builder.build(events)
    ranked = scorer.rank(events, graph)

    assert ranked  # candidates exist for a realistic cascade
    for rc in ranked:
        assert 0.0 <= rc.confidence <= 1.0


def test_ranking_is_descending_by_confidence(builder, scorer):
    events = generate_incident(seed=0).events
    graph = builder.build(events)
    ranked = scorer.rank(events, graph)

    confidences = [rc.confidence for rc in ranked]
    assert confidences == sorted(confidences, reverse=True)


# --- Zero-span / single-event edge cases -----------------------------------------


def test_single_event_incident_has_temporal_pos_one_and_no_zero_division(builder, scorer):
    events = [_event(0, "api-gateway", LogLevel.CRITICAL, event_id="solo")]
    graph = builder.build(events)

    ranked = scorer.rank(events, graph)  # must not raise ZeroDivisionError

    assert len(ranked) == 1
    assert ranked[0].event_id == "solo"
    # Zero-span window -> temporal_pos == 1.0; out_degree 0 -> centrality 0.
    # confidence = 0.6 (CRITICAL) + 0.3*1.0 + 0.2*0 = 0.9.
    assert ranked[0].confidence == pytest.approx(0.9)


def test_score_zero_span_window_defined_as_temporal_pos_one(scorer):
    graph = nx.DiGraph()
    _add_node(graph, "solo", "api-gateway", LogLevel.ERROR, offset=0)
    same = _BASE

    conf = scorer.score("solo", graph, same, same, max_out_degree=0)

    # incident_end == incident_start -> temporal_pos 1.0: 0.4 sev + 0.3*1.0 + 0 = 0.7.
    assert conf == pytest.approx(0.7)


def test_empty_graph_ranks_to_empty_list(scorer):
    assert scorer.rank([], nx.DiGraph()) == []


# --- Ground-truth success criterion (Req) ----------------------------------------


@pytest.mark.parametrize("seed", [0, 1, 2, 3, 4])
def test_ground_truth_root_cause_ranks_in_top_3(seed):
    scenario = generate_incident(seed=seed)
    report = RCAAnalyzer(get_settings()).analyze(scenario.events)

    assert report.root_causes, "analyze() must populate root_causes"
    top3 = [rc.event_id for rc in report.root_causes[:3]]
    assert scenario.root_cause_event_id in top3, (
        f"seed={seed}: ground-truth {scenario.root_cause_event_id} not in top-3 {top3}"
    )
    # The injected root is the earliest event, the sole CRITICAL, and the highest
    # out-degree source, so it is not merely top-3 but ranked #1 with confidence 1.0.
    assert report.root_causes[0].event_id == scenario.root_cause_event_id
    assert report.root_causes[0].confidence == pytest.approx(1.0)
