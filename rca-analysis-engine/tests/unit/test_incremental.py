"""Unit tests for the IncrementalAnalyzer (C8, feature area B).

Pin the streaming contract:

* **consistency** — adding events one at a time yields the same window and ranking as a
  single batch add on the same events;
* **bounded window** — events aging out past ``temporal_window`` (time) or beyond
  ``incremental_max_events`` (count) are evicted, so the graph stays small;
* **warm-started re-rank** — a warm re-rank (reusing the previous ``pi``) converges in
  strictly fewer power-iteration steps than a cold one while landing on the identical
  ranking (the fixed point is unchanged), asserted via ``last_iterations``; and
* **degenerate windows** — empty and single-event windows re-rank / snapshot without
  crashing, and unlabeled events get stable derived ids that don't churn as the window
  slides.
"""

from datetime import datetime, timedelta, timezone

import networkx as nx
import numpy as np
import pytest

from src.analysis.hypotheses import MultiHypothesisTracker
from src.analysis.incremental import IncrementalAnalyzer
from src.config import Settings
from src.generators import generate_incident
from src.models import LogEvent, LogLevel

_BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _ts(offset_seconds: float) -> str:
    return (_BASE + timedelta(seconds=offset_seconds)).isoformat()


def _event(offset, service, level=LogLevel.ERROR, event_id=None) -> LogEvent:
    return LogEvent(
        timestamp=_ts(offset), service=service, level=level, message="boom", event_id=event_id
    )


def _graph(edges) -> nx.DiGraph:
    """Build a DiGraph with ``(u, v, strength)`` edges (weight == strength)."""
    graph = nx.DiGraph()
    for source, target, strength in edges:
        for node_id in (source, target):
            if node_id not in graph:
                graph.add_node(node_id, service="svc", level="ERROR")
        graph.add_edge(source, target, weight=strength, strength=strength)
    return graph


@pytest.fixture()
def settings():
    return Settings(_env_file=None)


# --- Consistency: incremental == batch -------------------------------------------


def test_one_at_a_time_matches_batch_ranking(settings):
    events = generate_incident(seed=1).events

    one = IncrementalAnalyzer(settings)
    for event in events:
        one.add_event(event)
    batch = IncrementalAnalyzer(settings)
    batch.add_events(events)

    assert one.window_size() == batch.window_size() == len(events)
    assert [h.root_cause_event_id for h in one.rerank()] == [
        h.root_cause_event_id for h in batch.rerank()
    ]


# --- Bounded window --------------------------------------------------------------


def test_evicts_events_older_than_temporal_window():
    settings = Settings(_env_file=None, temporal_window=10, incremental_max_events=1000)
    inc = IncrementalAnalyzer(settings)

    inc.add_events(
        [
            _event(0.0, "auth", event_id="old0"),
            _event(5.0, "auth", event_id="old5"),
            _event(20.0, "database", event_id="new20"),  # newest -> window_start = 10
        ]
    )

    assert inc.window_size() == 1
    assert {event.event_id for _dt, event in inc._records} == {"new20"}


def test_evicts_beyond_max_events():
    settings = Settings(_env_file=None, temporal_window=100000, incremental_max_events=3)
    inc = IncrementalAnalyzer(settings)

    inc.add_events([_event(float(i), "database", event_id=f"e{i}") for i in range(6)])

    # Only the newest 3 survive, in chronological order.
    assert inc.window_size() == 3
    assert [event.event_id for _dt, event in inc._records] == ["e3", "e4", "e5"]


# --- Warm-started re-rank --------------------------------------------------------


def test_incremental_warm_rerank_uses_fewer_iterations(settings):
    inc = IncrementalAnalyzer(settings)
    inc.add_events(generate_incident(seed=1).events)

    inc.rerank()  # first re-rank -> cold start (no prior pi)
    cold_iters = inc.last_iterations
    inc.rerank()  # second re-rank on the SAME window -> warm start from the converged pi
    warm_iters = inc.last_iterations

    assert cold_iters > 0
    # Warm-starting at (near) the fixed point converges immediately, well under cold.
    assert warm_iters <= cold_iters
    assert warm_iters < cold_iters


def test_warm_and_cold_rerank_agree_on_ranking(settings):
    inc = IncrementalAnalyzer(settings)
    inc.add_events(generate_incident(seed=1).events)

    cold = [h.root_cause_event_id for h in inc.rerank(warm=False)]
    warm = [h.root_cause_event_id for h in inc.rerank(warm=True)]

    # Warm start changes only the iteration count, never the fixed point / ranking.
    assert warm == cold


def test_tracker_warm_start_hook_is_backward_compatible(settings):
    # The tracker's warm-start hook: seeding from a converged pi converges faster and lands
    # on the same distribution; omitting `initial` is the unchanged cold-start behaviour.
    tracker = MultiHypothesisTracker(settings)
    graph = _graph(
        [("root", "a", 1.0), ("root", "b", 1.0), ("a", "leaf", 1.0), ("b", "leaf", 1.0)]
    )
    anomaly = {"root": 0.3, "a": 0.4, "b": 0.4, "leaf": 1.0}

    nodes, pi_cold, cold_iters = tracker.random_walk_with_restart(graph, anomaly)
    warm_init = {node: float(pi_cold[i]) for i, node in enumerate(nodes)}
    _nodes, pi_warm, warm_iters = tracker.random_walk_with_restart(
        graph, anomaly, initial=warm_init
    )

    assert warm_iters < cold_iters
    # Same fixed point regardless of the starting vector (warm start only changes speed).
    np.testing.assert_allclose(pi_cold, pi_warm, atol=1e-5)


# --- Degenerate windows ----------------------------------------------------------


def test_empty_window_reranks_and_snapshots_without_crash(settings):
    inc = IncrementalAnalyzer(settings)

    assert inc.rerank() == []
    snapshot = inc.snapshot()
    assert snapshot.events == []
    assert snapshot.timeline == []
    assert snapshot.root_causes == []
    assert snapshot.incident_id.startswith("live-")


def test_single_event_window(settings):
    inc = IncrementalAnalyzer(settings)
    inc.add_event(_event(0.0, "api-gateway", level=LogLevel.CRITICAL, event_id="solo"))

    hyps = inc.rerank()
    assert len(hyps) == 1
    assert hyps[0].root_cause_event_id == "solo"

    snapshot = inc.snapshot()
    assert len(snapshot.events) == 1
    assert snapshot.causal_graph["nodes"]


def test_snapshot_is_a_full_report(settings):
    inc = IncrementalAnalyzer(settings)
    inc.add_events(generate_incident(seed=1).events)

    report = inc.snapshot()

    assert report.incident_id.startswith("live-")
    assert report.timeline
    assert report.root_causes
    assert report.causal_graph["nodes"]
    assert report.hypotheses
    assert set(report.anomaly_scores) <= {event.event_id for _dt, event in inc._records}


def test_unlabeled_events_get_stable_ids_that_do_not_churn(settings):
    inc = IncrementalAnalyzer(settings)
    inc.add_event(_event(0.0, "api-gateway", level=LogLevel.CRITICAL))  # no event_id
    inc.add_event(_event(1.0, "auth"))  # no event_id

    first_ids = [event.event_id for _dt, event in inc._records]
    assert all(eid and eid.startswith("evt-") for eid in first_ids)

    inc.add_event(_event(2.0, "database"))  # a later add must not renumber earlier ids
    ids_after = {event.event_id for _dt, event in inc._records}
    assert set(first_ids) <= ids_after
