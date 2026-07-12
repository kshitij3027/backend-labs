"""Integration test for the C7 anomaly + multi-hypothesis wiring in RCAAnalyzer.analyze.

Drive the full analyzer over a generated ground-truth cascade and assert that ``analyze``
now folds both C7 stages into the report: per-event ``anomaly_scores`` in ``[0, 1]`` and a
non-empty set of concurrent ``hypotheses`` whose top-ranked explanations include the
injected ``api-gateway`` root cause (personalized PageRank on the reversed causal graph
concentrates mass on the causal source). This exercises the real ``score`` -> ``observe``
ordering inside ``analyze`` (an incident is graded against prior history only).
"""

import pytest

from src.analysis import RCAAnalyzer
from src.config import get_settings
from src.generators import generate_incident


def test_analyze_populates_anomaly_scores_and_hypotheses():
    scenario = generate_incident(seed=1)

    report = RCAAnalyzer(get_settings()).analyze(scenario.events)

    # Anomaly amplification produced a per-event score for every scored event, all in
    # [0, 1], and every score key is a real event id.
    assert report.anomaly_scores
    event_ids = {event.event_id for event in scenario.events}
    assert set(report.anomaly_scores) <= event_ids
    assert all(0.0 <= score <= 1.0 for score in report.anomaly_scores.values())

    # Multi-hypothesis tracking retained at least two concurrent hypotheses...
    assert len(report.hypotheses) >= 2
    for hyp in report.hypotheses:
        assert 0.0 <= hyp.confidence <= 1.0
        assert hyp.hypothesis_id.startswith("hyp-")

    # ...and the injected api-gateway root cause surfaces among the top hypotheses (PPR
    # concentrates mass on the causal source that explains the anomalous downstream cone).
    hyp_ids = [hyp.root_cause_event_id for hyp in report.hypotheses]
    assert scenario.root_cause_event_id in hyp_ids[:3]


def test_first_incident_uses_empty_history_fallback_without_crashing():
    # A fresh analyzer has no anomaly baseline; the very first analyze must still produce
    # valid anomaly scores (the surprise/severity fallback) and hypotheses.
    scenario = generate_incident(seed=7)

    report = RCAAnalyzer(get_settings()).analyze(scenario.events)

    assert report.anomaly_scores
    assert all(0.0 <= score <= 1.0 for score in report.anomaly_scores.values())
    assert report.hypotheses


def test_scoring_precedes_learning_across_incidents():
    # observe() runs AFTER score() inside analyze, so the baseline grows incident over
    # incident. Two analyses on one analyzer must both succeed and keep scores in range.
    analyzer = RCAAnalyzer(get_settings())

    first = analyzer.analyze(generate_incident(seed=1).events)
    second = analyzer.analyze(generate_incident(seed=2).events)

    assert first.anomaly_scores and second.anomaly_scores
    assert all(0.0 <= s <= 1.0 for s in first.anomaly_scores.values())
    assert all(0.0 <= s <= 1.0 for s in second.anomaly_scores.values())
    # The baseline learned from both incidents (folded in via observe()).
    assert analyzer.anomaly_amplifier._incident_count == 2
