from __future__ import annotations

import uuid

import pytest

from src.analysis.detector import Bottleneck
from src.analysis.recommender import RULES, Recommendation, RecommendationEngine


def _b(type_, stage, severity="medium", z=3.0):
    return Bottleneck(
        id=uuid.uuid4().hex,
        type=type_,
        stage=stage,
        severity=severity,
        evidence_window=(0.0, 10.0),
        z_score=z,
        started_at=0.0,
        details={},
    )


def test_each_rule_produces_recommendation():
    engine = RecommendationEngine()
    # Feed one bottleneck per rule key; each should produce its mapped recommendation.
    # Different bottlenecks → different optimization_name → dedup does NOT swallow any.
    # But ("resource","write") and ("contention","transform->write") both map to batch_writer,
    # and ("architectural","all") maps to object_pool which also maps from ("resource","transform").
    # So feed each separately to validate each rule entry maps correctly.
    for (type_, stage), (_, _, expected_stage, expected_opt) in RULES.items():
        recs = engine.recommend([_b(type_, stage)])
        assert len(recs) == 1, f"Rule ({type_},{stage}) should produce 1 rec"
        rec = recs[0]
        assert rec.optimization_name == expected_opt
        assert rec.applies_to_stage == expected_stage


def test_unknown_returns_empty():
    engine = RecommendationEngine()
    # Unknown (type, stage) pair. Use a type literal that is valid Python but not in RULES.
    # ("serial", "transform") is not in RULES.
    b = _b("serial", "transform")
    assert engine.recommend([b]) == []


def test_dedup_by_optimization_name():
    engine = RecommendationEngine()
    # Both map to "batch_writer".
    b1 = _b("resource", "write")
    b2 = _b("contention", "transform->write")
    recs = engine.recommend([b1, b2])
    assert len(recs) == 1
    assert recs[0].optimization_name == "batch_writer"


def test_severity_sort():
    engine = RecommendationEngine()
    # Three distinct mapped rules with different severities.
    low_b = _b("serial", "parse", severity="low")
    high_b = _b("resource", "write", severity="high")
    med_b = _b("contention", "validate->transform", severity="medium")
    recs = engine.recommend([low_b, high_b, med_b])
    assert len(recs) == 3
    # Should be ordered high → medium → low by source bottleneck severity.
    assert recs[0].optimization_name == "batch_writer"  # from high (resource/write)
    assert recs[1].optimization_name == "async_io_variant"  # from medium
    assert recs[2].optimization_name == "fsm_parser"  # from low


def test_empty_input_returns_empty():
    engine = RecommendationEngine()
    assert engine.recommend([]) == []


def test_optimization_name_set():
    engine = RecommendationEngine()
    # Feed one bottleneck per rule key.
    bottlenecks = [_b(t, s) for (t, s) in RULES.keys()]
    recs = engine.recommend(bottlenecks)
    for rec in recs:
        assert rec.optimization_name, "optimization_name must be non-empty for mapped rules"


def test_to_dict_has_all_fields():
    engine = RecommendationEngine()
    b = _b("serial", "parse")
    recs = engine.recommend([b])
    assert len(recs) == 1
    d = recs[0].to_dict()
    expected_keys = {
        "bottleneck_id",
        "suggestion",
        "expected_impact",
        "applies_to_stage",
        "optimization_name",
    }
    assert expected_keys == set(d.keys())
