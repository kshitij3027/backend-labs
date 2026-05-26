"""Unit tests for the FinHealth composite framework ruleset.

These tests build ``LogEvent`` instances directly (no DB session
required — the ORM constructor accepts kwargs and the model is happy
to live in-memory). Each test exercises one branch of the FinHealth
rules: registry membership, composite-risk overlap detection, the
"no overlap" zero case, and the meta dual-signature finding.
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.frameworks import FRAMEWORK_REGISTRY
from src.frameworks.finhealth import FinHealthRules
from src.persistence.models import LogEvent


_DUAL_SIGN_FINDING = (
    "DUAL-SIGNED REPORT: signed under SOX-scope key and HIPAA-scope key"
)


def _make_event(
    event_type: str,
    *,
    actor: str = "alice@example.com",
    outcome: str = "success",
) -> LogEvent:
    """Construct a minimal ``LogEvent`` suitable for rule unit-testing.

    All non-rule columns are populated with stable defaults so subtle
    differences (``actor``, ``event_type``) only show up when a test
    sets them explicitly.
    """
    return LogEvent(
        timestamp=datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc),
        framework_tags=["SOX", "HIPAA"],
        event_type=event_type,
        actor=actor,
        resource="/finhealth/composite-1",
        action="execute",
        outcome=outcome,
        sensitivity="restricted",
        payload={},
    )


def test_registry_contains_finhealth() -> None:
    """FinHealthRules registers itself at import time under ``"FINHEALTH"``."""
    assert "FINHEALTH" in FRAMEWORK_REGISTRY
    assert FRAMEWORK_REGISTRY["FINHEALTH"] is FinHealthRules


def test_categories_and_mapping_stay_consistent() -> None:
    """Every mapped category appears in the categories list.

    ``composite_risk`` is in ``categories`` but NOT in
    ``event_type_to_category`` because it's a derived field, not a
    single-event-type bucket.
    """
    mapped_categories = set(FinHealthRules.event_type_to_category.values())
    declared_categories = set(FinHealthRules.categories)
    # Every mapped target must be a declared category.
    assert mapped_categories.issubset(declared_categories)
    # composite_risk is derived, not mapped — make sure it's still declared.
    assert "composite_risk" in declared_categories


def test_composite_risk_fires_when_actor_straddles_sox_and_hipaa() -> None:
    """Actor in both SOX-financial and HIPAA-PHI -> composite_risk finding fires.

    Three events: ``financial_transaction`` by alice, ``phi_access`` by
    alice, ``phi_access`` by bob. Only alice straddles both buckets, so
    the overlap count is 1.
    """
    events = [
        _make_event("financial_transaction", actor="alice@example.com"),
        _make_event("phi_access", actor="alice@example.com"),
        _make_event("phi_access", actor="bob@example.com"),
    ]
    findings = FinHealthRules.findings(events)
    expected = (
        "Composite risk: 1 actor(s) appear in both "
        "SOX-financial and HIPAA-PHI events"
    )
    assert expected in findings
    # And the summary's composite_risk count matches.
    summary = FinHealthRules.summarize(events)
    assert summary["composite_risk"] == 1


def test_no_overlap_means_no_composite_risk_finding() -> None:
    """SOX-only or HIPAA-only actors never overlap -> no composite_risk finding.

    The dual-sign meta finding is still present (it always fires), but
    the composite-risk line is absent.
    """
    events = [
        _make_event("financial_transaction", actor="alice@example.com"),
        _make_event("admin_login", actor="bob@example.com"),
        _make_event("phi_access", actor="carol@example.com"),
        _make_event("phi_modification", actor="dave@example.com"),
    ]
    findings = FinHealthRules.findings(events)
    # No actor straddles both subsets, so no composite_risk line.
    assert not any("Composite risk" in finding for finding in findings)
    # But the meta dual-sign finding is still present.
    assert _DUAL_SIGN_FINDING in findings
    # And the summary counts composite_risk as zero.
    summary = FinHealthRules.summarize(events)
    assert summary["composite_risk"] == 0


def test_dual_sign_meta_finding_always_present() -> None:
    """Even an empty events list still surfaces the dual-sign meta finding.

    The dual-sign line is descriptive metadata (not a count) so it
    should fire unconditionally to keep the auditor-facing report body
    self-describing.
    """
    findings = FinHealthRules.findings([])
    assert _DUAL_SIGN_FINDING in findings


def test_summarize_counts_categories_correctly() -> None:
    """One event per mapped event_type -> each count is 1, composite_risk is 0."""
    events = [
        _make_event("financial_transaction", actor="alice@example.com"),
        _make_event("admin_login", actor="bob@example.com"),
        _make_event("phi_access", actor="carol@example.com"),
        _make_event("phi_modification", actor="dave@example.com"),
    ]
    summary = FinHealthRules.summarize(events)
    assert summary == {
        "financial_transactions": 1,
        "admin_access": 1,
        "phi_access": 1,
        "phi_modifications": 1,
        "composite_risk": 0,
    }
    # All five categories accounted for.
    for category in FinHealthRules.categories:
        assert category in summary


def test_multiple_overlapping_actors_counted_once_each() -> None:
    """Two actors appearing in both subsets -> composite_risk count is 2.

    Alice has SOX + HIPAA events (one overlap); bob also has SOX + HIPAA
    events (another overlap). The intersection has size 2 regardless
    of how many times each actor shows up.
    """
    events = [
        _make_event("financial_transaction", actor="alice@example.com"),
        _make_event("phi_access", actor="alice@example.com"),
        _make_event("phi_modification", actor="alice@example.com"),
        _make_event("admin_login", actor="bob@example.com"),
        _make_event("phi_access", actor="bob@example.com"),
        _make_event("financial_transaction", actor="carol@example.com"),  # SOX-only
    ]
    summary = FinHealthRules.summarize(events)
    assert summary["composite_risk"] == 2
    findings = FinHealthRules.findings(events)
    assert any("2 actor(s)" in finding for finding in findings)


def test_unknown_event_types_silently_skipped() -> None:
    """Unrecognised event_types don't bump any counter."""
    events = [
        _make_event("financial_transaction", actor="alice@example.com"),
        _make_event("totally_made_up_event_type", actor="alice@example.com"),
    ]
    summary = FinHealthRules.summarize(events)
    assert summary == {
        "financial_transactions": 1,
        "admin_access": 0,
        "phi_access": 0,
        "phi_modifications": 0,
        "composite_risk": 0,
    }
