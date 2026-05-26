"""Unit tests for the SOX framework ruleset.

These tests build ``LogEvent`` instances directly (no DB session
required — the ORM constructor accepts kwargs and the model is happy
to live in-memory). Each test exercises one branch of the SOX rules:
empty input, the full event-type alphabet, and each of the three
``findings`` rules in isolation.
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.frameworks import FRAMEWORK_REGISTRY, FrameworkRules
from src.frameworks.sox import SOXRules
from src.persistence.models import LogEvent


def _make_event(
    event_type: str,
    *,
    outcome: str = "success",
    actor: str = "user@example.com",
) -> LogEvent:
    """Construct a minimal ``LogEvent`` suitable for rule unit-testing.

    All non-rule columns are populated with stable defaults so subtle
    differences (e.g. ``actor``) only show up when a test sets them
    explicitly.
    """
    return LogEvent(
        timestamp=datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc),
        framework_tags=["SOX"],
        event_type=event_type,
        actor=actor,
        resource="/billing/abc-123",
        action="execute",
        outcome=outcome,
        sensitivity="internal",
        payload={},
    )


def test_registry_contains_sox() -> None:
    """SOXRules registers itself at import time under ``"SOX"``."""
    assert "SOX" in FRAMEWORK_REGISTRY
    assert FRAMEWORK_REGISTRY["SOX"] is SOXRules
    # Sanity check: the base class is NOT itself registered.
    assert FrameworkRules not in FRAMEWORK_REGISTRY.values()


def test_categories_and_mapping_stay_consistent() -> None:
    """Every category referenced in the mapping appears in the category list."""
    mapped_categories = set(SOXRules.event_type_to_category.values())
    declared_categories = set(SOXRules.categories)
    # Every mapped target must be a declared category.
    assert mapped_categories.issubset(declared_categories)


def test_empty_list_yields_zero_summary_and_no_findings() -> None:
    """Empty input -> all categories zero-filled, no findings emitted."""
    summary = SOXRules.summarize([])
    assert summary == {
        "admin_access": 0,
        "financial_transactions": 0,
        "system_changes": 0,
        "approval_workflows": 0,
        "sod_violations": 0,
    }
    assert SOXRules.findings([]) == []


def test_one_of_each_event_type_summary() -> None:
    """One event per category -> each count is 1; all 5 keys present."""
    events = [
        _make_event("admin_login"),
        _make_event("financial_transaction"),
        _make_event("system_config_change"),
        _make_event("approval_workflow"),
        _make_event("sod_violation"),
    ]
    summary = SOXRules.summarize(events)
    assert summary == {
        "admin_access": 1,
        "financial_transactions": 1,
        "system_changes": 1,
        "approval_workflows": 1,
        "sod_violations": 1,
    }
    # All five categories accounted for, even if a fresh subclass added more.
    for category in SOXRules.categories:
        assert category in summary


def test_sod_violations_finding_emitted() -> None:
    """3 sod_violation events -> the SoD finding string is present."""
    events = [_make_event("sod_violation") for _ in range(3)]
    findings = SOXRules.findings(events)
    assert "3 SoD violations detected in period" in findings


def test_failed_admin_logins_finding_emitted() -> None:
    """2 failed admin_login events + 1 successful -> finding mentions 2 failures."""
    events = [
        _make_event("admin_login", outcome="failure"),
        _make_event("admin_login", outcome="failure"),
        _make_event("admin_login", outcome="success"),
    ]
    findings = SOXRules.findings(events)
    assert "2 admin access events with outcome=failure" in findings


def test_system_changes_exceeding_approvals_finding_emitted() -> None:
    """5 system_config_change + 2 approval_workflow -> finding reports delta of 3."""
    events = [
        *[_make_event("system_config_change") for _ in range(5)],
        *[_make_event("approval_workflow") for _ in range(2)],
    ]
    findings = SOXRules.findings(events)
    assert (
        "3 system changes without an associated approval workflow" in findings
    )


def test_unknown_event_types_silently_skipped() -> None:
    """Unrecognised event_types don't bump any counter and don't add findings."""
    events = [
        _make_event("admin_login"),
        _make_event("unknown_event_type_42"),
        _make_event("definitely_not_a_sox_event"),
    ]
    summary = SOXRules.summarize(events)
    # Only admin_login should have been counted; everything else ignored.
    assert summary == {
        "admin_access": 1,
        "financial_transactions": 0,
        "system_changes": 0,
        "approval_workflows": 0,
        "sod_violations": 0,
    }
    # No findings because no SoD violations, no failed admin logins, and
    # zero system_changes vs zero approvals (not strictly greater).
    assert SOXRules.findings(events) == []
