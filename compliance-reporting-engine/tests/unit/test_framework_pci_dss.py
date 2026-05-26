"""Unit tests for the PCI-DSS framework ruleset.

These tests build ``LogEvent`` instances directly (no DB session
required — the ORM constructor accepts kwargs and the model is happy
to live in-memory). Each test exercises one branch of the PCI-DSS rules:
empty input, the full event-type alphabet, the unique-actor gauge for
cardholder access, the failed-auth threshold edges, and the key-rotation
freshness check with a pinned ``period_end`` so the clock stays out of
the picture.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.frameworks import FRAMEWORK_REGISTRY, FrameworkRules
from src.frameworks.pci_dss import PCIDSSRules
from src.persistence.models import LogEvent


# Anchor "now" used by every test so the key-rotation rule is
# deterministic. Picked to roughly match the project's "today" memory.
_NOW = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)


def _make_event(
    event_type: str,
    *,
    outcome: str = "success",
    actor: str = "user@example.com",
    timestamp: datetime | None = None,
) -> LogEvent:
    """Construct a minimal ``LogEvent`` suitable for rule unit-testing.

    All non-rule columns are populated with stable defaults so subtle
    differences (e.g. ``actor`` or ``timestamp``) only show up when a
    test sets them explicitly.
    """
    return LogEvent(
        timestamp=timestamp or _NOW,
        framework_tags=["PCI_DSS"],
        event_type=event_type,
        actor=actor,
        resource="/payments/abc-123",
        action="execute",
        outcome=outcome,
        sensitivity="restricted",
        payload={},
    )


def test_registry_contains_pci_dss() -> None:
    """PCIDSSRules registers itself at import time under ``"PCI_DSS"``."""
    assert "PCI_DSS" in FRAMEWORK_REGISTRY
    assert FRAMEWORK_REGISTRY["PCI_DSS"] is PCIDSSRules
    # Sanity check: the base class is NOT itself registered.
    assert FrameworkRules not in FRAMEWORK_REGISTRY.values()


def test_categories_and_mapping_stay_consistent() -> None:
    """Every category referenced in the mapping appears in the category list."""
    mapped_categories = set(PCIDSSRules.event_type_to_category.values())
    declared_categories = set(PCIDSSRules.categories)
    # Every mapped target must be a declared category.
    assert mapped_categories.issubset(declared_categories)


def test_empty_list_emits_only_key_rotation_overdue() -> None:
    """Empty input -> all categories zero, only the key-rotation finding fires.

    With no events at all, the key-rotation rule treats the window as
    "overdue by definition" and emits a single finding. The
    cardholder-actor gauge is skipped (no events), and the failed-auth
    rule does not fire below threshold.
    """
    summary = PCIDSSRules.summarize([])
    assert summary == {
        "cardholder_access": 0,
        "payment_processing": 0,
        "key_rotation": 0,
        "failed_auth": 0,
        "config_changes": 0,
    }
    findings = PCIDSSRules.findings([], period_end=_NOW)
    assert findings == ["Key rotation overdue — last rotation > 90 days ago"]
    assert len(findings) == 1


def test_summary_counts_all_categories() -> None:
    """One event per category -> each count is 1; all 5 keys present."""
    events = [
        _make_event("cardholder_data_access"),
        _make_event("payment_processing"),
        _make_event("key_rotation"),
        _make_event("failed_auth_pci"),
        _make_event("pci_config_change"),
    ]
    summary = PCIDSSRules.summarize(events)
    assert summary == {
        "cardholder_access": 1,
        "payment_processing": 1,
        "key_rotation": 1,
        "failed_auth": 1,
        "config_changes": 1,
    }
    # All five categories accounted for, even if a fresh subclass added more.
    for category in PCIDSSRules.categories:
        assert category in summary


def test_cardholder_access_actor_gauge() -> None:
    """3 cardholder events from 2 distinct actors -> gauge reports 2 unique actors."""
    events = [
        _make_event("cardholder_data_access", actor="alice@example.com"),
        _make_event("cardholder_data_access", actor="alice@example.com"),
        _make_event("cardholder_data_access", actor="bob@example.com"),
        # A fresh key_rotation keeps the key-rotation rule quiet so this
        # test asserts the gauge in isolation.
        _make_event("key_rotation", timestamp=_NOW),
    ]
    findings = PCIDSSRules.findings(events, period_end=_NOW)
    assert "Cardholder data accessed by 2 unique actors" in findings


def test_failed_auth_above_threshold() -> None:
    """11 failed_auth_pci events -> a finding mentions the count of 11."""
    events = [_make_event("failed_auth_pci") for _ in range(11)]
    # Add a fresh key_rotation so the rotation rule doesn't pollute output.
    events.append(_make_event("key_rotation", timestamp=_NOW))
    findings = PCIDSSRules.findings(events, period_end=_NOW)
    assert "11 failed auth attempts targeting cardholder data" in findings


def test_failed_auth_at_threshold_no_finding() -> None:
    """Exactly 10 failed_auth_pci events -> rule is strictly >, so no finding.

    The key-rotation rule still emits because no ``key_rotation``
    events are present.
    """
    events = [_make_event("failed_auth_pci") for _ in range(10)]
    findings = PCIDSSRules.findings(events, period_end=_NOW)
    # No failed-auth finding should be present.
    assert not any(
        "failed auth attempts targeting cardholder data" in f for f in findings
    )
    # The only finding that should fire is the key-rotation-overdue one.
    assert findings == ["Key rotation overdue — last rotation > 90 days ago"]


def test_key_rotation_within_window_no_overdue() -> None:
    """One key_rotation 30 days before period_end -> rotation rule stays quiet."""
    recent_rotation = _NOW - timedelta(days=30)
    events = [_make_event("key_rotation", timestamp=recent_rotation)]
    findings = PCIDSSRules.findings(events, period_end=_NOW)
    assert not any("Key rotation overdue" in f for f in findings)


def test_key_rotation_overdue() -> None:
    """One key_rotation 100 days before period_end -> overdue finding emitted."""
    stale_rotation = _NOW - timedelta(days=100)
    events = [_make_event("key_rotation", timestamp=stale_rotation)]
    findings = PCIDSSRules.findings(events, period_end=_NOW)
    assert "Key rotation overdue — last rotation > 90 days ago" in findings


def test_unknown_event_types_silently_skipped() -> None:
    """Unrecognised event_types don't bump any counter."""
    events = [
        _make_event("cardholder_data_access"),
        _make_event("unknown_event_type_42"),
        _make_event("not_a_pci_event"),
    ]
    summary = PCIDSSRules.summarize(events)
    # Only the cardholder event should have been counted.
    assert summary == {
        "cardholder_access": 1,
        "payment_processing": 0,
        "key_rotation": 0,
        "failed_auth": 0,
        "config_changes": 0,
    }
