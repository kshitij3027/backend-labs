"""Unit tests for the GDPR framework ruleset.

These tests build ``LogEvent`` instances directly (no DB session
required — the ORM constructor accepts kwargs and the model is happy
to live in-memory). Each test exercises one branch of the GDPR rules:
empty input, the full event-type alphabet, and each of the four
``findings`` rules in isolation — including the per-actor
processing-without-consent heuristic's positive and negative paths.
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.frameworks import FRAMEWORK_REGISTRY
from src.frameworks.gdpr import GDPRRules
from src.persistence.models import LogEvent


def _make_event(
    event_type: str,
    *,
    outcome: str = "success",
    actor: str = "subject@example.com",
) -> LogEvent:
    """Construct a minimal ``LogEvent`` suitable for rule unit-testing.

    All non-rule columns are populated with stable defaults so subtle
    differences (e.g. ``actor``) only show up when a test sets them
    explicitly.
    """
    return LogEvent(
        timestamp=datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc),
        framework_tags=["GDPR"],
        event_type=event_type,
        actor=actor,
        resource="/subjects/abc-123",
        action="process",
        outcome=outcome,
        sensitivity="confidential",
        payload={},
    )


def test_registry_contains_gdpr() -> None:
    """GDPRRules registers itself at import time under ``"GDPR"``."""
    assert "GDPR" in FRAMEWORK_REGISTRY
    assert FRAMEWORK_REGISTRY["GDPR"] is GDPRRules


def test_empty_list() -> None:
    """Empty input -> all categories zero-filled, no findings emitted."""
    summary = GDPRRules.summarize([])
    assert summary == {
        "personal_data_processing": 0,
        "consent_records": 0,
        "dsr_requests": 0,
        "breach_notifications": 0,
        "cross_border_transfers": 0,
    }
    assert GDPRRules.findings([]) == []


def test_summary_counts_all_categories() -> None:
    """One event per category -> each count is 1; all 5 keys present."""
    events = [
        _make_event("personal_data_processing"),
        _make_event("consent_record"),
        _make_event("dsr_request"),
        _make_event("breach_notification"),
        _make_event("cross_border_transfer"),
    ]
    summary = GDPRRules.summarize(events)
    assert summary == {
        "personal_data_processing": 1,
        "consent_records": 1,
        "dsr_requests": 1,
        "breach_notifications": 1,
        "cross_border_transfers": 1,
    }
    # All five categories accounted for, even if a fresh subclass added more.
    for category in GDPRRules.categories:
        assert category in summary


def test_dsr_requests_finding() -> None:
    """4 dsr_request events -> finding mentions 4 DSRs processed."""
    events = [_make_event("dsr_request") for _ in range(4)]
    findings = GDPRRules.findings(events)
    assert "4 data-subject requests (DSRs) processed" in findings


def test_breach_notifications_finding() -> None:
    """1 breach_notification -> finding emitted with count and 72-hour reminder."""
    events = [_make_event("breach_notification")]
    findings = GDPRRules.findings(events)
    # The exact string format is "Breach notifications: <n>; verify 72-hour disclosure timing".
    assert any("Breach notifications: 1" in finding for finding in findings)
    assert any(
        "verify 72-hour disclosure timing" in finding for finding in findings
    )


def test_cross_border_transfers_finding() -> None:
    """3 cross_border_transfer events -> finding emitted with SCC reminder."""
    events = [_make_event("cross_border_transfer") for _ in range(3)]
    findings = GDPRRules.findings(events)
    assert any(
        "3 cross-border transfers" in finding for finding in findings
    )
    assert any("confirm SCCs" in finding for finding in findings)


def test_processing_without_consent() -> None:
    """3 processing events from alice + 1 consent_record from bob -> 3 without consent."""
    events = [
        _make_event("personal_data_processing", actor="alice@x.com"),
        _make_event("personal_data_processing", actor="alice@x.com"),
        _make_event("personal_data_processing", actor="alice@x.com"),
        _make_event("consent_record", actor="bob@x.com"),
    ]
    findings = GDPRRules.findings(events)
    assert (
        "3 processing events without an associated consent record" in findings
    )


def test_processing_with_full_consent_no_finding() -> None:
    """2 processing events from alice + 1 consent_record from alice -> no without-consent finding."""
    events = [
        _make_event("personal_data_processing", actor="alice@x.com"),
        _make_event("personal_data_processing", actor="alice@x.com"),
        _make_event("consent_record", actor="alice@x.com"),
    ]
    findings = GDPRRules.findings(events)
    # The processing-without-consent finding should NOT appear — every
    # processing actor has at least one consent_record in the same window.
    assert not any(
        "processing events without an associated consent record" in finding
        for finding in findings
    )
