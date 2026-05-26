"""Unit tests for the HIPAA framework ruleset.

These tests build ``LogEvent`` instances directly (no DB session
required — the ORM constructor accepts kwargs and the model is happy
to live in-memory). Each test exercises one branch of the HIPAA rules:
empty input, the full event-type alphabet, and each of the three
``findings`` rules in isolation — including the 50-event auth-failure
threshold's upper and lower edges.
"""
from __future__ import annotations

from datetime import datetime, timezone

from src.frameworks import FRAMEWORK_REGISTRY
from src.frameworks.hipaa import HIPAARules
from src.persistence.models import LogEvent


def _make_event(
    event_type: str,
    *,
    outcome: str = "success",
    actor: str = "clinician@example.com",
) -> LogEvent:
    """Construct a minimal ``LogEvent`` suitable for rule unit-testing.

    All non-rule columns are populated with stable defaults so subtle
    differences (e.g. ``outcome``) only show up when a test sets them
    explicitly.
    """
    return LogEvent(
        timestamp=datetime(2026, 5, 1, 12, 0, 0, tzinfo=timezone.utc),
        framework_tags=["HIPAA"],
        event_type=event_type,
        actor=actor,
        resource="/ehr/patient-42",
        action="read",
        outcome=outcome,
        sensitivity="restricted",
        payload={},
    )


def test_registry_contains_hipaa() -> None:
    """HIPAARules registers itself at import time under ``"HIPAA"``."""
    assert "HIPAA" in FRAMEWORK_REGISTRY
    assert FRAMEWORK_REGISTRY["HIPAA"] is HIPAARules


def test_empty_list() -> None:
    """Empty input -> all categories zero-filled, no findings emitted."""
    summary = HIPAARules.summarize([])
    assert summary == {
        "phi_access": 0,
        "auth_failures": 0,
        "phi_modifications": 0,
        "breach_events": 0,
        "user_audit": 0,
    }
    assert HIPAARules.findings([]) == []


def test_summary_counts_all_categories() -> None:
    """One event per category -> each count is 1; all 5 keys present."""
    events = [
        _make_event("phi_access"),
        _make_event("auth_failure"),
        _make_event("phi_modification"),
        _make_event("breach_event"),
        _make_event("user_audit"),
    ]
    summary = HIPAARules.summarize(events)
    assert summary == {
        "phi_access": 1,
        "auth_failures": 1,
        "phi_modifications": 1,
        "breach_events": 1,
        "user_audit": 1,
    }
    # All five categories accounted for, even if a fresh subclass added more.
    for category in HIPAARules.categories:
        assert category in summary


def test_unauthorized_phi_access_finding() -> None:
    """2 denied phi_access + 1 successful -> finding mentions 2 unauthorized."""
    events = [
        _make_event("phi_access", outcome="denied"),
        _make_event("phi_access", outcome="denied"),
        _make_event("phi_access", outcome="success"),
    ]
    findings = HIPAARules.findings(events)
    assert "2 unauthorized PHI access events (outcome=denied)" in findings


def test_breach_events_finding() -> None:
    """3 breach_event rows -> finding mentions the count + notification workflow."""
    events = [_make_event("breach_event") for _ in range(3)]
    findings = HIPAARules.findings(events)
    assert (
        "Breach events detected (3) — notification workflow required" in findings
    )


def test_high_auth_failure_finding_above_threshold() -> None:
    """51 auth_failure events -> finding emitted with (51)."""
    events = [_make_event("auth_failure", outcome="failure") for _ in range(51)]
    findings = HIPAARules.findings(events)
    assert any("(51)" in finding for finding in findings)
    assert any(
        "High auth failure volume" in finding for finding in findings
    )


def test_low_auth_failure_no_finding() -> None:
    """50 auth_failure events -> no high-volume finding (threshold is strict >)."""
    events = [_make_event("auth_failure", outcome="failure") for _ in range(50)]
    findings = HIPAARules.findings(events)
    assert not any(
        "High auth failure volume" in finding for finding in findings
    )
