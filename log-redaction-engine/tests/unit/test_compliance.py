"""Unit tests for the C8 compliance report aggregator.

Coverage:

* Per-regime counting (HIPAA, GDPR, PCI_DSS) over synthesized events.
* Outcome filter — only ``outcome == "success"`` events count.
* Empty ring buffer returns a well-formed empty report.
* Closed-Literal validation rejects unknown ``rule_set`` values.
* **Performance**: 100 k events aggregated in well under the 30 s
  spec budget.
* Strategy counts derived from the ``strategy`` field.
* ``since`` filter excludes events older than the cutoff.

These tests synthesize :class:`AuditEvent` records directly (via the
:func:`make_redaction_event` helper) and append them to a fresh
:class:`RingBuffer` per test. That isolates the aggregation under test
from every other moving part (detector / processor / config) and
keeps the suite fast enough to run inside the unit test loop without
any Docker dependencies.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from src.audit.events import AuditEvent
from src.audit.ring_buffer import RingBuffer
from src.compliance.reports import ComplianceReport, generate_report


# ---------------------------------------------------------------------------
# Helper: synthesize redaction audit events without touching the processor
# ---------------------------------------------------------------------------


def make_redaction_event(
    *,
    pattern_name: str,
    strategy: str,
    compliance_tags: list[str],
    outcome: str = "success",
) -> AuditEvent:
    """Build a redaction-event audit record for use in compliance tests.

    Mirrors the kwargs the production
    :class:`~src.audit.audit_logger.AuditLogger.record` call uses from
    the redaction processor so the synthesized events look identical to
    real ones on the wire. The default ``outcome="success"`` matches
    the happy path; tests that exercise the outcome filter pass
    ``outcome="failure"`` explicitly.
    """
    return AuditEvent(
        event_type="redaction",
        outcome=outcome,
        pattern_name=pattern_name,
        strategy=strategy,
        compliance_tags=compliance_tags,
    )


# ---------------------------------------------------------------------------
# Test 1: pattern breakdown from a single regime
# ---------------------------------------------------------------------------


class TestSingleRegime:
    """Per-pattern counting when every event is tagged with one regime."""

    def test_hundred_hipaa_events_breakdown_counts(self) -> None:
        """100 HIPAA events split 60/40 across mrn/ssn → exact counts."""
        buf = RingBuffer(maxlen=200)
        # 60 mrn + 40 ssn events, all HIPAA-tagged success redactions.
        for _ in range(60):
            buf.append(
                make_redaction_event(
                    pattern_name="mrn",
                    strategy="mask",
                    compliance_tags=["HIPAA"],
                )
            )
        for _ in range(40):
            buf.append(
                make_redaction_event(
                    pattern_name="ssn",
                    strategy="mask",
                    compliance_tags=["HIPAA"],
                )
            )

        report = generate_report(buf, rule_set="HIPAA")

        # Totals add up and the breakdown reflects the split.
        assert report.total_redactions == 100
        assert report.breakdown == {"mrn": 60, "ssn": 40}
        # rule_set echoed back as the requested regime.
        assert report.rule_set == "HIPAA"


# ---------------------------------------------------------------------------
# Test 2: mixed tags — only the requested regime is counted
# ---------------------------------------------------------------------------


class TestMixedTags:
    """Events with disjoint compliance tags are isolated per regime."""

    def test_fifty_hipaa_fifty_gdpr_each_report_sees_only_its_own(self) -> None:
        """Mixed 50 HIPAA + 50 GDPR → HIPAA report=50, GDPR report=50."""
        buf = RingBuffer(maxlen=200)
        for _ in range(50):
            buf.append(
                make_redaction_event(
                    pattern_name="ssn",
                    strategy="mask",
                    compliance_tags=["HIPAA"],
                )
            )
        for _ in range(50):
            buf.append(
                make_redaction_event(
                    pattern_name="email",
                    strategy="partial",
                    compliance_tags=["GDPR"],
                )
            )

        hipaa = generate_report(buf, rule_set="HIPAA")
        gdpr = generate_report(buf, rule_set="GDPR")
        pci = generate_report(buf, rule_set="PCI_DSS")

        # Each regime sees exactly its own slice.
        assert hipaa.total_redactions == 50
        assert hipaa.breakdown == {"ssn": 50}
        assert gdpr.total_redactions == 50
        assert gdpr.breakdown == {"email": 50}
        # PCI_DSS has no events in this buffer.
        assert pci.total_redactions == 0
        assert pci.breakdown == {}


# ---------------------------------------------------------------------------
# Test 3: outcome filter — failures excluded
# ---------------------------------------------------------------------------


class TestOutcomeFilter:
    """Only ``outcome == "success"`` events feed the report."""

    def test_failures_excluded_from_count(self) -> None:
        """50 successes + 50 failures (all HIPAA) → total=50."""
        buf = RingBuffer(maxlen=200)
        for _ in range(50):
            buf.append(
                make_redaction_event(
                    pattern_name="mrn",
                    strategy="mask",
                    compliance_tags=["HIPAA"],
                    outcome="success",
                )
            )
        for _ in range(50):
            buf.append(
                make_redaction_event(
                    pattern_name="mrn",
                    strategy="mask",
                    compliance_tags=["HIPAA"],
                    outcome="failure",
                )
            )

        report = generate_report(buf, rule_set="HIPAA")

        # Only the 50 success events should count.
        assert report.total_redactions == 50
        assert report.breakdown == {"mrn": 50}


# ---------------------------------------------------------------------------
# Test 4: empty buffer → well-formed empty report
# ---------------------------------------------------------------------------


class TestEmptyBuffer:
    """Report against an empty ring buffer is well-formed and zeroed."""

    def test_empty_buffer_returns_zero_totals(self) -> None:
        """No events → total=0, empty dicts, valid window bounds."""
        buf = RingBuffer(maxlen=10)
        report = generate_report(buf, rule_set="HIPAA")

        assert report.total_redactions == 0
        assert report.breakdown == {}
        assert report.strategies_used == {}
        # Window bounds default to (generated_at, generated_at) when
        # neither ``since`` nor any event provides a value.
        assert report.report_window_start <= report.report_window_end
        # The report is still a valid pydantic instance.
        assert report.rule_set == "HIPAA"


# ---------------------------------------------------------------------------
# Test 5: rule_set Literal validation
# ---------------------------------------------------------------------------


class TestRuleSetLiteral:
    """The Literal closes over the three supported regimes; nothing else."""

    def test_invalid_rule_set_raises_validation_error(self) -> None:
        """Constructing a report with an unknown regime name raises."""
        # Pydantic surfaces a ValidationError when the rule_set value
        # is outside the closed Literal — this is also what gives the
        # FastAPI route its 422 behavior for unknown values.
        with pytest.raises(ValidationError):
            ComplianceReport(
                rule_set="INVALID",  # type: ignore[arg-type]
                generated_at=datetime.now(timezone.utc),
                report_window_start=datetime.now(timezone.utc),
                report_window_end=datetime.now(timezone.utc),
                total_redactions=0,
                report_generation_time_ms=0,
            )


# ---------------------------------------------------------------------------
# Test 6: PERFORMANCE — 100 k events in under 30 s
# ---------------------------------------------------------------------------


class TestPerformance:
    """Spec requires 100 k events aggregated in under 30 s."""

    def test_100k_events_under_30_seconds(self) -> None:
        """Synthesize 100 000 HIPAA events; assert generation time < 30 s.

        We use ``RingBuffer(maxlen=200_000)`` so the ring's eviction
        policy doesn't silently drop events while we're filling it —
        the test would be meaningless if half the events fell out
        before the report ran.
        """
        # Maxlen comfortably above 100k so no event is evicted.
        buf = RingBuffer(maxlen=200_000)
        for _ in range(100_000):
            buf.append(
                make_redaction_event(
                    pattern_name="ssn",
                    strategy="mask",
                    compliance_tags=["HIPAA"],
                )
            )

        report = generate_report(buf, rule_set="HIPAA")

        # All events accounted for.
        assert report.total_redactions == 100_000
        # Spec bound: < 30 s for 100k events. The actual time should be
        # well under a second on laptop-class hardware — the 30 s ceiling
        # is the contractual SLO from the project requirements.
        assert report.report_generation_time_ms < 30_000


# ---------------------------------------------------------------------------
# Test 7: strategies_used counts each strategy
# ---------------------------------------------------------------------------


class TestStrategyCounts:
    """``strategies_used`` reflects the per-strategy frequencies."""

    def test_strategies_used_counts_each_strategy(self) -> None:
        """Mix of mask + partial + hash + tokenize → each gets its own count."""
        buf = RingBuffer(maxlen=50)
        # 5 mask + 3 partial + 2 hash + 1 tokenize, all HIPAA-tagged.
        for _ in range(5):
            buf.append(
                make_redaction_event(
                    pattern_name="ssn",
                    strategy="mask",
                    compliance_tags=["HIPAA"],
                )
            )
        for _ in range(3):
            buf.append(
                make_redaction_event(
                    pattern_name="ssn",
                    strategy="partial",
                    compliance_tags=["HIPAA"],
                )
            )
        for _ in range(2):
            buf.append(
                make_redaction_event(
                    pattern_name="ssn",
                    strategy="hash",
                    compliance_tags=["HIPAA"],
                )
            )
        buf.append(
            make_redaction_event(
                pattern_name="ssn",
                strategy="tokenize",
                compliance_tags=["HIPAA"],
            )
        )

        report = generate_report(buf, rule_set="HIPAA")

        # Exact per-strategy distribution.
        assert report.strategies_used == {
            "mask": 5,
            "partial": 3,
            "hash": 2,
            "tokenize": 1,
        }
        # Sanity: totals match the sum of strategies.
        assert report.total_redactions == 11


# ---------------------------------------------------------------------------
# Test 8: since-filter excludes older events
# ---------------------------------------------------------------------------


class TestSinceFilter:
    """``since`` cuts off events older than the cutoff timestamp."""

    def test_since_excludes_old_events(self) -> None:
        """3 older + 2 newer events; since=cutoff → only 2 counted."""
        buf = RingBuffer(maxlen=20)
        # Anchor time and a cutoff 5s later.
        t0 = datetime(2026, 5, 19, 10, 0, 0, tzinfo=timezone.utc)
        # Three "old" events (before t0+5).
        for i in range(3):
            buf.append(
                AuditEvent(
                    event_type="redaction",
                    outcome="success",
                    pattern_name="ssn",
                    strategy="mask",
                    compliance_tags=["HIPAA"],
                    timestamp_utc=t0 + timedelta(seconds=i),
                )
            )
        # Two "new" events (after t0+5).
        for i in range(2):
            buf.append(
                AuditEvent(
                    event_type="redaction",
                    outcome="success",
                    pattern_name="mrn",
                    strategy="mask",
                    compliance_tags=["HIPAA"],
                    timestamp_utc=t0 + timedelta(seconds=10 + i),
                )
            )

        # Cutoff at t0+5s — only the two "new" events should count.
        report = generate_report(buf, rule_set="HIPAA", since=t0 + timedelta(seconds=5))

        assert report.total_redactions == 2
        assert report.breakdown == {"mrn": 2}
        # Window start honors the caller's ``since`` if it precedes the
        # earliest observed event; here the earliest observed is t0+10
        # and ``since`` is t0+5, so the window should start at t0+5.
        assert report.report_window_start == t0 + timedelta(seconds=5)
