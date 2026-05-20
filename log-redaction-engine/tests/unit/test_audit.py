"""Unit tests for the C6 audit subsystem (events, ring buffer, logger).

Three classes:

* :class:`TestAuditEvent` — sealed-schema invariants of the pydantic
  model (extra="forbid", frozen, closed Literals). The canonical
  ``test_no_plaintext_leak`` lives here as the leak-proof regression
  test the spec explicitly requires.
* :class:`TestRingBuffer` — bounded thread-safe ring buffer behavior:
  append + snapshot round-trip, overflow-drops-oldest, and the three
  filter dimensions (event_type, compliance_tag, since).
* :class:`TestAuditLogger` — :meth:`AuditLogger.record` constructs +
  appends + emits a structured log line. Also asserts the leak-proof
  property end-to-end via ``caplog``.

NER is intentionally NOT loaded in any test here — the audit / stats
layer is independent of the detection layer.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from src.audit.audit_logger import AuditLogger
from src.audit.events import AuditEvent
from src.audit.ring_buffer import RingBuffer


# ---------------------------------------------------------------------------
# TestAuditEvent — sealed-schema invariants
# ---------------------------------------------------------------------------


class TestAuditEvent:
    """Schema-level invariants of the :class:`AuditEvent` pydantic model."""

    def test_construct_with_required_fields_only(self) -> None:
        """Minimal construction succeeds and populates defaults."""
        # Only event_type and outcome are positional-required; the
        # rest are defaults (event_id, timestamp_utc, actor, etc.).
        event = AuditEvent(event_type="redaction", outcome="success")
        assert event.event_type == "redaction"
        assert event.outcome == "success"
        # Default factory populates a fresh UUID and a UTC timestamp.
        assert event.event_id is not None
        assert event.timestamp_utc.tzinfo is not None
        # Default actor is the documented "system" value.
        assert event.actor == "system"
        # Compliance tags default to an empty list (not None).
        assert event.compliance_tags == []

    def test_construct_with_all_optional_fields(self) -> None:
        """Every declared field can be populated; values round-trip."""
        event = AuditEvent(
            event_type="redaction",
            outcome="success",
            pattern_name="ssn",
            strategy="mask",
            compliance_tags=["HIPAA", "PCI_DSS"],
            actor="api",
            failure_reason=None,
        )
        assert event.pattern_name == "ssn"
        assert event.strategy == "mask"
        assert event.compliance_tags == ["HIPAA", "PCI_DSS"]
        assert event.actor == "api"

    def test_invalid_event_type_raises(self) -> None:
        """An ``event_type`` outside the Literal set fails validation."""
        # The Literal closes over a known 5-element set; "encrypt" is
        # not one of them so pydantic must reject it.
        with pytest.raises(ValidationError):
            AuditEvent(event_type="encrypt", outcome="success")

    def test_invalid_outcome_raises(self) -> None:
        """An ``outcome`` outside the Literal set fails validation."""
        with pytest.raises(ValidationError):
            AuditEvent(event_type="redaction", outcome="maybe")

    def test_frozen_model_disallows_mutation(self) -> None:
        """``frozen=True`` means attribute assignment raises."""
        event = AuditEvent(event_type="redaction", outcome="success")
        with pytest.raises(ValidationError):
            event.actor = "someone_else"

    # -- THE canonical leak-proof regression test ----------------------

    def test_no_plaintext_leak(self) -> None:
        """``extra="forbid"`` makes plaintext fields a hard construction error.

        This is the load-bearing test the C6 spec calls out by name. It
        documents the schema-level enforcement of "no PII in audit
        records": any attempt to smuggle a plaintext / value /
        redacted_value field must raise ``ValidationError`` at
        construction, BEFORE any side effect (log emission, buffer
        append) can leak the secret.
        """
        # 1) plaintext= must be rejected.
        with pytest.raises(ValidationError):
            AuditEvent(
                event_type="redaction",
                outcome="success",
                plaintext="123-45-6789",
            )
        # 2) value= must be rejected (the second common leak vector).
        with pytest.raises(ValidationError):
            AuditEvent(
                event_type="redaction",
                outcome="success",
                value="alice@example.com",
            )
        # 3) redacted_value= must be rejected (third leak vector — the
        #    redacted form is technically not plaintext but the schema
        #    forbids it as a matter of policy).
        with pytest.raises(ValidationError):
            AuditEvent(
                event_type="redaction",
                outcome="success",
                redacted_value="***-**-6789",
            )


# ---------------------------------------------------------------------------
# TestRingBuffer — bounded thread-safe FIFO with audit filtering
# ---------------------------------------------------------------------------


class TestRingBuffer:
    """Behavior tests for :class:`RingBuffer` (append, snapshot, filter)."""

    def test_append_and_snapshot_round_trip(self) -> None:
        """Two events go in, snapshot returns them in append order."""
        buf = RingBuffer(maxlen=10)
        e1 = AuditEvent(event_type="redaction", outcome="success", pattern_name="ssn")
        e2 = AuditEvent(event_type="detect", outcome="success")
        buf.append(e1)
        buf.append(e2)
        snap = buf.snapshot()
        assert len(snap) == 2
        # Oldest-first ordering — e1 was appended first.
        assert snap[0] is e1
        assert snap[1] is e2

    def test_overflow_drops_oldest(self) -> None:
        """A 3-slot buffer receiving 5 events keeps only the last 3."""
        buf = RingBuffer(maxlen=3)
        # Append 5 events with distinguishing pattern names.
        events = [
            AuditEvent(event_type="redaction", outcome="success", pattern_name=f"p{i}")
            for i in range(5)
        ]
        for e in events:
            buf.append(e)
        snap = buf.snapshot()
        # Only the last 3 survive (p2, p3, p4).
        assert len(snap) == 3
        assert [e.pattern_name for e in snap] == ["p2", "p3", "p4"]

    def test_len_reflects_current_size(self) -> None:
        """``__len__`` returns the current bucket count, not maxlen."""
        buf = RingBuffer(maxlen=10)
        assert len(buf) == 0
        buf.append(AuditEvent(event_type="redaction", outcome="success"))
        buf.append(AuditEvent(event_type="redaction", outcome="success"))
        assert len(buf) == 2

    def test_filter_by_event_type(self) -> None:
        """``filter(event_type=...)`` returns only matching events."""
        buf = RingBuffer(maxlen=10)
        # Mix two event types.
        buf.append(AuditEvent(event_type="redaction", outcome="success"))
        buf.append(AuditEvent(event_type="detect", outcome="success"))
        buf.append(AuditEvent(event_type="redaction", outcome="success"))
        # Filter to redactions only — must return exactly 2.
        result = buf.filter(event_type="redaction")
        assert len(result) == 2
        assert all(e.event_type == "redaction" for e in result)

    def test_filter_by_compliance_tag(self) -> None:
        """``filter(compliance_tag=...)`` returns only events with that tag."""
        buf = RingBuffer(maxlen=10)
        # Mix three events: HIPAA-tagged, GDPR-tagged, and untagged.
        buf.append(
            AuditEvent(
                event_type="redaction", outcome="success", compliance_tags=["HIPAA"]
            )
        )
        buf.append(
            AuditEvent(
                event_type="redaction", outcome="success", compliance_tags=["GDPR"]
            )
        )
        buf.append(AuditEvent(event_type="redaction", outcome="success"))
        # Filter to HIPAA — only the first event qualifies.
        result = buf.filter(compliance_tag="HIPAA")
        assert len(result) == 1
        assert "HIPAA" in result[0].compliance_tags

    def test_filter_by_since(self) -> None:
        """``filter(since=...)`` drops events older than the cutoff."""
        buf = RingBuffer(maxlen=10)
        # Construct three events with explicit, increasing timestamps.
        # Using explicit timestamps avoids any reliance on the
        # default-factory wall clock.
        t0 = datetime(2026, 5, 19, 10, 0, 0, tzinfo=timezone.utc)
        e1 = AuditEvent(
            event_type="redaction", outcome="success", timestamp_utc=t0
        )
        e2 = AuditEvent(
            event_type="redaction",
            outcome="success",
            timestamp_utc=t0 + timedelta(seconds=10),
        )
        e3 = AuditEvent(
            event_type="redaction",
            outcome="success",
            timestamp_utc=t0 + timedelta(seconds=20),
        )
        buf.append(e1)
        buf.append(e2)
        buf.append(e3)
        # Cutoff at t0+5 — only e2 and e3 should survive.
        result = buf.filter(since=t0 + timedelta(seconds=5))
        assert len(result) == 2
        # Identity check rather than equality — same model instances.
        assert result[0] is e2
        assert result[1] is e3

    def test_filter_combined_criteria(self) -> None:
        """Multiple filter criteria intersect (AND, not OR)."""
        buf = RingBuffer(maxlen=10)
        t0 = datetime(2026, 5, 19, 10, 0, 0, tzinfo=timezone.utc)
        # Three events distinguishable along all three axes:
        #  e1: redaction + HIPAA + old        -> filtered out (too old)
        #  e2: redaction + HIPAA + new        -> kept (matches everything)
        #  e3: detect    + HIPAA + new        -> filtered out (event_type)
        e1 = AuditEvent(
            event_type="redaction",
            outcome="success",
            compliance_tags=["HIPAA"],
            timestamp_utc=t0,
        )
        e2 = AuditEvent(
            event_type="redaction",
            outcome="success",
            compliance_tags=["HIPAA"],
            timestamp_utc=t0 + timedelta(seconds=10),
        )
        e3 = AuditEvent(
            event_type="detect",
            outcome="success",
            compliance_tags=["HIPAA"],
            timestamp_utc=t0 + timedelta(seconds=10),
        )
        buf.append(e1)
        buf.append(e2)
        buf.append(e3)
        # Combined query — only e2 should match all three axes.
        result = buf.filter(
            since=t0 + timedelta(seconds=5),
            event_type="redaction",
            compliance_tag="HIPAA",
        )
        assert len(result) == 1
        assert result[0] is e2


# ---------------------------------------------------------------------------
# TestAuditLogger — record() integration
# ---------------------------------------------------------------------------


class TestAuditLogger:
    """Behavior tests for :class:`AuditLogger.record`."""

    def test_record_returns_event_and_appends_to_buffer(self) -> None:
        """``record(...)`` builds an event, returns it, and the buffer has it."""
        buf = RingBuffer(maxlen=10)
        logger_ = AuditLogger(buf)
        event = logger_.record(
            event_type="redaction",
            outcome="success",
            pattern_name="ssn",
            strategy="mask",
            compliance_tags=["HIPAA"],
        )
        # Returned event reflects the inputs.
        assert event.event_type == "redaction"
        assert event.pattern_name == "ssn"
        # The buffer has exactly this one event.
        snap = buf.snapshot()
        assert len(snap) == 1
        assert snap[0] is event

    def test_record_emits_structured_log_line_without_plaintext(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """``record`` logs the event via stdlib logging; no plaintext is captured.

        The structured log line is JSON-encoded via
        ``model_dump_json()``. Because the schema forbids plaintext
        fields entirely, the dump cannot include any PII even if the
        caller's pattern_name / strategy values are present.
        """
        buf = RingBuffer(maxlen=10)
        logger_ = AuditLogger(buf)
        # Capture INFO-level records from the audit_logger module.
        caplog.set_level(logging.INFO, logger="src.audit.audit_logger")
        logger_.record(
            event_type="redaction",
            outcome="success",
            pattern_name="ssn",
            strategy="mask",
            compliance_tags=["HIPAA"],
        )
        # At least one record was emitted.
        assert len(caplog.records) >= 1
        # The captured log line must NOT contain the plaintext we
        # imagined a buggy caller could leak — we never passed it as a
        # kwarg (it would have raised), but the property we're pinning
        # here is "the structured emission contains no value-shaped
        # strings". We assert via a concrete PII string we never
        # passed; this is the canary that ``model_dump_json`` doesn't
        # accidentally pick up an attribute we don't see.
        full_log = " ".join(r.getMessage() for r in caplog.records)
        assert "123-45-6789" not in full_log
        assert "alice@example.com" not in full_log
        # But the safe metadata IS in the log (this confirms the log
        # line was actually emitted as JSON).
        assert "ssn" in full_log
        assert "mask" in full_log

    def test_record_invalid_event_type_raises_before_buffer_append(self) -> None:
        """A bad event_type fails validation; the buffer remains empty."""
        buf = RingBuffer(maxlen=10)
        logger_ = AuditLogger(buf)
        # "bogus" is outside the closed Literal — pydantic must reject.
        with pytest.raises(ValidationError):
            logger_.record(event_type="bogus", outcome="success")
        # Critical leak-proof property: if the event couldn't be built,
        # nothing was appended to the buffer.
        assert len(buf) == 0

    def test_record_rejects_plaintext_kwargs(self) -> None:
        """Attempting to leak plaintext via record() fails at validation."""
        buf = RingBuffer(maxlen=10)
        logger_ = AuditLogger(buf)
        # The schema forbids extra kwargs; record() forwards everything
        # to AuditEvent so this raises before any side effect.
        with pytest.raises(ValidationError):
            logger_.record(
                event_type="redaction",
                outcome="success",
                plaintext="123-45-6789",
            )
        # Buffer is empty — no side effect from a rejected event.
        assert len(buf) == 0

    def test_audit_logger_wired_into_processor_records_event(self) -> None:
        """End-to-end: AuditLogger + RingBuffer + RedactionProcessor.

        Wire a real audit logger backed by a real ring buffer into the
        processor, run one redaction, and assert the buffer contains
        at least one redaction event. This is the integration test
        for C6 — it proves the C5-shipped audit hook points actually
        match the C6 audit API.
        """
        # Inline imports so the audit test file doesn't pull in the
        # entire processor stack at module load time (helps test
        # collection speed when audit tests are run alone).
        from src.config.manager import ConfigurationManager
        from src.config.models import PatternRule, RedactionConfig
        from src.detection.detector import Detector
        from src.processor.redaction_processor import RedactionProcessor
        from src.redaction.strategies import StrategyRegistry
        from src.redaction.token_store import TokenStore

        buf = RingBuffer(maxlen=10)
        audit_logger = AuditLogger(buf)
        # Minimal in-line config: ssn -> mask, HIPAA-tagged. confidence_min=0.0
        # so a regex hit (1.0) always clears the bar.
        config = RedactionConfig(
            rules={
                "ssn": PatternRule(
                    pattern_name="ssn",
                    strategy="mask",
                    confidence_min=0.0,
                    compliance_tags=["HIPAA"],
                )
            }
        )
        # Build a detector with NO NER (regex-only).
        detector = Detector(ner_detector=None)
        # Hash salt is irrelevant for mask strategy but the registry
        # requires one — 32-byte test salt.
        registry = StrategyRegistry(
            salt=bytes.fromhex("ab" * 32), token_store=TokenStore()
        )
        proc = RedactionProcessor(
            detector=detector,
            strategy_registry=registry,
            config_manager=ConfigurationManager(config),
            audit_logger=audit_logger,
            stats=None,
        )
        # One PII hit -> one audit event.
        proc.redact_entry(
            {
                "message": "User SSN 123-45-6789 logged in",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        )
        snap = buf.snapshot()
        assert len(snap) >= 1
        # At least one redaction event with the expected pattern.
        redactions = [e for e in snap if e.event_type == "redaction"]
        assert len(redactions) == 1
        assert redactions[0].pattern_name == "ssn"
        assert redactions[0].strategy == "mask"
        assert "HIPAA" in redactions[0].compliance_tags
