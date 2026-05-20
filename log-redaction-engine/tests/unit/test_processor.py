"""Unit tests for the C5 :class:`RedactionProcessor` pipeline.

Coverage layout:

* :class:`TestRedactionMetadata` — frozen-ness and ``extra="forbid"`` of the
  per-redaction metadata model. Verifies the no-plaintext invariant at the
  schema level.
* :class:`TestRedactedEntry`     — ``extra="allow"`` round-trip of arbitrary
  caller-passed fields.
* :class:`TestProcessor`         — end-to-end behavior: spec-exact partial
  output, default/healthcare/financial presets, Luhn-invalid CC pass-through,
  right-to-left splicing across multiple patterns, confidence-threshold
  filtering, audit + stats invocation counts, metadata shape (no plaintext),
  fixture round-trip, empty/missing message handling, batch APIs, and
  ``detect_entry`` (no-redaction preview).

A few cross-cutting fixtures live at the top of :class:`TestProcessor` —
the strategy registry, salt, token store, and the path to the project's
``config/`` directory used by ``load_preset``.

NER is intentionally NOT loaded — every Detector instance in this file is
constructed with ``ner_detector=None``. The redaction-pipeline tests don't
need spaCy and disabling it shaves multiple seconds off the test run.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, List

import pytest
from pydantic import ValidationError

from src.config.loader import load_preset
from src.config.manager import ConfigurationManager
from src.config.models import PatternRule, RedactionConfig
from src.detection.detector import Detector
from src.detection.patterns import Detection
from src.processor.redaction_processor import (
    RedactedEntry,
    RedactionMetadata,
    RedactionProcessor,
)
from src.redaction.strategies import StrategyRegistry
from src.redaction.token_store import TokenStore


# ---------------------------------------------------------------------------
# Shared path helpers
# ---------------------------------------------------------------------------

# tests/unit/test_processor.py -> project root is two directories up. We
# rely on the on-disk presets in <root>/config and the fixtures in
# <root>/tests/fixtures.
PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"


def _build_strategy_registry() -> StrategyRegistry:
    """Build a registry with a deterministic 32-byte test salt + fresh store.

    Using a hardcoded ``bytes.fromhex("ab" * 32)`` keeps the hash strategy
    deterministic across this test file (so any future hash-strategy
    assertion wouldn't depend on the conftest-injected salt). The token
    store is fresh per registry — no cross-test token contamination.
    """
    # 32 bytes of 0xAB — matches HashStrategy's expected salt length, and
    # is distinct from the conftest salt so we'd catch any code path that
    # accidentally loads ``Settings().REDACTION_HASH_SALT``.
    salt = bytes.fromhex("ab" * 32)
    return StrategyRegistry(salt=salt, token_store=TokenStore())


def _load_config_manager(preset: str = "default") -> ConfigurationManager:
    """Build a :class:`ConfigurationManager` from a preset name on disk.

    Defaulting to ``"default"`` matches the preset the prompt's tests
    exercise most often. Callers pass ``"healthcare"`` / ``"financial"``
    / ``"general"`` to test the other presets.
    """
    return ConfigurationManager(load_preset(preset, CONFIG_DIR))


def _build_processor(
    config_manager: ConfigurationManager | None = None,
    *,
    detector: Detector | None = None,
    audit_logger: Any | None = None,
    stats: Any | None = None,
) -> RedactionProcessor:
    """Convenience constructor for the per-test :class:`RedactionProcessor`.

    Each parameter has a sensible default so a test that only cares about
    one axis (e.g., "the audit_logger is invoked") doesn't have to spell
    out the other four. Detector is built with ``ner_detector=None`` so
    we never load spaCy from a unit test.
    """
    if detector is None:
        # ``ner_detector=None`` skips spaCy entirely — regex is the only
        # detection source in these tests.
        detector = Detector(ner_detector=None)
    if config_manager is None:
        config_manager = _load_config_manager("default")
    return RedactionProcessor(
        detector=detector,
        strategy_registry=_build_strategy_registry(),
        config_manager=config_manager,
        audit_logger=audit_logger,
        stats=stats,
    )


# ---------------------------------------------------------------------------
# Mock collaborators (audit + stats) — minimal interfaces matching what
# the processor calls on them.
# ---------------------------------------------------------------------------

class _RecordingAudit:
    """Minimal audit sink: capture every ``record(...)`` call as a dict."""

    def __init__(self) -> None:
        # Public attribute so tests can read it directly; no need for a
        # property — the contract is "this is a list of dicts".
        self.calls: list[dict[str, Any]] = []

    def record(self, **kwargs: Any) -> None:
        # Store the entire kwargs payload so tests can assert on any field
        # the processor passed (event_type, pattern_name, etc.).
        self.calls.append(kwargs)


class _RecordingCounters:
    """Captures every ``incr(name)`` into a list."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def incr(self, name: str) -> None:
        self.calls.append(name)


class _RecordingMetric:
    """Captures every ``record(value=None)`` call as a list of values."""

    def __init__(self) -> None:
        self.values: list[Any] = []

    def record(self, value: Any = None) -> None:
        # We accept ``value`` so latency.record(latency_ms) works AND
        # throughput.record() (no args) works.
        self.values.append(value)


class _RecordingStats:
    """Bundles counters + throughput + latency for the processor's stats hooks."""

    def __init__(self) -> None:
        self.counters = _RecordingCounters()
        self.throughput = _RecordingMetric()
        self.latency = _RecordingMetric()


# ---------------------------------------------------------------------------
# Detection factories — used by tests that need a stub Detector returning
# pre-built Detection objects.
# ---------------------------------------------------------------------------

class _FixedDetector:
    """Detector stub that returns a hardcoded detection list per text.

    Only used for the confidence-threshold test where we need an NER-style
    hit at 0.85 confidence without loading spaCy. ``detect`` ignores the
    text and returns whatever was passed to ``__init__``.
    """

    def __init__(self, detections: list[Detection]) -> None:
        self._detections = detections

    def detect(self, text: str) -> list[Detection]:
        # Filter to detections whose [start, end) lies within ``text`` so a
        # caller passing an empty string doesn't get back impossible spans.
        return [d for d in self._detections if d.end <= len(text)]


# ---------------------------------------------------------------------------
# TestRedactionMetadata — schema-level invariants
# ---------------------------------------------------------------------------

class TestRedactionMetadata:
    """Per-redaction metadata model contract tests."""

    def test_construct_with_valid_fields(self) -> None:
        # All four fields supplied; should construct without error.
        m = RedactionMetadata(pattern="ssn", strategy="mask", start=0, end=11)
        assert m.pattern == "ssn"
        assert m.strategy == "mask"
        assert m.start == 0
        assert m.end == 11

    def test_frozen_model_disallows_mutation(self) -> None:
        # ``frozen=True`` means attribute assignment raises ValidationError
        # in pydantic v2 — this is the property that makes the metadata
        # safe to share across threads.
        m = RedactionMetadata(pattern="ssn", strategy="mask", start=0, end=11)
        with pytest.raises(ValidationError):
            m.pattern = "credit_card"

    def test_extra_fields_are_forbidden(self) -> None:
        # ``extra="forbid"`` — any attempt to sneak a plaintext field into
        # the metadata at construction time must raise. This is the
        # schema-level enforcement of the no-plaintext invariant.
        with pytest.raises(ValidationError):
            RedactionMetadata(
                pattern="ssn",
                strategy="mask",
                start=0,
                end=11,
                # Should be rejected — the model carries no value field.
                value="123-45-6789",
            )


# ---------------------------------------------------------------------------
# TestRedactedEntry — extra="allow" round-trip
# ---------------------------------------------------------------------------

class TestRedactedEntry:
    """RedactedEntry preserves arbitrary caller-passed extra fields."""

    def test_extra_fields_preserved(self) -> None:
        # ``extra="allow"`` lets callers attach trace_id / service / etc.
        # We just have to be able to construct + read them back.
        entry = RedactedEntry(
            message="hi",
            timestamp="2026-05-19T10:00:00Z",
            level="INFO",
            redactions=[],
            # Arbitrary extras — must round-trip.
            trace_id="abc123",
            service="api",
        )
        # Direct attribute access works thanks to extra="allow"; we
        # also assert via model_dump() so a future API layer that
        # serializes the entry sees the extras too.
        assert getattr(entry, "trace_id") == "abc123"
        dumped = entry.model_dump()
        assert dumped["trace_id"] == "abc123"
        assert dumped["service"] == "api"


# ---------------------------------------------------------------------------
# TestProcessor — end-to-end pipeline behavior
# ---------------------------------------------------------------------------

class TestProcessor:
    """End-to-end ``RedactionProcessor`` behavior tests."""

    # -- spec exact-string verification ---------------------------------

    def test_spec_ssn_partial_exact_string(self) -> None:
        """Spec verification: SSN under partial yields the exact documented output."""
        # Build a custom in-line config so the assertion doesn't depend on
        # any on-disk preset's choices. Only ``ssn=partial`` is configured;
        # any other detection would be a no-op (no rule, skipped).
        config = RedactionConfig(
            rules={
                "ssn": PatternRule(
                    pattern_name="ssn",
                    strategy="partial",
                    # confidence_min=0.0 so any hit (regex@1.0 always
                    # passes; this is just defensive) clears the bar.
                    confidence_min=0.0,
                    compliance_tags=["HIPAA"],
                )
            }
        )
        proc = _build_processor(ConfigurationManager(config))

        result = proc.redact_entry(
            {
                "message": "User SSN 123-45-6789 logged in",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        )
        # Exact-string assertion from the C5 spec.
        assert result.message == "User SSN ***-**-6789 logged in"
        assert len(result.redactions) == 1
        assert result.redactions[0].pattern == "ssn"
        assert result.redactions[0].strategy == "partial"

    # -- default preset (mask strategy across the board) ----------------

    def test_default_preset_phi_fixture_masks_mrn_and_ssn(self) -> None:
        """Default preset (mask) wipes both MRN and SSN to asterisks.

        The default preset has ``ssn=mask`` and ``mrn=mask``; mask replaces
        every char (including separators) with ``*``. So we assert that
        the original sensitive substrings are GONE and that the masked
        forms (``"*" * len(original)``) appear in their place.
        """
        proc = _build_processor()

        entry = json.loads((FIXTURES_DIR / "log_phi.json").read_text())
        result = proc.redact_entry(entry)

        # The originals must NOT survive into the output.
        assert "MRN-123456" not in result.message
        assert "123-45-6789" not in result.message
        # Mask is length-preserving, so the masked forms have a known shape.
        # MRN "MRN-123456" -> 10 chars -> "**********".
        # SSN "123-45-6789" -> 11 chars -> "***********".
        assert "**********" in result.message  # MRN's 10 stars
        assert "***********" in result.message  # SSN's 11 stars
        # Metadata: two redactions in left-to-right order (MRN before SSN).
        assert len(result.redactions) == 2
        patterns = [r.pattern for r in result.redactions]
        assert patterns == ["mrn", "ssn"]

    # -- healthcare preset (partial for MRN + SSN) ----------------------

    def test_healthcare_preset_phi_fixture_partial_redaction(self) -> None:
        """Healthcare preset keeps the last 3 of MRN and last 4 of SSN."""
        proc = _build_processor(_load_config_manager("healthcare"))

        entry = json.loads((FIXTURES_DIR / "log_phi.json").read_text())
        result = proc.redact_entry(entry)

        # Partial-strategy exact outputs as documented in PartialStrategy.
        assert "MRN-***456" in result.message
        assert "***-**-6789" in result.message
        # Originals must NOT appear.
        assert "MRN-123456" not in result.message
        assert "123-45-6789" not in result.message

    # -- default preset (mask) on PCI fixture ---------------------------

    def test_default_preset_pci_fixture_masks_card(self) -> None:
        """Default preset (mask) wipes the credit card to asterisks."""
        proc = _build_processor()

        entry = json.loads((FIXTURES_DIR / "log_pci.json").read_text())
        result = proc.redact_entry(entry)

        # "4111-1111-1111-1111" is 19 chars; mask -> 19 asterisks.
        assert "4111-1111-1111-1111" not in result.message
        assert "*" * 19 in result.message
        # The non-PII parts should still be present.
        assert "Charge to" in result.message
        assert "for $42" in result.message

    # -- financial preset (credit_card=mask) ---------------------------

    def test_financial_preset_pci_fixture_masks_card(self) -> None:
        """Financial preset also masks the credit card (same behavior, different reason).

        The financial preset has ``credit_card=mask`` (per
        config/presets/financial.json), so the output for the PCI fixture
        should match the default-preset case. This test pins the contract
        rather than the implementation — if a future preset edit changes
        the financial CC strategy, this test should fail loudly.
        """
        proc = _build_processor(_load_config_manager("financial"))

        entry = json.loads((FIXTURES_DIR / "log_pci.json").read_text())
        result = proc.redact_entry(entry)

        assert "4111-1111-1111-1111" not in result.message
        assert "*" * 19 in result.message

    # -- Luhn-invalid CC: NOT detected, NOT redacted --------------------

    def test_luhn_invalid_credit_card_is_not_redacted(self) -> None:
        """A 16-digit number that fails Luhn never reaches the strategy."""
        proc = _build_processor()

        # Last digit flipped from 1 to 2 -> fails Luhn (verified in setup).
        result = proc.redact_entry(
            {
                "message": "Charge to 4111-1111-1111-1112 for $42",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        )
        # Number survives intact AND no redactions were recorded.
        assert "4111-1111-1111-1112" in result.message
        assert result.redactions == []

    # -- right-to-left splicing preserves offsets ----------------------

    def test_right_to_left_splicing_preserves_offsets(self) -> None:
        """Two non-overlapping patterns in one message are both redacted correctly.

        The processor sorts detections by ``start`` DESCENDING and splices
        from right to left so the leftmost detection's offsets remain
        valid even after a length-changing splice on the right. Mask is
        length-preserving here so the test would pass without the sort,
        but we still pin the behavior: both patterns must transform.
        """
        proc = _build_processor()

        result = proc.redact_entry(
            {
                "message": "SSN 123-45-6789 and MRN-111222",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        )
        # Default (mask) — both transform to "*"*len(original).
        assert "123-45-6789" not in result.message
        assert "MRN-111222" not in result.message
        assert "***********" in result.message  # SSN mask (11 stars)
        assert "**********" in result.message  # MRN mask (10 stars)
        assert len(result.redactions) == 2

    # -- confidence-threshold filtering --------------------------------

    def test_below_confidence_threshold_skips_redaction(self) -> None:
        """A detection below the rule's confidence_min is left alone."""
        # Build an in-line config that gates ``person`` at 0.99 — strictly
        # higher than the NER's 0.85 confidence, so the stubbed detection
        # below should be skipped.
        config = RedactionConfig(
            rules={
                "person": PatternRule(
                    pattern_name="person",
                    strategy="mask",
                    confidence_min=0.99,
                    compliance_tags=["GDPR"],
                )
            }
        )
        # Stub a Detector that returns one PERSON hit at 0.85 confidence —
        # below the threshold. The processor should observe that and skip.
        original_message = "Alice was here"
        person_hit = Detection(
            pattern_name="person",
            value="Alice",
            start=0,
            end=5,
            confidence=0.85,
            source="ner",
        )
        proc = _build_processor(
            ConfigurationManager(config),
            detector=_FixedDetector([person_hit]),
        )

        result = proc.redact_entry(
            {
                "message": original_message,
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        )
        # Nothing should change — the hit was below confidence_min.
        assert result.message == original_message
        assert result.redactions == []

    # -- audit_logger is called once per applied redaction --------------

    def test_audit_called_once_per_redaction_when_enabled(self) -> None:
        """With ``audit_all_redactions=True`` and audit_logger set, fires once per redaction."""
        audit = _RecordingAudit()
        proc = _build_processor(audit_logger=audit)

        # Message has 2 PHI patterns; default config has audit_all=True.
        proc.redact_entry(
            {
                "message": "SSN 123-45-6789 and MRN-111222",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        )
        # Two redactions -> two audit events.
        assert len(audit.calls) == 2
        # Both must carry event_type="redaction".
        for c in audit.calls:
            assert c["event_type"] == "redaction"
            assert c["outcome"] == "success"
            # The strategy name must be carried through (default = mask).
            assert c["strategy"] == "mask"
            # compliance_tags must be a list (we passed list(rule.compliance_tags)).
            assert isinstance(c["compliance_tags"], list)

    # -- audit_logger is NOT called when None ---------------------------

    def test_audit_not_called_when_audit_logger_is_none(self) -> None:
        """With ``audit_logger=None``, audit hooks are skipped entirely."""
        # Building without injecting audit means ``self._audit_logger is None``;
        # the processor should short-circuit and never call any sink. We
        # don't have a sink to assert against — the test instead verifies
        # the call returns cleanly (i.e., the None-guard works).
        proc = _build_processor(audit_logger=None)
        result = proc.redact_entry(
            {
                "message": "SSN 123-45-6789",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        )
        # No crash + redaction still happened.
        assert "123-45-6789" not in result.message

    # -- stats hooks fire on every entry --------------------------------

    def test_stats_counters_throughput_latency_recorded(self) -> None:
        """Stats facade receives one counter increment per redaction + one throughput / latency call per entry."""
        stats = _RecordingStats()
        proc = _build_processor(stats=stats)

        proc.redact_entry(
            {
                "message": "SSN 123-45-6789 and MRN-111222",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        )
        # Counters: one ``incr`` per redaction, by pattern name.
        assert sorted(stats.counters.calls) == ["mrn", "ssn"]
        # Throughput + latency: exactly one record() each per entry.
        assert len(stats.throughput.values) == 1
        assert len(stats.latency.values) == 1
        # Latency value must be a non-negative number (monotonic_ns can
        # legitimately measure as 0.0 on ultra-fast paths, so we use >=).
        assert stats.latency.values[0] >= 0.0

    # -- metadata shape: count + fields + no plaintext ------------------

    def test_metadata_shape_count_fields_no_plaintext(self) -> None:
        """RedactionMetadata count == applied redactions; each carries
        ``pattern``/``strategy``/``start``/``end``; no plaintext leaks.
        """
        proc = _build_processor()

        original_message = "SSN 123-45-6789 here"
        result = proc.redact_entry(
            {
                "message": original_message,
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
            }
        )
        assert len(result.redactions) == 1
        meta = result.redactions[0]
        # Required fields present.
        assert meta.pattern == "ssn"
        assert meta.strategy == "mask"
        # Offsets index into the ORIGINAL message (positions of "123-45-6789").
        assert original_message[meta.start : meta.end] == "123-45-6789"

        # No plaintext anywhere in the metadata's dump — neither the
        # original value nor the redacted form should be present as a
        # field. ``model_dump`` returns all declared + extra fields.
        dumped = meta.model_dump()
        # The model carries no value/redacted field; the only string
        # values are the pattern + strategy NAMES, not PII.
        assert "123-45-6789" not in json.dumps(dumped)
        # Pin the exact key set so a future field addition forces a
        # conscious review of this invariant.
        assert set(dumped.keys()) == {"pattern", "strategy", "start", "end"}

    # -- caller's dict must not be mutated ------------------------------

    def test_input_dict_is_not_mutated(self) -> None:
        """``redact_entry`` is pure with respect to its input dict."""
        proc = _build_processor()

        original = {
            "message": "SSN 123-45-6789 logged in",
            "timestamp": "2026-05-19T10:00:00Z",
            "level": "INFO",
        }
        # Deep copy of the original so we can compare after the call.
        snapshot = json.loads(json.dumps(original))
        _ = proc.redact_entry(original)
        # The input must be byte-identical to before.
        assert original == snapshot

    # -- fixtures load and round-trip cleanly ---------------------------

    @pytest.mark.parametrize(
        "fixture_name",
        ["log_pii.json", "log_phi.json", "log_pci.json"],
    )
    def test_fixtures_load_and_round_trip(self, fixture_name: str) -> None:
        """Each single-entry fixture loads, redacts, and preserves shape."""
        proc = _build_processor()
        entry = json.loads((FIXTURES_DIR / fixture_name).read_text())

        result = proc.redact_entry(entry)
        # timestamp + level must be passed through unchanged.
        assert result.timestamp == entry["timestamp"]
        assert result.level == entry["level"]
        # Some redactions occurred (every fixture carries PII).
        assert len(result.redactions) >= 1

    # -- mixed batch: 10 entries, batch API, last entry plain -----------

    def test_mixed_batch_fixture_redacts_each_entry(self) -> None:
        """The 10-entry batch fixture round-trips: 9 with redactions, 1 plain."""
        proc = _build_processor()
        entries = json.loads((FIXTURES_DIR / "log_mixed_batch.json").read_text())

        # Batch API returns one RedactedEntry per input entry, same order.
        results = proc.redact_batch(entries)
        assert len(results) == 10

        # Entry index 8 is the "no-PII" line; should have zero redactions.
        assert results[8].redactions == []
        # Every other entry must have at least one redaction.
        for i, r in enumerate(results):
            if i == 8:
                continue
            assert len(r.redactions) >= 1, (
                f"entry {i} had no redactions but should have at least one"
            )

    # -- empty-batch and small-batch sanity -----------------------------

    def test_redact_batch_empty_returns_empty_list(self) -> None:
        """``redact_batch([])`` -> ``[]``."""
        proc = _build_processor()
        assert proc.redact_batch([]) == []

    def test_redact_batch_two_entries_returns_two_results(self) -> None:
        """``redact_batch`` preserves cardinality."""
        proc = _build_processor()
        entries = [
            {"message": "SSN 123-45-6789", "timestamp": "t", "level": "INFO"},
            {"message": "nothing here", "timestamp": "t", "level": "INFO"},
        ]
        results = proc.redact_batch(entries)
        assert len(results) == 2
        # First has a redaction, second has none.
        assert len(results[0].redactions) == 1
        assert results[1].redactions == []

    # -- empty / missing message handling -------------------------------

    def test_empty_message_field(self) -> None:
        """Empty-string message yields no redactions, preserved output."""
        proc = _build_processor()
        result = proc.redact_entry(
            {"message": "", "timestamp": "2026-05-19T10:00:00Z", "level": "INFO"}
        )
        assert result.message == ""
        assert result.redactions == []

    def test_missing_message_field(self) -> None:
        """Missing message key defaults to empty string in output."""
        proc = _build_processor()
        result = proc.redact_entry(
            {"timestamp": "2026-05-19T10:00:00Z", "level": "INFO"}
        )
        # ``setdefault("message", "")`` populates the field even when the
        # caller omitted it.
        assert result.message == ""
        assert result.redactions == []
        assert result.timestamp == "2026-05-19T10:00:00Z"
        assert result.level == "INFO"

    # -- detect_entry (preview, no redaction) ---------------------------

    def test_detect_entry_returns_detections_without_redacting(self) -> None:
        """``detect_entry`` returns hits without modifying the input."""
        proc = _build_processor()

        entry = json.loads((FIXTURES_DIR / "log_phi.json").read_text())
        snapshot = json.loads(json.dumps(entry))

        detections = proc.detect_entry(entry)
        # PHI fixture has at least MRN + SSN.
        pattern_names = {d.pattern_name for d in detections}
        assert "mrn" in pattern_names
        assert "ssn" in pattern_names
        # Input must NOT be mutated.
        assert entry == snapshot

    def test_detect_entry_audit_emits_one_event_when_logger_set(self) -> None:
        """``detect_entry`` fires exactly one ``detect`` audit event per call."""
        audit = _RecordingAudit()
        proc = _build_processor(audit_logger=audit)

        # Multi-detection entry — but detect_entry should emit ONE event,
        # not N. This is the "low-volume on detect" contract.
        _ = proc.detect_entry(
            {
                "message": "SSN 123-45-6789 and MRN-111222",
                "timestamp": "t",
                "level": "INFO",
            }
        )
        assert len(audit.calls) == 1
        assert audit.calls[0]["event_type"] == "detect"
        assert audit.calls[0]["outcome"] == "success"

    # -- extra (non-shape) fields round-trip ---------------------------

    def test_extra_input_fields_round_trip_through_output(self) -> None:
        """Caller-passed extras (trace_id, etc.) flow through unchanged."""
        proc = _build_processor()
        result = proc.redact_entry(
            {
                "message": "SSN 123-45-6789",
                "timestamp": "2026-05-19T10:00:00Z",
                "level": "INFO",
                "trace_id": "trace-abc",
                "service": "auth",
            }
        )
        # Pydantic exposes extras via model_dump (or direct getattr).
        dumped = result.model_dump()
        assert dumped["trace_id"] == "trace-abc"
        assert dumped["service"] == "auth"
        # And the redaction still occurred.
        assert "123-45-6789" not in result.message
