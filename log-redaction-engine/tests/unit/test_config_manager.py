"""Unit tests for the C4 configuration layer.

Coverage layout:

* :class:`TestModels`  — pydantic validation surface for ``PatternRule``
  and ``RedactionConfig`` (frozen, extra-forbid, Literal-narrowed fields,
  numeric bounds).
* :class:`TestLoader`  — JSON-on-disk loaders, including all three
  presets, the special-cased ``default`` name, and the not-found path.
* :class:`TestManager` — atomic hot-reload semantics under contention:
  single-threaded swap correctness, the "old config remains active on
  invalid JSON" post-condition, and a multi-threaded soak test with two
  writers + eight readers that must finish well under 5 s without
  deadlocking.

The concurrency test
--------------------
We deliberately do NOT assert that "exactly N writes succeeded" because
the precise interleaving depends on the OS scheduler. What we DO assert
is the invariant that matters: the final config is one of the two
presets we cycled through, and the entire test completed within the
wall-time budget (proxy for "no deadlock").
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.config.loader import load_config_file, load_preset
from src.config.manager import ConfigurationManager
from src.config.models import PatternRule, RedactionConfig


# ---------------------------------------------------------------------------
# Shared helpers and fixtures
# ---------------------------------------------------------------------------

# Tests live at tests/unit/test_config_manager.py; the project root is two
# directories up, and the on-disk config lives at <root>/config.
# We compute this once at module import so individual tests don't pay the
# Path arithmetic cost.
PROJECT_ROOT = Path(__file__).parent.parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"


def _minimal_rules() -> dict[str, PatternRule]:
    """Build the smallest valid rules dict ``RedactionConfig`` will accept.

    A single rule is enough to satisfy the schema; multiple tests want
    "any valid config" without caring about the contents, so this helper
    keeps test bodies short.
    """
    return {
        "ssn": PatternRule(
            pattern_name="ssn",
            strategy="mask",
            confidence_min=0.9,
            compliance_tags=["HIPAA"],
        )
    }


def _make_config(version: str = "1.0") -> RedactionConfig:
    """Construct a valid :class:`RedactionConfig` for manager-level tests.

    The ``version`` knob lets writer threads in the concurrency test
    produce distinguishable configs without going through JSON.
    """
    return RedactionConfig(version=version, rules=_minimal_rules())


# ---------------------------------------------------------------------------
# TestModels — schema validation surface
# ---------------------------------------------------------------------------

class TestModels:
    """Pydantic-level invariants for ``PatternRule`` and ``RedactionConfig``."""

    def test_pattern_rule_constructs_with_valid_args(self) -> None:
        """Happy-path construction with every field explicitly set."""
        # Constructing with every field forces us to notice if a default
        # ever changes underneath us — the tests below cover defaults.
        rule = PatternRule(
            pattern_name="ssn",
            strategy="mask",
            confidence_min=0.95,
            compliance_tags=["HIPAA", "PCI_DSS"],
        )
        assert rule.pattern_name == "ssn"
        assert rule.strategy == "mask"
        assert rule.confidence_min == 0.95
        assert rule.compliance_tags == ["HIPAA", "PCI_DSS"]

    def test_pattern_rule_rejects_unknown_pattern_name(self) -> None:
        """``Literal``-narrowed ``pattern_name`` rejects unrecognised values."""
        # "unknown" is not in the seven-name Literal; pydantic raises
        # ValidationError with a clear ``literal_error`` type.
        with pytest.raises(ValidationError):
            PatternRule(pattern_name="unknown", strategy="mask")

    def test_pattern_rule_rejects_unknown_strategy(self) -> None:
        """``Literal``-narrowed ``strategy`` rejects unrecognised values."""
        with pytest.raises(ValidationError):
            # ``shred`` is not one of the four supported strategies.
            PatternRule(pattern_name="ssn", strategy="shred")

    def test_pattern_rule_rejects_extra_field(self) -> None:
        """``extra="forbid"`` catches operator typos at validation time."""
        # If an operator misspells a field name in JSON, we want a hard
        # error rather than silently dropping the value. ``extra_forbidden``
        # is the pydantic v2 error type.
        with pytest.raises(ValidationError):
            PatternRule(
                pattern_name="ssn",
                strategy="mask",
                # ``confidance_min`` (typo) instead of ``confidence_min``
                # — common kind of operator error this catches.
                confidance_min=0.9,
            )

    def test_pattern_rule_rejects_out_of_range_confidence(self) -> None:
        """``confidence_min`` must lie in [0, 1] inclusive."""
        # ``Field(ge=0, le=1)`` produces ``greater_than_equal`` /
        # ``less_than_equal`` validation errors for boundary violations.
        with pytest.raises(ValidationError):
            PatternRule(pattern_name="ssn", strategy="mask", confidence_min=1.5)
        with pytest.raises(ValidationError):
            PatternRule(pattern_name="ssn", strategy="mask", confidence_min=-0.1)

    def test_redaction_config_is_frozen(self) -> None:
        """Re-assignment of any attribute on a built config raises ValidationError.

        This is the property the manager relies on to hand out
        references that "no one else can mutate" — without freezing,
        a reader holding a snapshot after a reload would still see
        any concurrent in-place edits.
        """
        cfg = _make_config()
        # ``frozen=True`` causes attribute assignment to raise
        # ``ValidationError`` with a ``frozen_instance`` error type
        # in pydantic v2.
        with pytest.raises(ValidationError):
            cfg.version = "2.0"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# TestLoader — JSON-on-disk paths
# ---------------------------------------------------------------------------

class TestLoader:
    """File-system-backed loader correctness, including all three presets."""

    def test_load_config_file_loads_default_json(self) -> None:
        """``config/default.json`` parses and validates cleanly."""
        cfg = load_config_file(CONFIG_DIR / "default.json")
        # Spot-check the things the project-requirements lock in:
        assert cfg.version == "1.0"
        assert cfg.fields_to_redact == ["message", "user_data", "details"]
        assert cfg.audit_all_redactions is True
        # The default ships rules for ALL seven patterns; if any go
        # missing in future edits a regression bubbles up here.
        assert set(cfg.rules) == {
            "ssn", "credit_card", "email", "us_phone", "mrn", "person", "org",
        }

    def test_load_preset_default_loads_default_json(self) -> None:
        """``load_preset("default", ...)`` resolves to ``default.json``.

        This is the special-cased branch in the loader — every other
        name goes under ``presets/``.
        """
        cfg = load_preset("default", CONFIG_DIR)
        # Compare against the direct-load result to prove the alias
        # is identity-of-content (model_dump survives both paths).
        direct = load_config_file(CONFIG_DIR / "default.json")
        assert cfg.model_dump() == direct.model_dump()

    def test_load_preset_healthcare_uses_partial_for_mrn(self) -> None:
        """Healthcare preset must keep MRN tail visible for clinician workflows."""
        cfg = load_preset("healthcare", CONFIG_DIR)
        assert cfg.rules["mrn"].strategy == "partial"
        assert cfg.rules["ssn"].strategy == "partial"
        # The preset is HIPAA-only by design.
        assert cfg.active_compliance_sets == ["HIPAA"]

    def test_load_preset_financial_uses_hash_for_ssn(self) -> None:
        """Financial preset hashes SSNs (PCI tokenizable correlation key)."""
        cfg = load_preset("financial", CONFIG_DIR)
        assert cfg.rules["credit_card"].strategy == "mask"
        assert cfg.rules["ssn"].strategy == "hash"
        assert cfg.rules["email"].strategy == "tokenize"
        # Active sets cover PCI_DSS + GDPR per the project requirements.
        assert set(cfg.active_compliance_sets) == {"PCI_DSS", "GDPR"}

    def test_load_preset_general_uses_partial_for_pii(self) -> None:
        """General preset uses partial preservation for soft PII."""
        cfg = load_preset("general", CONFIG_DIR)
        assert cfg.rules["email"].strategy == "partial"
        assert cfg.rules["us_phone"].strategy == "partial"
        assert cfg.rules["person"].strategy == "tokenize"
        assert cfg.active_compliance_sets == ["GDPR"]

    def test_load_preset_unknown_raises_file_not_found(self) -> None:
        """Missing preset surfaces as :class:`FileNotFoundError` with path."""
        # The error message embeds the resolved path so operators can
        # paste it into ``ls`` without guessing. We assert on a
        # substring rather than the full path so the test stays portable.
        with pytest.raises(FileNotFoundError) as excinfo:
            load_preset("nonexistent", CONFIG_DIR)
        assert "nonexistent" in str(excinfo.value)

    def test_load_config_file_rejects_malformed_json(self, tmp_path: Path) -> None:
        """Malformed JSON surfaces as :class:`ValidationError`.

        Pydantic v2's ``model_validate_json`` wraps JSON-decode failures
        in ``ValidationError`` rather than re-raising ``JSONDecodeError``,
        so the API caller can handle one exception class for everything
        config-related.
        """
        bad = tmp_path / "bad.json"
        bad.write_text("{not valid json", encoding="utf-8")
        # We accept either ValidationError (pydantic v2's wrapper) or
        # the raw JSONDecodeError — both are correct in spirit; the
        # actual behavior depends on pydantic-internals we don't pin.
        with pytest.raises((ValidationError, json.JSONDecodeError)):
            load_config_file(bad)


# ---------------------------------------------------------------------------
# TestManager — atomic hot-reload semantics
# ---------------------------------------------------------------------------

class TestManager:
    """Atomic-swap, validation-failure, and concurrency invariants."""

    def test_get_returns_initial_config(self) -> None:
        """A fresh manager returns the config it was constructed with."""
        initial = _make_config(version="1.0")
        mgr = ConfigurationManager(initial)
        # Identity check — get() must NOT deep-copy because frozen models
        # already guarantee safe sharing.
        assert mgr.get() is initial

    def test_reload_swaps_active_config(self) -> None:
        """``reload`` makes the new config visible to subsequent ``get`` calls."""
        old = _make_config(version="1.0")
        mgr = ConfigurationManager(old)
        new = _make_config(version="2.0")
        mgr.reload(new)
        assert mgr.get() is new
        # Confirm we didn't accidentally accept aliasing — the OLD
        # reference must still be valid AND must still report its own
        # version (proves the swap was via rebind, not in-place edit).
        assert old.version == "1.0"

    def test_reload_from_json_valid_swaps(self) -> None:
        """Well-formed JSON parses, validates, and swaps under the lock."""
        mgr = ConfigurationManager(_make_config(version="1.0"))
        # Build the new-config JSON via ``model_dump_json`` so the test
        # stays robust to future field additions — we don't have to
        # update a string-literal payload every time the schema grows.
        new_cfg = _make_config(version="2.0")
        returned = mgr.reload_from_json(new_cfg.model_dump_json())
        assert returned.version == "2.0"
        assert mgr.get().version == "2.0"

    def test_reload_from_json_invalid_leaves_old_active(self) -> None:
        """Bad JSON raises ValidationError and the previous config stays active.

        This is THE property the API layer in C7 relies on to return
        4xx without taking the service down.
        """
        original = _make_config(version="1.0")
        mgr = ConfigurationManager(original)
        # Missing required ``rules`` field — pydantic raises before the
        # manager touches its internal reference.
        invalid_json = '{"version": "broken"}'
        with pytest.raises(ValidationError):
            mgr.reload_from_json(invalid_json)
        # Post-condition: the manager still hands out the original.
        assert mgr.get() is original
        assert mgr.get().version == "1.0"

    def test_concurrent_reads_and_writes_complete_quickly(self) -> None:
        """Soak test: 2 writers + 8 readers, alternating presets, < 5 s wall time.

        We rotate between the on-disk ``healthcare`` and ``financial``
        presets so each writer flip is a real reload (not a no-op),
        and we assert two things at the end:

        1. ``mgr.get()`` returns one of the two presets we cycled
           through — i.e., no torn-read produced a Frankenstein config.
        2. The whole thing finished well under 5 s — proxy for "no
           deadlock and no pathological lock contention".

        Each reader thread also pulls a snapshot mid-loop and asserts
        the snapshot's ``version`` matches one of the two known
        presets, which catches in-place mutation regressions if
        someone ever removes ``frozen=True`` by accident.
        """
        # Load the two presets we'll rotate between. Both are real
        # files on disk so the test exercises the full loader pipeline,
        # not just the in-memory manager.
        healthcare = load_preset("healthcare", CONFIG_DIR)
        financial = load_preset("financial", CONFIG_DIR)
        # Tag versions so we can assert "snapshot is one of the two".
        healthcare_tagged = healthcare.model_copy(update={"version": "hc"})
        financial_tagged = financial.model_copy(update={"version": "fin"})
        # Start at healthcare so the initial snapshot is well-known.
        mgr = ConfigurationManager(healthcare_tagged)

        writes_per_writer = 50
        reads_per_reader = 100
        valid_versions = {"hc", "fin"}
        # Track any reader-side invariant failures so we can re-raise
        # them on the main thread (assertions inside worker threads
        # would otherwise be swallowed).
        reader_errors: list[str] = []
        reader_lock = threading.Lock()

        def writer(start_with_hc: bool) -> None:
            """Alternate between the two presets ``writes_per_writer`` times."""
            # ``start_with_hc`` is just so the two writers offset by one
            # — increases the chance the readers see both versions
            # without needing artificial sleeps.
            cycle = [healthcare_tagged, financial_tagged]
            if not start_with_hc:
                cycle.reverse()
            for i in range(writes_per_writer):
                mgr.reload(cycle[i % 2])

        def reader() -> None:
            """Pull ``reads_per_reader`` snapshots and validate each one."""
            for _ in range(reads_per_reader):
                snap = mgr.get()
                if snap.version not in valid_versions:
                    # Record the failure with thread context — we'll
                    # surface it on the main thread after the join.
                    with reader_lock:
                        reader_errors.append(
                            f"unexpected version {snap.version!r}"
                        )
                    return

        threads: list[threading.Thread] = []
        for i in range(2):
            # Two writers, offset by start preset.
            threads.append(threading.Thread(target=writer, args=(i == 0,)))
        for _ in range(8):
            threads.append(threading.Thread(target=reader))

        start = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            # Generous join timeout — if we hit this we definitely
            # have a deadlock and the assertion below will fire.
            t.join(timeout=5.0)
        elapsed = time.monotonic() - start

        # No reader observed a torn / Frankenstein config.
        assert not reader_errors, f"reader invariant violations: {reader_errors}"
        # Final state is one of the two known presets — proves we
        # didn't lose track of the active reference.
        assert mgr.get().version in valid_versions
        # Wall time bound from the plan; on a CI box we expect this
        # to finish in well under a second.
        assert elapsed < 5.0, f"concurrency soak took {elapsed:.2f}s (>5s budget)"
        # Sanity: every thread actually finished.
        for t in threads:
            assert not t.is_alive(), "thread still running past join timeout"
