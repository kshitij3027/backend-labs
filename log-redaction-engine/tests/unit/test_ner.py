"""Unit tests for the C2 NER detector.

These tests are spaCy-free: every NLP call is intercepted via a MagicMock
injected into ``NERDetector._nlp`` so the suite runs in milliseconds and
doesn't depend on the ``en_core_web_sm`` model being present in the test
environment. The real model is exercised in the C7+ integration tests.

What we verify here is the **wrapping logic**: length gate, label filter
(PERSON / ORG only), confidence / source values, and the lazy-load contract.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.detection.ner import NERDetector
from src.detection.patterns import Detection


# ---------------------------------------------------------------------------
# Helpers — build mock spaCy docs/entities
# ---------------------------------------------------------------------------

def _mock_ent(label: str, text: str, start_char: int, end_char: int) -> MagicMock:
    """Build a stand-in for ``spacy.tokens.Span`` with just the fields we use."""
    ent = MagicMock()
    ent.label_ = label
    ent.text = text
    ent.start_char = start_char
    ent.end_char = end_char
    return ent


def _mock_doc(ents: list[MagicMock]) -> MagicMock:
    """Build a stand-in for ``spacy.tokens.Doc`` exposing ``.ents``."""
    doc = MagicMock()
    doc.ents = ents
    return doc


@pytest.fixture()
def stubbed_detector() -> NERDetector:
    """NERDetector with ``_nlp`` pre-populated so ``_load`` is a no-op.

    Tests then reassign ``detector._nlp = MagicMock(return_value=mock_doc)``
    per case to control which entities spaCy "finds".
    """
    detector = NERDetector()
    # Default: every call returns a doc with NO entities. Individual tests
    # override this when they need a richer mock.
    detector._nlp = MagicMock(return_value=_mock_doc([]))
    return detector


# ---------------------------------------------------------------------------
# Length-gate behaviour
# ---------------------------------------------------------------------------

def test_short_text_below_min_length_returns_empty(stubbed_detector: NERDetector) -> None:
    """Below the threshold we never even invoke spaCy — the gate is the win."""
    out = stubbed_detector.detect("short", min_length=40)
    assert out == []
    # And spaCy was NOT called, even though _nlp is stubbed and callable.
    stubbed_detector._nlp.assert_not_called()


# ---------------------------------------------------------------------------
# PERSON / ORG / GPE label handling
# ---------------------------------------------------------------------------

def test_person_entity_produces_lowercased_pattern_name(
    stubbed_detector: NERDetector,
) -> None:
    text = "a" * 100  # exceeds default min_length of 40
    stubbed_detector._nlp = MagicMock(
        return_value=_mock_doc([_mock_ent("PERSON", "Alice", 0, 5)])
    )

    out = stubbed_detector.detect(text, min_length=40)

    assert len(out) == 1
    det = out[0]
    assert isinstance(det, Detection)
    assert det.pattern_name == "person"
    assert det.value == "Alice"
    assert det.start == 0
    assert det.end == 5
    assert det.confidence == 0.85
    assert det.source == "ner"


def test_org_entity_produces_lowercased_pattern_name(
    stubbed_detector: NERDetector,
) -> None:
    text = "a" * 100
    stubbed_detector._nlp = MagicMock(
        return_value=_mock_doc([_mock_ent("ORG", "Acme Corp", 10, 19)])
    )

    out = stubbed_detector.detect(text, min_length=40)

    assert len(out) == 1
    assert out[0].pattern_name == "org"
    assert out[0].value == "Acme Corp"


def test_gpe_entity_is_ignored(stubbed_detector: NERDetector) -> None:
    """GPE (cities, countries) is intentionally excluded from PII policy."""
    text = "a" * 100
    stubbed_detector._nlp = MagicMock(
        return_value=_mock_doc(
            [
                _mock_ent("PERSON", "Bob", 0, 3),
                _mock_ent("GPE", "Berlin", 10, 16),  # must be skipped
                _mock_ent("ORG", "Globex", 20, 26),
            ]
        )
    )

    out = stubbed_detector.detect(text, min_length=40)

    # GPE is dropped; PERSON and ORG survive.
    kinds = {d.pattern_name for d in out}
    assert kinds == {"person", "org"}
    assert all(d.source == "ner" for d in out)


# ---------------------------------------------------------------------------
# Lazy-load contract
# ---------------------------------------------------------------------------

def test_lazy_load_not_triggered_before_first_call_above_threshold() -> None:
    """``_nlp`` stays None until the first detect() call clears the length gate.

    We deliberately do NOT use the ``stubbed_detector`` fixture here because
    that pre-populates ``_nlp``. We need a fresh instance to observe the
    real lazy-load gating.
    """
    detector = NERDetector()
    assert detector._nlp is None

    # Below-threshold call returns early without loading.
    out = detector.detect("short", min_length=40)
    assert out == []
    assert detector._nlp is None  # still not loaded — gate worked


def test_lazy_load_idempotent_when_nlp_already_present(
    stubbed_detector: NERDetector,
) -> None:
    """Calling ``_load`` a second time must not replace the cached pipeline."""
    sentinel = stubbed_detector._nlp
    stubbed_detector._load()
    assert stubbed_detector._nlp is sentinel
