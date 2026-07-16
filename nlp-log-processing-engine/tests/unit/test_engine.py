"""Unit tests for the C7 orchestrator (:class:`src.nlp.NLPEngine`).

These pin the wiring the API relies on: :meth:`~src.nlp.NLPEngine.load` builds all four
analyzers once, :meth:`~src.nlp.NLPEngine.analyze` emits the full response schema with in-range
scores, :meth:`~src.nlp.NLPEngine.analyze_batch` is per-item-identical and order-preserving
(and empty-safe), and an unloaded engine refuses to analyze.

Loading the engine (spaCy + the intent pipeline + VADER + YAKE) is expensive, so a single
loaded :class:`NLPEngine` is built once per module and shared by every test.
"""

import pytest

from src.generators import INTENTS
from src.nlp import NLPEngine
from src.nlp.intent import OTHER_LABEL
from src.nlp.sentiment import SENTIMENT_LABELS

#: Every label the intent field may legitimately carry — a real intent or the reject bucket.
VALID_INTENTS = set(INTENTS) | {OTHER_LABEL}

#: A crafted line that exercises several log-entity labels plus a clear negative severity.
CRAFTED = "auth-svc rejected login for user 4821 from 10.0.0.1: invalid token"


@pytest.fixture(scope="module")
def engine() -> NLPEngine:
    """One fully-loaded engine for the whole module (amortises the model load)."""
    return NLPEngine().load()


def _assert_schema(result: dict, expected_message: str) -> None:
    """Assert ``result`` is a full, well-typed, in-range analysis of ``expected_message``."""
    assert set(result) == {"message", "entities", "intent", "sentiment", "keywords"}
    assert result["message"] == expected_message

    assert isinstance(result["entities"], list)
    for entity in result["entities"]:
        assert isinstance(entity["text"], str)
        assert isinstance(entity["label"], str)

    intent = result["intent"]
    assert isinstance(intent["label"], str)
    assert intent["label"] in VALID_INTENTS
    assert isinstance(intent["confidence"], float)
    assert 0.0 <= intent["confidence"] <= 1.0

    sentiment = result["sentiment"]
    assert sentiment["label"] in SENTIMENT_LABELS
    assert isinstance(sentiment["score"], float)
    assert -1.0 <= sentiment["score"] <= 1.0

    assert isinstance(result["keywords"], list)
    assert all(isinstance(keyword, str) for keyword in result["keywords"])


# --------------------------------------------------------------------------------------
# load()
# --------------------------------------------------------------------------------------
def test_load_sets_ready_and_is_idempotent(engine):
    assert engine.ready is True
    # Re-loading an already-loaded engine is a no-op that returns the same instance and does
    # not rebuild the analyzers (identity is preserved).
    entity_before = engine._entity
    assert engine.load() is engine
    assert engine.ready is True
    assert engine._entity is entity_before


# --------------------------------------------------------------------------------------
# analyze()
# --------------------------------------------------------------------------------------
def test_analyze_returns_full_schema(engine):
    _assert_schema(engine.analyze(CRAFTED), CRAFTED)


def test_analyze_extracts_expected_entities_and_signals(engine):
    result = engine.analyze(CRAFTED)
    labels = {entity["label"] for entity in result["entities"]}
    # A SERVICE (auth-svc) and a user/host network identifier (USER_ID 4821 / IP 10.0.0.1).
    assert "SERVICE" in labels
    assert labels & {"USER_ID", "IP"}
    # Intent is a valid label, severity is one of the four classes, and there are keywords.
    assert result["intent"]["label"] in VALID_INTENTS
    assert result["sentiment"]["label"] in SENTIMENT_LABELS
    assert result["keywords"]


def test_analyze_handles_empty_string(engine):
    result = engine.analyze("")
    _assert_schema(result, "")
    assert result["entities"] == []
    assert result["keywords"] == []
    assert result["intent"] == {"label": OTHER_LABEL, "confidence": 0.0}
    assert result["sentiment"] == {"label": "neutral", "score": 0.0}


# --------------------------------------------------------------------------------------
# analyze_batch()
# --------------------------------------------------------------------------------------
def test_analyze_batch_matches_per_item_and_preserves_order(engine):
    messages = [
        CRAFTED,
        "deployment of payments-api succeeded on gateway",
        "health check passed on user-svc",
    ]
    batch = engine.analyze_batch(messages)

    assert len(batch) == len(messages)
    # Order preserved and each batch element is byte-for-byte the per-item analyze() result
    # (the batched nlp.pipe / predict_proba paths are deterministic and equal to the singles).
    assert [item["message"] for item in batch] == messages
    for message, item in zip(messages, batch):
        _assert_schema(item, message)
        assert item == engine.analyze(message)


def test_analyze_batch_empty_list_returns_empty(engine):
    assert engine.analyze_batch([]) == []


def test_analyze_batch_handles_empty_string_item(engine):
    batch = engine.analyze_batch(["", CRAFTED])
    assert len(batch) == 2
    _assert_schema(batch[0], "")
    _assert_schema(batch[1], CRAFTED)
    assert batch[0]["entities"] == []
    assert batch[0]["keywords"] == []


# --------------------------------------------------------------------------------------
# readiness guard
# --------------------------------------------------------------------------------------
def test_analyze_before_load_raises():
    with pytest.raises(RuntimeError):
        NLPEngine().analyze("anything")


def test_analyze_batch_before_load_raises():
    with pytest.raises(RuntimeError):
        NLPEngine().analyze_batch(["anything"])
