"""Unit tests for the synthetic labeled log corpus generator (:mod:`src.generators`).

The corpus is the project's ground truth, so these tests pin the properties every
downstream consumer (C4 intent training, C10 E2E accuracy gates, NLP unit tests) relies
on: determinism, class balance, label validity, entity-span integrity, per-template
sentiment diversity, and realism-noise variance.
"""

from collections import Counter

from src.generators import (
    ENTITY_LABELS,
    INTENTS,
    SENTIMENTS,
    LogSample,
    produce_corpus,
    sample_messages,
)


def test_produce_corpus_is_deterministic_for_a_seed():
    # Same seed -> byte-identical corpus (frozen dataclass equality compares every field,
    # including the entities tuple). Reproducible training / stable assertions depend on it.
    assert produce_corpus(n_per_intent=10, seed=42) == produce_corpus(n_per_intent=10, seed=42)


def test_produce_corpus_varies_with_seed():
    a = produce_corpus(n_per_intent=25, seed=42)
    b = produce_corpus(n_per_intent=25, seed=7)
    # Structure (which intent/sentiment in which position) is seed-independent...
    assert [s.intent for s in a] == [s.intent for s in b]
    assert [s.sentiment for s in a] == [s.sentiment for s in b]
    # ...but the seeded slot fills + noise make the actual messages differ.
    assert [s.message for s in a] != [s.message for s in b]


def test_corpus_is_balanced_across_all_intents():
    corpus = produce_corpus(n_per_intent=20, seed=42)
    counts = Counter(s.intent for s in corpus)
    assert set(counts) == set(INTENTS)              # every intent present, nothing extra
    assert all(counts[intent] == 20 for intent in INTENTS)  # exactly n_per_intent each
    assert len(corpus) == 20 * len(INTENTS)


def test_labels_are_from_the_published_vocabularies():
    for s in produce_corpus(n_per_intent=10, seed=1):
        assert s.intent in INTENTS
        assert s.sentiment in SENTIMENTS
        assert all(label in ENTITY_LABELS for _, label in s.entities)


def test_every_sample_has_at_least_one_entity_that_is_an_exact_substring():
    # NER recall (C10) is scored against these surfaces, so each must be findable verbatim.
    for s in produce_corpus(n_per_intent=15, seed=3):
        assert len(s.entities) >= 1
        for surface, _label in s.entities:
            assert surface in s.message


def test_noise_never_mutates_entity_spans():
    # Directly exercises the core invariant: on samples that received the timestamp/level
    # prefix (visible realism noise), every recorded entity surface is still an exact
    # substring of the (now noisier) message.
    corpus = produce_corpus(n_per_intent=40, seed=11)
    prefixed = [s for s in corpus if s.message.startswith("2026-07-1")]
    assert prefixed, "expected some samples to receive the timestamp prefix"
    for s in prefixed:
        for surface, _label in s.entities:
            assert surface in s.message


def test_sentiment_is_per_template_not_per_intent():
    corpus = produce_corpus(n_per_intent=20, seed=42)
    # More than one sentiment across the corpus overall...
    assert len({s.sentiment for s in corpus}) > 1
    # ...and a single intent yields multiple sentiments (proves per-template assignment):
    # authentication carries both a success and a failure template.
    auth_sentiments = {s.sentiment for s in corpus if s.intent == "authentication"}
    assert len(auth_sentiments) > 1
    assert {"positive", "negative"} <= auth_sentiments


def test_realism_noise_creates_message_variance():
    corpus = produce_corpus(n_per_intent=50, seed=42)
    # A high-volume intent's lines are not all byte-identical (slot fills + noise vary).
    err_messages = [s.message for s in corpus if s.intent == "error_report"]
    assert len(set(err_messages)) > 1
    # And the timestamp/level prefix perturbation demonstrably fires somewhere.
    assert any(s.message.startswith("2026-07-1") for s in corpus)


def test_sample_messages_returns_exactly_n_valid_samples():
    out = sample_messages(30, seed=3)
    assert len(out) == 30
    for s in out:
        assert isinstance(s, LogSample)
        assert s.intent in INTENTS
        assert s.sentiment in SENTIMENTS
        assert len(s.entities) >= 1
        for surface, label in s.entities:
            assert label in ENTITY_LABELS
            assert surface in s.message


def test_sample_messages_is_deterministic_for_a_seed():
    assert sample_messages(15, seed=5) == sample_messages(15, seed=5)


def test_sample_messages_mixes_intents():
    # Uniform draw over the full template table -> a spread of intents, not a single one.
    out = sample_messages(50, seed=9)
    assert len({s.intent for s in out}) > 1
