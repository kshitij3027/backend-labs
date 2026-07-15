"""Unit tests for the intent classifier (:mod:`src.nlp.intent`).

These pin the behaviour C7/C10 rely on: the model actually learns the intents (a real
held-out accuracy gate), ``predict`` has the right shape and range, the low-confidence
reject bucket (``"other"``) is wired to the threshold, the whole pipeline round-trips through
joblib, ``predict_batch`` matches per-item ``predict`` in order, and an untrained analyzer
refuses to predict.

Training on the synthetic corpus is fast but not free, so a single trained
:class:`IntentAnalyzer` is built once per module and shared by every test.
"""

import pytest

from src.generators import INTENTS, produce_corpus, sample_messages
from src.nlp.intent import DEFAULT_THRESHOLD, OTHER_LABEL, IntentAnalyzer


@pytest.fixture(scope="module")
def analyzer() -> IntentAnalyzer:
    """One analyzer trained once on the seed-42 corpus (amortises the fit)."""
    return IntentAnalyzer().train(produce_corpus(seed=42))


# --------------------------------------------------------------------------------------
# The model learns: held-out accuracy on a *different-seed* corpus (new slot fills / noise)
# --------------------------------------------------------------------------------------
def test_held_out_accuracy(analyzer):
    # Evaluate raw classification power (no reject bucket) via a zero-threshold view of the
    # same trained pipeline: it never falls back to "other", so this measures pure argmax
    # accuracy on genuinely unseen fills. seed=123 gives fresh IPs/hosts/users + noise.
    holdout = produce_corpus(seed=123)
    raw = IntentAnalyzer(pipeline=analyzer.pipeline, threshold=0.0)
    preds = [label for label, _ in raw.predict_batch([s.message for s in holdout])]
    truth = [s.intent for s in holdout]
    accuracy = sum(p == t for p, t in zip(preds, truth)) / len(truth)
    # Gate at 0.85 (a conservative, safe bar); the observed number on this distinctive
    # synthetic corpus is expected to be materially higher (~0.95+).
    assert accuracy >= 0.85, f"held-out intent accuracy {accuracy:.3f} below 0.85"


# --------------------------------------------------------------------------------------
# predict: shape, label domain, confidence range
# --------------------------------------------------------------------------------------
def test_predict_shape_and_range(analyzer):
    for sample in sample_messages(25, seed=7):
        label, confidence = analyzer.predict(sample.message)
        assert label in INTENTS + [OTHER_LABEL]
        assert 0.0 <= confidence <= 1.0


def test_predict_classifies_a_canonical_line(analyzer):
    # A crisp, in-distribution deployment line is confidently a real intent (not rejected).
    label, confidence = analyzer.predict(
        "deployment of payments-api to web-01 completed successfully"
    )
    assert label in INTENTS
    assert confidence >= DEFAULT_THRESHOLD


# --------------------------------------------------------------------------------------
# Low-confidence -> "other": both the OOD path and the explicit-threshold path
# --------------------------------------------------------------------------------------
def test_out_of_distribution_falls_back_to_other(analyzer):
    # Pure gibberish shares no vocabulary with the corpus -> ~uniform probabilities -> the
    # max is well under the threshold -> the reject bucket.
    label, confidence = analyzer.predict("qwerty zxcvb foobar lorem ipsum")
    assert label == OTHER_LABEL
    assert confidence < analyzer.threshold


def test_threshold_drives_the_reject_bucket(analyzer):
    line = "deployment of payments-api to web-01 completed successfully"
    normal_label, normal_conf = analyzer.predict(line)
    assert normal_label in INTENTS  # accepted at the default 0.45 threshold

    # Set the threshold *just above* this line's own confidence: the exact same underlying
    # max-proba now fails the (proba >= threshold) test, so the verdict flips to "other".
    # This proves the fallback is driven by threshold-vs-max-proba, with no dependence on the
    # model's absolute confidence (robust even if the model is ~1.0 confident on this line).
    strict = IntentAnalyzer(pipeline=analyzer.pipeline, threshold=normal_conf + 1e-6)
    strict_label, strict_conf = strict.predict(line)
    assert strict_label == OTHER_LABEL
    # Confidence is threshold-independent: the real max-proba is reported either way.
    assert strict_conf == normal_conf


def test_empty_input_is_other_zero(analyzer):
    assert analyzer.predict("") == (OTHER_LABEL, 0.0)
    assert analyzer.predict("   ") == (OTHER_LABEL, 0.0)


# --------------------------------------------------------------------------------------
# joblib round-trip: the whole pipeline persists and predicts identically after reload
# --------------------------------------------------------------------------------------
def test_joblib_round_trip(analyzer, tmp_path):
    path = tmp_path / "intent.joblib"
    analyzer.save(path)
    loaded = IntentAnalyzer.load(path)  # default threshold matches the fixture's (0.45)
    for sample in sample_messages(12, seed=21):
        assert loaded.predict(sample.message) == analyzer.predict(sample.message)


# --------------------------------------------------------------------------------------
# predict_batch matches per-item predict and preserves order (incl. empties)
# --------------------------------------------------------------------------------------
def test_predict_batch_matches_predict_and_preserves_order(analyzer):
    messages = [s.message for s in sample_messages(20, seed=4)]
    # Interleave empty / whitespace items to exercise the empty-safe reassembly.
    messages = messages[:6] + ["", "   "] + messages[6:]
    batch = analyzer.predict_batch(messages)
    assert len(batch) == len(messages)
    assert batch == [analyzer.predict(m) for m in messages]


def test_predict_batch_empty_list(analyzer):
    assert analyzer.predict_batch([]) == []


# --------------------------------------------------------------------------------------
# Not-trained guard: a fresh analyzer refuses to predict with a clear error
# --------------------------------------------------------------------------------------
def test_untrained_analyzer_raises():
    fresh = IntentAnalyzer()
    with pytest.raises(RuntimeError):
        fresh.predict("deployment of payments-api to web-01 completed successfully")
    with pytest.raises(RuntimeError):
        fresh.predict_batch(["anything at all"])
    with pytest.raises(RuntimeError):
        fresh.save()


# --------------------------------------------------------------------------------------
# train accepts both input forms: iterable[LogSample] and (texts, labels)
# --------------------------------------------------------------------------------------
def test_train_accepts_texts_labels_tuple():
    corpus = produce_corpus(n_per_intent=40, seed=1)
    texts = [s.message for s in corpus]
    labels = [s.intent for s in corpus]
    analyzer = IntentAnalyzer().train((texts, labels))
    label, confidence = analyzer.predict("health check passed for auth-svc on web-01")
    assert label in INTENTS + [OTHER_LABEL]
    assert 0.0 <= confidence <= 1.0
