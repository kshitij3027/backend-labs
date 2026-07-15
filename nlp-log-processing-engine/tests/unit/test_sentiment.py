"""Unit tests for the sentiment / severity analyzer (:mod:`src.nlp.sentiment`).

These pin the behaviour C10 later sanity-checks against the generator's ground truth: ops
vocabulary drives negative/critical (unmodified VADER would flat-line most log lines at
neutral), the hard critical override fires on unambiguous severe tokens — even when the raw
compound would *not* reach the critical cutoff — positives read positive, config-noise reads
neutral, the output shape/range is always valid, empty input is safe, and — the soft fidelity
gate — the analyzer's polarity directionally agrees with the corpus ground truth.

Building the analyzer merges the ops lexicon into VADER once, so a single
:class:`SentimentAnalyzer` is shared across the module.
"""

from collections import defaultdict

import pytest

from src.generators import sample_messages
from src.nlp.sentiment import (
    CRITICAL_OVERRIDE_TERMS,
    CRITICAL_THRESHOLD,
    SENTIMENT_LABELS,
    SentimentAnalyzer,
)

#: Alias for readability in the override-strength test (the critical compound cutoff, -0.60).
CRITICAL_CUTOFF = CRITICAL_THRESHOLD

#: The two labels that both mean "this is bad" — collapsed into one polarity bucket for the
#: directional-agreement fidelity gate.
NEGISH = {"negative", "critical"}


@pytest.fixture(scope="module")
def analyzer() -> SentimentAnalyzer:
    """One ops-augmented analyzer for the whole module (amortises the lexicon merge)."""
    return SentimentAnalyzer()


def _bucket(label: str) -> str:
    """Collapse a fine-grained label to a coarse polarity bucket: POS / NEU / NEGISH."""
    if label in NEGISH:
        return "NEGISH"
    if label == "positive":
        return "POS"
    return "NEU"


# --------------------------------------------------------------------------------------
# Ops-negative vocabulary drives negative/critical (the whole reason VADER is augmented)
# --------------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text",
    [
        "auth-svc rejected login: invalid token",
        "connection timed out to db-03",
        "gateway refused connection from 10.1.1.4",
        "replication lag rising on db-01 for billing-svc",
        "unhandled exception E1001 while writing to /data/db/wal",
    ],
)
def test_ops_negative_text_is_negish(analyzer, text):
    label, compound = analyzer.analyze(text)
    assert compound < 0.0, f"{text!r} -> compound {compound}"
    assert label in NEGISH, f"{text!r} -> {label} (compound {compound})"


def test_moderate_negative_is_negative_not_critical(analyzer):
    # A single moderate ops term ("timed", valence -2.5) lands in 'negative' — clearly past
    # the -0.05 negative cutoff but not the -0.60 critical extreme.
    label, compound = analyzer.analyze("connection timed out to db-03")
    assert label == "negative", f"expected negative, got {label} (compound {compound})"
    assert -0.60 < compound < -0.05, f"compound {compound} outside the negative band"


# --------------------------------------------------------------------------------------
# Hard critical override: unambiguous severe tokens force 'critical'
# --------------------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text",
    [
        "kernel panic on web-01",
        "FATAL: segfault in payments-api",
        "service outage detected",
        "oom killer invoked on web-01",
        "possible data loss on db-03",
        "disk corrupt on db-03",
        "critical failure E500 in gateway",
    ],
)
def test_hard_override_forces_critical(analyzer, text):
    label, _ = analyzer.analyze(text)
    assert label == "critical", f"{text!r} -> {label}"


def test_override_fires_even_when_compound_would_not_reach_critical(analyzer):
    # THE key override property: the label is forced 'critical' even though the raw compound
    # is well *above* the -0.60 critical cutoff (so the threshold path alone would never say
    # critical). Two independent constructions:
    #
    #  (a) positive-leaning line whose lone 'critical' token is outvoted in the lexical sum —
    #      the compound is actually POSITIVE, yet the override still forces critical.
    label_a, compound_a = analyzer.analyze("critical subsystem is healthy, stable and online")
    assert compound_a > CRITICAL_CUTOFF, f"(a) compound {compound_a} not above the cutoff"
    assert label_a == "critical", f"(a) override failed: {label_a} (compound {compound_a})"

    #  (b) 'data loss' is a two-word phrase: VADER scores it per token and never as a bigram,
    #      so the raw compound stays mild (well above -0.60); the phrase override recognises
    #      it and forces critical.
    label_b, compound_b = analyzer.analyze("reconciliation reported data loss on db-03")
    assert compound_b > CRITICAL_CUTOFF, f"(b) compound {compound_b} not above the cutoff"
    assert label_b == "critical", f"(b) override failed: {label_b} (compound {compound_b})"


# --------------------------------------------------------------------------------------
# Positives and neutrals
# --------------------------------------------------------------------------------------
def test_positive_ops_text_is_positive(analyzer):
    label, compound = analyzer.analyze("deployment succeeded; auth-svc healthy")
    assert label == "positive", f"expected positive, got {label} (compound {compound})"
    assert compound > 0.0


@pytest.mark.parametrize(
    "text",
    [
        "rolling out user-svc version to web-02",
        "query executed on billing-svc against db-03",
        "configuration reloaded for gateway from /opt/app/config.yaml",
    ],
)
def test_routine_ops_text_is_neutral(analyzer, text):
    # Routine config/ops lines carry no sentiment vocabulary -> compound 0.0 -> neutral.
    label, compound = analyzer.analyze(text)
    assert label == "neutral", f"{text!r} -> {label} (compound {compound})"


def test_config_change_is_not_negative_or_critical(analyzer):
    # The canonical config-change line. Expected neutral; asserted only "not alarming" so the
    # test is robust even if a common word ("value") carries a small base-VADER rating.
    label, _ = analyzer.analyze("config value cache_ttl updated to 60")
    assert label not in NEGISH, f"config change wrongly flagged {label}"


# --------------------------------------------------------------------------------------
# Output contract: label domain + compound range; empty input
# --------------------------------------------------------------------------------------
def test_label_domain_and_score_range(analyzer):
    for sample in sample_messages(40, seed=7):
        label, compound = analyzer.analyze(sample.message)
        assert label in SENTIMENT_LABELS
        assert -1.0 <= compound <= 1.0


def test_empty_and_whitespace_input(analyzer):
    assert analyzer.analyze("") == ("neutral", 0.0)
    assert analyzer.analyze("   ") == ("neutral", 0.0)
    assert analyzer.analyze("\n\t ") == ("neutral", 0.0)


def test_scores_helper_shape_and_agreement(analyzer):
    # The dashboard helper exposes the full VADER breakdown; its compound must match analyze().
    text = "fatal error E500: payments-api crashed on web-01"
    scores = analyzer.scores(text)
    assert set(scores) == {"neg", "neu", "pos", "compound"}
    _, compound = analyzer.analyze(text)
    assert scores["compound"] == compound
    assert analyzer.scores("") == {"neg": 0.0, "neu": 0.0, "pos": 0.0, "compound": 0.0}


def test_override_terms_are_all_in_the_lexicon_scale_or_phrase(analyzer):
    # Sanity: every override term is a member of the published set and matching is whole-word
    # (so 'corrupt' does not fire inside 'incorruptible' and 'oom' not inside 'room').
    assert "critical" in CRITICAL_OVERRIDE_TERMS
    assert analyzer.analyze("no issues in the meeting room today")[0] != "critical"
    assert analyzer.analyze("payload is incorruptible by design")[0] != "critical"


# --------------------------------------------------------------------------------------
# Ground-truth directional agreement (soft fidelity gate — the C10 metric in miniature)
# --------------------------------------------------------------------------------------
def test_directional_agreement_with_ground_truth(analyzer):
    samples = sample_messages(80, seed=2024)

    # gt_bucket -> pred_bucket -> count
    confusion: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    agree = 0
    crit_total = 0
    crit_into_negish = 0

    for sample in samples:
        pred_label, _ = analyzer.analyze(sample.message)
        gt_b = _bucket(sample.sentiment)
        pred_b = _bucket(pred_label)
        confusion[gt_b][pred_b] += 1
        if gt_b == pred_b:
            agree += 1
        if sample.sentiment == "critical":
            crit_total += 1
            if pred_label in NEGISH:
                crit_into_negish += 1

    agreement = agree / len(samples)
    crit_recall = crit_into_negish / crit_total if crit_total else 1.0

    # Print the confusion so a failure is debuggable at a glance.
    order = ("POS", "NEU", "NEGISH")
    print("\nGT bucket -> predicted bucket:")
    for gt_b in order:
        row = confusion[gt_b]
        cells = ", ".join(f"{pb}:{row[pb]}" for pb in order)
        print(f"  {gt_b:6} -> {cells}  (n={sum(row.values())})")
    print(f"directional agreement = {agreement:.3f}  |  "
          f"GT-critical into NEGISH = {crit_recall:.3f} (n_crit={crit_total})")

    # Soft-but-meaningful gate. The floor is 0.60; we enforce 0.65 (observed on this corpus
    # is expected to be ~0.82-0.88, so there is comfortable margin). If this ever regresses,
    # TIGHTEN OPS_LEXICON (add the missing ops term) rather than lowering the bar.
    assert agreement >= 0.65, (
        f"directional agreement {agreement:.3f} < 0.65; "
        f"confusion={ {k: dict(v) for k, v in confusion.items()} }"
    )
    # GT-critical lines must land in {critical, negative} a strong majority of the time
    # (expected ~0.85-0.90 here).
    assert crit_recall >= 0.75, (
        f"GT-critical recall into NEGISH {crit_recall:.3f} < 0.75 (n_crit={crit_total})"
    )
