"""Build-time trainer for the intent classifier — bakes the pipeline into the image.

Run as ``python -m scripts.train_intent`` (the Dockerfiles invoke it during ``docker build``
with ``PYTHONPATH=/app``, so ``src.*`` imports resolve). It:

1. Builds a balanced, seeded corpus with :func:`~src.generators.produce_corpus`.
2. Holds out a stratified split, trains an :class:`~src.nlp.intent.IntentAnalyzer` on the
   rest, and prints the held-out accuracy + a per-class report.
3. **Gates**: exits non-zero if held-out accuracy is below :data:`ACCURACY_FLOOR`, so a
   broken model can never silently bake into the image.
4. Refits on the *full* corpus (use every labeled row for the shipped model) and
   ``save()``s the whole pipeline to :data:`~src.nlp.intent.DEFAULT_ARTIFACT_PATH`.

Everything is deterministic (fixed corpus seed + fixed ``train_test_split`` ``random_state``),
so the same image build always produces the same artifact and the same reported metrics.
"""

from __future__ import annotations

import sys

from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split

from src.generators import produce_corpus
from src.nlp.intent import DEFAULT_ARTIFACT_PATH, IntentAnalyzer

#: Corpus size per intent for training (balanced -> total = N_PER_INTENT * len(INTENTS)).
N_PER_INTENT: int = 200
#: Corpus + split seed. Fixed for a reproducible build-time artifact.
SEED: int = 42
#: Fraction held out (stratified) to score model quality before the artifact is written.
TEST_SIZE: float = 0.2
#: Sanity floor on held-out accuracy. Below this the build fails (non-zero exit) rather than
#: baking a broken model. The real number on this synthetic corpus runs well above it.
ACCURACY_FLOOR: float = 0.80


def main() -> int:
    """Train, gate on held-out accuracy, then refit-on-full and persist. Returns an exit code."""
    corpus = produce_corpus(n_per_intent=N_PER_INTENT, seed=SEED)
    texts = [s.message for s in corpus]
    labels = [s.intent for s in corpus]
    print(f"[train_intent] corpus: {len(texts)} lines across {len(set(labels))} intents")

    # (1) stratified held-out split -> validate raw model quality (no reject threshold here;
    # the gate is about whether the classifier discriminates the intents at all).
    x_train, x_test, y_train, y_test = train_test_split(
        texts, labels, test_size=TEST_SIZE, stratify=labels, random_state=SEED
    )
    validation = IntentAnalyzer().train((x_train, y_train))
    preds = list(validation.pipeline.predict(x_test))
    accuracy = accuracy_score(y_test, preds)

    print(f"[train_intent] held-out accuracy: {accuracy:.4f} on {len(y_test)} samples")
    print(classification_report(y_test, preds, zero_division=0))

    if accuracy < ACCURACY_FLOOR:
        print(
            f"[train_intent] ERROR: held-out accuracy {accuracy:.4f} below floor "
            f"{ACCURACY_FLOOR:.2f} — refusing to bake a broken model.",
            file=sys.stderr,
        )
        return 1

    # (2) refit on the FULL corpus for the shipped artifact, then persist the whole pipeline.
    final = IntentAnalyzer().train((texts, labels))
    final.save(DEFAULT_ARTIFACT_PATH)
    print(f"[train_intent] refit on full corpus ({len(texts)} lines); saved -> {DEFAULT_ARTIFACT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
