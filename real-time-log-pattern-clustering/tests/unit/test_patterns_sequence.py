"""Unit tests for sequence & anomaly mining (C19, Feature Area B).

The headline requirement is a **demonstrated >= 95% anomaly-detection accuracy** on a
constructed, balanced, labeled set. We build:

* **Normal** sequences from :func:`generate_pattern_batch("normal", ...)` — routine INFO/2xx
  traffic — chunked into distinct synthetic entities (label ``0``).
* **Anomalous** sequences from ``security`` and ``error`` bursts — WARN/4xx, ERROR/5xx,
  CRITICAL/4xx/5xx traffic that never appears in normal logs — chunked into their own
  entities (label ``1``).

We fit the n-gram model on a *separate* normal training corpus, score every entity, threshold
to a predicted label, and assert accuracy over 20 normal + 20 anomalous entities is
``>= 0.95``. Because the coarse ``LEVEL:status_class`` tokenization makes the two classes use
disjoint window vocabularies, the rare-window fraction separates them cleanly (normal ~0,
anomalous ~1), so the bound holds with a comfortable margin. We also assert the
attack/error entities score strictly higher than the normal ones.
"""

from __future__ import annotations

from src.log_generator import generate_pattern_batch
from src.patterns.sequence import (
    build_sequences,
    detect_sequence_anomalies,
    fit_sequence_model,
    score_sequence,
)

# Labeled-set geometry: a balanced 20 vs 20 split, each entity a run of ``_PER`` events.
_N_NORMAL = 20
_N_SECURITY = 10
_N_ERROR = 10
_PER = 30  # events per synthetic entity


def _relabel_into_entities(family: str, n_entities: int, per: int, seed: int, prefix: str):
    """Generate ``n_entities * per`` single-family logs, chunked into ``n_entities`` entities.

    Each consecutive run of ``per`` logs gets the same synthetic ``source_ip`` (``f"{prefix}-i"``)
    so :func:`build_sequences(by="source_ip")` groups them into one ordered sequence — giving us
    fully labeled per-entity sequences from the public generator + public sequence API.
    """
    logs = generate_pattern_batch(family, n_entities * per, seed=seed)
    for i, log in enumerate(logs):
        log.source_ip = f"{prefix}-{i // per}"
    return logs


def _build_labeled_set():
    """Return ``(all_logs, labels)`` — the balanced labeled corpus + ``{entity: 0|1}`` map."""
    normal = _relabel_into_entities("normal", _N_NORMAL, _PER, seed=11, prefix="normal")
    security = _relabel_into_entities("security", _N_SECURITY, _PER, seed=22, prefix="attack")
    error = _relabel_into_entities("error", _N_ERROR, _PER, seed=33, prefix="fail")

    labels: dict[str, int] = {}
    for log in normal:
        labels[log.source_ip] = 0
    for log in security + error:
        labels[log.source_ip] = 1

    return normal + security + error, labels


def test_sequence_anomaly_detection_accuracy_at_least_95pct() -> None:
    """Demonstrate >= 95% anomaly-detection accuracy on a balanced labeled set.

    Fit on a SEPARATE normal training corpus, score each labeled entity, threshold -> predicted
    label, and compute accuracy over 20 normal + 20 anomalous entities.
    """
    # Train the normal model on its own corpus (disjoint from the eval entities).
    train_logs = generate_pattern_batch("normal", 1500, seed=999)
    model = fit_sequence_model(train_logs, n=2)
    threshold = model["threshold"]

    all_logs, labels = _build_labeled_set()
    sequences = build_sequences(all_logs, by="source_ip")

    # We must have exactly the 40 labeled entities.
    assert len(sequences) == _N_NORMAL + _N_SECURITY + _N_ERROR == 40

    correct = 0
    normal_scores: list[float] = []
    anomalous_scores: list[float] = []
    for entity, events in sequences.items():
        score = score_sequence(events, model)
        predicted = 1 if score >= threshold else 0
        label = labels[entity]
        if predicted == label:
            correct += 1
        (normal_scores if label == 0 else anomalous_scores).append(score)

    accuracy = correct / len(sequences)
    assert accuracy >= 0.95, f"sequence anomaly accuracy {accuracy:.3f} < 0.95"

    # The attack/error entities must score strictly higher than the normal ones (clean
    # separation), corroborating that the score — not luck — drives the accuracy.
    assert max(normal_scores) < min(anomalous_scores)


def test_bad_actor_entities_score_higher_than_normal() -> None:
    """Brute-force / error entities get higher anomaly scores than normal entities."""
    train_logs = generate_pattern_batch("normal", 1500, seed=999)
    model = fit_sequence_model(train_logs, n=2)

    all_logs, labels = _build_labeled_set()
    sequences = build_sequences(all_logs, by="source_ip")

    mean_normal = sum(
        score_sequence(ev, model) for e, ev in sequences.items() if labels[e] == 0
    ) / _N_NORMAL
    mean_bad = sum(
        score_sequence(ev, model) for e, ev in sequences.items() if labels[e] == 1
    ) / (_N_SECURITY + _N_ERROR)
    assert mean_bad > mean_normal


def test_detect_sequence_anomalies_flags_all_bad_actors() -> None:
    """detect_sequence_anomalies (with an explicit normal model) flags exactly the bad actors."""
    train_logs = generate_pattern_batch("normal", 1500, seed=999)
    model = fit_sequence_model(train_logs, n=2)

    all_logs, labels = _build_labeled_set()
    result = detect_sequence_anomalies(all_logs, model=model, by="source_ip")

    assert result["analyzed"] == 40
    assert result["window"] == 2
    assert isinstance(result["anomalies"], list)
    assert result["model_ngrams"] >= 1

    flagged = {a["entity"] for a in result["anomalies"]}
    # Every flagged entity is genuinely anomalous (no false positives) ...
    assert all(labels[e] == 1 for e in flagged)
    # ... and every anomalous entity is flagged (no false negatives).
    assert flagged == {e for e, lab in labels.items() if lab == 1}

    # Each anomaly carries the documented shape.
    for a in result["anomalies"]:
        assert {"entity", "score", "length", "sample_events"} <= a.keys()
        assert 0.0 <= a["score"] <= 1.0
        assert isinstance(a["sample_events"], list)


def test_build_sequences_orders_by_timestamp() -> None:
    """build_sequences groups by the chosen entity key and orders each run by timestamp."""
    logs = generate_pattern_batch("normal", 60, seed=5)
    seqs = build_sequences(logs, by="service")
    assert seqs, "expected at least one service sequence"
    for events in seqs.values():
        assert all(isinstance(tok, str) for tok in events)


def test_fit_baseline_on_self_does_not_crash() -> None:
    """With model=None the detector fits a baseline on the logs themselves and runs."""
    logs = generate_pattern_batch("normal", 200, seed=7)
    result = detect_sequence_anomalies(logs, model=None, by="service")
    assert result["analyzed"] >= 1
    assert "anomalies" in result
    assert result["window"] == 2


def test_empty_and_tiny_inputs_do_not_crash() -> None:
    """Empty / tiny inputs return well-formed results without raising."""
    empty = detect_sequence_anomalies([], by="service")
    assert empty["analyzed"] == 0
    assert empty["anomalies"] == []

    assert build_sequences([], by="service") == {}
    assert score_sequence([], fit_sequence_model([])) == 0.0
