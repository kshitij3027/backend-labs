"""Unit tests for the hierarchical multi-service classifier (Commit 11).

Exercises :class:`src.multiservice.MultiServiceClassifier` directly (no HTTP):

* the canonical 8-key result shape (service / severity / category, each with its
  own confidence, plus the overall confidence and the cross-service anomaly score),
* every numeric field staying a ``float`` in ``[0, 1]`` across varied inputs,
* the hierarchy routing (predicted service must be one the model knows, with a
  per-service severity model behind it),
* mixed ``str`` / ``dict`` batch inputs agreeing with single-record classify,
* service-flavoured logs (database / cache) returning valid, deterministic labels,
* a save/load round-trip reproducing the canonical prediction, and
* the before-fit / missing-artifact error contract.

A single module-scoped, *tiny-estimator* model fit on a modest corpus keeps the
whole file fast while still covering the real (fitted) behaviour.
"""

from __future__ import annotations

import pytest

from src.config import Settings
from src.log_generator import CATEGORIES, SERVICES, SEVERITIES, generate_logs
from src.multiservice import MultiServiceClassifier

# The spec's canonical input (also used by the base-classifier tests). It is a
# SYSTEM/ERROR connection-timeout log produced by the ``database`` service.
CANONICAL_LOG = "Database connection failed with timeout error"

# Exactly the keys ``MultiServiceClassifier.classify`` is contracted to emit.
RESULT_KEYS = {
    "service",
    "service_confidence",
    "severity",
    "severity_confidence",
    "category",
    "category_confidence",
    "confidence",
    "anomaly_score",
}

# The confidence/score fields that must always be floats in [0, 1].
NUMERIC_KEYS = (
    "service_confidence",
    "severity_confidence",
    "category_confidence",
    "confidence",
    "anomaly_score",
)


@pytest.fixture(scope="module")
def model() -> MultiServiceClassifier:
    """A fitted, tiny-estimator classifier shared across this module's tests.

    Four trees per forest/boosting model on a 200-record corpus (seed 42) trains
    in well under a second yet produces a real multi-class hierarchy for every
    service, so the assertions exercise genuine fitted behaviour.
    """
    cfg = Settings(rf_n_estimators=4, gb_n_estimators=4)
    return MultiServiceClassifier(cfg).fit(generate_logs(200, 42))


def _assert_valid_result(result: dict) -> None:
    """Assert ``result`` has exactly the 8 keys with valid label/numeric types."""
    assert set(result) == RESULT_KEYS, f"unexpected keys: {sorted(result)}"
    assert result["service"] in SERVICES, f"bad service: {result['service']!r}"
    assert result["severity"] in SEVERITIES, f"bad severity: {result['severity']!r}"
    assert result["category"] in CATEGORIES, f"bad category: {result['category']!r}"
    for key in NUMERIC_KEYS:
        value = result[key]
        assert isinstance(value, float), f"{key} is {type(value).__name__}, not float"
        assert 0.0 <= value <= 1.0, f"{key}={value} out of [0, 1]"


# --------------------------------------------------------------------------- #
# 1) canonical shape + label/numeric validity
# --------------------------------------------------------------------------- #


def test_classify_canonical_shape(model):
    """The canonical input yields exactly the 8 keys with valid labels + floats."""
    result = model.classify(CANONICAL_LOG)
    _assert_valid_result(result)


# --------------------------------------------------------------------------- #
# 2) anomaly_score (and every score) stays in [0, 1] across varied inputs
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "raw_log",
    [
        CANONICAL_LOG,
        "SELECT * FROM users; slow query took 5000ms",
        "redis cache eviction key=session:abc memory pressure high",
        "GET /api/v1/users 200 OK in 12ms",
        "Worker process crashed unexpectedly and is being restarted pid=42",
        "User login successful user_id=7 from 10.0.0.1",
        "",  # near-empty input must still produce a valid, bounded result
    ],
)
def test_scores_bounded_across_inputs(model, raw_log):
    """anomaly_score and every confidence are floats in [0, 1] for any input."""
    result = model.classify(raw_log)
    _assert_valid_result(result)
    assert 0.0 <= result["anomaly_score"] <= 1.0


# --------------------------------------------------------------------------- #
# 3) hierarchical routing: predicted service is known + has a severity model
# --------------------------------------------------------------------------- #


def test_routing_through_per_service_severity_model(model):
    """The chosen service is one the model knows and routes to its severity model."""
    result = model.classify(CANONICAL_LOG)
    service = result["service"]
    # The service the hierarchy emitted must be a real, known service ...
    assert service in model.services_
    # ... and there must be a per-service severity model behind it (the routing).
    assert service in model.severity_by_service
    assert model.severity_by_service[service] is not None
    # The metadata exposes that service's emittable severity classes.
    assert service in model.severity_classes_by_service_
    assert model.severity_classes_by_service_[service]


# --------------------------------------------------------------------------- #
# 4) classify_batch accepts mixed str/dict and agrees with classify
# --------------------------------------------------------------------------- #


def test_classify_batch_mixed_inputs(model):
    """A mixed str/dict batch returns 8-key dicts agreeing with single classify."""
    inputs = [CANONICAL_LOG, {"raw_log": "Slow query detected took 800ms rows=10"}]
    results = model.classify_batch(inputs)

    assert isinstance(results, list) and len(results) == 2
    for result in results:
        _assert_valid_result(result)

    # A single-element batch must agree with classify() on the same input.
    single = model.classify(CANONICAL_LOG)
    batched = model.classify_batch([CANONICAL_LOG])[0]
    assert batched["service"] == single["service"]
    assert batched["severity"] == single["severity"]
    assert batched["category"] == single["category"]


def test_classify_batch_empty_returns_empty(model):
    """An empty batch yields an empty list (no error)."""
    assert model.classify_batch([]) == []


# --------------------------------------------------------------------------- #
# 5) service-flavoured logs return valid + deterministic labels
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "raw_log",
    [
        "SELECT * FROM users; slow query took 5000ms rows=200",
        "redis cache eviction key=session:abc123 evicting keys memory pressure",
    ],
)
def test_service_flavoured_logs_valid_and_deterministic(model, raw_log):
    """DB- and cache-flavoured logs route to valid labels, deterministically.

    We deliberately do not pin the exact predicted service (a tiny model can be
    noisy); we assert the result is well-formed and that two calls on the same
    input agree exactly (the classifier is deterministic).
    """
    first = model.classify(raw_log)
    second = model.classify(raw_log)
    _assert_valid_result(first)
    assert first == second, "classify must be deterministic for a fixed input"


# --------------------------------------------------------------------------- #
# 6) save / load round-trip reproduces the canonical prediction
# --------------------------------------------------------------------------- #


def test_save_load_round_trip(model, tmp_path):
    """A reloaded model gives identical service/severity/category for the canonical input."""
    target = tmp_path / "ms_model"
    model.save(str(target))

    reloaded = MultiServiceClassifier.load(str(target))
    assert reloaded.is_fitted
    assert reloaded.services_ == model.services_

    before = model.classify(CANONICAL_LOG)
    after = reloaded.classify(CANONICAL_LOG)
    assert after["service"] == before["service"]
    assert after["severity"] == before["severity"]
    assert after["category"] == before["category"]
    # The reloaded result is still a fully valid 8-key dict.
    _assert_valid_result(after)


# --------------------------------------------------------------------------- #
# 7) error contract: before-fit RuntimeError + missing-artifact FileNotFoundError
# --------------------------------------------------------------------------- #


def test_classify_before_fit_raises_runtime_error():
    """Calling classify before fit/load raises a clear RuntimeError."""
    fresh = MultiServiceClassifier(Settings(rf_n_estimators=4, gb_n_estimators=4))
    with pytest.raises(RuntimeError):
        fresh.classify(CANONICAL_LOG)
    with pytest.raises(RuntimeError):
        fresh.classify_batch([CANONICAL_LOG])


def test_load_nonexistent_dir_raises_file_not_found():
    """Loading from a directory with no artifacts raises FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        MultiServiceClassifier.load("/nonexistent/multiservice/path")


def test_fit_empty_records_raises_value_error():
    """Fitting on an empty corpus raises ValueError."""
    fresh = MultiServiceClassifier(Settings(rf_n_estimators=4, gb_n_estimators=4))
    with pytest.raises(ValueError):
        fresh.fit([])


# --------------------------------------------------------------------------- #
# 8) anomaly sanity: a garbled log is at least as anomalous as a clean one
# --------------------------------------------------------------------------- #


def test_garbled_log_not_less_anomalous_than_clean(model):
    """A clearly in-distribution log and a garbled one both score in [0, 1].

    Leniently assert the garbled/unfamiliar input is *not less* anomalous than a
    crisp, on-distribution canonical log. Equality is allowed (a tiny model may
    saturate the term), so this stays robust while still checking the signal's
    direction.
    """
    clean = model.classify(CANONICAL_LOG)["anomaly_score"]
    garbled = model.classify("zzzz qwerty !!! ??? \x00 lorem ipsum 9f8a")["anomaly_score"]
    assert 0.0 <= clean <= 1.0
    assert 0.0 <= garbled <= 1.0
    assert garbled >= clean - 1e-9, (
        f"garbled anomaly {garbled} should be >= clean anomaly {clean}"
    )
