"""Unit tests for runtime-config validation (:mod:`src.runtime_config`).

Pure validation only — these exercise the *local, pre-Redis* validation surface:

* :func:`src.runtime_config._coerce_and_validate` — per-key type coercion + range
  checks (the authoritative backstop the router maps to 422). No Redis needed.
* :func:`src.runtime_config.set_overrides`'s **empty-body** guard, which raises
  ``ValueError`` *before* any Redis write, so it too needs no live Redis.

The valid-partial-update path (which writes to Redis and returns the recomputed
effective config) is covered end-to-end in the integration suite against a real
Redis; here we stay strictly on the no-I/O validation logic.
"""

from __future__ import annotations

import pytest

from src import runtime_config
from src.runtime_config import (
    TUNABLE_KEYS,
    _coerce_and_validate,
    set_overrides,
)


# --------------------------------------------------------------------------- #
# Tunable surface sanity
# --------------------------------------------------------------------------- #
def test_tunable_keys_count_and_membership() -> None:
    """Exactly the 9 documented tunable keys are exposed."""
    assert len(TUNABLE_KEYS) == 9
    assert set(TUNABLE_KEYS) == {
        "weight_semantic",
        "weight_contextual",
        "weight_feedback",
        "epsilon_explore",
        "diversity_threshold",
        "recency_half_life_days",
        "top_k",
        "high_confidence_threshold",
        "medium_confidence_threshold",
    }


# --------------------------------------------------------------------------- #
# Unknown key → ValueError
# --------------------------------------------------------------------------- #
def test_unknown_key_raises() -> None:
    with pytest.raises(ValueError, match="unknown config key"):
        _coerce_and_validate("bogus", 1)


# --------------------------------------------------------------------------- #
# Out-of-range values → ValueError
# --------------------------------------------------------------------------- #
def test_epsilon_above_one_raises() -> None:
    with pytest.raises(ValueError, match=r"epsilon_explore"):
        _coerce_and_validate("epsilon_explore", 1.5)


def test_epsilon_below_zero_raises() -> None:
    with pytest.raises(ValueError):
        _coerce_and_validate("epsilon_explore", -0.1)


def test_diversity_threshold_out_of_range_raises() -> None:
    with pytest.raises(ValueError, match=r"\[0, 1\]"):
        _coerce_and_validate("diversity_threshold", 1.5)


def test_confidence_thresholds_out_of_range_raise() -> None:
    with pytest.raises(ValueError):
        _coerce_and_validate("high_confidence_threshold", 2.0)
    with pytest.raises(ValueError):
        _coerce_and_validate("medium_confidence_threshold", -0.5)


def test_top_k_zero_raises() -> None:
    with pytest.raises(ValueError, match=r"top_k must be >= 1"):
        _coerce_and_validate("top_k", 0)


def test_top_k_negative_raises() -> None:
    with pytest.raises(ValueError, match=r"top_k must be >= 1"):
        _coerce_and_validate("top_k", -3)


def test_top_k_non_integral_float_raises() -> None:
    """A fractional ``top_k`` (3.5) is rejected — it doesn't represent a whole number."""
    with pytest.raises(ValueError, match=r"top_k must be an integer"):
        _coerce_and_validate("top_k", 3.5)


def test_negative_weight_raises() -> None:
    with pytest.raises(ValueError, match=r"weight_semantic must be >= 0"):
        _coerce_and_validate("weight_semantic", -0.2)


def test_recency_half_life_zero_raises() -> None:
    """A half-life of 0 is undefined → rejected (must be strictly > 0)."""
    with pytest.raises(ValueError, match=r"recency_half_life_days must be > 0"):
        _coerce_and_validate("recency_half_life_days", 0)


def test_non_numeric_value_raises() -> None:
    with pytest.raises(ValueError, match=r"must be a number"):
        _coerce_and_validate("weight_semantic", "not-a-number")


# --------------------------------------------------------------------------- #
# Valid values → coerced to the right type + returned
# --------------------------------------------------------------------------- #
def test_valid_weight_coerces_to_float() -> None:
    out = _coerce_and_validate("weight_semantic", 0.95)
    assert out == pytest.approx(0.95)
    assert isinstance(out, float)


def test_valid_top_k_coerces_to_int() -> None:
    out = _coerce_and_validate("top_k", 7)
    assert out == 7
    assert isinstance(out, int)
    # An integral float (JSON has no int type) is accepted and coerced to int.
    out2 = _coerce_and_validate("top_k", 3.0)
    assert out2 == 3
    assert isinstance(out2, int)


def test_valid_epsilon_at_bounds_ok() -> None:
    assert _coerce_and_validate("epsilon_explore", 0.0) == pytest.approx(0.0)
    assert _coerce_and_validate("epsilon_explore", 1.0) == pytest.approx(1.0)


def test_valid_diversity_threshold_ok() -> None:
    assert _coerce_and_validate("diversity_threshold", 0.85) == pytest.approx(0.85)


# --------------------------------------------------------------------------- #
# set_overrides: empty body rejected before any Redis write (no I/O)
# --------------------------------------------------------------------------- #
def test_set_overrides_empty_dict_raises() -> None:
    """An all-empty body is rejected up front with ``ValueError`` (→ 422 at the router),
    before any Redis interaction."""
    with pytest.raises(ValueError, match="no config overrides supplied"):
        set_overrides({})


def test_set_overrides_unknown_key_raises_before_write(monkeypatch) -> None:
    """A batch containing an unknown key is rejected during validation, so nothing is
    written to Redis (all-or-nothing).

    We stub the Redis write/bump helpers to explode if called — proving validation
    short-circuits before any persistence.
    """

    def _boom(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("Redis must not be touched when validation fails")

    monkeypatch.setattr(runtime_config.redis_client, "set_runtime_config", _boom)
    monkeypatch.setattr(runtime_config.redis_client, "bump_config_version", _boom)

    with pytest.raises(ValueError, match="unknown config key"):
        set_overrides({"weight_semantic": 0.5, "bogus": 1})


def test_set_overrides_out_of_range_raises_before_write(monkeypatch) -> None:
    """A batch with an out-of-range value writes nothing (all-or-nothing)."""

    def _boom(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("Redis must not be touched when validation fails")

    monkeypatch.setattr(runtime_config.redis_client, "set_runtime_config", _boom)
    monkeypatch.setattr(runtime_config.redis_client, "bump_config_version", _boom)

    with pytest.raises(ValueError):
        set_overrides({"epsilon_explore": 1.5})
