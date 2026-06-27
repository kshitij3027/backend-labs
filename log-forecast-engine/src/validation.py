"""Validation and accuracy-threshold deploy gate for the forecast ensemble.

This module is the **deploy-gating layer**: it decides which ensemble members are
good enough to actually use for a given series. It builds directly on top of each
model's :meth:`~src.models.base.BaseForecaster.validate` (held-out backtest scored
with :func:`src.models.metrics.compute_metrics`) — it does **not** reimplement any
metric math.

Two requirements from ``project_requirements.md`` drive the design:

* *"Validate each model against held-out test data; only deploy models meeting an
  accuracy threshold."* -> :func:`backtest_model` + :func:`evaluate_models`, gated
  on :attr:`Settings.accuracy_deploy_threshold` (default ``0.6``).
* *"Gracefully degrade when individual models fail."* -> every public function in
  this module is non-raising: a single model that blows up is recorded with
  ``accuracy=0.0``, ``passed=False`` and an ``error`` string, and the rest of the
  evaluation proceeds normally.

Design contract
---------------
* **Pure logic.** No DB, no API, no Celery, no I/O. Operates on already-built model
  instances plus a series (anything :func:`src.features.to_series` accepts).
  Persistence of these results to ``ModelMetadata`` happens later (C8/C9/C10).
* **Reuse, don't reimplement.** Scoring goes through ``model.validate`` (which
  itself calls :func:`compute_metrics`); the accuracy field is the canonical
  ``1 - sMAPE`` from :func:`src.models.metrics.accuracy_score_ts`.
* **Never raise** from the public functions (``backtest_model``,
  ``walk_forward_validate``, ``evaluate_models``, ``accuracy_to_weights``) —
  degrade to a clear low-accuracy / empty result instead. The one exception:
  invalid argument *types* are tolerated too (an empty model list simply yields an
  empty, ``any_deployed=False`` result).
* **Light compute.** Walk-forward uses a small number of expanding-window folds
  (default 3) and modest horizons so the whole thing runs fast inside Docker tests.

``accuracy_to_weights`` is intentionally clean and reusable: it is the seed for the
dynamic-weighting feedback loop in C10.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, Sequence

import numpy as np

from src import features
from src.config import get_settings
from src.models.base import ForecastError
from src.models.metrics import compute_metrics

if TYPE_CHECKING:  # pragma: no cover - typing only
    from src.models.base import BaseForecaster


# The standard error-metric keys returned by ``model.validate`` /
# ``compute_metrics``. Accuracy is handled separately (it is the gate signal).
_ERROR_KEYS = ("mape", "smape", "rmse", "mae")

#: Degenerate metric bundle used when a backtest cannot run at all. Matches the
#: shape returned by ``BaseForecaster.validate`` on failure.
_DEGENERATE: dict[str, float] = {
    "mape": float("inf"),
    "smape": float("inf"),
    "rmse": float("inf"),
    "mae": float("inf"),
    "accuracy": 0.0,
}

#: Error string attached when a backtest's metrics are degenerate (the model
#: could not produce a real forecast). ``validate`` never raises, so this is how a
#: failed validation surfaces the promised ``error`` key.
_DEGENERATE_ERROR = (
    "validation failed: model produced no valid forecast (non-finite error metrics)"
)


def _model_name(model: "BaseForecaster") -> str:
    """Best-effort member name (the ``name`` class attribute), defaulting safely."""
    name = getattr(model, "name", None)
    return str(name) if name else model.__class__.__name__


def _finite_or(value: object, default: float) -> float:
    """Coerce ``value`` to a finite float, falling back to ``default``."""
    try:
        v = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return v if math.isfinite(v) else default


# ---------------------------------------------------------------------------
# Single-model backtest (thin wrapper over model.validate)
# ---------------------------------------------------------------------------
def backtest_model(
    model: "BaseForecaster",
    series: object,
    horizon: int,
    test_fraction: float = 0.2,
    threshold: float | None = None,
) -> dict[str, Any]:
    """Backtest one model on a held-out tail and apply the deploy gate.

    Thin wrapper around :meth:`BaseForecaster.validate`: it runs the model's own
    held-out split / forecast / scoring (which reuses :func:`compute_metrics`) and
    augments the resulting metric bundle with the model ``name`` and a ``passed``
    flag (``accuracy >= threshold``).

    Args:
        model: A fitted-or-fittable :class:`BaseForecaster` (``validate`` fits a
            fresh clone internally, so the instance need not be pre-fitted).
        series: Any input accepted by :func:`src.features.to_series`.
        horizon: Forecast horizon in steps (the backtest forecasts
            ``min(horizon, len(test))`` steps).
        test_fraction: Fraction of the tail held out for testing (default ``0.2``).
        threshold: Accuracy deploy threshold; defaults to
            ``get_settings().accuracy_deploy_threshold``.

    Returns:
        ``{"name", "mape", "smape", "rmse", "mae", "accuracy", "passed"}`` and, on
        failure, an additional ``"error"`` string. **Never raises.**
    """
    name = _model_name(model)
    thr = _resolve_threshold(threshold)

    try:
        metrics = model.validate(series, horizon, test_fraction=test_fraction)
    except ForecastError as exc:  # validate() shouldn't raise, but be defensive
        return _failed_result(name, str(exc))
    except Exception as exc:  # noqa: BLE001 - never let a member crash the gate
        return _failed_result(name, str(exc))

    result = _normalise_metrics(metrics)
    result["name"] = name
    if _is_degenerate(result):
        # validate() swallows internal failures and returns a degenerate bundle
        # (non-finite errors) instead of raising. Treat that as a real failure:
        # it must never deploy, regardless of threshold (even threshold 0.0).
        result["accuracy"] = 0.0
        result["passed"] = False
        result["error"] = _DEGENERATE_ERROR
    else:
        result["passed"] = result["accuracy"] >= thr
    return result


# ---------------------------------------------------------------------------
# Walk-forward (expanding-window) validation
# ---------------------------------------------------------------------------
def walk_forward_validate(
    model: "BaseForecaster",
    series: object,
    horizon: int,
    n_splits: int = 3,
    threshold: float | None = None,
) -> dict[str, Any]:
    """Expanding-window walk-forward backtest, averaged over a few folds.

    Performs up to ``n_splits`` expanding-window folds: each fold grows the train
    set and validates on the following block via :meth:`BaseForecaster.validate`,
    then the per-fold accuracy / error metrics are averaged. This is more robust
    than a single holdout but deliberately uses only a handful of folds so it stays
    fast in tests.

    Short series degrade gracefully: the fold count is reduced to what the series
    can support, and a series too short for even one expanding fold falls back to a
    single holdout (equivalent to :func:`backtest_model`). **Never raises.**

    Args:
        model: The :class:`BaseForecaster` to validate.
        series: Any input accepted by :func:`src.features.to_series`.
        horizon: Forecast horizon in steps per fold.
        n_splits: Desired number of expanding-window folds (default ``3``).
        threshold: Accuracy deploy threshold; defaults to settings.

    Returns:
        ``{"name", "accuracy" (== accuracy_mean), "accuracy_mean", "accuracy_std",
        "fold_accuracies", "mape_mean", "smape_mean", "rmse_mean", "mae_mean",
        "mape", "smape", "rmse", "mae", "n_folds", "passed"}``. Mirrors the
        single-model keys (so it is a drop-in for :func:`backtest_model`) plus the
        fold-level detail. On total failure, an ``"error"`` key is added.
    """
    name = _model_name(model)
    thr = _resolve_threshold(threshold)

    # Resolve the series length defensively; fall back to a single holdout if we
    # cannot inspect it.
    try:
        s = features.to_series(series)
        n = len(s)
    except (ValueError, TypeError) as exc:
        return _failed_result(name, str(exc), walk_forward=True)
    except Exception as exc:  # noqa: BLE001
        return _failed_result(name, str(exc), walk_forward=True)

    h = max(1, int(horizon)) if _is_int_like(horizon) else 1

    # Build expanding-window fold boundaries. We need at least 2 training points
    # before the first test block, and at least `h` (capped) points per test block.
    # min_train keeps a sane head; test_block is the size of each validation slice.
    folds = _expanding_folds(n=n, horizon=h, n_splits=max(1, int(n_splits)))

    if not folds:
        # Too short for expanding folds -> single holdout via the model's validate.
        single = backtest_model(model, s, h, threshold=thr)
        return _wrap_single_as_walk_forward(single)

    fold_accuracies: list[float] = []
    fold_errors: dict[str, list[float]] = {k: [] for k in _ERROR_KEYS}
    n_ran = 0

    for train_end, test_end in folds:
        train = s.iloc[:train_end]
        test = s.iloc[train_end:test_end]
        steps = len(test)
        if steps < 1 or len(train) < 2:
            continue
        try:
            # validate() on the train+test concatenation with a test_fraction that
            # selects exactly this fold's holdout reuses all the model's own
            # split/fit/predict/score logic (no metric duplication).
            window = s.iloc[:test_end]
            frac = steps / len(window)
            metrics = model.validate(window, steps, test_fraction=frac)
        except Exception:  # noqa: BLE001 - skip a bad fold, keep the rest
            continue
        m = _normalise_metrics(metrics)
        fold_accuracies.append(m["accuracy"])
        for k in _ERROR_KEYS:
            if math.isfinite(m[k]):
                fold_errors[k].append(m[k])
        n_ran += 1

    if not fold_accuracies:
        # Every fold failed -> single holdout fallback, then degenerate.
        single = backtest_model(model, s, h, threshold=thr)
        return _wrap_single_as_walk_forward(single)

    acc_mean = float(np.mean(fold_accuracies))
    acc_std = float(np.std(fold_accuracies))
    result: dict[str, Any] = {
        "name": name,
        "accuracy": acc_mean,
        "accuracy_mean": acc_mean,
        "accuracy_std": acc_std,
        "fold_accuracies": [float(a) for a in fold_accuracies],
        "n_folds": n_ran,
    }
    for k in _ERROR_KEYS:
        vals = fold_errors[k]
        mean_v = float(np.mean(vals)) if vals else float("inf")
        result[f"{k}_mean"] = mean_v
        # Also expose the bare key so this dict is interchangeable with a holdout
        # result (used by evaluate_models / accuracy_to_weights downstream).
        result[k] = mean_v
    if _is_degenerate(result):
        # Aggregate has non-finite error means (e.g. every fold produced no valid
        # error) -> a failed validation. Force rejection + attach an error, just
        # like backtest_model. Stays graceful (never raises).
        result["accuracy"] = 0.0
        result["accuracy_mean"] = 0.0
        result["passed"] = False
        result["error"] = _DEGENERATE_ERROR
    else:
        result["passed"] = acc_mean >= thr
    return result


# ---------------------------------------------------------------------------
# Cross-model deploy gate
# ---------------------------------------------------------------------------
def evaluate_models(
    models: "Sequence[BaseForecaster]",
    series: object,
    horizon: int,
    threshold: float | None = None,
    method: str = "holdout",
    test_fraction: float = 0.2,
    n_splits: int = 3,
) -> dict[str, Any]:
    """Backtest every model and partition them into deployed vs rejected.

    For each model a backtest is run (a single holdout by default for speed, or
    walk-forward for robustness) and its accuracy is compared against
    ``threshold``. Models whose accuracy meets the threshold are *deployed*; the
    rest are *rejected*. A model that fails entirely is rejected (``passed=False``),
    not propagated as an exception — satisfying the graceful-degradation
    requirement.

    Args:
        models: The ensemble members to evaluate (e.g. ARIMA, exp-smoothing,
            linear, XGBoost instances).
        series: Any input accepted by :func:`src.features.to_series`.
        horizon: Forecast horizon in steps.
        threshold: Accuracy deploy threshold; defaults to
            ``get_settings().accuracy_deploy_threshold``.
        method: ``"holdout"`` (default, fast — one split via
            :func:`backtest_model`) or ``"walk_forward"`` (robust — averaged folds
            via :func:`walk_forward_validate`).
        test_fraction: Holdout fraction (only used when ``method="holdout"``).
        n_splits: Fold count (only used when ``method="walk_forward"``).

    Returns:
        ``{
            "threshold": float,
            "method": str,
            "results": {name: {accuracy, mape, smape, rmse, mae, passed, error?}},
            "deployed": [names that passed],
            "rejected": [names that failed/below threshold],
            "any_deployed": bool,
        }``.
        An empty / non-iterable ``models`` yields the same shape with empty
        ``results`` and ``any_deployed=False``. **Never raises.**
    """
    thr = _resolve_threshold(threshold)
    method_norm = method if method in ("holdout", "walk_forward") else "holdout"

    results: dict[str, dict[str, Any]] = {}
    deployed: list[str] = []
    rejected: list[str] = []

    try:
        model_list = list(models) if models is not None else []
    except TypeError:
        model_list = []

    for model in model_list:
        if method_norm == "walk_forward":
            res = walk_forward_validate(
                model, series, horizon, n_splits=n_splits, threshold=thr
            )
        else:
            res = backtest_model(
                model, series, horizon, test_fraction=test_fraction, threshold=thr
            )
        name = res.get("name") or _model_name(model)
        results[name] = res
        if res.get("passed"):
            deployed.append(name)
        else:
            rejected.append(name)

    return {
        "threshold": thr,
        "method": method_norm,
        "results": results,
        "deployed": deployed,
        "rejected": rejected,
        "any_deployed": bool(deployed),
    }


# ---------------------------------------------------------------------------
# Accuracy-weighted deploy weights (seed for C10 dynamic weighting)
# ---------------------------------------------------------------------------
def accuracy_to_weights(
    results: dict[str, Any],
    base_weights: dict[str, float] | None = None,
) -> dict[str, float]:
    """Blend configured base weights with measured accuracy into deploy weights.

    Formula
    -------
    For every **deployed** model ``i`` (the rest get weight ``0``)::

        raw_i    = base_weight_i * accuracy_i
        weight_i = raw_i / sum_j(raw_j)        # normalise over deployed models

    i.e. each deployed model's weight is proportional to its configured prior
    weight times its recently measured accuracy, renormalised so the deployed
    weights sum to ``1.0``. A model the operator already trusts (high base weight)
    that *also* validates well dominates; a deployed-but-mediocre model is
    down-weighted. This is the clean seed reused by the C10 feedback loop, which
    feeds *updated* accuracies back through the same function.

    Fallbacks (all keep the contract "deployed weights sum to 1.0", never raises):

    * No deployed models -> returns ``{}``.
    * All raw products are zero/non-finite (e.g. every base weight missing, or all
      accuracies ``0``) -> falls back to a **uniform** split over the deployed
      models.

    Args:
        results: Either the full :func:`evaluate_models` output (a dict with a
            ``"results"`` and ``"deployed"`` key) or a bare per-model results map
            (``{name: {accuracy, passed, ...}}``). Both are accepted.
        base_weights: Prior/configured weights keyed by model name. Defaults to
            ``get_settings().model_weights``. Missing names default to ``0`` in the
            product (so an unknown model contributes nothing unless it is the only
            deployed model, in which case the uniform fallback gives it ``1.0``).

    Returns:
        ``{model_name: weight}`` covering exactly the deployed models, summing to
        ``1.0`` (or ``{}`` if nothing is deployed). **Never raises.**
    """
    if base_weights is None:
        try:
            base_weights = get_settings().model_weights
        except Exception:  # noqa: BLE001 - config must not break weighting
            base_weights = {}

    per_model, deployed = _extract_results(results)
    if not deployed:
        return {}

    raw: dict[str, float] = {}
    for name in deployed:
        bw = _finite_or(base_weights.get(name, 0.0), 0.0)
        acc = _finite_or(per_model.get(name, {}).get("accuracy", 0.0), 0.0)
        bw = max(0.0, bw)
        acc = max(0.0, acc)
        raw[name] = bw * acc

    total = float(sum(raw.values()))
    if not math.isfinite(total) or total <= 0.0:
        # Uniform fallback over deployed models.
        uniform = 1.0 / len(deployed)
        return {name: uniform for name in deployed}

    return {name: raw[name] / total for name in deployed}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _resolve_threshold(threshold: float | None) -> float:
    """Return ``threshold`` if given/valid, else the configured deploy threshold."""
    if threshold is not None:
        try:
            t = float(threshold)
            if math.isfinite(t):
                return t
        except (TypeError, ValueError):
            pass
    try:
        return float(get_settings().accuracy_deploy_threshold)
    except Exception:  # noqa: BLE001 - never fail on config access
        return 0.6


def _is_degenerate(metrics: dict[str, Any]) -> bool:
    """Return True when a (normalised) metric bundle represents a FAILED validation.

    ``BaseForecaster.validate`` never raises; on internal failure (fit/predict
    error, too-short series) it returns a degenerate bundle with non-finite error
    metrics. A genuinely poor-but-valid model still has *finite* errors and a real
    (low) accuracy — so non-finite errors are the only reliable failure signal.
    """
    rmse = metrics.get("rmse", float("inf"))
    mape = metrics.get("mape", float("inf"))
    return not math.isfinite(_finite_or(rmse, float("inf"))) or not math.isfinite(
        _finite_or(mape, float("inf"))
    )


def _normalise_metrics(metrics: object) -> dict[str, float]:
    """Coerce a metrics bundle to the standard keys with safe float values."""
    if not isinstance(metrics, dict):
        return dict(_DEGENERATE)
    out: dict[str, float] = {}
    for k in _ERROR_KEYS:
        out[k] = _finite_or(metrics.get(k, float("inf")), float("inf"))
    out["accuracy"] = _finite_or(metrics.get("accuracy", 0.0), 0.0)
    return out


def _failed_result(
    name: str, error: str, walk_forward: bool = False
) -> dict[str, Any]:
    """Build a degenerate, rejected result for a model that could not validate."""
    result: dict[str, Any] = dict(_DEGENERATE)
    result["name"] = name
    result["passed"] = False
    result["error"] = error
    if walk_forward:
        result["accuracy_mean"] = 0.0
        result["accuracy_std"] = 0.0
        result["fold_accuracies"] = []
        result["n_folds"] = 0
        for k in _ERROR_KEYS:
            result[f"{k}_mean"] = float("inf")
    return result


def _wrap_single_as_walk_forward(single: dict[str, Any]) -> dict[str, Any]:
    """Present a single-holdout result with the walk-forward key shape."""
    acc = _finite_or(single.get("accuracy", 0.0), 0.0)
    result = dict(single)
    result["accuracy_mean"] = acc
    result["accuracy_std"] = 0.0
    result["fold_accuracies"] = [acc] if "error" not in single else []
    result["n_folds"] = 1 if "error" not in single else 0
    for k in _ERROR_KEYS:
        result[f"{k}_mean"] = _finite_or(single.get(k, float("inf")), float("inf"))
    return result


def _expanding_folds(
    n: int, horizon: int, n_splits: int
) -> list[tuple[int, int]]:
    """Compute ``(train_end, test_end)`` index pairs for expanding-window folds.

    Each fold trains on ``s[:train_end]`` and tests on ``s[train_end:test_end]``.
    Folds expand the training set across the series. Returns an empty list when the
    series is too short to support even a single fold with >= 2 training points and
    >= 1 test point (callers then fall back to a single holdout).
    """
    if n < 4:
        return []
    # Each test block is at most `horizon` steps but small enough that we get the
    # requested number of folds out of the tail of the series.
    min_train = max(2, n // 3)
    available = n - min_train
    if available < 1:
        return []

    test_block = max(1, min(int(horizon), available // max(1, n_splits)))
    n_folds = min(n_splits, available // test_block)
    if n_folds < 1:
        return []

    folds: list[tuple[int, int]] = []
    for i in range(n_folds):
        train_end = min_train + i * test_block
        test_end = min(train_end + test_block, n)
        if test_end - train_end < 1 or train_end < 2:
            continue
        folds.append((train_end, test_end))
        if test_end >= n:
            break
    return folds


def _extract_results(
    results: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], list[str]]:
    """Normalise either an ``evaluate_models`` dict or a bare results map.

    Returns ``(per_model_results, deployed_names)``. For a bare map, "deployed" is
    inferred from each entry's ``passed`` flag.
    """
    if not isinstance(results, dict):
        return {}, []

    if "results" in results and isinstance(results["results"], dict):
        per_model = results["results"]
        deployed = results.get("deployed")
        if not isinstance(deployed, list):
            deployed = [
                name
                for name, r in per_model.items()
                if isinstance(r, dict) and r.get("passed")
            ]
        return per_model, list(deployed)

    # Bare {name: {...}} map.
    per_model = {
        name: r for name, r in results.items() if isinstance(r, dict)
    }
    deployed = [name for name, r in per_model.items() if r.get("passed")]
    return per_model, deployed


def _is_int_like(value: object) -> bool:
    try:
        int(value)  # type: ignore[arg-type]
        return True
    except (TypeError, ValueError):
        return False


__all__ = [
    "backtest_model",
    "walk_forward_validate",
    "evaluate_models",
    "accuracy_to_weights",
]
