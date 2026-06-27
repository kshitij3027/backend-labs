"""Celery tasks: scheduled forecasting + model retraining (C9).

These tasks are the recurring-work bodies driven by the Beat schedule in
:mod:`src.celery_app`. They are deliberately thin: each one opens its own
``SessionLocal`` session, delegates to the already-built integration / validation
layers (:mod:`src.prediction_service`, :mod:`src.validation`,
:mod:`src.db.repository`) and never reimplements forecasting logic.

Contracts
---------
* **Own your session.** Every task creates a session, commits its own writes and
  closes the session in a ``finally`` — no leaked connections in a long-lived
  worker.
* **Graceful, always.** A single metric or model blowing up must not crash the
  task or the worker. Per-item failures are caught, logged and surfaced in the
  returned summary dict; the loop continues.
* **Synchronously callable.** The task *bodies* are plain functions that need no
  broker. Calling ``run_forecast("cpu")`` (or ``run_forecast.run(...)``) executes
  the body in-process, which is exactly how the test suite exercises them — no
  Celery worker or Redis broker required to run the logic.

The four tasks:

* :func:`run_forecast` — forecast one metric (persist + cache).
* :func:`run_scheduled_forecasts` — forecast every known metric (sliding recent
  window; ``generate_prediction`` already loads ``training_window_days`` of data).
* :func:`run_retrain` — (re)fit + validate the 4 models for one metric and persist
  per-model state (accuracy / weight / deploy flag) to ``ModelMetadata``.
* :func:`run_scheduled_retrain` — retrain every known metric.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from src import prediction_service, validation
from src.celery_app import celery_app
from src.config import get_settings
from src.db import repository
from src.db.session import SessionLocal

logger = logging.getLogger(__name__)

# Where optional fitted-model artifacts are written (gitignored, mounted volume in
# Docker). Artifact saving is best-effort: failures here never fail a retrain.
_MODELS_DIR = os.environ.get("MODELS_DIR", "models")


# --------------------------------------------------------------------------- #
# Forecast tasks
# --------------------------------------------------------------------------- #
@celery_app.task(name="tasks.run_forecast")
def run_forecast(
    metric_name: str,
    horizon_minutes: int | None = None,
    use_multi_window: bool = False,
) -> dict[str, Any]:
    """Generate, persist and cache a forecast for ``metric_name``.

    Delegates to :func:`src.prediction_service.generate_prediction` (which loads
    the recent sliding window, fits the ensemble, persists the ``Forecast`` row
    and writes the Redis cache). Returns a small summary; on any error returns
    ``{"metric_name": ..., "error": ...}`` instead of propagating (the error is
    also logged), so a scheduled fan-out never crashes the worker.
    """
    session = SessionLocal()
    try:
        result = prediction_service.generate_prediction(
            session,
            metric_name,
            horizon_minutes,
            use_multi_window=use_multi_window,
            persist=True,
            cache=True,
        )
        return {
            "metric_name": metric_name,
            "alert_level": result.get("alert_level"),
            "confidence": result.get("confidence"),
            "steps": result.get("horizon_steps"),
            "persisted": True,
            "failed_models": result.get("failed_models", []),
        }
    except Exception as exc:  # noqa: BLE001 - never crash the worker on one metric
        logger.exception("run_forecast failed for %r", metric_name)
        try:
            session.rollback()
        except Exception:  # noqa: BLE001
            pass
        return {"metric_name": metric_name, "error": str(exc)}
    finally:
        session.close()


@celery_app.task(name="tasks.run_scheduled_forecasts")
def run_scheduled_forecasts() -> dict[str, Any]:
    """Forecast every known metric (the periodic prediction job).

    Discovers distinct metric names from the metrics table and runs
    :func:`run_forecast` synchronously for each (calling the underlying function
    keeps the job simple and self-contained — no broker fan-out needed). Each
    metric uses the standard sliding recent window inside ``generate_prediction``.
    A failure on one metric is recorded and the loop continues.
    """
    session = SessionLocal()
    try:
        metric_names = repository.list_metric_names(session)
    except Exception as exc:  # noqa: BLE001
        logger.exception("run_scheduled_forecasts: could not list metrics")
        return {"metrics": [], "count": 0, "error": str(exc)}
    finally:
        session.close()

    summaries: list[dict[str, Any]] = []
    for name in metric_names:
        # run_forecast manages its own session and never raises.
        summaries.append(run_forecast(name))

    return {"metrics": summaries, "count": len(summaries)}


# --------------------------------------------------------------------------- #
# Retrain tasks
# --------------------------------------------------------------------------- #
def _save_artifact(model: Any, metric_name: str) -> str | None:
    """Best-effort save of a fitted model to ``models/{metric}/{model}.joblib``.

    Returns the artifact path on success, ``None`` on any failure (never raises).
    """
    try:
        directory = os.path.join(_MODELS_DIR, metric_name)
        os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, f"{model.name}.joblib")
        model.save(path)
        return path
    except Exception as exc:  # noqa: BLE001 - artifacts are optional
        logger.warning(
            "could not save artifact for %s/%s: %s",
            metric_name,
            getattr(model, "name", "?"),
            exc,
        )
        return None


@celery_app.task(name="tasks.run_retrain")
def run_retrain(metric_name: str) -> dict[str, Any]:
    """Retrain (refit + validate) the ensemble for ``metric_name`` and persist state.

    Steps:

    1. Load the recent training window and normalise to a series.
    2. Build the four models, evaluate them with
       :func:`src.validation.evaluate_models` (accuracy + deploy decisions) and
       compute accuracy-blended weights via :func:`src.validation.accuracy_to_weights`.
    3. Persist per-model state to ``ModelMetadata`` via
       :func:`src.db.repository.upsert_model_metadata` (``last_trained_at``,
       ``accuracy``, ``weight``, ``is_deployed``, ``params``, optional
       ``artifact_path``).

    Returns ``{metric_name, deployed, rejected, weights}``; on error returns
    ``{metric_name, error}``. Never raises (graceful degradation).
    """
    settings = get_settings()
    now = datetime.now(timezone.utc)
    session = SessionLocal()
    try:
        # 1. Load the recent training window (reuse generate_prediction's loader
        #    via the repository directly, to keep this task self-contained).
        from datetime import timedelta

        from src import features

        since = now - timedelta(days=int(settings.training_window_days))
        metrics = repository.get_metrics(session, metric_name, since=since)
        points = [(m.timestamp, m.value) for m in metrics]
        if not points:
            return {
                "metric_name": metric_name,
                "deployed": [],
                "rejected": [],
                "weights": {},
                "note": "no recent data to retrain on",
            }
        try:
            series = features.to_series(points)
        except (ValueError, TypeError) as exc:
            return {"metric_name": metric_name, "error": f"bad series: {exc}"}

        # 2. Build + evaluate the 4 models against held-out data.
        models = prediction_service.build_models()
        horizon = int(settings.default_horizon_min)
        evaluation = validation.evaluate_models(models, series, horizon)
        weights = validation.accuracy_to_weights(
            evaluation, base_weights=dict(settings.model_weights)
        )

        per_model = evaluation.get("results", {})
        deployed = list(evaluation.get("deployed", []))
        rejected = list(evaluation.get("rejected", []))

        # Map model name -> fitted instance for optional artifact saving. Only fit
        # (again) for deployed models; a fit failure here is tolerated.
        fitted_by_name: dict[str, Any] = {}
        for model in models:
            if model.name in deployed:
                try:
                    model.fit(series)
                    fitted_by_name[model.name] = model
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "retrain: %s failed to refit for artifact: %s",
                        model.name,
                        exc,
                    )

        # 3. Persist per-model metadata.
        for model in models:
            name = model.name
            res = per_model.get(name, {})
            accuracy = res.get("accuracy")
            is_deployed = name in deployed
            weight = float(weights.get(name, 0.0))
            artifact_path = None
            if is_deployed and name in fitted_by_name:
                artifact_path = _save_artifact(fitted_by_name[name], metric_name)

            fields: dict[str, Any] = {
                "last_trained_at": now,
                "accuracy": float(accuracy) if accuracy is not None else None,
                "weight": weight,
                "is_deployed": is_deployed,
                "params": {
                    "metric_name": metric_name,
                    "horizon_minutes": horizon,
                    "method": evaluation.get("method"),
                    "threshold": evaluation.get("threshold"),
                },
            }
            if artifact_path is not None:
                fields["artifact_path"] = artifact_path

            try:
                repository.upsert_model_metadata(session, name, commit=False, **fields)
            except Exception as exc:  # noqa: BLE001 - one model's write shouldn't abort
                logger.warning(
                    "retrain: failed to upsert metadata for %s: %s", name, exc
                )

        session.commit()
        return {
            "metric_name": metric_name,
            "deployed": deployed,
            "rejected": rejected,
            "weights": {k: float(v) for k, v in weights.items()},
        }
    except Exception as exc:  # noqa: BLE001 - never crash the worker
        logger.exception("run_retrain failed for %r", metric_name)
        try:
            session.rollback()
        except Exception:  # noqa: BLE001
            pass
        return {"metric_name": metric_name, "error": str(exc)}
    finally:
        session.close()


@celery_app.task(name="tasks.run_scheduled_retrain")
def run_scheduled_retrain() -> dict[str, Any]:
    """Retrain every known metric (the periodic retrain job).

    Discovers distinct metric names and runs :func:`run_retrain` synchronously for
    each. Per-metric failures are recorded in the per-metric summary and never
    abort the loop.
    """
    session = SessionLocal()
    try:
        metric_names = repository.list_metric_names(session)
    except Exception as exc:  # noqa: BLE001
        logger.exception("run_scheduled_retrain: could not list metrics")
        return {"metrics": [], "count": 0, "error": str(exc)}
    finally:
        session.close()

    summaries: list[dict[str, Any]] = []
    for name in metric_names:
        summaries.append(run_retrain(name))

    return {"metrics": summaries, "count": len(summaries)}


__all__ = [
    "run_forecast",
    "run_scheduled_forecasts",
    "run_retrain",
    "run_scheduled_retrain",
]
