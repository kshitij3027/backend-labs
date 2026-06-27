"""Celery application + Beat schedule for the Predictive Log Analytics Engine (C9).

This is the scheduling layer that turns the synchronous forecast/retrain logic
into recurring background work, satisfying two ``project_requirements.md`` core
requirements:

* *"Generate predictions on a recurring schedule using sliding windows of recent
  data"* -> the ``forecast-all-metrics`` Beat entry fires every
  ``prediction_interval_min`` minutes and runs :func:`src.tasks.run_scheduled_forecasts`.
* *"Automatically retrain models on a recurring schedule"* -> the
  ``retrain-all-models`` Beat entry fires every ``retrain_interval_hr`` hours and
  runs :func:`src.tasks.run_scheduled_retrain`.

Both the broker and the result backend point at ``settings.redis_url`` (the same
Redis instance used for the prediction cache). Schedule cadences are read from
:func:`src.config.get_settings`, so they are environment-overridable
(``PREDICTION_INTERVAL_MIN`` / ``RETRAIN_INTERVAL_HR``) like every other knob.

The app is importable as ``from src.celery_app import celery_app`` and the
conventional ``celery -A src.celery_app worker`` / ``... beat`` invocations work
because the module exposes both ``celery_app`` and the alias ``app``. Tasks are
registered by importing :mod:`src.tasks` at the bottom of this module (after the
app exists, to avoid a circular import at definition time).
"""

from __future__ import annotations

from datetime import timedelta

from celery import Celery

from src.config import get_settings

_settings = get_settings()

# Broker + result backend both use the configured Redis URL. (A separate DB index
# for results would also be fine; using the same URL keeps configuration to one
# knob and is perfectly safe for this workload.)
celery_app = Celery(
    "log_forecast",
    broker=_settings.redis_url,
    backend=_settings.redis_url,
    include=["src.tasks"],
)

celery_app.conf.update(
    # Serialization: JSON only (no pickle) — tasks pass/return plain dicts.
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # Timekeeping.
    timezone="UTC",
    enable_utc=True,
    # Time limits so a pathologically slow model can never hang a worker forever.
    # The soft limit raises SoftTimeLimitExceeded inside the task; the hard limit
    # kills the worker child.
    task_soft_time_limit=120,
    task_time_limit=180,
    # Recycle worker children periodically to avoid memory creep from the ML libs.
    worker_max_tasks_per_child=100,
    # Surface late results / acks sensibly for idempotent scheduled jobs.
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    result_expires=3600,
)

# --- Beat schedule (built from settings so env overrides take effect) --------
celery_app.conf.beat_schedule = {
    "forecast-all-metrics": {
        "task": "tasks.run_scheduled_forecasts",
        "schedule": timedelta(minutes=int(_settings.prediction_interval_min)),
    },
    "retrain-all-models": {
        "task": "tasks.run_scheduled_retrain",
        "schedule": timedelta(hours=int(_settings.retrain_interval_hr)),
    },
}

# Conventional alias so `celery -A src.celery_app worker` (which looks for `app`
# or `celery`) and `from src.celery_app import celery_app` both work.
app = celery_app

# Import tasks for registration. Done last to avoid a circular import: src.tasks
# imports `celery_app` from this module.
from src import tasks as _tasks  # noqa: E402,F401  (import for side-effect: task registration)

__all__ = ["celery_app", "app"]
