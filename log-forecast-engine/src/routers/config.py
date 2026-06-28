"""Runtime configuration routes (Feature Area B, C11).

Exposes the process-local override store in :mod:`src.runtime_config` over HTTP so
the dashboard can adjust **model weights, confidence thresholds and alert
settings without restarting** the service:

* ``GET /config`` — current runtime overrides + static context (intervals /
  horizon bounds the dashboard needs).
* ``PUT /config`` — partial, validated update; returns the new effective config.

Validation (non-negative weights; thresholds in ``[0, 1]`` with ``high > medium``)
lives in :mod:`src.runtime_config`; a :class:`ValueError` there is mapped to a 422
here so an invalid update never mutates the store. The override is **per process**
(documented in :mod:`src.runtime_config`) — the on-demand forecast routes read it
and pass weights/thresholds into the prediction path.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from src import runtime_config
from src.config import get_settings
from src.schemas import ConfigResponse, ConfigUpdateRequest

router = APIRouter(tags=["config"])


def _build_response() -> ConfigResponse:
    """Assemble the current runtime overrides + static settings context."""
    state = runtime_config.get_runtime_config()
    settings = get_settings()
    return ConfigResponse(
        model_weights={k: float(v) for k, v in state["model_weights"].items()},
        high_confidence_threshold=float(state["high_confidence_threshold"]),
        medium_confidence_threshold=float(state["medium_confidence_threshold"]),
        alert_settings=dict(state["alert_settings"]),
        prediction_interval_min=int(settings.prediction_interval_min),
        default_horizon_min=int(settings.default_horizon_min),
        horizon_min_steps=int(settings.horizon_min_steps),
        horizon_max_steps=int(settings.horizon_max_steps),
    )


@router.get(
    "/config",
    response_model=ConfigResponse,
    summary="Current runtime config (weights, thresholds, alert + static context)",
)
def get_config() -> ConfigResponse:
    """Return the live runtime overrides plus the static settings the UI needs."""
    return _build_response()


@router.put(
    "/config",
    response_model=ConfigResponse,
    summary="Update runtime config (weights / thresholds / alerts) without restart",
)
def put_config(body: ConfigUpdateRequest) -> ConfigResponse:
    """Apply a partial, validated update and return the new effective config.

    Only the supplied fields change. Validation runs against the resulting state
    (so ``high > medium`` is enforced even on a single-threshold update); any
    failure yields 422 and leaves the store untouched.
    """
    try:
        runtime_config.update_runtime_config(
            model_weights=body.model_weights,
            high_confidence_threshold=body.high_confidence_threshold,
            medium_confidence_threshold=body.medium_confidence_threshold,
            alert_settings=body.alert_settings,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc
    return _build_response()


__all__ = ["router"]
