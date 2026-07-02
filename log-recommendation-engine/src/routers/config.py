"""Runtime config route — live, restart-free retuning of the ranking knobs (C12).

``GET /config`` returns the current *effective* runtime config (the static defaults
overlaid with any live overrides); ``PUT /config`` pushes a partial set of overrides
that takes effect on the very next ``/recommend`` — no restart, and shared across every
replica (both the overrides and a global version counter live in Redis; see
:mod:`src.runtime_config`).

The endpoint is a thin adapter over :mod:`src.runtime_config`:

* the tunable surface + validation live in :func:`src.runtime_config.set_overrides`,
  which raises :class:`ValueError` for an unknown key or an out-of-range value —
  mapped here to **422**;
* the effective config + version are read via
  :func:`src.runtime_config.get_effective_config` /
  :func:`src.runtime_config.get_config_version`.

The Pydantic ``ConfigUpdate`` body already rejects unknown fields and gross range
violations at the schema boundary (also 422); the service-layer validation is the
authoritative backstop (and rejects an all-empty body).
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status

from src import observability, runtime_config
from src.schemas import ConfigResponse, ConfigUpdate

logger = observability.get_logger(__name__)

router = APIRouter(tags=["config"])


@router.get(
    "/config",
    response_model=ConfigResponse,
    status_code=status.HTTP_200_OK,
    summary="Get the current effective runtime config",
)
def get_config() -> ConfigResponse:
    """Return the effective tunable config and its global version.

    The ``config`` map is the static defaults overlaid with any live overrides pushed
    via ``PUT /config`` (read from the shared Redis hash; static-only when Redis is
    down). ``version`` is the global config version folded into the recommendation
    cache key — it changes on every successful ``PUT``.
    """
    return ConfigResponse(
        version=runtime_config.get_config_version(),
        config=runtime_config.get_effective_config(),
    )


@router.put(
    "/config",
    response_model=ConfigResponse,
    status_code=status.HTTP_200_OK,
    summary="Update runtime-tunable ranking config (takes effect on the next request)",
)
def update_config(body: ConfigUpdate) -> ConfigResponse:
    """Apply a partial set of runtime overrides and return the new effective config.

    Only the supplied fields are overridden (a merge). Validation is two-layered: the
    ``ConfigUpdate`` schema rejects unknown fields / gross range violations (422), and
    :func:`src.runtime_config.set_overrides` is the authoritative backstop — it rejects
    an unknown key, an out-of-range value, or an all-empty body with a
    :class:`ValueError` mapped to **422** here. On success the global config version is
    bumped (so every replica's next ``/recommend`` recomputes under the new values) and
    the recomputed effective config is returned.

    ``exclude_unset`` is used so only the fields the client actually sent are forwarded
    — an omitted field keeps its current override rather than being reset.
    """
    updates = body.model_dump(exclude_unset=True, exclude_none=True)
    try:
        effective = runtime_config.set_overrides(updates)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc

    logger.info("runtime config updated", keys=sorted(updates.keys()))
    return ConfigResponse(
        version=runtime_config.get_config_version(),
        config=effective,
    )
