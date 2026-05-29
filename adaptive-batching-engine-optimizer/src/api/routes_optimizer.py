"""Optimizer status / control endpoints.

Exposes the current :class:`~src.models.OptimizerStatus`, a partial
reconfiguration patch, and a hard reset. A live ``optimization_interval``
change is mirrored onto ``app.state.loop_interval`` so the running background
loop retunes its cadence on its next iteration without a restart.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request

from src.api.dependencies import get_batcher
from src.batcher import AdaptiveBatcher
from src.models import OptimizerConfigUpdate, OptimizerStatus

router = APIRouter(prefix="/api/optimizer", tags=["optimizer"])


@router.get("")
async def get_status(
    batcher: Annotated[AdaptiveBatcher, Depends(get_batcher)],
) -> OptimizerStatus:
    """Return the current optimizer status."""
    return batcher.status()


@router.post("/config")
async def update_config(
    update: OptimizerConfigUpdate,
    request: Request,
    batcher: Annotated[AdaptiveBatcher, Depends(get_batcher)],
) -> OptimizerStatus:
    """Apply a partial reconfiguration and return the updated status.

    Delegates the patch to :meth:`AdaptiveBatcher.apply_config`. When the patch
    carries a new ``optimization_interval`` we also update
    ``app.state.loop_interval`` so the background loop picks up the new cadence
    on its next iteration (the batcher's own override only affects ticks that
    omit an explicit ``interval``).
    """
    batcher.apply_config(update)
    if update.optimization_interval is not None:
        request.app.state.loop_interval = update.optimization_interval
    return batcher.status()


@router.post("/reset")
async def reset(
    batcher: Annotated[AdaptiveBatcher, Depends(get_batcher)],
) -> OptimizerStatus:
    """Reset every control-loop component and return the cleared status."""
    batcher.reset()
    return batcher.status()
