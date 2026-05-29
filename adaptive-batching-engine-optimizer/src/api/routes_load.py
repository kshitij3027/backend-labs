"""Load-simulation control endpoint.

Retargets the live synthetic traffic feeding the control loop so a demo or e2e
test can drive the optimizer through different arrival patterns at runtime.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from src.api.dependencies import get_batcher
from src.api.schemas import LoadResponse
from src.batcher import AdaptiveBatcher
from src.models import LoadConfig

router = APIRouter(prefix="/api", tags=["load"])


@router.post("/load")
async def set_load(
    config: LoadConfig,
    batcher: Annotated[AdaptiveBatcher, Depends(get_batcher)],
) -> LoadResponse:
    """Retarget the synthetic traffic and echo back the effective rate."""
    batcher.set_load(config)
    return LoadResponse(
        applied=config,
        current_rate=batcher.load_simulator.current_rate(),
        message=(
            f"load set to {config.messages_per_second} msg/s "
            f"(burst p={config.burst_probability})"
        ),
    )
