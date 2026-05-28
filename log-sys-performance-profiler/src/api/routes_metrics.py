from __future__ import annotations

import asyncio
import json
from typing import Annotated, AsyncIterator

from fastapi import APIRouter, Depends
from sse_starlette.sse import EventSourceResponse

from src.api.dependencies import get_buffer, get_collector, get_settings_dep
from src.metrics.collector import MetricsCollector
from src.metrics.ring_buffer import RingBuffer
from src.settings import Settings

router = APIRouter(prefix="/api/metrics", tags=["metrics"])


@router.get("/snapshot")
async def snapshot(
    buffer: Annotated[RingBuffer, Depends(get_buffer)],
    collector: Annotated[MetricsCollector, Depends(get_collector)],
    window_sec: float = 60.0,
) -> dict:
    samples = buffer.snapshot(window_sec=window_sec)
    return {
        "samples": [s.to_dict() for s in samples],
        "dropped": collector.metrics_dropped(),
        "window_sec": window_sec,
    }


async def _live_event_stream(
    buffer: RingBuffer,
    collector: MetricsCollector,
    settings: Settings,
) -> AsyncIterator[dict]:
    period = max(0.1, float(settings.dashboard_refresh_sec))
    keepalive_every = 15.0
    last_keepalive = 0.0
    while True:
        samples = buffer.snapshot(window_sec=60.0)
        payload = {
            "samples": [s.to_dict() for s in samples],
            "dropped": collector.metrics_dropped(),
        }
        yield {"event": "metrics", "data": json.dumps(payload)}
        elapsed = 0.0
        while elapsed < period:
            await asyncio.sleep(min(1.0, period - elapsed))
            elapsed += 1.0
            last_keepalive += 1.0
            if last_keepalive >= keepalive_every:
                yield {"comment": "keepalive"}
                last_keepalive = 0.0


@router.get("/live")
async def live(
    buffer: Annotated[RingBuffer, Depends(get_buffer)],
    collector: Annotated[MetricsCollector, Depends(get_collector)],
    settings: Annotated[Settings, Depends(get_settings_dep)],
) -> EventSourceResponse:
    return EventSourceResponse(_live_event_stream(buffer, collector, settings))
