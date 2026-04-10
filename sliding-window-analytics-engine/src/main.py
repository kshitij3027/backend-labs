"""FastAPI entrypoint for the sliding-window analytics engine.

Commit 4 scope: wire the window manager + generator into a FastAPI
lifespan, and expose the initial HTTP surface:

* ``GET /api/health`` — liveness plus the number of active windows.
* ``POST /api/metric`` — ingest a single user-supplied metric event.
* ``GET /api/stats`` — multi-resolution snapshot of all registered
  windows, serialised as JSON.

The background :class:`LogEventGenerator` task is started in the
lifespan unless ``DISABLE_GENERATOR=1`` is set in the environment (the
test suite toggles this so unit tests don't race against a 600 evt/s
producer).
"""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException

from src.config import get_config
from src.generator import LogEventGenerator
from src.ingest import MetricRequest
from src.models import Event
from src.window_manager import build_default_manager


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build singletons and manage the background generator task.

    The window manager is always built. The generator task is spawned
    only when ``config.disable_generator`` is false — tests disable it
    via the ``DISABLE_GENERATOR`` env var to keep the event loop idle.
    """
    config = get_config()
    window_manager = build_default_manager(config)
    stop_event = asyncio.Event()

    app.state.config = config
    app.state.window_manager = window_manager
    app.state.stop_event = stop_event
    app.state.generator_task = None

    if not config.disable_generator:
        generator = LogEventGenerator(
            spike_probability=config.spike_probability,
            rate_per_second=600.0,
        )

        async def sink(event: Event) -> None:
            # The dispatch itself is synchronous (pure in-memory state
            # updates); the async wrapper exists only so the generator
            # can ``await`` its sink uniformly.
            window_manager.dispatch(event)

        task = asyncio.create_task(
            generator.run(sink, stop_event),
            name="log-event-generator",
        )
        app.state.generator_task = task

    try:
        yield
    finally:
        stop_event.set()
        task = app.state.generator_task
        if task is not None:
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except asyncio.TimeoutError:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
            except asyncio.CancelledError:
                pass
            except Exception:
                # Don't let generator shutdown errors mask a clean exit.
                pass


app = FastAPI(title="Sliding Window Analytics Engine", lifespan=lifespan)


@app.get("/api/health")
async def health() -> dict[str, object]:
    """Liveness/readiness probe.

    Reports the number of active sliding windows currently registered
    with the manager — becomes non-zero once the lifespan has run.
    """
    manager = getattr(app.state, "window_manager", None)
    active = manager.active_count if manager is not None else 0
    return {"status": "healthy", "active_windows": active}


@app.post("/api/metric")
async def ingest_metric(request: MetricRequest) -> dict[str, object]:
    """Ingest a single metric event submitted via HTTP.

    The event is dispatched synchronously to every window matching its
    metric. Unknown metrics are silently accepted (the window manager
    is a passive router) so callers can pre-register custom metrics in
    later commits without breaking compatibility.
    """
    manager = app.state.window_manager
    if manager is None:  # pragma: no cover - defensive
        raise HTTPException(status_code=503, detail="service not ready")

    event_id = str(uuid.uuid4())
    event = Event(
        event_id=event_id,
        timestamp=time.time(),
        value=request.value,
        metric=request.metric,
        metadata=dict(request.metadata),
    )
    manager.dispatch(event)
    return {"accepted": True, "event_id": event_id}


@app.get("/api/stats")
async def stats() -> dict[str, object]:
    """Return a nested snapshot of every active window.

    Shape::

        {
          "metrics": {
            "<metric>": {
              "<resolution>": { ...WindowResult fields... },
              ...
            },
            ...
          },
          "active_windows": <int>,
          "timestamp": <float unix seconds>
        }
    """
    manager = app.state.window_manager
    if manager is None:  # pragma: no cover - defensive
        raise HTTPException(status_code=503, detail="service not ready")

    now = time.time()
    raw = manager.snapshot_all(now)
    serialised: dict[str, dict[str, dict[str, object]]] = {}
    for metric, by_resolution in raw.items():
        serialised[metric] = {
            resolution: asdict(result)
            for resolution, result in by_resolution.items()
        }
    return {
        "metrics": serialised,
        "active_windows": manager.active_count,
        "timestamp": now,
    }


if __name__ == "__main__":
    import uvicorn

    config = get_config()
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=config.api_port,
        log_level="info",
    )
