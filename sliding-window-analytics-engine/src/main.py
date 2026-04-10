"""FastAPI entrypoint for the sliding-window analytics engine.

Commit 5 scope: on top of the Commit 4 HTTP surface, this module now
also drives the live dashboard:

* ``GET /`` — serves the Chart.js dashboard HTML.
* ``GET /api/health`` — liveness plus the number of active windows.
* ``POST /api/metric`` — ingest a single user-supplied metric event.
* ``GET /api/stats`` — multi-resolution snapshot of all registered
  windows, serialised as JSON.
* ``WS /ws`` — pushes a ``metrics_update`` payload to every connected
  client every ``config.ws_update_interval_seconds`` seconds.

The background :class:`LogEventGenerator` task is started in the
lifespan unless ``DISABLE_GENERATOR=1`` is set in the environment (the
test suite toggles this so unit tests don't race against a 600 evt/s
producer). A second background task drives the WebSocket broadcast
loop; both tasks share the same ``stop_event`` so shutdown is prompt.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from src.config import get_config
from src.generator import LogEventGenerator
from src.ingest import MetricRequest
from src.models import Event
from src.websocket import ConnectionManager, broadcast_loop
from src.window_manager import build_default_manager


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Build singletons and manage background tasks.

    The window manager is always built. The generator task is spawned
    only when ``config.disable_generator`` is false — tests disable it
    via the ``DISABLE_GENERATOR`` env var to keep the event loop idle.
    The broadcast task is always spawned so unit tests that drive the
    WebSocket endpoint get live pushes.
    """
    config = get_config()
    window_manager = build_default_manager(config)
    stop_event = asyncio.Event()
    connection_manager = ConnectionManager()

    app.state.config = config
    app.state.window_manager = window_manager
    app.state.stop_event = stop_event
    app.state.connection_manager = connection_manager
    app.state.generator_task = None
    app.state.broadcast_task = None

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

    # Always run the broadcast loop so WebSocket clients see pushes
    # even when the generator is disabled (unit tests rely on this).
    app.state.broadcast_task = asyncio.create_task(
        broadcast_loop(
            connection_manager,
            window_manager,
            config.ws_update_interval_seconds,
            stop_event,
        ),
        name="broadcast-loop",
    )

    try:
        yield
    finally:
        stop_event.set()
        for task_attr in ("generator_task", "broadcast_task"):
            task = getattr(app.state, task_attr, None)
            if task is None:
                continue
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
                # Don't let background task shutdown errors mask a clean exit.
                pass


app = FastAPI(title="Sliding Window Analytics Engine", lifespan=lifespan)

_DASHBOARD_PATH = Path(__file__).parent / "templates" / "dashboard.html"


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    """Serve the Chart.js dashboard HTML.

    The template lives next to this module under ``templates/`` so it
    is picked up by the Dockerfile's ``COPY src/ src/`` line without
    any extra wiring.
    """
    try:
        html = _DASHBOARD_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:  # pragma: no cover - defensive
        raise HTTPException(status_code=500, detail="dashboard template missing")
    return HTMLResponse(content=html)


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


@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket) -> None:
    """Accept a dashboard client and keep it subscribed to broadcasts.

    The broadcast loop owned by the lifespan is doing all the work —
    this handler just tracks the client lifecycle. We call
    ``receive_text`` in a loop purely to detect disconnects (the client
    is expected to ping periodically; any inbound frame is discarded).
    """
    manager: ConnectionManager = websocket.app.state.connection_manager
    await manager.connect(websocket)
    try:
        while True:
            # Discard any inbound frames (keepalive pings, etc).
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        # Any other receive-side error is treated as a disconnect — we
        # never want a single misbehaving client to tear down the loop.
        pass
    finally:
        await manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn

    config = get_config()
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=config.api_port,
        log_level="info",
    )
