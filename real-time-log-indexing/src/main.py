"""FastAPI app entrypoint — placeholder stub for Commit 1.

Subsequent commits will replace this module with the full lifespan
(index load, Redis consumer, merger, WS broadcast) wiring. For now
we expose just a bare FastAPI instance with ``GET /health`` so that:

* ``docker compose build`` succeeds (the CMD target imports cleanly).
* ``start.sh`` can poll ``/health`` to detect when the container is up.
* The test harness can import ``src.main:app`` without side effects.
"""

from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(title="real-time-log-indexing", version="0.1.0")


@app.get("/health")
async def health() -> dict[str, str]:
    """Minimal liveness probe used by the compose healthcheck."""
    return {"status": "ok"}
