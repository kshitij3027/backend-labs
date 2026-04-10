"""FastAPI entrypoint for the sliding-window analytics engine.

Commit 1 scope: minimal app exposing only the health endpoint. Window
management, ingestion, WebSocket streaming, and the dashboard will be added in
subsequent commits.
"""

from __future__ import annotations

from fastapi import FastAPI

from src.config import get_config

app = FastAPI(title="Sliding Window Analytics Engine")


@app.get("/api/health")
async def health() -> dict[str, object]:
    """Liveness/readiness probe.

    `active_windows` is hard-coded to 0 in Commit 1 — it becomes dynamic once
    the `WindowManager` is wired in Commit 4.
    """
    return {"status": "healthy", "active_windows": 0}


if __name__ == "__main__":
    import uvicorn

    config = get_config()
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=config.api_port,
        log_level="info",
    )
