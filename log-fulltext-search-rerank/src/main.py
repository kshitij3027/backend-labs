"""FastAPI application entry point.

Commit 01 scope: just the app object and a liveness ``/health``
endpoint. Subsequent commits will extend this module with a
``build_app`` factory, lifespan, and router wiring — keep the shape
minimal and expandable.
"""

from __future__ import annotations

from fastapi import FastAPI

app = FastAPI(title="log-fulltext-search-rerank")


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe used by Docker and the start.sh wait loop."""
    return {"status": "ok"}
