"""Health router — liveness probe for Docker + ``start.sh``.

Kept in its own module (even though it's a single route) so the
concerns stay cleanly separated: ``routes_logs.py`` owns ingest,
``routes_search.py`` owns query, and this file owns infrastructure.
The compose healthcheck and ``tests/test_bootstrap.py`` both assert
the exact ``{"status": "ok"}`` JSON shape, so the response model is
deliberately locked to that single literal state.
"""

from fastapi import APIRouter

from src.models import HealthResponse


router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Liveness probe used by Docker and the ``start.sh`` wait loop."""
    return HealthResponse()
