"""HTTP route definitions.

Grows commit-by-commit:
- C9: /api/health.
- C10: /v1/audit/append, /v1/records, /v1/records/{seq}.
- C11: /v1/verify (full + range).
- C12: /api/stats.
- C13/C14: /v1/reports/{framework}.
- C15: / (dashboard) and /partials/*.
"""
from __future__ import annotations

import time

from fastapi import APIRouter

router = APIRouter()


@router.get("/api/health", tags=["health"])
async def health() -> dict[str, int | str]:
    """Liveness probe — used by docker healthcheck and external monitors."""
    return {"status": "healthy", "timestamp": int(time.time())}
