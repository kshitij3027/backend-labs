from __future__ import annotations

from fastapi import APIRouter

from src.api.v1 import auth as auth_router
from src.api.v1 import health
from src.api.v1 import ingest as ingest_router
from src.config import get_settings

settings = get_settings()

router = APIRouter(prefix=settings.API_V1_PREFIX)
router.include_router(health.router)
router.include_router(auth_router.router)
router.include_router(ingest_router.router)

if settings.DEBUG_ENDPOINTS_ENABLED:
    from src.api.v1 import debug as debug_router

    router.include_router(debug_router.router)
