from __future__ import annotations

from fastapi import APIRouter

from src.optimizations.registry import list_optimizations

router = APIRouter(prefix="/api/optimizations", tags=["optimizations"])


@router.get("")
async def list_all() -> list[dict]:
    return list_optimizations()
