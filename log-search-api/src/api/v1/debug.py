from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(prefix="/_debug", tags=["debug"], include_in_schema=False)


@router.get("/boom")
async def boom() -> dict[str, str]:
    raise RuntimeError("boom for tests")
