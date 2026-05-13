"""Admin / kill-switch endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from .dependencies import get_run_manager
from .schemas import AbortAllResponse, DryRunResponse

router = APIRouter(prefix="/admin", tags=["admin"])


@router.post("/abort", response_model=AbortAllResponse)
async def admin_abort_all(run_manager=Depends(get_run_manager)) -> AbortAllResponse:
    count = await run_manager.abort_all()
    return AbortAllResponse(aborted_count=count)


@router.post("/dry-run", response_model=DryRunResponse)
async def admin_set_dry_run(
    enabled: bool, run_manager=Depends(get_run_manager)
) -> DryRunResponse:
    state = run_manager.set_dry_run(enabled)
    return DryRunResponse(dry_run=state)


@router.get("/dry-run", response_model=DryRunResponse)
async def admin_get_dry_run(run_manager=Depends(get_run_manager)) -> DryRunResponse:
    return DryRunResponse(dry_run=run_manager.dry_run)
