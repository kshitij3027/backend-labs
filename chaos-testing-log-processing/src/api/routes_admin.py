"""Admin / kill-switch endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request

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


@router.get("/circuit-breaker-state")
async def circuit_breaker_state(request: Request) -> dict:
    """Return the current SafetySupervisor circuit-breaker state.

    503 if the supervisor wasn't wired into ``app.state`` --- this
    should never happen in production (lifespan installs it) but the
    explicit error keeps unit tests honest.
    """
    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is None:
        raise HTTPException(status_code=503, detail="supervisor not wired")
    return supervisor.state.to_dict()


@router.post("/circuit-breaker/reset")
async def circuit_breaker_reset(request: Request) -> dict:
    """Clear the tripped circuit-breaker state (operator action).

    The supervisor preserves ``total_trips`` across resets so the
    historical counter survives.
    """
    supervisor = getattr(request.app.state, "supervisor", None)
    if supervisor is None:
        raise HTTPException(status_code=503, detail="supervisor not wired")
    return supervisor.reset().to_dict()
