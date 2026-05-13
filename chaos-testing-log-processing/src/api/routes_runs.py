"""Run-control endpoints (start / abort / fetch)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.experiments import ExperimentRun
from ..persistence.repo import ExperimentDefinitionRepo, ExperimentRunRepo
from .dependencies import get_db, get_run_manager
from .schemas import AbortRunResponse, StartRunResponse

router = APIRouter(tags=["runs"])


@router.post("/experiments/{experiment_id}/run", response_model=StartRunResponse)
async def start_run(
    experiment_id: str,
    session: AsyncSession = Depends(get_db),
    run_manager=Depends(get_run_manager),
) -> StartRunResponse:
    defn = await ExperimentDefinitionRepo(session).get(experiment_id)
    if defn is None:
        raise HTTPException(status_code=404, detail="experiment not found")
    run = await run_manager.start(defn)
    return StartRunResponse(
        run_id=run.run_id,
        experiment_id=run.experiment_id,
        status=run.status,
        started_at=run.started_at,
        dry_run=run_manager.dry_run,
    )


@router.post("/experiments/{experiment_id}/abort", response_model=AbortRunResponse)
async def abort_experiment_runs(
    experiment_id: str,
    session: AsyncSession = Depends(get_db),
    run_manager=Depends(get_run_manager),
) -> AbortRunResponse:
    """Abort every active run for the given experiment id."""
    aborted_any = False
    active_runs = await ExperimentRunRepo(session).list_by_experiment(experiment_id)
    for run in active_runs:
        if await run_manager.abort_run(run.run_id):
            aborted_any = True
    # If nothing was active in DB, also check the in-memory map (most cases).
    if not aborted_any:
        for rid in run_manager.active_run_ids():
            in_mem = run_manager.get_run(rid)
            if in_mem is not None and in_mem.experiment_id == experiment_id:
                if await run_manager.abort_run(rid):
                    aborted_any = True
    return AbortRunResponse(aborted=aborted_any, run_id=experiment_id)


@router.get("/runs/{run_id}", response_model=ExperimentRun)
async def get_run(
    run_id: str,
    session: AsyncSession = Depends(get_db),
    run_manager=Depends(get_run_manager),
) -> ExperimentRun:
    # Prefer the in-memory record (most current), fall back to DB.
    in_mem = run_manager.get_run(run_id)
    if in_mem is not None:
        return in_mem
    db = await ExperimentRunRepo(session).get(run_id)
    if db is None:
        raise HTTPException(status_code=404, detail="run not found")
    return db
