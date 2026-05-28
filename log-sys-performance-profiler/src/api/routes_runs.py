from __future__ import annotations

import asyncio
import uuid
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException

from src.api.dependencies import get_harness, get_runner, get_store
from src.api.schemas import RunSummaryOut, StartRunRequest, StartRunResponse
from src.benchmark.harness import BeforeAfterHarness
from src.loadgen.runner import LoadRunner, RunSummary
from src.logging_config import get_logger
from src.store.run_store import RunStore

router = APIRouter(prefix="/api/runs", tags=["runs"])
_logger = get_logger("api.routes_runs")


async def _execute_baseline(
    runner: LoadRunner, store: RunStore, body: StartRunRequest, run_id: str
) -> None:
    try:
        summary = await runner.run(
            label="baseline",
            log_count=body.log_count,
            concurrency=body.concurrency,
            seed=body.seed,
            optimization_name=None,
        )
        # Overwrite the runner-generated id with the one we returned to the client.
        summary = RunSummary(**{**summary.to_dict(), "run_id": run_id})
        store.put(summary)
    except Exception as exc:
        _logger.exception("baseline_run_failed", run_id=run_id, error=str(exc))


async def _execute_compare(
    harness: BeforeAfterHarness, body: StartRunRequest, run_id: str
) -> None:
    try:
        await harness.compare(
            optimization_name=body.optimization_name,
            log_count=body.log_count,
            concurrency=body.concurrency,
            seed=body.seed,
        )
        _logger.info("compare_run_complete", run_id=run_id)
    except Exception as exc:
        _logger.exception("compare_run_failed", run_id=run_id, error=str(exc))


@router.post("", response_model=StartRunResponse, status_code=202)
async def start_run(
    body: StartRunRequest,
    background: BackgroundTasks,
    runner: Annotated[LoadRunner, Depends(get_runner)],
    store: Annotated[RunStore, Depends(get_store)],
    harness: Annotated[BeforeAfterHarness, Depends(get_harness)],
) -> StartRunResponse:
    if body.optimization_name:
        run_id = uuid.uuid4().hex
        background.add_task(_execute_compare, harness, body, run_id)
        return StartRunResponse(run_id=run_id, mode="compare")

    run_id = uuid.uuid4().hex
    background.add_task(_execute_baseline, runner, store, body, run_id)
    return StartRunResponse(run_id=run_id, mode="baseline")


@router.get("", response_model=list[RunSummaryOut])
async def list_runs(
    limit: int = 50,
    store: Annotated[RunStore, Depends(get_store)] = None,
) -> list[RunSummaryOut]:
    return [RunSummaryOut(**s.to_dict()) for s in store.list(limit=limit)]


@router.get("/{run_id}", response_model=RunSummaryOut)
async def get_run(
    run_id: str,
    store: Annotated[RunStore, Depends(get_store)],
) -> RunSummaryOut:
    summary = store.get(run_id)
    if summary is None:
        raise HTTPException(status_code=404, detail=f"run not found: {run_id}")
    return RunSummaryOut(**summary.to_dict())
