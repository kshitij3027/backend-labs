from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from src.api.dependencies import get_store
from src.benchmark.harness import diff_summaries
from src.store.run_store import RunStore

router = APIRouter(prefix="/api/compare", tags=["compare"])


@router.get("")
async def compare(
    a: str,
    b: str,
    store: Annotated[RunStore, Depends(get_store)],
) -> dict:
    baseline = store.get(a)
    optimized = store.get(b)
    if baseline is None or optimized is None:
        missing = [rid for rid, summary in [(a, baseline), (b, optimized)] if summary is None]
        raise HTTPException(status_code=404, detail=f"run(s) not found: {missing}")
    opt_name = optimized.optimization_name or "unknown"
    diff = diff_summaries(baseline, optimized, opt_name)
    return {
        "baseline": baseline.to_dict(),
        "optimized": optimized.to_dict(),
        "diff": diff.to_dict(),
    }
