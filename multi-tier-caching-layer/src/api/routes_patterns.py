"""The ``GET /patterns`` query-pattern analysis endpoint (project §3 Feature A).

Surfaces the heuristic :class:`~src.patterns.PatternEngine`'s view of recent
traffic: hour-of-day / day-of-week temporal histograms, per-source query counts,
and the ranked frecency-with-cost warming recommendations. The engine is the
single source of truth — this route is a thin projection of
:meth:`PatternEngine.analyze` plus :meth:`PatternEngine.recommendations` into the
:class:`~src.api.schemas.PatternReport` envelope the dashboard consumes.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends

from src.api.dependencies import get_patterns
from src.api.schemas import PatternReport
from src.patterns import PatternEngine

router = APIRouter(tags=["patterns"])


@router.get("/patterns", response_model=PatternReport)
async def query_patterns(
    patterns: Annotated[PatternEngine, Depends(get_patterns)],
) -> PatternReport:
    """Return temporal + per-source analysis and ranked warming recommendations.

    Folds the engine's ``analyze()`` output into a ``temporal`` block (hour-of-day
    and day-of-week histograms) alongside ``per_source`` counts and the
    ``total_observations`` window size, then attaches the top-N recommendations.
    """
    a = patterns.analyze()
    return PatternReport(
        temporal={
            "hour_of_day": a["hour_of_day"],
            "day_of_week": a["day_of_week"],
        },
        per_source=a["per_source"],
        total_observations=a["total_observations"],
        recommendations=patterns.recommendations(top_n=20),
    )
