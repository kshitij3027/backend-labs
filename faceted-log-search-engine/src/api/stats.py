"""HTTP endpoint for server-side diagnostics.

``GET /api/stats`` returns a snapshot of:

* total logs stored (``SELECT COUNT(*) FROM logs``)
* per-facet cardinality (``SELECT COUNT(DISTINCT <dim>) FROM logs``
  for each dim in ``FACET_DIMS``)
* Redis cache counters (hits, misses, errors, derived hit rate)
* ``redis_reachable`` — a live PING at request time

No caching here — the endpoint is already fast enough on a single-
table count + five distinct-counts, and we want fresh numbers for
ops dashboards.
"""

from __future__ import annotations

import logging
from typing import Dict

from fastapi import APIRouter, Request

from src.models import CacheStatsModel, StatsResponse
from src.search.query_builder import FACET_DIMS
from src.storage import redis_cache, sqlite_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["stats"])


async def _facet_cardinality(db) -> Dict[str, int]:
    """Return ``dim -> COUNT(DISTINCT dim)`` for every facet dim.

    ``dim`` is trusted (sourced from ``FACET_DIMS``) so it is safe to
    interpolate into the SQL text. We run one query per dim — SQLite's
    index on each dim makes this cheap, and five sequential SELECTs
    is easier to reason about than a single multi-dim aggregate.
    """
    out: Dict[str, int] = {}
    for dim in FACET_DIMS:
        sql = f"SELECT COUNT(DISTINCT {dim}) FROM logs"
        async with db.execute(sql) as cur:
            row = await cur.fetchone()
        out[dim] = int(row[0]) if row and row[0] is not None else 0
    return out


@router.get("/stats", response_model=StatsResponse)
async def get_stats(request: Request) -> StatsResponse:
    """Return a snapshot of server statistics and cache counters."""
    pool = request.app.state.db_pool
    redis_client = getattr(request.app.state, "redis", None)

    # Run total + cardinality reads through a pooled read connection
    # so the stats endpoint doesn't compete with the search hot path
    # on the single write handle.
    async with pool.read() as db:
        total = await sqlite_store.count_logs(db)
        cardinality = await _facet_cardinality(db)

    # Cache counters are process-local; read the shared dataclass.
    raw_stats = redis_cache.stats
    denom = raw_stats.hits + raw_stats.misses
    hit_rate = (raw_stats.hits / denom) if denom > 0 else None

    cache_model = CacheStatsModel(
        hits=raw_stats.hits,
        misses=raw_stats.misses,
        errors=raw_stats.errors,
        hit_rate=hit_rate,
    )

    reachable = await redis_cache.ping(redis_client)

    return StatsResponse(
        total_logs=total,
        facet_cardinality=cardinality,
        cache=cache_model,
        redis_reachable=reachable,
    )
