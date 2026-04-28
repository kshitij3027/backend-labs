import logging
from datetime import UTC, datetime
from typing import Annotated

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends, Request, Response

from src.auth.dependencies import RequireUser
from src.clients.elasticsearch import get_es
from src.config import Settings, get_settings
from src.middleware.rate_limit import DEFAULT_LIMIT, limiter
from src.schemas.stats import CacheStats, IndexStats, StatsResponse
from src.services.cache import CacheCounters

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/stats", tags=["stats"])


async def _read_index_stats(es: AsyncElasticsearch, index: str) -> IndexStats:
    try:
        result = await es.indices.stats(index=index)
        all_block = (result.get("_all") or {}).get("primaries") or {}
        docs = (all_block.get("docs") or {}).get("count", 0)
        store = (all_block.get("store") or {}).get("size_in_bytes", 0)
        return IndexStats(index=index, doc_count=int(docs), size_in_bytes=int(store))
    except Exception as exc:
        logger.warning("index stats unavailable for %s: %s", index, exc)
        return IndexStats(index=index, doc_count=0, size_in_bytes=0)


@router.get("", response_model=StatsResponse)
@limiter.limit(DEFAULT_LIMIT)
async def get_stats(
    request: Request,
    response: Response,
    current_user: RequireUser,
    settings: Annotated[Settings, Depends(get_settings)],
    es: Annotated[AsyncElasticsearch, Depends(get_es)],
) -> StatsResponse:
    counters: CacheCounters | None = getattr(request.app.state, "cache_counters", None)
    if counters is None:
        counters = CacheCounters()
    cache_dict = counters.as_dict()
    cache_stats = CacheStats(**cache_dict)

    index_stats = await _read_index_stats(es, settings.ELASTICSEARCH_INDEX)

    logger.info(
        "stats user=%s hits=%d misses=%d errors=%d index_docs=%d",
        current_user,
        cache_stats.hits,
        cache_stats.misses,
        cache_stats.errors,
        index_stats.doc_count,
    )

    return StatsResponse(
        cache=cache_stats,
        index=index_stats,
        timestamp=datetime.now(UTC),
    )
