import logging
from datetime import datetime
from typing import Annotated

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends, Query, Request, Response

from src.auth.dependencies import RequireUser
from src.clients.elasticsearch import get_es
from src.config import Settings, get_settings
from src.middleware.rate_limit import DEFAULT_LIMIT, limiter
from src.schemas.search import SearchRequest, SearchResponse, SortBy, SortOrder
from src.services.cache import SearchCache, canonical_key
from src.services.search import SearchService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/logs", tags=["search"])


async def _execute_search(
    request: Request,
    es: AsyncElasticsearch,
    settings: Settings,
    payload: SearchRequest,
) -> SearchResponse:
    cache: SearchCache | None = getattr(request.app.state, "search_cache", None)
    key: str | None = None

    if cache is not None:
        key = canonical_key(payload)
        hit = await cache.get(key)
        if hit is not None:
            return hit

    service = SearchService(es, settings.ELASTICSEARCH_INDEX)
    result = await service.search(payload)

    if cache is not None and key is not None:
        await cache.set(key, result)

    return result


@router.post("/search", response_model=SearchResponse)
@limiter.limit(DEFAULT_LIMIT)
async def search_logs_post(
    request: Request,
    response: Response,
    payload: SearchRequest,
    current_user: RequireUser,
    settings: Annotated[Settings, Depends(get_settings)],
    es: Annotated[AsyncElasticsearch, Depends(get_es)],
) -> SearchResponse:
    result = await _execute_search(request, es, settings, payload)
    logger.info(
        "search post user=%s q=%r total=%d cache_hit=%s",
        current_user,
        payload.q,
        result.total_hits,
        result.cache_hit,
    )
    return result


@router.get("/search", response_model=SearchResponse)
@limiter.limit(DEFAULT_LIMIT)
async def search_logs_get(
    request: Request,
    response: Response,
    current_user: RequireUser,
    settings: Annotated[Settings, Depends(get_settings)],
    es: Annotated[AsyncElasticsearch, Depends(get_es)],
    q: Annotated[str | None, Query(max_length=512)] = None,
    start_time: Annotated[datetime | None, Query()] = None,
    end_time: Annotated[datetime | None, Query()] = None,
    levels: Annotated[list[str] | None, Query()] = None,
    services: Annotated[list[str] | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
    include_content: Annotated[bool, Query()] = True,
    sort_by: Annotated[SortBy, Query()] = SortBy.RELEVANCE,
    sort_order: Annotated[SortOrder, Query()] = SortOrder.DESC,
) -> SearchResponse:
    payload = SearchRequest(
        q=q,
        start_time=start_time,
        end_time=end_time,
        levels=levels,
        services=services,
        limit=limit,
        offset=offset,
        include_content=include_content,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    result = await _execute_search(request, es, settings, payload)
    logger.info(
        "search get user=%s q=%r total=%d cache_hit=%s",
        current_user,
        payload.q,
        result.total_hits,
        result.cache_hit,
    )
    return result
