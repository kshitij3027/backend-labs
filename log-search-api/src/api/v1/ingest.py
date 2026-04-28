import logging
from typing import Annotated

from elasticsearch import AsyncElasticsearch
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from src.auth.dependencies import RequireUser
from src.clients.elasticsearch import get_es
from src.config import Settings, get_settings
from src.middleware.rate_limit import DEFAULT_LIMIT, limiter
from src.schemas.logs import (
    BulkIngestRequest,
    BulkIngestResponse,
    IngestResponse,
    LogEntry,
    LogIngestRequest,
)
from src.services.ingest import bulk_index_logs, get_log_by_id, index_log

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/logs", tags=["logs"])


@router.post(
    "",
    response_model=IngestResponse,
    status_code=status.HTTP_201_CREATED,
)
@limiter.limit(DEFAULT_LIMIT)
async def ingest_log(
    request: Request,
    response: Response,
    payload: LogIngestRequest,
    current_user: RequireUser,
    settings: Annotated[Settings, Depends(get_settings)],
    es: Annotated[AsyncElasticsearch, Depends(get_es)],
) -> IngestResponse:
    result = await index_log(es, settings.ELASTICSEARCH_INDEX, payload)
    logger.info(
        "indexed log id=%s service=%s level=%s user=%s",
        result.id,
        payload.service_name,
        payload.level,
        current_user,
    )
    return result


@router.post(
    "/bulk",
    response_model=BulkIngestResponse,
    status_code=status.HTTP_200_OK,
)
@limiter.limit(DEFAULT_LIMIT)
async def ingest_logs_bulk(
    request: Request,
    response: Response,
    payload: BulkIngestRequest,
    current_user: RequireUser,
    settings: Annotated[Settings, Depends(get_settings)],
    es: Annotated[AsyncElasticsearch, Depends(get_es)],
) -> BulkIngestResponse:
    result = await bulk_index_logs(es, settings.ELASTICSEARCH_INDEX, payload.entries)
    logger.info(
        "bulk indexed total=%d created=%d errors=%d user=%s",
        result.total,
        result.created,
        result.errors,
        current_user,
    )
    return result


@router.get(
    "/{doc_id}",
    response_model=LogEntry,
)
@limiter.limit(DEFAULT_LIMIT)
async def get_log(
    request: Request,
    response: Response,
    doc_id: str,
    current_user: RequireUser,
    settings: Annotated[Settings, Depends(get_settings)],
    es: Annotated[AsyncElasticsearch, Depends(get_es)],
) -> LogEntry:
    entry = await get_log_by_id(es, settings.ELASTICSEARCH_INDEX, doc_id)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"log {doc_id} not found",
        )
    return entry
