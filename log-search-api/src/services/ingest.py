from __future__ import annotations

import logging
import uuid
from typing import Any

from elasticsearch import AsyncElasticsearch, BadRequestError, NotFoundError

from src.schemas.logs import (
    BulkIngestResponse,
    IngestResponse,
    LogEntry,
    LogIngestRequest,
)

logger = logging.getLogger(__name__)


INDEX_SETTINGS: dict[str, Any] = {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "5s",
}


INDEX_MAPPING: dict[str, Any] = {
    "properties": {
        "timestamp": {"type": "date"},
        "level": {"type": "keyword"},
        "service_name": {"type": "keyword"},
        "message": {"type": "text", "analyzer": "standard"},
        "content": {"type": "object", "dynamic": True},
    }
}


_MAX_ERROR_ITEMS = 20


async def ensure_index(es: AsyncElasticsearch, index: str) -> None:
    exists = await es.indices.exists(index=index)
    if bool(exists):
        return
    try:
        await es.indices.create(
            index=index,
            settings=INDEX_SETTINGS,
            mappings=INDEX_MAPPING,
        )
        logger.info("created elasticsearch index %s", index)
    except BadRequestError as exc:
        message = str(exc)
        if "resource_already_exists_exception" in message or "index_already_exists_exception" in message:
            logger.info("index %s already exists (race), skipping create", index)
            return
        raise


async def index_log(
    es: AsyncElasticsearch, index: str, entry: LogIngestRequest
) -> IngestResponse:
    doc_id = entry.id or uuid.uuid4().hex
    body = entry.model_dump(exclude={"id"}, mode="json")
    result = await es.index(index=index, id=doc_id, document=body, refresh="false")
    raw_result = result.get("result", "created")
    if raw_result not in ("created", "updated"):
        raw_result = "updated"
    return IngestResponse(id=doc_id, result=raw_result, index=index)


async def bulk_index_logs(
    es: AsyncElasticsearch, index: str, entries: list[LogIngestRequest]
) -> BulkIngestResponse:
    if not entries:
        return BulkIngestResponse(total=0, created=0, errors=0, error_items=[])

    operations: list[dict[str, Any]] = []
    for entry in entries:
        doc_id = entry.id or uuid.uuid4().hex
        operations.append({"index": {"_index": index, "_id": doc_id}})
        operations.append(entry.model_dump(exclude={"id"}, mode="json"))

    result = await es.bulk(operations=operations, refresh="false")

    created = 0
    errors = 0
    error_items: list[dict[str, Any]] = []
    items = result.get("items", []) or []
    for item in items:
        op = item.get("index") or item.get("create") or {}
        if op.get("error"):
            errors += 1
            if len(error_items) < _MAX_ERROR_ITEMS:
                error_items.append(
                    {
                        "id": op.get("_id"),
                        "status": op.get("status"),
                        "error": op.get("error"),
                    }
                )
            continue
        op_result = op.get("result")
        if op_result in ("created", "updated"):
            created += 1

    return BulkIngestResponse(
        total=len(entries),
        created=created,
        errors=errors,
        error_items=error_items,
    )


async def get_log_by_id(
    es: AsyncElasticsearch, index: str, doc_id: str
) -> LogEntry | None:
    try:
        res = await es.get(index=index, id=doc_id)
    except NotFoundError:
        return None
    source = res.get("_source") or {}
    return LogEntry(id=res["_id"], **source)
