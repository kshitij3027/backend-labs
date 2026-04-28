import logging
import time
from typing import Any

from elasticsearch import AsyncElasticsearch

from src.schemas.logs import LogEntry
from src.schemas.search import (
    Aggregations,
    LevelBucket,
    Pagination,
    SearchRequest,
    SearchResponse,
    ServiceBucket,
    TimelineBucket,
)
from src.services.query_builder import build_es_body

logger = logging.getLogger(__name__)


class SearchService:
    def __init__(self, es: AsyncElasticsearch, index: str) -> None:
        self.es = es
        self.index = index

    async def search(self, req: SearchRequest) -> SearchResponse:
        body = build_es_body(req)

        kwargs: dict[str, Any] = {
            "index": self.index,
            "query": body["query"],
            "aggs": body["aggs"],
            "sort": body["sort"],
            "from_": body.get("from", 0),
            "size": body["size"],
            "track_total_hits": True,
        }
        if "_source" in body:
            kwargs["source"] = body["_source"]

        t0 = time.perf_counter()
        result = await self.es.search(**kwargs)
        elapsed_ms = (time.perf_counter() - t0) * 1000

        hits = result.get("hits", {}) or {}
        raw_hits = hits.get("hits", []) or []
        results: list[LogEntry] = []
        for hit in raw_hits:
            source = hit.get("_source") or {}
            results.append(
                LogEntry(
                    id=hit["_id"],
                    score=hit.get("_score"),
                    **source,
                )
            )

        total_value = (hits.get("total") or {}).get("value", 0)

        agg_payload = result.get("aggregations") or {}
        level_buckets = (agg_payload.get("levels") or {}).get("buckets") or []
        service_buckets = (agg_payload.get("services") or {}).get("buckets") or []
        timeline_buckets = (agg_payload.get("timeline") or {}).get("buckets") or []

        aggregations = Aggregations(
            levels=[
                LevelBucket(key=str(b["key"]), doc_count=int(b["doc_count"]))
                for b in level_buckets
            ],
            services=[
                ServiceBucket(key=str(b["key"]), doc_count=int(b["doc_count"]))
                for b in service_buckets
            ],
            timeline=[
                TimelineBucket(
                    key_as_string=str(b.get("key_as_string") or b.get("key")),
                    doc_count=int(b["doc_count"]),
                )
                for b in timeline_buckets
            ],
        )

        pagination = Pagination(
            offset=req.offset,
            limit=req.limit,
            has_more=(req.offset + req.limit) < int(total_value),
        )

        logger.info(
            "search executed q=%r total_hits=%d elapsed_ms=%.2f",
            req.q,
            int(total_value),
            elapsed_ms,
        )

        return SearchResponse(
            query=req.q,
            total_hits=int(total_value),
            execution_time_ms=round(elapsed_ms, 2),
            cache_hit=False,
            results=results,
            pagination=pagination,
            aggregations=aggregations,
        )
