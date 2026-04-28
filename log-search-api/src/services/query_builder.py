from typing import Any

from src.schemas.search import SearchRequest, SortBy


def build_es_body(req: SearchRequest) -> dict[str, Any]:
    body: dict[str, Any] = {
        "from": req.offset,
        "size": req.limit,
        "track_total_hits": True,
    }

    must_clause: dict[str, Any]
    if req.q is not None and req.q.strip():
        must_clause = {
            "bool": {
                "should": [
                    {
                        "multi_match": {
                            "query": req.q,
                            "type": "best_fields",
                            "fields": ["message^3", "service_name^2"],
                            "fuzziness": "AUTO",
                            "tie_breaker": 0.3,
                        }
                    },
                    {
                        "query_string": {
                            "query": req.q,
                            "fields": ["content.*"],
                            "lenient": True,
                            "default_operator": "AND",
                        }
                    },
                ],
                "minimum_should_match": 1,
            }
        }
    else:
        must_clause = {"match_all": {}}

    filters: list[dict[str, Any]] = []

    if req.start_time is not None or req.end_time is not None:
        range_body: dict[str, Any] = {}
        if req.start_time is not None:
            range_body["gte"] = req.start_time.isoformat()
        if req.end_time is not None:
            range_body["lte"] = req.end_time.isoformat()
        filters.append({"range": {"timestamp": range_body}})

    if req.levels:
        filters.append({"terms": {"level": req.levels}})

    if req.services:
        filters.append({"terms": {"service_name": req.services}})

    bool_query: dict[str, Any] = {"must": [must_clause]}
    if filters:
        bool_query["filter"] = filters

    body["query"] = {"bool": bool_query}

    if req.sort_by == SortBy.RELEVANCE:
        body["sort"] = [
            {"_score": {"order": req.sort_order.value}},
            {"timestamp": {"order": "desc"}},
        ]
    else:
        body["sort"] = [{"timestamp": {"order": req.sort_order.value}}]

    body["aggs"] = {
        "levels": {"terms": {"field": "level", "size": 10}},
        "services": {"terms": {"field": "service_name", "size": 20}},
        "timeline": {
            "date_histogram": {
                "field": "timestamp",
                "calendar_interval": "1h",
                "min_doc_count": 1,
            }
        },
    }

    if not req.include_content:
        body["_source"] = {"excludes": ["content"]}

    return body
