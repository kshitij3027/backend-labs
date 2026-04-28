from datetime import UTC, datetime

from src.schemas.search import SearchRequest, SortBy, SortOrder
from src.services.query_builder import build_es_body


def test_bare_query_uses_multi_match_with_fuzziness_auto() -> None:
    req = SearchRequest(q="error")
    body = build_es_body(req)

    bool_block = body["query"]["bool"]
    must = bool_block["must"]
    assert len(must) == 1
    should = must[0]["bool"]["should"]
    assert must[0]["bool"]["minimum_should_match"] == 1

    fuzzy = next(c["multi_match"] for c in should if "multi_match" in c)
    assert fuzzy["query"] == "error"
    assert fuzzy["type"] == "best_fields"
    assert fuzzy["fuzziness"] == "AUTO"
    assert fuzzy["fields"] == ["message^3", "service_name^2"]
    assert fuzzy["tie_breaker"] == 0.3

    content_qs = next(c["query_string"] for c in should if "query_string" in c)
    assert content_qs["query"] == "error"
    assert content_qs["fields"] == ["content.*"]
    assert content_qs["lenient"] is True

    assert "filter" not in bool_block


def test_query_with_level_filter_adds_terms_filter() -> None:
    req = SearchRequest(q="error", levels=["ERROR"])
    body = build_es_body(req)

    bool_block = body["query"]["bool"]
    assert "filter" in bool_block
    filters = bool_block["filter"]
    assert {"terms": {"level": ["ERROR"]}} in filters


def test_query_with_time_range_and_service_filter() -> None:
    start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    end = datetime(2024, 1, 2, 0, 0, 0, tzinfo=UTC)
    req = SearchRequest(
        q="error",
        start_time=start,
        end_time=end,
        services=["payment-service"],
    )
    body = build_es_body(req)

    filters = body["query"]["bool"]["filter"]
    range_filters = [f for f in filters if "range" in f]
    assert len(range_filters) == 1
    range_body = range_filters[0]["range"]["timestamp"]
    assert range_body["gte"] == start.isoformat()
    assert range_body["lte"] == end.isoformat()

    terms_filters = [f for f in filters if "terms" in f]
    service_filter = [f for f in terms_filters if "service_name" in f["terms"]]
    assert len(service_filter) == 1
    assert service_filter[0]["terms"]["service_name"] == ["payment-service"]


def test_empty_query_uses_match_all_with_filter() -> None:
    req = SearchRequest(levels=["ERROR"])
    body = build_es_body(req)

    bool_block = body["query"]["bool"]
    must = bool_block["must"]
    assert len(must) == 1
    assert "match_all" in must[0]
    assert {"terms": {"level": ["ERROR"]}} in bool_block["filter"]


def test_sort_by_timestamp_asc_uses_only_timestamp_sort() -> None:
    req = SearchRequest(q="error", sort_by=SortBy.TIMESTAMP, sort_order=SortOrder.ASC)
    body = build_es_body(req)

    assert body["sort"] == [{"timestamp": {"order": "asc"}}]


def test_sort_by_relevance_includes_secondary_timestamp_sort() -> None:
    req = SearchRequest(q="error")
    body = build_es_body(req)

    assert body["sort"] == [
        {"_score": {"order": "desc"}},
        {"timestamp": {"order": "desc"}},
    ]


def test_include_content_false_excludes_content_source() -> None:
    req = SearchRequest(q="error", include_content=False)
    body = build_es_body(req)

    assert body["_source"] == {"excludes": ["content"]}


def test_include_content_true_omits_source_excludes() -> None:
    req = SearchRequest(q="error", include_content=True)
    body = build_es_body(req)

    assert "_source" not in body


def test_offset_and_limit_translate_to_from_and_size() -> None:
    req = SearchRequest(q="error", offset=20, limit=10)
    body = build_es_body(req)

    assert body["from"] == 20
    assert body["size"] == 10
    assert body["track_total_hits"] is True


def test_aggs_block_has_levels_services_timeline() -> None:
    req = SearchRequest()
    body = build_es_body(req)

    aggs = body["aggs"]
    assert aggs["levels"] == {"terms": {"field": "level", "size": 10}}
    assert aggs["services"] == {"terms": {"field": "service_name", "size": 20}}
    assert aggs["timeline"]["date_histogram"]["calendar_interval"] == "1h"
    assert aggs["timeline"]["date_histogram"]["field"] == "timestamp"
    assert aggs["timeline"]["date_histogram"]["min_doc_count"] == 1


def test_levels_normalization_uppercases_values() -> None:
    req = SearchRequest(levels=["error", "WaRn"])
    body = build_es_body(req)
    filters = body["query"]["bool"]["filter"]
    assert {"terms": {"level": ["ERROR", "WARN"]}} in filters


def test_only_start_time_set_omits_lte_bound() -> None:
    start = datetime(2024, 1, 1, tzinfo=UTC)
    req = SearchRequest(start_time=start)
    body = build_es_body(req)

    range_block = body["query"]["bool"]["filter"][0]["range"]["timestamp"]
    assert range_block == {"gte": start.isoformat()}


def test_only_end_time_set_omits_gte_bound() -> None:
    end = datetime(2024, 1, 1, tzinfo=UTC)
    req = SearchRequest(end_time=end)
    body = build_es_body(req)

    range_block = body["query"]["bool"]["filter"][0]["range"]["timestamp"]
    assert range_block == {"lte": end.isoformat()}
