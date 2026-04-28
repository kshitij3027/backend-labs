from datetime import UTC, datetime

from src.schemas.search import SearchRequest
from src.services.cache import CACHE_KEY_PREFIX, canonical_key, canonical_request_payload


def test_cache_key_has_prefix_and_fixed_digest_length() -> None:
    req = SearchRequest(q="error", limit=10, offset=0)
    key = canonical_key(req)
    assert key.startswith(CACHE_KEY_PREFIX)
    digest = key[len(CACHE_KEY_PREFIX):]
    assert len(digest) == 16
    assert all(c in "0123456789abcdef" for c in digest)


def test_param_construction_order_does_not_change_key() -> None:
    a = SearchRequest(q="error", limit=10, offset=0)
    b = SearchRequest(q="error", offset=0, limit=10)
    assert canonical_key(a) == canonical_key(b)


def test_levels_list_order_does_not_change_key() -> None:
    a = SearchRequest(q="error", levels=["ERROR", "CRITICAL"])
    b = SearchRequest(q="error", levels=["CRITICAL", "ERROR"])
    assert canonical_key(a) == canonical_key(b)


def test_services_list_order_does_not_change_key() -> None:
    a = SearchRequest(q="error", services=["payment-service", "auth-service"])
    b = SearchRequest(q="error", services=["auth-service", "payment-service"])
    assert canonical_key(a) == canonical_key(b)


def test_none_fields_excluded_from_key() -> None:
    a = SearchRequest(q="error")
    b = SearchRequest(q="error", start_time=None)
    assert canonical_key(a) == canonical_key(b)


def test_different_limit_changes_key() -> None:
    a = SearchRequest(q="error", limit=10)
    b = SearchRequest(q="error", limit=11)
    assert canonical_key(a) != canonical_key(b)


def test_different_offset_changes_key() -> None:
    a = SearchRequest(q="error", offset=0)
    b = SearchRequest(q="error", offset=10)
    assert canonical_key(a) != canonical_key(b)


def test_different_query_changes_key() -> None:
    a = SearchRequest(q="error")
    b = SearchRequest(q="warning")
    assert canonical_key(a) != canonical_key(b)


def test_canonical_payload_excludes_none() -> None:
    req = SearchRequest(q="error")
    payload = canonical_request_payload(req)
    assert "start_time" not in payload
    assert "end_time" not in payload
    assert "levels" not in payload
    assert "services" not in payload
    assert payload["q"] == "error"


def test_time_range_fields_change_key() -> None:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = datetime(2025, 1, 2, tzinfo=UTC)
    a = SearchRequest(q="error")
    b = SearchRequest(q="error", start_time=start, end_time=end)
    assert canonical_key(a) != canonical_key(b)
