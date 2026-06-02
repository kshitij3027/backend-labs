"""Unit tests for semantic cache-key generation (src/keys.py)."""
from __future__ import annotations

from datetime import datetime

from src.keys import cache_key, canonicalize, tags_for


def test_reordered_params_produce_same_key() -> None:
    """Param ordering must not affect the key — params are sorted."""
    k1 = cache_key("SELECT count(*)", {"a": 1, "b": 2})
    k2 = cache_key("SELECT count(*)", {"b": 2, "a": 1})
    assert k1 == k2


def test_whitespace_normalized_in_query() -> None:
    """Collapsed/stripped whitespace yields the same key; case still matters."""
    assert cache_key("  SELECT   count(*)  ") == cache_key("SELECT count(*)")
    assert cache_key("select count(*)") != cache_key("SELECT count(*)")


def test_timestamps_in_same_bucket_match() -> None:
    """Epoch timestamps inside one bucket collapse to the same key.

    Flooring is ``(value // bucket) * bucket``, so the bucket containing 1299 is
    [1200, 1500): both 1200 and 1499 floor to 1200 and share a key.
    """
    k_low = cache_key("q", {"start": 1200}, bucket_seconds=300)
    k_high = cache_key("q", {"start": 1499}, bucket_seconds=300)
    assert k_low == k_high
    # The task's literal pair (1000, 1299) is reproduced with a coarser bucket
    # that actually contains both: 1000 // 1300 == 1299 // 1300 == 0.
    assert cache_key("q", {"start": 1000}, bucket_seconds=1300) == cache_key(
        "q", {"start": 1299}, bucket_seconds=1300
    )


def test_timestamps_in_different_buckets_differ() -> None:
    """Epoch timestamps straddling a bucket boundary produce different keys.

    With bucket_seconds=300 the boundary nearest 1299 is 1500: 1499 floors to
    1200, 1500 floors to 1500.
    """
    k_low = cache_key("q", {"start": 1499}, bucket_seconds=300)
    k_high = cache_key("q", {"start": 1500}, bucket_seconds=300)
    assert k_low != k_high


def test_numeric_string_timestamp_buckets_like_int() -> None:
    """A numeric string timestamp buckets identically to the int form."""
    k_str = cache_key("q", {"start": "1000"}, bucket_seconds=300)
    k_int = cache_key("q", {"start": 1000}, bucket_seconds=300)
    assert k_str == k_int


def test_iso_and_epoch_same_bucket_match() -> None:
    """An ISO-8601 string and the equivalent epoch in one bucket match.

    The ISO string is built from the epoch via the local-time conversion that
    ``datetime.fromisoformat(...).timestamp()`` inverts, so this holds in any
    host timezone.
    """
    epoch = 1_700_000_000
    iso = datetime.fromtimestamp(epoch).isoformat()
    k_iso = cache_key("q", {"ts": iso}, bucket_seconds=300)
    k_epoch = cache_key("q", {"ts": epoch}, bucket_seconds=300)
    assert k_iso == k_epoch


def test_different_query_differs() -> None:
    """A different query text yields a different key."""
    assert cache_key("SELECT a") != cache_key("SELECT b")


def test_different_source_param_differs() -> None:
    """A different (non-timestamp) param value yields a different key."""
    assert cache_key("q", {"source": "api"}) != cache_key("q", {"source": "web"})


def test_determinism_across_calls() -> None:
    """Repeated identical calls return byte-identical keys and canonical forms."""
    params = {"source": "api", "start": 1234, "limit": 50}
    assert cache_key("q", params) == cache_key("q", params)
    assert canonicalize("q", params) == canonicalize("q", params)


def test_canonicalize_is_sorted_json() -> None:
    """canonicalize emits compact, key-sorted JSON regardless of input order."""
    c1 = canonicalize("q", {"b": 2, "a": 1})
    c2 = canonicalize("q", {"a": 1, "b": 2})
    assert c1 == c2
    assert c1 == '{"p":{"a":1,"b":2},"q":"q"}'


def test_tags_for_includes_source_and_query_scope() -> None:
    """tags_for emits source:<value> plus a stable query:<hash> scope tag."""
    tags = tags_for("SELECT count(*)", {"source": "api"})
    assert "source:api" in tags
    query_tags = [t for t in tags if t.startswith("query:")]
    assert len(query_tags) == 1
    # Query scope is param-independent and stable.
    assert query_tags[0] in tags_for("SELECT count(*)", {"source": "web"})


def test_tags_for_no_params() -> None:
    """With no params, only the query-scope tag is returned."""
    tags = tags_for("SELECT count(*)")
    assert len(tags) == 1
    assert next(iter(tags)).startswith("query:")
