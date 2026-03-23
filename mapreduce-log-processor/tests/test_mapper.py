"""Tests for map functions, mapper logic, and combiner."""

import pytest

# Import map function modules to trigger registration
import src.mapfunctions.error_code  # noqa: F401
import src.mapfunctions.reducers  # noqa: F401
import src.mapfunctions.url_path  # noqa: F401
import src.mapfunctions.word_count  # noqa: F401
from src.mapfunctions.registry import get_map_fn
from src.worker.combiner import combine


class TestWordCountMap:
    """Tests for the word_count map function."""

    def test_basic_word_count(self):
        fn = get_map_fn("word_count")
        log_line = {"message": "hello world"}
        result = list(fn(log_line))
        assert ("hello", 1) in result
        assert ("world", 1) in result
        assert len(result) == 2

    def test_punctuation_stripped(self):
        fn = get_map_fn("word_count")
        log_line = {"message": "hello, world! test."}
        result = list(fn(log_line))
        keys = [k for k, v in result]
        assert "hello" in keys
        assert "world" in keys
        assert "test" in keys

    def test_case_normalized(self):
        fn = get_map_fn("word_count")
        log_line = {"message": "Hello WORLD Test"}
        result = list(fn(log_line))
        keys = [k for k, v in result]
        assert "hello" in keys
        assert "world" in keys
        assert "test" in keys

    def test_empty_message(self):
        fn = get_map_fn("word_count")
        log_line = {"message": ""}
        result = list(fn(log_line))
        assert result == []

    def test_missing_message_field(self):
        fn = get_map_fn("word_count")
        log_line = {"level": "INFO"}
        result = list(fn(log_line))
        assert result == []

    def test_realistic_log_message(self):
        fn = get_map_fn("word_count")
        log_line = {"message": "Request processed successfully"}
        result = list(fn(log_line))
        assert len(result) == 3
        assert ("request", 1) in result
        assert ("processed", 1) in result
        assert ("successfully", 1) in result


class TestErrorCodeMap:
    """Tests for the error_code map function."""

    def test_with_error_code(self):
        fn = get_map_fn("error_code")
        log_line = {"error_code": "500"}
        result = list(fn(log_line))
        assert result == [("500", 1)]

    def test_numeric_error_code(self):
        fn = get_map_fn("error_code")
        log_line = {"error_code": 404}
        result = list(fn(log_line))
        assert result == [("404", 1)]

    def test_no_error_code(self):
        fn = get_map_fn("error_code")
        log_line = {"message": "all good"}
        result = list(fn(log_line))
        assert result == []

    def test_none_error_code(self):
        fn = get_map_fn("error_code")
        log_line = {"error_code": None}
        result = list(fn(log_line))
        assert result == []


class TestUrlPathMap:
    """Tests for the url_path map function."""

    def test_with_url(self):
        fn = get_map_fn("url_path")
        log_line = {"url": "/api/users"}
        result = list(fn(log_line))
        assert result == [("/api/users", 1)]

    def test_no_url(self):
        fn = get_map_fn("url_path")
        log_line = {"message": "no url here"}
        result = list(fn(log_line))
        assert result == []

    def test_none_url(self):
        fn = get_map_fn("url_path")
        log_line = {"url": None}
        result = list(fn(log_line))
        assert result == []


class TestCombiner:
    """Tests for the combiner (local pre-reducer)."""

    def test_sum_combiner_reduces_pairs(self):
        """Combiner with sum reducer should aggregate values by key."""
        kv_pairs = [("a", 1), ("a", 1), ("b", 1)]
        result = combine(kv_pairs, "sum")
        result_dict = dict(result)
        assert result_dict["a"] == "2"
        assert result_dict["b"] == "1"
        assert len(result) == 2

    def test_count_combiner(self):
        """Combiner with count reducer should count values per key."""
        kv_pairs = [("x", 1), ("x", 1), ("x", 1), ("y", 1)]
        result = combine(kv_pairs, "count")
        result_dict = dict(result)
        assert result_dict["x"] == "3"
        assert result_dict["y"] == "1"

    def test_empty_input_returns_empty(self):
        """Combiner with empty input should return empty list."""
        result = combine([], "sum")
        assert result == []

    def test_single_key_single_value(self):
        """Combiner with a single pair should return that pair reduced."""
        kv_pairs = [("only", 5)]
        result = combine(kv_pairs, "sum")
        assert len(result) == 1
        assert result[0] == ("only", "5")

    def test_many_duplicate_keys(self):
        """Combiner should handle many duplicate keys efficiently."""
        kv_pairs = [("error_404", 1)] * 100 + [("error_500", 1)] * 50
        result = combine(kv_pairs, "sum")
        result_dict = dict(result)
        assert result_dict["error_404"] == "100"
        assert result_dict["error_500"] == "50"
        assert len(result) == 2


class TestHashPartitioning:
    """Test that hash partitioning distributes keys across reducers."""

    def test_distribution_across_reducers(self):
        """Keys should be distributed across multiple reducer buckets."""
        num_reducers = 4
        keys = ["/api/users", "/api/orders", "/api/products", "/health", "/login",
                "/api/payments", "/api/search", "/api/auth"]
        buckets: dict[int, list[str]] = {i: [] for i in range(num_reducers)}

        for key in keys:
            reducer_id = hash(key) % num_reducers
            buckets[reducer_id].append(key)

        # At least 2 different buckets should have keys (probabilistic but very likely)
        non_empty = sum(1 for b in buckets.values() if b)
        assert non_empty >= 2, "Hash partitioning should distribute keys across multiple reducers"

    def test_same_key_same_reducer(self):
        """The same key should always go to the same reducer."""
        num_reducers = 4
        key = "/api/users"
        reducer_ids = set()
        for _ in range(100):
            reducer_ids.add(hash(key) % num_reducers)
        assert len(reducer_ids) == 1, "Same key must always map to same reducer"

    def test_all_reducer_ids_in_range(self):
        """All reducer IDs should be in [0, num_reducers)."""
        num_reducers = 3
        keys = [f"key_{i}" for i in range(100)]
        for key in keys:
            rid = hash(key) % num_reducers
            assert 0 <= rid < num_reducers
