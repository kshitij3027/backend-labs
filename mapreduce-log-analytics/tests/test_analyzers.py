"""Tests for the analyzer registry and built-in analyzers."""

import pytest

from src.analyzers.registry import (
    MAP_FUNCTIONS,
    REDUCE_FUNCTIONS,
    get_map_fn,
    get_reduce_fn,
    list_analyzers,
)
from src.analyzers.word_count import word_count_map, word_count_reduce
from src.analyzers.pattern_frequency import pattern_frequency_map
from src.analyzers.service_distribution import service_distribution_map
from src.engine import MapReduceEngine


class TestRegistry:
    def test_all_analyzers_registered(self):
        """All 3 analyzers should be registered."""
        assert "word_count" in MAP_FUNCTIONS
        assert "pattern_frequency" in MAP_FUNCTIONS
        assert "service_distribution" in MAP_FUNCTIONS
        assert "word_count" in REDUCE_FUNCTIONS
        assert "pattern_frequency" in REDUCE_FUNCTIONS
        assert "service_distribution" in REDUCE_FUNCTIONS

    def test_get_map_fn(self):
        fn = get_map_fn("word_count")
        assert callable(fn)

    def test_get_unknown_fn_raises(self):
        with pytest.raises(KeyError):
            get_map_fn("nonexistent")

    def test_list_analyzers(self):
        analyzers = list_analyzers()
        assert "word_count" in analyzers
        assert analyzers["word_count"]["has_map"]
        assert analyzers["word_count"]["has_reduce"]


class TestWordCount:
    def test_map_basic(self):
        record = {"message": "Hello world this is a test message"}
        pairs = word_count_map(record)
        keys = [k for k, v in pairs]
        assert "hello" in keys
        assert "world" in keys
        assert "this" in keys
        assert "test" in keys
        # "is" and "a" should be filtered (len <= 2)
        assert "is" not in keys
        assert "a" not in keys

    def test_map_strips_punctuation(self):
        record = {"message": "error! failed. timeout,"}
        pairs = word_count_map(record)
        keys = [k for k, v in pairs]
        assert "error" in keys
        assert "failed" in keys
        assert "timeout" in keys

    def test_reduce_sums(self):
        assert word_count_reduce("word", [1, 1, 1]) == 3
        assert word_count_reduce("word", [5, 3, 2]) == 10

    def test_full_pipeline(self, sample_json_logs):
        engine = MapReduceEngine(num_workers=2, chunk_size=500)
        results = engine.run([sample_json_logs], "word_count", "word_count")
        assert len(results) > 0
        # All values should be positive
        for v in results.values():
            assert v > 0


class TestPatternFrequency:
    def test_map_detects_error_keywords(self):
        record = {
            "message": "Connection timeout to service",
            "ip": "10.0.0.1",
            "status_code": 500,
        }
        pairs = pattern_frequency_map(record)
        keys = [k for k, v in pairs]
        assert "error_pattern:timeout" in keys
        assert "ip_address:10.0.0.1" in keys
        assert "http_status:500" in keys

    def test_map_detects_ip_in_message(self):
        record = {
            "message": "Request from 192.168.1.10 blocked",
            "ip": "192.168.1.10",
            "status_code": 403,
        }
        pairs = pattern_frequency_map(record)
        keys = [k for k, v in pairs]
        ip_keys = [k for k in keys if k.startswith("ip_address:")]
        assert len(ip_keys) >= 1

    def test_full_pipeline(self, sample_json_logs):
        engine = MapReduceEngine(num_workers=2, chunk_size=500)
        results = engine.run(
            [sample_json_logs], "pattern_frequency", "pattern_frequency"
        )
        assert len(results) > 0
        # Should have some http_status keys
        status_keys = [k for k in results if k.startswith("http_status:")]
        assert len(status_keys) > 0


class TestServiceDistribution:
    def test_map_emits_service_and_level(self):
        record = {"service": "auth-service", "level": "INFO"}
        pairs = service_distribution_map(record)
        keys = [k for k, v in pairs]
        assert "service:auth-service" in keys
        assert "level:INFO" in keys

    def test_full_pipeline(self, sample_json_logs):
        engine = MapReduceEngine(num_workers=2, chunk_size=500)
        results = engine.run(
            [sample_json_logs], "service_distribution", "service_distribution"
        )
        assert len(results) > 0
        service_keys = [k for k in results if k.startswith("service:")]
        assert len(service_keys) > 0
        level_keys = [k for k in results if k.startswith("level:")]
        assert len(level_keys) > 0
