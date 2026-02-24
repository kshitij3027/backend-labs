"""Tests for performance tracking and adaptive normalizer."""
import pytest
from src.performance import HandlerStats, PerformanceTracker, PerformanceAwareNormalizer
from src.models import LogEntry, LogLevel
import src.handlers  # noqa: F401


class TestHandlerStats:
    def test_initial_state(self):
        stats = HandlerStats(format_name="json")
        assert stats.total_calls == 0
        assert stats.successes == 0
        assert stats.failures == 0
        assert stats.avg_time_ms == 0.0
        assert stats.success_rate == 1.0  # no calls = 100% success

    def test_record_parse_success(self):
        stats = HandlerStats(format_name="json")
        stats.record_parse(1.0, True)
        assert stats.total_calls == 1
        assert stats.successes == 1
        assert stats.avg_time_ms == 1.0

    def test_record_parse_failure(self):
        stats = HandlerStats(format_name="json")
        stats.record_parse(1.0, False)
        assert stats.total_calls == 1
        assert stats.failures == 1
        assert stats.success_rate == 0.0

    def test_ewma_calculation(self):
        stats = HandlerStats(format_name="json")
        stats.record_parse(10.0, True)  # first value = 10.0
        stats.record_parse(20.0, True)  # ewma = 0.1*20 + 0.9*10 = 11.0
        assert abs(stats.avg_time_ms - 11.0) < 0.01

    def test_score_computation(self):
        stats = HandlerStats(format_name="json")
        stats.record_parse(2.0, True)
        # score = avg_time / success_rate = 2.0 / 1.0 = 2.0
        assert abs(stats.score - 2.0) < 0.01

    def test_score_with_failures(self):
        stats = HandlerStats(format_name="json")
        stats.record_parse(2.0, True)
        stats.record_parse(2.0, False)
        # success_rate = 0.5, score = 2.0 / 0.5 = 4.0
        assert abs(stats.score - 4.0) < 0.5  # approximate due to EWMA

    def test_score_all_failures(self):
        stats = HandlerStats(format_name="json")
        stats.record_parse(2.0, False)
        assert stats.score == float('inf')


class TestPerformanceTracker:
    def test_get_stats_creates(self):
        tracker = PerformanceTracker()
        stats = tracker.get_stats("json")
        assert stats.format_name == "json"

    def test_optimal_order(self):
        tracker = PerformanceTracker()
        # json: fast
        tracker.get_stats("json").record_parse(1.0, True)
        # text: slow
        tracker.get_stats("text").record_parse(10.0, True)
        order = tracker.optimal_order()
        assert order[0] == "json"
        assert order[1] == "text"

    def test_report(self):
        tracker = PerformanceTracker()
        tracker.get_stats("json").record_parse(1.0, True)
        report = tracker.report()
        assert "json" in report
        assert "Performance Report" in report


class TestPerformanceAwareNormalizer:
    @pytest.fixture
    def normalizer(self):
        return PerformanceAwareNormalizer(reorder_interval=10, report_interval=20)

    def test_normalize_tracks_stats(self, normalizer, sample_json_bytes):
        normalizer.normalize(sample_json_bytes)
        stats = normalizer.tracker.all_stats
        assert len(stats) > 0

    def test_normalize_increments_call_count(self, normalizer, sample_json_bytes):
        normalizer.normalize(sample_json_bytes)
        assert normalizer.call_count == 1

    def test_reorder_after_interval(self, normalizer, sample_json_bytes, sample_text_bytes):
        # Process enough calls to trigger reorder
        for _ in range(10):
            normalizer.normalize(sample_json_bytes)
        assert normalizer.call_count == 10

    def test_report_generation(self, normalizer, sample_json_bytes):
        for _ in range(20):
            normalizer.normalize(sample_json_bytes)
        report = normalizer.stats_report
        assert "json" in report

    def test_explicit_format_tracks(self, normalizer, sample_json_bytes):
        normalizer.normalize(sample_json_bytes, source_format="json")
        stats = normalizer.tracker.get_stats("json")
        assert stats.total_calls == 1

    def test_all_formats_tracked(self, normalizer, sample_json_bytes, sample_text_bytes, sample_protobuf_bytes, sample_avro_bytes):
        normalizer.normalize(sample_json_bytes)
        normalizer.normalize(sample_text_bytes)
        normalizer.normalize(sample_protobuf_bytes)
        normalizer.normalize(sample_avro_bytes)
        assert normalizer.call_count == 4
        # All detect attempts tracked
        stats = normalizer.tracker.all_stats
        assert len(stats) >= 4
