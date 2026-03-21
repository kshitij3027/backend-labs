"""Unit tests for CompactionMonitor."""

from unittest.mock import MagicMock, patch

import pytest

from src.config import Settings


@pytest.fixture
def mock_consumer_stats():
    """Return a mock StateConsumer with configurable stats."""
    mock = MagicMock()
    mock.total_messages_consumed = 100
    mock.get_stats.return_value = {
        "total_consumed": 100,
        "active_profiles": 15,
        "total_in_state": 20,
        "tombstones_processed": 5,
        "updates_by_type": {"CREATE": 50, "UPDATE": 40, "DELETE": 10},
    }
    return mock


@pytest.fixture
def monitor(config, mock_consumer_stats):
    """Return a CompactionMonitor with Kafka clients mocked out."""
    with patch("src.monitor.Consumer") as MockConsumer, patch(
        "src.monitor.AdminClient"
    ) as MockAdmin:
        MockConsumer.return_value = MagicMock()
        MockAdmin.return_value = MagicMock()

        from src.monitor import CompactionMonitor

        mon = CompactionMonitor(config, mock_consumer_stats)
        yield mon


class TestCompactionMetricsStructure:
    def test_returns_expected_keys(self, monitor):
        # Mock watermark offsets
        monitor._consumer_for_watermarks.get_watermark_offsets.return_value = (0, 100)

        metrics = monitor.get_compaction_metrics()

        expected_keys = {
            "offsets",
            "consumer_stats",
            "total_messages",
            "unique_keys",
            "compaction_ratio",
            "estimated_storage_saved_bytes",
            "messages_per_second",
        }
        assert set(metrics.keys()) == expected_keys


class TestCompactionRatioCalculation:
    def test_ratio_equals_unique_over_total(self, monitor, mock_consumer_stats):
        # total_messages from watermarks = 100, unique_keys from state = 20
        monitor._consumer_for_watermarks.get_watermark_offsets.return_value = (0, 100)
        mock_consumer_stats.get_stats.return_value["total_in_state"] = 20

        metrics = monitor.get_compaction_metrics()

        assert metrics["total_messages"] == 100
        assert metrics["unique_keys"] == 20
        assert metrics["compaction_ratio"] == pytest.approx(0.2)


class TestTopicOffsets:
    def test_returns_low_high_total(self, monitor):
        monitor._consumer_for_watermarks.get_watermark_offsets.return_value = (5, 55)

        offsets = monitor.get_topic_offsets()

        assert offsets["low"] == 5
        assert offsets["high"] == 55
        assert offsets["total_messages"] == 50
