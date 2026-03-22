"""Tests for src.consumer module (mocked Kafka)."""

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from src.config import Settings
from src.consumer import KafkaStreamConsumer
from src.metrics_store import MetricsStore
from src.stream_processor import StreamProcessor


@pytest.fixture
def consumer_config():
    return Settings(
        bootstrap_servers="localhost:9092",
        poll_timeout_s=0.1,
    )


@pytest.fixture
def consumer_deps():
    store = MetricsStore(max_length=100)
    processor = StreamProcessor(store)
    return store, processor


class TestConsumerLifecycle:
    """Verify start/stop behaviour."""

    @patch("src.consumer.Consumer")
    def test_consumer_start_stop(self, mock_consumer_cls, consumer_config, consumer_deps):
        store, processor = consumer_deps
        mock_instance = MagicMock()
        mock_instance.poll.return_value = None
        mock_consumer_cls.return_value = mock_instance

        consumer = KafkaStreamConsumer(consumer_config, processor)
        consumer.start()
        assert consumer._running is True

        time.sleep(0.3)
        consumer.stop()
        assert consumer._running is False

    @patch("src.consumer.Consumer")
    def test_stop_without_start(self, mock_consumer_cls, consumer_config, consumer_deps):
        """Stopping a consumer that was never started should not crash."""
        _, processor = consumer_deps
        consumer = KafkaStreamConsumer(consumer_config, processor)
        consumer.stop()
        assert consumer._running is False


class TestConsumeLoop:
    """Verify message processing within the consume loop."""

    @patch("src.consumer.Consumer")
    def test_consume_loop_processes_messages(self, mock_consumer_cls, consumer_config, consumer_deps):
        store, processor = consumer_deps
        mock_instance = MagicMock()
        mock_consumer_cls.return_value = mock_instance

        # Build a fake message
        fake_msg = MagicMock()
        fake_msg.error.return_value = None
        fake_msg.value.return_value = json.dumps(
            {"path": "/test", "response_time": 10, "timestamp": time.time()}
        ).encode("utf-8")
        fake_msg.key.return_value = b"key-1"
        fake_msg.topic.return_value = "log-events"

        call_count = 0

        def poll_side_effect(timeout=1.0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return fake_msg
            return None

        mock_instance.poll.side_effect = poll_side_effect

        consumer = KafkaStreamConsumer(consumer_config, processor)
        consumer.start()
        time.sleep(0.5)
        consumer.stop()

        metrics = store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] >= 1

    @patch("src.consumer.Consumer")
    def test_consume_loop_handles_json_error(self, mock_consumer_cls, consumer_config, consumer_deps):
        store, processor = consumer_deps
        mock_instance = MagicMock()
        mock_consumer_cls.return_value = mock_instance

        fake_msg = MagicMock()
        fake_msg.error.return_value = None
        fake_msg.value.return_value = b"not-json"
        fake_msg.key.return_value = None

        call_count = 0

        def poll_side_effect(timeout=1.0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return fake_msg
            return None

        mock_instance.poll.side_effect = poll_side_effect

        consumer = KafkaStreamConsumer(consumer_config, processor)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        # Should not crash; no events stored
        metrics = store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 0

    @patch("src.consumer.Consumer")
    def test_consume_loop_handles_kafka_error(self, mock_consumer_cls, consumer_config, consumer_deps):
        store, processor = consumer_deps
        mock_instance = MagicMock()
        mock_consumer_cls.return_value = mock_instance

        fake_msg = MagicMock()
        mock_error = MagicMock()
        mock_error.code.return_value = 999  # Some non-EOF error code
        fake_msg.error.return_value = mock_error

        call_count = 0

        def poll_side_effect(timeout=1.0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return fake_msg
            return None

        mock_instance.poll.side_effect = poll_side_effect

        consumer = KafkaStreamConsumer(consumer_config, processor)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        # Should not crash
        metrics = store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 0

    @patch("src.consumer.Consumer")
    def test_consume_loop_handles_partition_eof(self, mock_consumer_cls, consumer_config, consumer_deps):
        store, processor = consumer_deps
        mock_instance = MagicMock()
        mock_consumer_cls.return_value = mock_instance

        fake_msg = MagicMock()
        mock_error = MagicMock()
        # confluent_kafka.KafkaError._PARTITION_EOF == -191
        mock_error.code.return_value = -191
        fake_msg.error.return_value = mock_error

        call_count = 0

        def poll_side_effect(timeout=1.0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return fake_msg
            return None

        mock_instance.poll.side_effect = poll_side_effect

        consumer = KafkaStreamConsumer(consumer_config, processor)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        # Should not crash; no events stored
        metrics = store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] == 0

    @patch("src.consumer.Consumer")
    def test_consume_loop_commits_every_100(self, mock_consumer_cls, consumer_config, consumer_deps):
        """After 100 messages the consumer should commit offsets."""
        store, processor = consumer_deps
        mock_instance = MagicMock()
        mock_consumer_cls.return_value = mock_instance

        fake_msg = MagicMock()
        fake_msg.error.return_value = None
        fake_msg.value.return_value = json.dumps(
            {"path": "/test", "timestamp": time.time()}
        ).encode("utf-8")
        fake_msg.key.return_value = b"key"
        fake_msg.topic.return_value = "log-events"

        call_count = 0

        def poll_side_effect(timeout=1.0):
            nonlocal call_count
            call_count += 1
            if call_count <= 101:
                return fake_msg
            return None

        mock_instance.poll.side_effect = poll_side_effect

        consumer = KafkaStreamConsumer(consumer_config, processor)
        consumer.start()
        time.sleep(1.0)
        consumer.stop()

        # commit should have been called at least once (at message 100)
        mock_instance.commit.assert_called()

    @patch("src.consumer.Consumer")
    def test_message_with_none_key(self, mock_consumer_cls, consumer_config, consumer_deps):
        """Messages with None key should be handled gracefully."""
        store, processor = consumer_deps
        mock_instance = MagicMock()
        mock_consumer_cls.return_value = mock_instance

        fake_msg = MagicMock()
        fake_msg.error.return_value = None
        fake_msg.value.return_value = json.dumps(
            {"path": "/test", "timestamp": time.time()}
        ).encode("utf-8")
        fake_msg.key.return_value = None
        fake_msg.topic.return_value = "log-events"

        call_count = 0

        def poll_side_effect(timeout=1.0):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return fake_msg
            return None

        mock_instance.poll.side_effect = poll_side_effect

        consumer = KafkaStreamConsumer(consumer_config, processor)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()

        metrics = store.get_windowed_metrics(window_seconds=60)
        assert metrics["total_events"] >= 1

    @patch("src.consumer.Consumer")
    def test_consumer_handles_create_exception(self, mock_consumer_cls, consumer_config, consumer_deps):
        """If Consumer constructor raises, the loop should handle it gracefully."""
        _, processor = consumer_deps
        mock_consumer_cls.side_effect = Exception("cannot connect")

        consumer = KafkaStreamConsumer(consumer_config, processor)
        consumer.start()
        time.sleep(0.3)
        consumer.stop()
        # Should not crash the thread
