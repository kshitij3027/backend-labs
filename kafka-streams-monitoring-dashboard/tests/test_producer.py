"""Tests for the MetricsProducer (all Kafka interactions mocked)."""

import pytest
from unittest.mock import MagicMock, patch

from src.producer import MetricsProducer


@pytest.fixture
def mock_kafka_producer():
    with patch("src.producer.Producer") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def producer(config, mock_kafka_producer):
    return MetricsProducer(config)


def test_produce_derived_metrics(producer, mock_kafka_producer):
    metrics = {"total_events": 100, "error_rate": 2.5, "window_seconds": 60}
    producer.produce_derived_metrics(metrics)
    mock_kafka_producer.produce.assert_called_once()
    call_args = mock_kafka_producer.produce.call_args
    assert call_args.kwargs["topic"] == "derived-metrics"
    assert call_args.kwargs["key"] == "windowed-metrics"


def test_produce_calls_poll(producer, mock_kafka_producer):
    producer.produce_derived_metrics({"total_events": 50, "window_seconds": 60})
    mock_kafka_producer.poll.assert_called_with(0)


def test_delivery_callback_success(producer):
    mock_msg = MagicMock()
    mock_msg.topic.return_value = "derived-metrics"
    mock_msg.partition.return_value = 0
    producer._delivery_callback(None, mock_msg)  # no error — should not raise


def test_delivery_callback_error(producer):
    producer._delivery_callback(Exception("delivery failed"), None)  # should log error


def test_flush(producer, mock_kafka_producer):
    producer.flush()
    mock_kafka_producer.flush.assert_called_once()


def test_close(producer, mock_kafka_producer):
    producer.close()
    mock_kafka_producer.flush.assert_called_once()


def test_produce_handles_exception(producer, mock_kafka_producer):
    mock_kafka_producer.produce.side_effect = Exception("kafka down")
    producer.produce_derived_metrics({"total_events": 0, "window_seconds": 60})
    # Should not raise
