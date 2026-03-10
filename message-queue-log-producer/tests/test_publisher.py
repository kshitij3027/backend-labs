"""Tests for PublisherThread — mocked RabbitMQ, no live connection."""

import pytest
import queue
import time
from unittest.mock import MagicMock, patch, PropertyMock

from src.publisher import PublisherThread
from src.config import Config
from src.circuit_breaker import CircuitBreaker, CircuitState
from src.fallback_storage import FallbackStorage
from src.metrics import MetricsCollector


@pytest.fixture
def publisher_deps(config, tmp_path):
    """Create all publisher dependencies with mocks."""
    internal_queue = queue.Queue()
    cb = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
    fallback = FallbackStorage(storage_dir=str(tmp_path / "fb"))
    metrics = MetricsCollector()
    return {
        "config": config,
        "internal_queue": internal_queue,
        "circuit_breaker": cb,
        "fallback": fallback,
        "metrics": metrics,
    }


@patch("src.publisher.setup_topology")
@patch("src.publisher.RabbitMQConnection")
class TestPublisher:

    def test_publish_calls_basic_publish(self, MockRMQConn, mock_setup, publisher_deps):
        """Put a batch on the queue — basic_publish should be called for each entry."""
        mock_conn_instance = MagicMock()
        mock_channel = MagicMock()
        mock_conn_instance.get_channel.return_value = mock_channel
        mock_conn_instance._connection = MagicMock()
        mock_conn_instance._connection.is_closed = False
        MockRMQConn.return_value = mock_conn_instance

        publisher = PublisherThread(**publisher_deps)
        time.sleep(0.3)

        batch = [
            {"level": "INFO", "source": "app", "msg": "hello"},
            {"level": "ERROR", "source": "app", "msg": "oops"},
        ]
        publisher_deps["internal_queue"].put(batch)
        time.sleep(0.5)

        assert mock_channel.basic_publish.call_count == 2
        publisher.stop()

    def test_circuit_open_writes_fallback(self, MockRMQConn, mock_setup, publisher_deps):
        """When circuit breaker is OPEN, batch goes to fallback storage."""
        mock_conn_instance = MagicMock()
        mock_channel = MagicMock()
        mock_conn_instance.get_channel.return_value = mock_channel
        mock_conn_instance._connection = MagicMock()
        mock_conn_instance._connection.is_closed = False
        MockRMQConn.return_value = mock_conn_instance

        # Force circuit to OPEN
        cb = publisher_deps["circuit_breaker"]
        for _ in range(5):
            cb.record_failure()
        assert cb.state == CircuitState.OPEN

        fallback = publisher_deps["fallback"]
        fallback.write = MagicMock()

        publisher = PublisherThread(**publisher_deps)
        time.sleep(0.3)

        batch = [{"level": "WARN", "source": "test", "msg": "fallback me"}]
        publisher_deps["internal_queue"].put(batch)
        time.sleep(0.5)

        fallback.write.assert_called_with(batch)
        # basic_publish should NOT have been called for this batch
        mock_channel.basic_publish.assert_not_called()
        publisher.stop()

    def test_publish_failure_records_failure(self, MockRMQConn, mock_setup, publisher_deps):
        """When basic_publish raises, circuit_breaker.record_failure and fallback.write are called."""
        mock_conn_instance = MagicMock()
        mock_channel = MagicMock()
        mock_channel.basic_publish.side_effect = Exception("connection lost")
        mock_conn_instance.get_channel.return_value = mock_channel
        mock_conn_instance._connection = MagicMock()
        mock_conn_instance._connection.is_closed = False
        MockRMQConn.return_value = mock_conn_instance

        cb = publisher_deps["circuit_breaker"]
        cb.record_failure = MagicMock()

        fallback = publisher_deps["fallback"]
        fallback.write = MagicMock()

        publisher = PublisherThread(**publisher_deps)
        time.sleep(0.3)

        batch = [{"level": "INFO", "source": "app", "msg": "fail"}]
        publisher_deps["internal_queue"].put(batch)
        time.sleep(0.5)

        cb.record_failure.assert_called()
        fallback.write.assert_called_with(batch)
        publisher.stop()

    def test_publish_success_records_metrics(self, MockRMQConn, mock_setup, publisher_deps):
        """Successful publish should call metrics.record_published."""
        mock_conn_instance = MagicMock()
        mock_channel = MagicMock()
        mock_conn_instance.get_channel.return_value = mock_channel
        mock_conn_instance._connection = MagicMock()
        mock_conn_instance._connection.is_closed = False
        MockRMQConn.return_value = mock_conn_instance

        metrics = publisher_deps["metrics"]
        metrics.record_published = MagicMock()

        publisher = PublisherThread(**publisher_deps)
        time.sleep(0.3)

        batch = [{"level": "INFO", "source": "app", "msg": "ok"}]
        publisher_deps["internal_queue"].put(batch)
        time.sleep(0.5)

        metrics.record_published.assert_called_with(len(batch))
        publisher.stop()

    def test_stop_joins_cleanly(self, MockRMQConn, mock_setup, publisher_deps):
        """Publisher thread should stop and no longer be alive after stop()."""
        mock_conn_instance = MagicMock()
        mock_channel = MagicMock()
        mock_conn_instance.get_channel.return_value = mock_channel
        mock_conn_instance._connection = MagicMock()
        mock_conn_instance._connection.is_closed = False
        MockRMQConn.return_value = mock_conn_instance

        publisher = PublisherThread(**publisher_deps)
        time.sleep(0.2)
        assert publisher._thread.is_alive()

        publisher.stop()
        assert not publisher._thread.is_alive()
