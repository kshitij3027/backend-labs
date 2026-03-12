"""Tests for the RedeliveryHandler class."""

from unittest.mock import MagicMock

from src.config import Settings
from src.redelivery_handler import RedeliveryHandler


def test_declare_infrastructure_calls_correct_declares(config: Settings) -> None:
    """Verify that declare_infrastructure declares all exchanges, queues, and bindings."""
    handler = RedeliveryHandler(config)
    channel = MagicMock()

    handler.declare_infrastructure(channel)

    # 3 exchanges: main, retry, DLQ
    assert channel.exchange_declare.call_count == 3
    exchange_names = [
        call.kwargs["exchange"]
        for call in channel.exchange_declare.call_args_list
    ]
    assert config.MAIN_EXCHANGE in exchange_names
    assert config.RETRY_EXCHANGE in exchange_names
    assert config.DLQ_EXCHANGE in exchange_names

    # Queues: 1 main + 4 retry (one per delay) + 1 DLQ = 6
    assert channel.queue_declare.call_count == 6
    queue_names = [
        call.kwargs["queue"]
        for call in channel.queue_declare.call_args_list
    ]
    assert config.MAIN_QUEUE in queue_names
    assert config.DLQ_QUEUE in queue_names
    for delay in config.RETRY_DELAYS:
        assert f"logs.retry.{delay}ms" in queue_names

    # Bindings: 1 main + 4 retry + 1 DLQ = 6
    assert channel.queue_bind.call_count == 6


def test_get_retry_count_from_headers(config: Settings) -> None:
    """Verify that get_retry_count extracts x-retry-count from headers."""
    handler = RedeliveryHandler(config)

    properties = MagicMock()
    properties.headers = {"x-retry-count": 3}
    assert handler.get_retry_count(properties) == 3

    # No headers at all
    properties_no_headers = MagicMock()
    properties_no_headers.headers = None
    assert handler.get_retry_count(properties_no_headers) == 0

    # Headers present but no x-retry-count key
    properties_empty = MagicMock()
    properties_empty.headers = {"other-key": "value"}
    assert handler.get_retry_count(properties_empty) == 0


def test_should_retry_within_limit(config: Settings) -> None:
    """Verify should_retry returns True below MAX_RETRIES and False at/above."""
    handler = RedeliveryHandler(config)

    for count in range(config.MAX_RETRIES):
        assert handler.should_retry(count) is True

    assert handler.should_retry(config.MAX_RETRIES) is False
    assert handler.should_retry(config.MAX_RETRIES + 1) is False


def test_get_retry_queue_name(config: Settings) -> None:
    """Verify correct queue name for each retry count level."""
    handler = RedeliveryHandler(config)

    # Each retry count maps to the corresponding delay index
    assert handler.get_retry_queue_name(0) == "logs.retry.1000ms"
    assert handler.get_retry_queue_name(1) == "logs.retry.2000ms"
    assert handler.get_retry_queue_name(2) == "logs.retry.4000ms"
    assert handler.get_retry_queue_name(3) == "logs.retry.8000ms"

    # Counts beyond the list length are capped at the last delay
    assert handler.get_retry_queue_name(4) == "logs.retry.8000ms"
    assert handler.get_retry_queue_name(10) == "logs.retry.8000ms"
