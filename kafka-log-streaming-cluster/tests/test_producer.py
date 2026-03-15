"""Unit tests for the multi-service log producer.

All tests mock confluent_kafka.Producer so they run without a live Kafka cluster.
"""

from collections import Counter
from unittest.mock import MagicMock, patch

import pytest

from src.config import Settings
from src.models import LogLevel, LogMessage, ServiceName, TOPIC_MAP
from src.producer import (
    LogProducer,
    ServiceSimulator,
    SERVICE_CONFIG,
    run_producer,
)


# -----------------------------------------------------------------------
# ServiceSimulator tests
# -----------------------------------------------------------------------


class TestServiceSimulator:
    """Tests for the ServiceSimulator class."""

    @pytest.mark.parametrize("service", list(ServiceName))
    def test_generates_valid_log_messages(self, service: ServiceName):
        """Each service simulator produces valid LogMessage instances."""
        sim = ServiceSimulator(service)
        msg = sim.generate_message()

        assert isinstance(msg, LogMessage)
        assert msg.service == service
        assert msg.level in list(LogLevel)
        assert msg.status_code > 0
        assert msg.user_id != ""
        assert msg.endpoint != ""
        assert msg.sequence_number == 1

    @pytest.mark.parametrize("service", list(ServiceName))
    def test_uses_consistent_user_pool(self, service: ServiceName):
        """All generated user_ids come from the pre-built 50-element pool."""
        sim = ServiceSimulator(service)
        pool = set(sim.user_pool)
        assert len(pool) == 50

        for _ in range(200):
            msg = sim.generate_message()
            assert msg.user_id in pool

    def test_sequence_number_increments_monotonically(self):
        """sequence_number increases by 1 on every call to generate_message."""
        sim = ServiceSimulator(ServiceName.WEB_API)
        prev = 0
        for _ in range(100):
            msg = sim.generate_message()
            assert msg.sequence_number == prev + 1
            prev = msg.sequence_number

    @pytest.mark.parametrize("service", list(ServiceName))
    def test_endpoints_match_service_config(self, service: ServiceName):
        """Generated endpoints are drawn from the service's configured set."""
        sim = ServiceSimulator(service)
        expected = set(SERVICE_CONFIG[service]["endpoints"])

        for _ in range(200):
            msg = sim.generate_message()
            assert msg.endpoint in expected

    @pytest.mark.parametrize("service", list(ServiceName))
    def test_status_codes_match_service_config(self, service: ServiceName):
        """Generated status codes are drawn from the service's configured set."""
        sim = ServiceSimulator(service)
        expected = set(SERVICE_CONFIG[service]["status_weights"].keys())

        for _ in range(500):
            msg = sim.generate_message()
            assert msg.status_code in expected

    def test_weighted_distribution_levels_roughly_correct(self):
        """Over 1000 messages, INFO should dominate for web-api (weight 80)."""
        sim = ServiceSimulator(ServiceName.WEB_API)
        counts: Counter = Counter()

        for _ in range(2000):
            msg = sim.generate_message()
            counts[msg.level] += 1

        total = sum(counts.values())
        info_pct = counts[LogLevel.INFO] / total * 100

        # Expected ~80%. Allow generous bounds for randomness.
        assert 60 < info_pct < 95, f"INFO percentage was {info_pct:.1f}%"

    def test_weighted_distribution_status_codes_roughly_correct(self):
        """Over 2000 messages, 200 should be most common for web-api (weight 70)."""
        sim = ServiceSimulator(ServiceName.WEB_API)
        counts: Counter = Counter()

        for _ in range(2000):
            msg = sim.generate_message()
            counts[msg.status_code] += 1

        total = sum(counts.values())
        ok_pct = counts[200] / total * 100

        # Expected ~70%. Allow generous bounds for randomness.
        assert 55 < ok_pct < 85, f"200 percentage was {ok_pct:.1f}%"

    def test_topic_resolves_correctly(self):
        """Each message's .topic property matches the TOPIC_MAP for its service."""
        for service in ServiceName:
            sim = ServiceSimulator(service)
            msg = sim.generate_message()
            assert msg.topic == TOPIC_MAP[service]


# -----------------------------------------------------------------------
# LogProducer tests
# -----------------------------------------------------------------------


class TestLogProducer:
    """Tests for the LogProducer wrapper around confluent_kafka.Producer."""

    @patch("src.producer.Producer")
    def test_produce_calls_underlying_producer(self, mock_producer_cls, settings, sample_log_message):
        """produce() delegates to confluent_kafka.Producer.produce() with correct args."""
        mock_instance = MagicMock()
        mock_producer_cls.return_value = mock_instance

        lp = LogProducer(settings)
        lp.produce(sample_log_message)

        mock_instance.produce.assert_called_once_with(
            topic=sample_log_message.topic,
            value=sample_log_message.to_kafka_value(),
            key=sample_log_message.partition_key,
            callback=lp._delivery_callback,
        )
        mock_instance.poll.assert_called_once_with(0)

    @patch("src.producer.Producer")
    def test_delivery_callback_success(self, mock_producer_cls):
        """_delivery_callback increments delivered_count on successful delivery."""
        mock_producer_cls.return_value = MagicMock()
        lp = LogProducer(Settings())

        mock_msg = MagicMock()
        lp._delivery_callback(None, mock_msg)

        assert lp.delivered_count == 1
        assert lp.failed_count == 0

    @patch("src.producer.Producer")
    def test_delivery_callback_failure(self, mock_producer_cls):
        """_delivery_callback increments failed_count when err is not None."""
        mock_producer_cls.return_value = MagicMock()
        lp = LogProducer(Settings())

        mock_msg = MagicMock()
        mock_msg.topic.return_value = "test-topic"
        lp._delivery_callback("some error", mock_msg)

        assert lp.failed_count == 1
        assert lp.delivered_count == 0

    @patch("src.producer.Producer")
    def test_delivery_callback_multiple_mixed(self, mock_producer_cls):
        """Counters accumulate correctly across many mixed callbacks."""
        mock_producer_cls.return_value = MagicMock()
        lp = LogProducer(Settings())

        mock_msg = MagicMock()
        mock_msg.topic.return_value = "t"

        for _ in range(5):
            lp._delivery_callback(None, mock_msg)
        for _ in range(3):
            lp._delivery_callback("err", mock_msg)

        assert lp.delivered_count == 5
        assert lp.failed_count == 3

    @patch("src.producer.Producer")
    def test_flush_calls_underlying_flush(self, mock_producer_cls):
        """flush() forwards to the underlying Producer.flush()."""
        mock_instance = MagicMock()
        mock_instance.flush.return_value = 0
        mock_producer_cls.return_value = mock_instance

        lp = LogProducer(Settings())
        result = lp.flush(timeout=10.0)

        mock_instance.flush.assert_called_once_with(timeout=10.0)
        assert result == 0

    @patch("src.producer.Producer")
    def test_close_flushes_and_logs(self, mock_producer_cls):
        """close() calls flush() before logging final stats."""
        mock_instance = MagicMock()
        mock_instance.flush.return_value = 0
        mock_producer_cls.return_value = mock_instance

        lp = LogProducer(Settings())
        lp.close()

        mock_instance.flush.assert_called_once()


# -----------------------------------------------------------------------
# run_producer integration test (mocked Kafka)
# -----------------------------------------------------------------------


class TestRunProducer:
    """Integration tests for the run_producer() entry point."""

    @patch("src.producer.Producer")
    def test_runs_for_duration_and_produces_messages(self, mock_producer_cls):
        """run_producer produces messages from all 3 simulators for the given duration."""
        mock_instance = MagicMock()
        mock_instance.flush.return_value = 0
        mock_producer_cls.return_value = mock_instance

        settings = Settings(
            producer_duration_seconds=2,
            producer_rate_per_second=50,
        )
        run_producer(settings)

        # Should have produced a meaningful number of messages.
        # Each iteration produces 3 messages (one per simulator), interval = 1/50 = 0.02s
        # Over ~2 seconds: roughly 100 iterations * 3 = 300 messages (plus critical dups)
        total_produce_calls = mock_instance.produce.call_count
        assert total_produce_calls > 50, f"Only {total_produce_calls} produce calls"

    @patch("src.producer.Producer")
    def test_error_messages_duplicated_to_critical_topic(self, mock_producer_cls):
        """ERROR-level messages should also be sent to the critical-logs topic."""
        mock_instance = MagicMock()
        mock_instance.flush.return_value = 0
        mock_producer_cls.return_value = mock_instance

        settings = Settings(
            producer_duration_seconds=3,
            producer_rate_per_second=100,
        )
        run_producer(settings)

        # Gather all topics that received produce() calls
        topics_produced = set()
        for call in mock_instance.produce.call_args_list:
            # produce() is called with keyword args: topic=..., value=..., key=...
            topic = call.kwargs.get("topic") or call.args[0] if call.args else call.kwargs.get("topic")
            if topic:
                topics_produced.add(topic)

        assert settings.critical_topic in topics_produced, (
            f"critical-logs topic not found among produced topics: {topics_produced}"
        )

    @patch("src.producer.Producer")
    def test_all_three_service_topics_produced(self, mock_producer_cls):
        """Messages are sent to all three service-specific topics."""
        mock_instance = MagicMock()
        mock_instance.flush.return_value = 0
        mock_producer_cls.return_value = mock_instance

        settings = Settings(
            producer_duration_seconds=2,
            producer_rate_per_second=50,
        )
        run_producer(settings)

        topics_produced = set()
        for call in mock_instance.produce.call_args_list:
            topic = call.kwargs.get("topic") or (call.args[0] if call.args else None)
            if topic:
                topics_produced.add(topic)

        for svc_topic in settings.all_service_topics:
            assert svc_topic in topics_produced, f"{svc_topic} not found in {topics_produced}"
