"""Tests for the Redis client wrapper and message producer."""

import asyncio
import json

import pytest

from src.config import Settings
from src.models import LogLevel, LogMessage
from src.producer import MessageProducer


# =====================================================================
# MessageProducer — unit tests
# =====================================================================


class TestGenerateValidMessage:
    """Tests for _generate_valid_message."""

    def test_returns_log_message(self, redis_client):
        producer = MessageProducer(redis_client, Settings())
        msg = producer._generate_valid_message()
        assert isinstance(msg, LogMessage)

    def test_has_all_expected_fields(self, redis_client):
        producer = MessageProducer(redis_client, Settings())
        msg = producer._generate_valid_message()
        assert msg.id
        assert msg.timestamp
        assert isinstance(msg.level, LogLevel)
        assert msg.source
        assert msg.message
        assert "source" in msg.metadata
        assert msg.metadata["environment"] == "production"
        assert msg.metadata["version"] == "1.0.0"

    def test_roundtrips_through_json(self, redis_client):
        producer = MessageProducer(redis_client, Settings())
        msg = producer._generate_valid_message()
        raw = msg.to_json()
        restored = LogMessage.from_json(raw)
        assert restored.id == msg.id
        assert restored.level == msg.level


class TestGenerateMalformedMessage:
    """Tests for _generate_malformed_message."""

    def test_returns_string(self, redis_client):
        producer = MessageProducer(redis_client, Settings())
        result = producer._generate_malformed_message()
        assert isinstance(result, str)

    def test_not_parseable_as_log_message(self, redis_client):
        """Most generated malformed messages fail parsing."""
        producer = MessageProducer(redis_client, Settings())
        failures = 0
        for _ in range(100):
            raw = producer._generate_malformed_message()
            try:
                LogMessage.from_json(raw)
            except Exception:
                failures += 1
        # The majority should fail (invalid_json, missing_fields, bad_level,
        # empty all fail; oversized may parse successfully).  At least 60%
        # should be un-parseable across 100 samples.
        assert failures >= 60


class TestProduceBatch:
    """Tests for produce_batch."""

    @pytest.mark.asyncio
    async def test_produces_exact_count(self, redis_client):
        settings = Settings(failure_rate=0.3)
        producer = MessageProducer(redis_client, settings)
        produced = await producer.produce_batch(50)

        assert produced == 50
        queue_len = await redis_client.get_queue_length(settings.main_queue)
        assert queue_len == 50

    @pytest.mark.asyncio
    async def test_malformed_fraction(self, redis_client):
        """Roughly 30% of messages should fail to parse as LogMessage."""
        settings = Settings(failure_rate=0.3)
        producer = MessageProducer(redis_client, settings)
        await producer.produce_batch(50)

        parse_failures = 0
        for _ in range(50):
            raw = await redis_client.dequeue(settings.main_queue, timeout=0.1)
            assert raw is not None
            try:
                LogMessage.from_json(raw)
            except Exception:
                parse_failures += 1

        # With 30% failure rate over 50 messages, expect ~15 failures.
        # Allow a wide tolerance for randomness.
        assert 5 <= parse_failures <= 30

    @pytest.mark.asyncio
    async def test_zero_count_produces_nothing(self, redis_client):
        settings = Settings()
        producer = MessageProducer(redis_client, settings)
        produced = await producer.produce_batch(0)

        assert produced == 0
        queue_len = await redis_client.get_queue_length(settings.main_queue)
        assert queue_len == 0


class TestProduceContinuous:
    """Tests for produce_continuous."""

    @pytest.mark.asyncio
    async def test_respects_stop_event(self, redis_client):
        settings = Settings(producer_rate=100.0, failure_rate=0.0)
        producer = MessageProducer(redis_client, settings)
        stop = asyncio.Event()

        # Let the producer run for a short window, then stop it.
        async def _stop_after():
            await asyncio.sleep(0.15)
            stop.set()

        await asyncio.gather(
            producer.produce_continuous(stop),
            _stop_after(),
        )

        queue_len = await redis_client.get_queue_length(settings.main_queue)
        # Should have produced *some* messages but not an infinite amount.
        assert queue_len > 0
