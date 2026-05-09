"""Tests for cached fallback behavior in LogProcessorService."""
from __future__ import annotations
import asyncio
import pytest
from src.breaker import CircuitBreaker
from src.config import CircuitBreakerConfig
from src.failure_injection import FailureInjector
from src.services.database import DatabaseService
from src.services.queue import MessageQueueService
from src.services.external_api import ExternalAPIService
from src.services.log_processor import LogProcessorService


def _cfg(name: str):
    return CircuitBreakerConfig(
        name=name,
        failure_threshold=2,
        recovery_timeout=10.0,
        timeout_duration=0.5,
        half_open_max_calls=2,
        monitoring_window=10.0,
        consecutive_failures_threshold=2,
        min_volume_threshold=2,
    )


def make_processor():
    primary = DatabaseService(
        "database_primary", CircuitBreaker(_cfg("database_primary")), FailureInjector()
    )
    backup = DatabaseService(
        "database_backup", CircuitBreaker(_cfg("database_backup")), FailureInjector()
    )
    queue = MessageQueueService(
        "queue_main", CircuitBreaker(_cfg("queue_main")), FailureInjector()
    )
    api = ExternalAPIService(
        "external_api", CircuitBreaker(_cfg("external_api")), FailureInjector()
    )
    return LogProcessorService(primary, backup, queue, api), primary, backup, queue, api


def _sample_log(seq: int = 0) -> dict:
    return {
        "id": f"test-id-{seq}",
        "timestamp": 1234567890.0 + seq,
        "level": "INFO",
        "message": f"hello {seq}",
        "service": "demo-app",
        "user_id": f"user_{seq}",
    }


async def test_queue_fallback_uses_cached_payload():
    processor, _primary, _backup, queue, _api = make_processor()

    # Drive 3 happy-path logs to populate the cache.
    for i in range(3):
        ok = await processor.process_log(_sample_log(i))
        assert ok["queue"]["status"] == "ok"

    # Now force the queue breaker open; next call should fallback.
    await queue.breaker.force_open()

    result = await processor.process_log(_sample_log(99))

    assert result["queue"]["status"] == "fallback"
    assert result["queue"]["from_cache"] is True
    # The cached real payload had topic & offset keys — they should appear
    # in the enriched fallback dict.
    assert "topic" in result["queue"]
    assert "offset" in result["queue"]
    assert result["queue"]["topic"] == "logs"


async def test_enrich_fallback_uses_cached_payload():
    processor, _primary, _backup, _queue, api = make_processor()

    # Populate the enrich cache with 3 successes.
    for i in range(3):
        ok = await processor.process_log(_sample_log(i))
        assert ok["enrich"]["status"] == "ok"
        assert ok["enrich"]["enrichment"]["geo"] == "us-west"

    await api.breaker.force_open()

    result = await processor.process_log(_sample_log(99))

    assert result["enrich"]["status"] == "fallback"
    assert result["enrich"]["from_cache"] is True
    # Cached enrichment ("us-west", "premium") should win over the static
    # fallback ("unknown", "unknown").
    assert result["enrich"]["enrichment"]["geo"] == "us-west"
    assert result["enrich"]["enrichment"]["tier"] == "premium"


async def test_fallback_without_cache_uses_static():
    processor, primary, backup, _queue, _api = make_processor()

    # No prior successes => no cache. Force both DB breakers open.
    await primary.breaker.force_open()
    await backup.breaker.force_open()

    result = await processor.process_log(_sample_log(0))

    assert result["db"]["status"] == "fallback"
    # Without a cached payload, we should NOT see from_cache: True. The static
    # fallback (from DatabaseService.fallback) should win, with cached: True.
    assert result["db"].get("from_cache") is not True
    assert result["db"].get("cached") is True


async def test_processing_stats_fallback_responses_counted():
    processor, _primary, _backup, queue, _api = make_processor()

    # Two happy-path logs.
    await processor.process_log(_sample_log(0))
    await processor.process_log(_sample_log(1))

    # Force queue breaker open and drive one more log -> fallback.
    await queue.breaker.force_open()
    result = await processor.process_log(_sample_log(2))

    assert result["queue"]["status"] == "fallback"
    assert processor.stats.fallback_responses >= 1
