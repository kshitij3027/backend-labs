"""Tests for LogProcessorService orchestration."""
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
from src.state import CircuitState


def _cfg(name: str):
    return CircuitBreakerConfig(
        name=name,
        failure_threshold=2,
        recovery_timeout=0.15,
        timeout_duration=0.5,
        half_open_max_calls=2,
        monitoring_window=10.0,
        consecutive_failures_threshold=2,
        min_volume_threshold=2,
    )


def make_processor():
    primary = DatabaseService("db_primary", CircuitBreaker(_cfg("db_primary")), FailureInjector())
    backup = DatabaseService("db_backup", CircuitBreaker(_cfg("db_backup")), FailureInjector())
    queue = MessageQueueService("queue", CircuitBreaker(_cfg("queue")), FailureInjector())
    api = ExternalAPIService("ext_api", CircuitBreaker(_cfg("ext_api")), FailureInjector())
    return LogProcessorService(primary, backup, queue, api), primary, backup, queue, api


def _sample_log() -> dict:
    return {
        "id": "test-id-1",
        "timestamp": 1234567890.0,
        "level": "INFO",
        "message": "hello",
        "service": "demo-app",
        "user_id": "user_1",
    }


async def test_process_log_happy_path():
    processor, _primary, _backup, _queue, _api = make_processor()
    result = await processor.process_log(_sample_log())

    assert result["db"]["status"] == "ok"
    assert result["had_failure"] is False
    assert result["used_backup"] is False
    assert processor.stats.total_processed == 1
    assert processor.stats.successful_processed == 1
    assert processor.stats.failed_processed == 0


async def test_failover_to_backup_when_primary_open():
    processor, primary, backup, _queue, _api = make_processor()
    await primary.breaker.force_open()

    result = await processor.process_log(_sample_log())

    assert result["used_backup"] is True
    # The backup db should serve the call from the real path (status ok), not via fallback.
    assert result["db"]["status"] == "ok"
    # Identifying field — DatabaseService should set its name on the result.
    assert result["db"].get("service") == "db_backup" or result["db"].get("name") == "db_backup"


async def test_fallback_counted_when_primary_open_and_backup_open():
    processor, primary, backup, _queue, _api = make_processor()
    await primary.breaker.force_open()
    await backup.breaker.force_open()

    result = await processor.process_log(_sample_log())

    assert result["db"]["status"] == "fallback"
    assert processor.stats.fallback_responses == 1


async def test_queue_fallback_counted():
    processor, _primary, _backup, queue, _api = make_processor()
    await queue.breaker.force_open()

    result = await processor.process_log(_sample_log())

    assert result["queue"]["status"] == "fallback"
    assert processor.stats.fallback_responses >= 1


async def test_enrich_fallback_counted():
    processor, _primary, _backup, _queue, api = make_processor()
    await api.breaker.force_open()

    result = await processor.process_log(_sample_log())

    assert result["enrich"]["status"] == "fallback"
    assert result["enrich"]["enrichment"]["geo"] == "unknown"


async def test_process_batch_aggregates_stats():
    processor, _primary, _backup, _queue, _api = make_processor()

    result = await processor.process_batch(20)

    assert result["processed"] == 20
    # All injectors are clean — expect all to succeed.
    assert result["successful"] >= 18
    assert result["duration_ms"] > 0


async def test_get_circuit_metrics_shape():
    processor, _primary, _backup, _queue, _api = make_processor()

    metrics = processor.get_circuit_metrics()

    assert set(metrics.keys()) == {"primary_db", "backup_db", "queue", "ext_api"}
    for key in ("primary_db", "backup_db", "queue", "ext_api"):
        assert isinstance(metrics[key], dict)
        assert "state" in metrics[key]


async def test_processing_stats_uptime_increases():
    processor, _primary, _backup, _queue, _api = make_processor()

    first = processor.get_processing_stats()
    await asyncio.sleep(0.05)
    second = processor.get_processing_stats()

    assert second["uptime_seconds"] > first["uptime_seconds"]
