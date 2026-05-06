"""Service-layer tests: MessageQueueService through the breaker."""
from __future__ import annotations

import pytest

from src.breaker import CircuitBreaker
from src.config import CircuitBreakerConfig
from src.failure_injection import FailureInjector
from src.services.queue import MessageQueueService
from src.state import CircuitState


def make_queue(**overrides) -> MessageQueueService:
    cfg = CircuitBreakerConfig(
        name="queue",
        failure_threshold=3,
        recovery_timeout=0.15,
        timeout_duration=0.3,
        half_open_max_calls=2,
        monitoring_window=10.0,
        consecutive_failures_threshold=3,
        min_volume_threshold=3,
        **overrides,
    )
    breaker = CircuitBreaker(cfg)
    injector = FailureInjector()
    return MessageQueueService("queue", breaker, injector)


@pytest.mark.asyncio
async def test_publish_happy_path():
    svc = make_queue()
    result = await svc.publish({"msg": "hello"})
    assert result["status"] == "ok"
    assert result["service"] == "queue"
    assert result["topic"] == "logs"
    assert result["offset"] == 0
    assert svc.breaker.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_publish_fallback_when_open():
    svc = make_queue()
    await svc.breaker.force_open()

    result = await svc.publish({"msg": "no-go"})
    assert result == {
        "status": "fallback",
        "service": "queue",
        "queued": False,
    }


@pytest.mark.asyncio
async def test_publish_offset_increments():
    svc = make_queue()
    offsets = []
    for _ in range(3):
        result = await svc.publish({"msg": "m"})
        assert result["status"] == "ok"
        offsets.append(result["offset"])
    assert offsets == [0, 1, 2]
