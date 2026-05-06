"""Service-layer tests: DatabaseService through the breaker."""
from __future__ import annotations
import asyncio

import pytest

from src.breaker import CircuitBreaker
from src.config import CircuitBreakerConfig
from src.failure_injection import FailureInjector, InjectedFailure
from src.services.database import DatabaseService
from src.state import CircuitState


def make_db(**overrides) -> DatabaseService:
    cfg = CircuitBreakerConfig(
        name="db",
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
    return DatabaseService("db", breaker, injector)


@pytest.mark.asyncio
async def test_insert_log_happy_path():
    svc = make_db()
    result = await svc.insert_log({"msg": "x"})
    assert result["status"] == "ok"
    assert result["service"] == "db"
    assert result["log"] == {"msg": "x"}
    assert "id" in result
    assert svc.breaker.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_breaker_opens_after_repeated_failures_then_returns_fallback():
    svc = make_db()
    svc.injector.set_failure_rate(1.0)

    fallback_seen = False
    real_failures = 0
    for _ in range(8):
        try:
            result = await svc.insert_log({"msg": "fail-me"})
        except InjectedFailure:
            # Real downstream failure surfaced from the breaker.
            real_failures += 1
            continue
        if result.get("status") == "fallback":
            fallback_seen = True
            break

    assert real_failures >= 1, "expected some calls to surface InjectedFailure"
    assert fallback_seen, "breaker never tripped to OPEN -> fallback path"
    assert svc.breaker.state == CircuitState.OPEN


@pytest.mark.asyncio
async def test_timeout_returns_fallback():
    svc = make_db()
    # Sleep longer than timeout_duration (0.3s).
    svc.injector.set_response_delay(0.5)

    result = await svc.insert_log({"msg": "slow"})
    assert result["status"] == "fallback"
    assert result["service"] == "db"
    assert result["cached"] is True


@pytest.mark.asyncio
async def test_after_breaker_opens_then_recovers():
    svc = make_db()
    svc.injector.set_failure_rate(1.0)

    # Drive the breaker OPEN.
    for _ in range(6):
        try:
            await svc.insert_log({"msg": "drive-open"})
        except InjectedFailure:
            pass
        if svc.breaker.state == CircuitState.OPEN:
            break
    assert svc.breaker.state == CircuitState.OPEN

    # Reset injector to healthy.
    svc.injector.reset()

    # Wait recovery_timeout + slack.
    await asyncio.sleep(0.2)

    # Probe enough times to drive HALF_OPEN -> CLOSED.
    for _ in range(8):
        result = await svc.insert_log({"msg": "recover"})
        if svc.breaker.state == CircuitState.CLOSED and result["status"] == "ok":
            break

    assert svc.breaker.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_fallback_response_shape():
    svc = make_db()
    await svc.breaker.force_open()

    result = await svc.insert_log({"msg": "y"})
    assert result == {
        "status": "fallback",
        "service": "db",
        "cached": True,
        "log": {"msg": "y"},
    }
