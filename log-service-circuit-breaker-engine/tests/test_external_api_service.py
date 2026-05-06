"""Service-layer tests: ExternalAPIService through the breaker."""
from __future__ import annotations
import asyncio

import pytest

from src.breaker import CircuitBreaker
from src.config import CircuitBreakerConfig
from src.failure_injection import FailureInjector, InjectedFailure
from src.services.external_api import ExternalAPIService
from src.state import CircuitState


def make_api(**overrides) -> ExternalAPIService:
    cfg = CircuitBreakerConfig(
        name="api",
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
    return ExternalAPIService("api", breaker, injector)


@pytest.mark.asyncio
async def test_enrich_happy_path():
    svc = make_api()
    result = await svc.enrich({"msg": "x"})
    assert result["status"] == "ok"
    assert result["service"] == "api"
    assert result["enrichment"]["geo"] == "us-west"
    assert result["enrichment"]["tier"] == "premium"


@pytest.mark.asyncio
async def test_enrich_fallback_returns_unknown_geo():
    svc = make_api()
    await svc.breaker.force_open()

    result = await svc.enrich({"msg": "y"})
    assert result["status"] == "fallback"
    assert result["service"] == "api"
    assert result["enrichment"]["geo"] == "unknown"
    assert result["enrichment"]["tier"] == "unknown"


@pytest.mark.asyncio
async def test_enrich_failure_then_recovery():
    svc = make_api()
    svc.injector.set_failure_rate(1.0)

    for _ in range(6):
        try:
            await svc.enrich({"msg": "fail"})
        except InjectedFailure:
            pass
        if svc.breaker.state == CircuitState.OPEN:
            break
    assert svc.breaker.state == CircuitState.OPEN

    # Heal upstream and wait for recovery_timeout.
    svc.injector.reset()
    await asyncio.sleep(0.2)

    # Probe enough times to walk HALF_OPEN -> CLOSED.
    final = None
    for _ in range(8):
        final = await svc.enrich({"msg": "ok"})
        if svc.breaker.state == CircuitState.CLOSED and final["status"] == "ok":
            break

    assert svc.breaker.state == CircuitState.CLOSED
    assert final is not None and final["status"] == "ok"
    assert final["enrichment"]["geo"] == "us-west"
