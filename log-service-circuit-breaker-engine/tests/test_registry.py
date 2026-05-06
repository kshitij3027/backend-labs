"""Tests for ``CircuitBreakerRegistry`` and the module-level singleton.

Covers registration semantics (idempotency, lookup), bulk operations
(``all``, ``names``, ``metrics_snapshot``, ``reset_all``), global listener
fan-out to both pre-existing and future-registered breakers, and the
singleton accessor including its test-only reset hook.
"""
from __future__ import annotations

import pytest

from src.breaker import CircuitBreaker
from src.config import CircuitBreakerConfig
from src.registry import (
    CircuitBreakerRegistry,
    get_registry,
    reset_registry_for_tests,
)
from src.state import CircuitState


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def make_cfg(name: str, **overrides) -> CircuitBreakerConfig:
    """Build a tunable ``CircuitBreakerConfig`` for tests."""
    cfg_kwargs = dict(
        name=name,
        failure_threshold=2,
        recovery_timeout=0.1,
        timeout_duration=0.5,
        half_open_max_calls=2,
        monitoring_window=10.0,
        consecutive_failures_threshold=2,
        min_volume_threshold=2,
    )
    cfg_kwargs.update(overrides)
    return CircuitBreakerConfig(**cfg_kwargs)


@pytest.fixture
def reg() -> CircuitBreakerRegistry:
    """Return a fresh, isolated registry for each test."""
    return CircuitBreakerRegistry()


# --------------------------------------------------------------------------- #
# Tests                                                                       #
# --------------------------------------------------------------------------- #


async def test_register_returns_new_breaker(reg: CircuitBreakerRegistry):
    """``register`` returns a CircuitBreaker that ``get`` retrieves identically."""
    br = reg.register(make_cfg("db"))
    assert isinstance(br, CircuitBreaker)
    assert reg.get("db") is br


async def test_register_idempotent_for_same_name(reg: CircuitBreakerRegistry):
    """Re-registering the same name returns the originally created instance."""
    first = reg.register(make_cfg("db"))
    second = reg.register(make_cfg("db", failure_threshold=99))
    assert first is second
    # The second registration must not have replaced the first.
    assert reg.get("db") is first


async def test_get_unknown_returns_none(reg: CircuitBreakerRegistry):
    """``get`` on a name that was never registered returns ``None``."""
    assert reg.get("missing") is None


async def test_all_returns_independent_dict(reg: CircuitBreakerRegistry):
    """``all`` returns a copy: mutating it must not corrupt internal state."""
    reg.register(make_cfg("a"))
    reg.register(make_cfg("b"))
    reg.register(make_cfg("c"))

    snapshot = reg.all()
    assert set(snapshot.keys()) == {"a", "b", "c"}
    assert len(snapshot) == 3

    # Mutate the returned dict; internal store must remain intact.
    snapshot.clear()
    snapshot["bogus"] = None  # type: ignore[assignment]

    assert reg.get("a") is not None
    assert reg.get("b") is not None
    assert reg.get("c") is not None
    assert reg.get("bogus") is None
    assert set(reg.names()) == {"a", "b", "c"}


async def test_names_sorted(reg: CircuitBreakerRegistry):
    """``names`` returns registered names in sorted order regardless of insertion."""
    reg.register(make_cfg("c"))
    reg.register(make_cfg("a"))
    reg.register(make_cfg("b"))
    assert reg.names() == ["a", "b", "c"]


async def test_metrics_snapshot_shape(reg: CircuitBreakerRegistry):
    """``metrics_snapshot`` returns a dict-of-dict; each inner dict has CLOSED state."""
    reg.register(make_cfg("svc1"))
    reg.register(make_cfg("svc2"))

    snap = reg.metrics_snapshot()
    assert isinstance(snap, dict)
    assert set(snap.keys()) == {"svc1", "svc2"}

    for name, metrics in snap.items():
        assert isinstance(metrics, dict), f"{name} metrics must be a dict"
        assert "state" in metrics
        # ``to_dict`` should serialize the enum to a string for the API.
        assert metrics["state"] == "CLOSED"


async def test_global_listener_attached_to_existing(reg: CircuitBreakerRegistry):
    """Adding a global listener attaches it to already-registered breakers."""
    captured: list[tuple] = []

    def listener(name, from_state, to_state, reason):
        captured.append((name, from_state, to_state, reason))

    # Register FIRST...
    br = reg.register(make_cfg("orders"))
    # ...then add the global listener.
    reg.add_global_listener(listener)

    # Drive a transition.
    await br.force_open()

    assert br.state == CircuitState.OPEN
    assert len(captured) == 1
    name, from_state, to_state, reason = captured[0]
    assert name == "orders"
    assert from_state == CircuitState.CLOSED
    assert to_state == CircuitState.OPEN
    assert reason  # non-empty reason string


async def test_global_listener_attached_to_new_breakers(reg: CircuitBreakerRegistry):
    """Global listeners apply to breakers registered AFTER the listener was added."""
    captured: list[tuple] = []

    def listener(name, from_state, to_state, reason):
        captured.append((name, from_state, to_state, reason))

    # Add listener FIRST...
    reg.add_global_listener(listener)
    # ...then register the breaker.
    br = reg.register(make_cfg("payments"))

    await br.force_open()

    assert br.state == CircuitState.OPEN
    assert len(captured) == 1
    name, from_state, to_state, _reason = captured[0]
    assert name == "payments"
    assert from_state == CircuitState.CLOSED
    assert to_state == CircuitState.OPEN


async def test_reset_all_clears_state(reg: CircuitBreakerRegistry):
    """``reset_all`` returns every registered breaker to CLOSED."""
    one = reg.register(make_cfg("one"))
    two = reg.register(make_cfg("two"))

    await one.force_open()
    assert one.state == CircuitState.OPEN
    assert two.state == CircuitState.CLOSED

    await reg.reset_all()

    assert one.state == CircuitState.CLOSED
    assert two.state == CircuitState.CLOSED


async def test_singleton_is_stable():
    """``get_registry`` returns the same instance across calls; reset hook gives a fresh one."""
    reset_registry_for_tests()  # start clean
    a = get_registry()
    b = get_registry()
    assert a is b

    # Mutate the shared registry so we can prove the reset wipes it.
    a.register(make_cfg("shared"))
    assert a.get("shared") is not None

    reset_registry_for_tests()
    fresh = get_registry()
    assert fresh is not a
    assert fresh.get("shared") is None
    assert fresh.names() == []
