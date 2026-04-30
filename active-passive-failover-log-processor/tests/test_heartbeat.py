"""Tests for src/heartbeat.py — emitter writes, monitor detects, with fakeredis."""

from __future__ import annotations

import asyncio

import pytest

from src.heartbeat import HeartbeatEmitter, HeartbeatMonitor
from src.models import HeartbeatMessage, NodeState, from_json
from src.redis_client import HEARTBEAT_KEY, RedisClient


# --- helpers --------------------------------------------------------------


class _StateBox:
    """Mutable holder so test code can flip the state after constructing the
    emitter/monitor (mirrors how the real ``NodeStateMachine.state`` getter
    works — a callable that pulls the current value, NOT a snapshot)."""

    def __init__(self, initial: NodeState) -> None:
        self.value: NodeState = initial

    def __call__(self) -> NodeState:
        return self.value


def _metrics() -> dict[str, float]:
    """Trivial metrics provider for the emitter."""
    return {"logs_per_sec": 1.5, "last_log_id": 42.0, "log_count": 10.0}


# =========================================================================
# HeartbeatEmitter
# =========================================================================


async def test_emitter_writes_heartbeat_when_primary(
    fake_redis_client: RedisClient,
) -> None:
    state = _StateBox(NodeState.PRIMARY)
    # Pre-acquire the lock so renew_lock has a value to extend.
    assert await fake_redis_client.acquire_lock(ttl=6) is True

    emitter = HeartbeatEmitter(
        redis_client=fake_redis_client,
        state_provider=state,
        metrics_provider=_metrics,
        node_id="node-test",
        interval=0.05,
        lock_ttl=6,
    )
    result = await emitter.emit_once()
    assert result is True

    raw = await fake_redis_client.read_heartbeat()
    assert raw is not None
    msg = from_json(HeartbeatMessage, raw)
    assert msg.node_id == "node-test"
    assert msg.state is NodeState.PRIMARY
    assert msg.role == "primary"
    assert msg.metrics == {"logs_per_sec": 1.5, "last_log_id": 42.0, "log_count": 10.0}
    assert emitter.heartbeats_emitted_total == 1
    assert emitter.lock_renewal_failures_total == 0


async def test_emitter_does_not_write_when_not_primary(
    fake_redis_client: RedisClient,
) -> None:
    state = _StateBox(NodeState.STANDBY)
    emitter = HeartbeatEmitter(
        redis_client=fake_redis_client,
        state_provider=state,
        metrics_provider=_metrics,
        node_id="node-test",
        interval=0.05,
        lock_ttl=6,
    )
    result = await emitter.emit_once()
    assert result is None  # signals "skipped — not primary"
    assert await fake_redis_client.read_heartbeat() is None
    assert emitter.heartbeats_emitted_total == 0
    assert emitter.lock_renewal_failures_total == 0


async def test_emitter_fires_on_lock_lost_when_renewal_fails(
    fake_redis_client: RedisClient,
) -> None:
    state = _StateBox(NodeState.PRIMARY)
    # Plant a different owner so renew_lock returns False — the Lua
    # script GET-then-PEXPIRE refuses to extend a lock owned by someone
    # else.
    from src.redis_client import LEADER_LOCK_KEY

    await fake_redis_client._client().set(LEADER_LOCK_KEY, b"other-node")

    fired = asyncio.Event()
    fire_count = {"n": 0}

    async def on_lock_lost() -> None:
        fire_count["n"] += 1
        fired.set()

    emitter = HeartbeatEmitter(
        redis_client=fake_redis_client,
        state_provider=state,
        metrics_provider=_metrics,
        node_id="node-test",
        interval=0.05,
        lock_ttl=6,
        on_lock_lost=on_lock_lost,
    )
    result = await emitter.emit_once()
    assert result is False
    await asyncio.wait_for(fired.wait(), timeout=1.0)
    assert fire_count["n"] == 1
    assert emitter.lock_renewal_failures_total == 1
    # Heartbeat write happens BEFORE lock renew, so it still incremented.
    assert emitter.heartbeats_emitted_total == 1


async def test_emitter_loop_starts_writing_then_stops(
    fake_redis_client: RedisClient,
) -> None:
    state = _StateBox(NodeState.PRIMARY)
    assert await fake_redis_client.acquire_lock(ttl=6) is True

    emitter = HeartbeatEmitter(
        redis_client=fake_redis_client,
        state_provider=state,
        metrics_provider=_metrics,
        node_id="node-test",
        interval=0.05,
        lock_ttl=6,
    )
    await emitter.start()
    try:
        # Wait for at least 2 ticks.
        for _ in range(40):
            if emitter.heartbeats_emitted_total >= 2:
                break
            await asyncio.sleep(0.02)
        assert emitter.heartbeats_emitted_total >= 2
        assert await fake_redis_client.read_heartbeat() is not None
    finally:
        await emitter.stop()
    assert emitter._task is None


async def test_emitter_idles_when_state_flips_to_standby(
    fake_redis_client: RedisClient,
) -> None:
    """Spec: 'When state ≠ PRIMARY, idle but still loop ... do not exit.'"""
    state = _StateBox(NodeState.PRIMARY)
    assert await fake_redis_client.acquire_lock(ttl=6) is True

    emitter = HeartbeatEmitter(
        redis_client=fake_redis_client,
        state_provider=state,
        metrics_provider=_metrics,
        node_id="node-test",
        interval=0.05,
        lock_ttl=6,
    )
    await emitter.start()
    try:
        # Let it tick at least once as primary.
        for _ in range(40):
            if emitter.heartbeats_emitted_total >= 1:
                break
            await asyncio.sleep(0.02)
        assert emitter.heartbeats_emitted_total >= 1

        # Flip to standby. Drop the heartbeat key so we can detect new writes.
        await fake_redis_client._client().delete(HEARTBEAT_KEY)
        state.value = NodeState.STANDBY
        before = emitter.heartbeats_emitted_total
        # Give it time to idle through several ticks.
        await asyncio.sleep(0.2)
        assert emitter.heartbeats_emitted_total == before
        assert await fake_redis_client.read_heartbeat() is None

        # Flip back to primary and verify the loop resumes writing.
        state.value = NodeState.PRIMARY
        for _ in range(40):
            if emitter.heartbeats_emitted_total > before:
                break
            await asyncio.sleep(0.02)
        assert emitter.heartbeats_emitted_total > before
    finally:
        await emitter.stop()


async def test_emitter_stop_is_idempotent(fake_redis_client: RedisClient) -> None:
    state = _StateBox(NodeState.STANDBY)
    emitter = HeartbeatEmitter(
        redis_client=fake_redis_client,
        state_provider=state,
        metrics_provider=_metrics,
        node_id="node-test",
        interval=0.05,
        lock_ttl=6,
    )
    await emitter.start()
    await emitter.stop()
    # Second stop must be a no-op rather than raising.
    await emitter.stop()


# =========================================================================
# HeartbeatMonitor
# =========================================================================


async def test_monitor_no_callback_within_grace_window(
    fake_redis_client: RedisClient,
) -> None:
    """During the bootstrap grace period (now - started_at < failure_timeout)
    AND no heartbeat ever, the monitor MUST NOT fire."""
    state = _StateBox(NodeState.STANDBY)
    fire_count = {"n": 0}

    async def on_failure() -> None:
        fire_count["n"] += 1

    monitor = HeartbeatMonitor(
        redis_client=fake_redis_client,
        state_provider=state,
        node_id="node-test",
        poll_interval=0.05,
        failure_timeout=10.0,  # huge: we will be inside the grace window
        on_primary_failed=on_failure,
    )
    # Drive a few ticks manually.
    for _ in range(5):
        await monitor.check_once()
    assert fire_count["n"] == 0


async def test_monitor_bootstrap_no_heartbeat_ever_fires_after_timeout(
    fake_redis_client: RedisClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No heartbeat key + monitor has been alive ≥ failure_timeout → fire once."""
    state = _StateBox(NodeState.STANDBY)
    fired = asyncio.Event()
    fire_count = {"n": 0}

    async def on_failure() -> None:
        fire_count["n"] += 1
        fired.set()

    monitor = HeartbeatMonitor(
        redis_client=fake_redis_client,
        state_provider=state,
        node_id="node-test",
        poll_interval=0.05,
        failure_timeout=0.2,
        on_primary_failed=on_failure,
    )
    # First tick: still in the grace window.
    await monitor.check_once()
    assert fire_count["n"] == 0

    # Advance the monotonic clock past the failure_timeout WITHOUT
    # writing any heartbeat. We patch time.monotonic in src.heartbeat to
    # fast-forward.
    import src.heartbeat as hb_module

    fake_now = {"t": monitor._started_at + monitor.failure_timeout + 0.05}

    def fake_monotonic() -> float:
        return fake_now["t"]

    monkeypatch.setattr(hb_module.time, "monotonic", fake_monotonic)
    await monitor.check_once()

    await asyncio.wait_for(fired.wait(), timeout=1.0)
    assert fire_count["n"] == 1
    assert monitor.primary_failures_detected_total == 1


async def test_monitor_fresh_heartbeat_keeps_callback_quiet(
    fake_redis_client: RedisClient,
) -> None:
    """If a fresh heartbeat is present at every poll, the failure callback
    must never fire."""
    state = _StateBox(NodeState.STANDBY)
    fire_count = {"n": 0}

    async def on_failure() -> None:
        fire_count["n"] += 1

    monitor = HeartbeatMonitor(
        redis_client=fake_redis_client,
        state_provider=state,
        node_id="node-test",
        poll_interval=0.05,
        failure_timeout=0.2,
        on_primary_failed=on_failure,
    )

    # Write a fresh heartbeat between polls.
    for _ in range(8):
        await fake_redis_client.write_heartbeat(b"hb-payload", ttl=6)
        await monitor.check_once()
        await asyncio.sleep(0.05)

    assert fire_count["n"] == 0
    assert monitor._last_seen_at is not None
    assert monitor._failure_fired is False


async def test_monitor_fires_only_once_per_failure_window(
    fake_redis_client: RedisClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Once on_primary_failed fires, it must not fire again until a fresh
    heartbeat resets the window."""
    import src.heartbeat as hb_module

    state = _StateBox(NodeState.STANDBY)
    fire_count = {"n": 0}

    async def on_failure() -> None:
        fire_count["n"] += 1

    monitor = HeartbeatMonitor(
        redis_client=fake_redis_client,
        state_provider=state,
        node_id="node-test",
        poll_interval=0.05,
        failure_timeout=0.2,
        on_primary_failed=on_failure,
    )

    # First, plant a real heartbeat so _last_seen_at gets set.
    await fake_redis_client.write_heartbeat(b"hb", ttl=6)
    await monitor.check_once()
    assert monitor._last_seen_at is not None
    base = monitor._last_seen_at

    # Drop the heartbeat and advance the clock past the timeout.
    await fake_redis_client._client().delete(HEARTBEAT_KEY)
    fake_now = {"t": base + monitor.failure_timeout + 0.05}
    monkeypatch.setattr(hb_module.time, "monotonic", lambda: fake_now["t"])

    # Multiple ticks across the failure window — should fire ONCE.
    for _ in range(5):
        await monitor.check_once()
        fake_now["t"] += 0.05

    assert fire_count["n"] == 1


async def test_monitor_fires_again_after_recovery(
    fake_redis_client: RedisClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After a fresh heartbeat reappears AND a second failure window opens,
    on_primary_failed must fire a SECOND time."""
    import src.heartbeat as hb_module

    state = _StateBox(NodeState.STANDBY)
    fire_count = {"n": 0}

    async def on_failure() -> None:
        fire_count["n"] += 1

    monitor = HeartbeatMonitor(
        redis_client=fake_redis_client,
        state_provider=state,
        node_id="node-test",
        poll_interval=0.05,
        failure_timeout=0.2,
        on_primary_failed=on_failure,
    )

    # Tick 1: heartbeat alive.
    await fake_redis_client.write_heartbeat(b"hb1", ttl=6)
    await monitor.check_once()

    base = monitor._last_seen_at
    assert base is not None

    # Drop heartbeat, advance time, expect 1st fire.
    await fake_redis_client._client().delete(HEARTBEAT_KEY)
    fake_now = {"t": base + monitor.failure_timeout + 0.05}
    monkeypatch.setattr(hb_module.time, "monotonic", lambda: fake_now["t"])
    await monitor.check_once()
    assert fire_count["n"] == 1
    assert monitor._failure_fired is True

    # Heartbeat recovers — the firing flag must reset.
    await fake_redis_client.write_heartbeat(b"hb2", ttl=6)
    await monitor.check_once()
    assert monitor._failure_fired is False

    # Drop heartbeat again, advance time, expect 2nd fire.
    new_base = monitor._last_seen_at
    assert new_base is not None
    await fake_redis_client._client().delete(HEARTBEAT_KEY)
    fake_now["t"] = new_base + monitor.failure_timeout + 0.05
    await monitor.check_once()
    assert fire_count["n"] == 2
    assert monitor.primary_failures_detected_total == 2


async def test_monitor_idle_when_state_is_primary(
    fake_redis_client: RedisClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A node that is itself PRIMARY shouldn't fire on_primary_failed even if
    the heartbeat key is missing — that's the post-promotion case."""
    import src.heartbeat as hb_module

    state = _StateBox(NodeState.PRIMARY)
    fire_count = {"n": 0}

    async def on_failure() -> None:
        fire_count["n"] += 1

    monitor = HeartbeatMonitor(
        redis_client=fake_redis_client,
        state_provider=state,
        node_id="node-test",
        poll_interval=0.05,
        failure_timeout=0.2,
        on_primary_failed=on_failure,
    )

    # Far past the failure_timeout, heartbeat absent.
    fake_now = {"t": monitor._started_at + 100.0}
    monkeypatch.setattr(hb_module.time, "monotonic", lambda: fake_now["t"])
    for _ in range(5):
        result = await monitor.check_once()
        assert result is None  # signals "monitoring skipped"
    assert fire_count["n"] == 0


async def test_monitor_idle_when_state_is_election(
    fake_redis_client: RedisClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ELECTION is mid-promotion — a fresh failure during it would race the
    election machinery, so the monitor stays quiet."""
    import src.heartbeat as hb_module

    state = _StateBox(NodeState.ELECTION)
    fire_count = {"n": 0}

    async def on_failure() -> None:
        fire_count["n"] += 1

    monitor = HeartbeatMonitor(
        redis_client=fake_redis_client,
        state_provider=state,
        node_id="node-test",
        poll_interval=0.05,
        failure_timeout=0.2,
        on_primary_failed=on_failure,
    )
    fake_now = {"t": monitor._started_at + 100.0}
    monkeypatch.setattr(hb_module.time, "monotonic", lambda: fake_now["t"])
    for _ in range(5):
        result = await monitor.check_once()
        assert result is None
    assert fire_count["n"] == 0


async def test_monitor_loop_lifecycle(fake_redis_client: RedisClient) -> None:
    """start() + stop() cleanly run the loop without hanging."""
    state = _StateBox(NodeState.STANDBY)
    monitor = HeartbeatMonitor(
        redis_client=fake_redis_client,
        state_provider=state,
        node_id="node-test",
        poll_interval=0.05,
        failure_timeout=10.0,
    )
    await monitor.start()
    await asyncio.sleep(0.15)  # let it tick a few times
    await monitor.stop()
    assert monitor._task is None


async def test_monitor_stop_is_idempotent(fake_redis_client: RedisClient) -> None:
    state = _StateBox(NodeState.STANDBY)
    monitor = HeartbeatMonitor(
        redis_client=fake_redis_client,
        state_provider=state,
        node_id="node-test",
        poll_interval=0.05,
        failure_timeout=10.0,
    )
    await monitor.start()
    await monitor.stop()
    await monitor.stop()
