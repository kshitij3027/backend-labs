"""Tests for src/node.py — FailoverNode lifecycle and callbacks.

We never spin up a real Redis here — the ``fake_redis_client`` fixture
already wires a fakeredis-backed ``RedisClient``, and the
``FailoverNode`` constructor accepts that fixture directly via the
``redis_client=`` keyword. The peer client is a small stub that records
calls so we can assert the election coordinator wired into the node
saw what we expect.
"""

from __future__ import annotations

import asyncio

from src.config import NodeConfig
from src.models import ElectionMessage, ElectionResult, NodeState
from src.node import FailoverNode
from src.redis_client import LEADER_LOCK_KEY, RedisClient


# =========================================================================
# Stub peer client — records nothing fancy; just must not raise.
# =========================================================================


class _StubPeerClient:
    def __init__(self) -> None:
        self.candidacy_calls: list[tuple[tuple[str, int], ElectionMessage]] = []
        self.result_calls: list[tuple[tuple[str, int], ElectionResult]] = []
        self.closed: bool = False

    async def send_candidacy(
        self, peer: tuple[str, int], msg: ElectionMessage
    ) -> bool:
        self.candidacy_calls.append((peer, msg))
        return True

    async def send_election_result(
        self, peer: tuple[str, int], result: ElectionResult
    ) -> bool:
        self.result_calls.append((peer, result))
        return True

    async def close(self) -> None:
        self.closed = True


# =========================================================================
# Helper: fresh config for tests.
# =========================================================================


def _config(node_id: str = "node-test", is_primary: bool = True) -> NodeConfig:
    """Build a NodeConfig directly without going through env vars.

    Pydantic settings still allow constructor args; we use them so each
    test gets a clean isolated config without monkeypatching the env.
    """
    return NodeConfig(  # type: ignore[call-arg]
        node_id=node_id,
        is_primary=is_primary,
        port=8001,
        redis_host="localhost",
        redis_port=6379,
        heartbeat_interval=2.0,
        heartbeat_timeout=6.0,
        election_timeout=2.0,  # keep election timeout short for the test path
        state_sync_interval=5.0,
        lock_ttl=6,
        peer_nodes="node-2:8002,node-3:8003",
    )


# =========================================================================
# Construction
# =========================================================================


def test_construct_does_not_raise(fake_redis_client: RedisClient) -> None:
    """FailoverNode(__init__) must NOT touch Redis or the network."""
    peer = _StubPeerClient()
    node = FailoverNode(
        _config(),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    # Smoke check: every component is wired up.
    assert node.state_machine.state is NodeState.INACTIVE
    assert node.log_processor.log_count == 0
    assert node.app is not None
    assert node.election_coordinator.peers == [("node-2", 8002), ("node-3", 8003)]


# =========================================================================
# start() — initial-state decision tree
# =========================================================================


async def test_start_with_is_primary_true_and_free_lock_becomes_primary(
    fake_redis_client: RedisClient,
) -> None:
    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=True),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    try:
        await node.start()
        assert node.state_machine.state is NodeState.PRIMARY
        # We hold the lock.
        holder = await fake_redis_client.read_lock_holder()
        assert holder == "node-test"
    finally:
        await node.stop()


async def test_start_with_is_primary_true_but_lock_held_falls_back_to_standby(
    fake_redis_client: RedisClient,
) -> None:
    """Bootstrap collision: configured initial primary can't get the lock."""
    # Pre-acquire as a different node.
    underlying = fake_redis_client._client()  # type: ignore[attr-defined]
    await underlying.set(LEADER_LOCK_KEY, b"node-other", nx=True, ex=6)

    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=True),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    try:
        await node.start()
        assert node.state_machine.state is NodeState.STANDBY
        holder = await fake_redis_client.read_lock_holder()
        assert holder == "node-other"
    finally:
        await node.stop()


async def test_start_with_is_primary_false_becomes_standby(
    fake_redis_client: RedisClient,
) -> None:
    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=False),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    try:
        await node.start()
        assert node.state_machine.state is NodeState.STANDBY
        # We did not race for the lock.
        holder = await fake_redis_client.read_lock_holder()
        assert holder is None
    finally:
        await node.stop()


# =========================================================================
# _on_lock_lost — self-demotion path
# =========================================================================


async def test_on_lock_lost_demotes_primary_to_standby(
    fake_redis_client: RedisClient,
) -> None:
    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=True),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    try:
        await node.start()
        assert node.state_machine.state is NodeState.PRIMARY

        await node._on_lock_lost()
        assert node.state_machine.state is NodeState.STANDBY
    finally:
        await node.stop()


async def test_on_lock_lost_is_noop_when_not_primary(
    fake_redis_client: RedisClient,
) -> None:
    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=False),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    try:
        await node.start()
        assert node.state_machine.state is NodeState.STANDBY
        await node._on_lock_lost()
        # Still STANDBY — no-op since we weren't PRIMARY.
        assert node.state_machine.state is NodeState.STANDBY
    finally:
        await node.stop()


# =========================================================================
# _on_primary_failed — election path
# =========================================================================


async def test_on_primary_failed_runs_election_and_wins(
    fake_redis_client: RedisClient,
) -> None:
    """Standby with a free lock detects primary failure → wins election."""
    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=False),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    try:
        await node.start()
        assert node.state_machine.state is NodeState.STANDBY

        await node._on_primary_failed()

        # Election fired and we won (lock was free).
        assert node.state_machine.state is NodeState.PRIMARY
        assert node.election_coordinator.elections_run_total == 1
        assert node.election_coordinator.elections_won_total == 1

        # Candidacy was broadcast to every peer.
        assert len(peer.candidacy_calls) == len(node.election_coordinator.peers)
    finally:
        await node.stop()


async def test_on_primary_failed_runs_election_and_loses(
    fake_redis_client: RedisClient,
) -> None:
    """Standby detects primary failure but lock is already held → loses."""
    underlying = fake_redis_client._client()  # type: ignore[attr-defined]
    await underlying.set(LEADER_LOCK_KEY, b"node-zzz", nx=True, ex=6)

    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=False),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    try:
        await node.start()
        await node._on_primary_failed()

        assert node.state_machine.state is NodeState.STANDBY
        assert node.election_coordinator.elections_run_total == 1
        assert node.election_coordinator.elections_lost_total == 1
    finally:
        await node.stop()


async def test_on_primary_failed_skips_when_already_primary(
    fake_redis_client: RedisClient,
) -> None:
    """A racing call when state is already PRIMARY must be a no-op."""
    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=True),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    try:
        await node.start()
        assert node.state_machine.state is NodeState.PRIMARY

        await node._on_primary_failed()
        # No election ran.
        assert node.election_coordinator.elections_run_total == 0
        assert node.state_machine.state is NodeState.PRIMARY
    finally:
        await node.stop()


async def test_on_primary_failed_lock_serialises_concurrent_calls(
    fake_redis_client: RedisClient,
) -> None:
    """Two parallel callbacks must not both run an election."""
    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=False),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    try:
        await node.start()
        await asyncio.gather(node._on_primary_failed(), node._on_primary_failed())
        # Exactly one election should have run.
        assert node.election_coordinator.elections_run_total == 1
    finally:
        await node.stop()


# =========================================================================
# _on_manual_failover
# =========================================================================


async def test_manual_failover_releases_lock_and_demotes(
    fake_redis_client: RedisClient,
) -> None:
    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=True),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    try:
        await node.start()
        assert node.state_machine.state is NodeState.PRIMARY
        assert await fake_redis_client.read_lock_holder() == "node-test"

        await node._on_manual_failover()

        assert node.state_machine.state is NodeState.STANDBY
        # Lock is released.
        assert await fake_redis_client.read_lock_holder() is None
    finally:
        await node.stop()


async def test_manual_failover_is_noop_when_not_primary(
    fake_redis_client: RedisClient,
) -> None:
    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=False),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    try:
        await node.start()
        await node._on_manual_failover()
        # Still standby; no exception.
        assert node.state_machine.state is NodeState.STANDBY
    finally:
        await node.stop()


# =========================================================================
# stop() cleanup
# =========================================================================


async def test_stop_releases_lock_when_primary(
    fake_redis_client: RedisClient,
) -> None:
    # Capture the underlying fakeredis instance up front; node.stop() will
    # close the RedisClient wrapper, but the underlying fake stays usable.
    underlying = fake_redis_client._client()  # type: ignore[attr-defined]

    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=True),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    await node.start()
    raw = await underlying.get(LEADER_LOCK_KEY)
    assert raw is not None and raw.decode("utf-8") == "node-test"

    await node.stop()

    # After stop(), read directly from the underlying fake — the wrapper is closed.
    raw = await underlying.get(LEADER_LOCK_KEY)
    assert raw is None
    assert node.heartbeat_emitter._task is None
    assert node.heartbeat_monitor._task is None
    assert peer.closed is True


async def test_stop_does_not_release_someone_elses_lock(
    fake_redis_client: RedisClient,
) -> None:
    """Defensive: a STANDBY stopping must NOT delete the lock held by
    the actual primary.
    """
    underlying = fake_redis_client._client()  # type: ignore[attr-defined]
    await underlying.set(LEADER_LOCK_KEY, b"node-other", nx=True, ex=6)

    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=False),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    await node.start()
    assert node.state_machine.state is NodeState.STANDBY

    await node.stop()

    # The other node's lock is still there. We read directly from the
    # underlying fake because node.stop() closed the wrapper client.
    raw = await underlying.get(LEADER_LOCK_KEY)
    holder = raw.decode("utf-8") if raw else None
    assert holder == "node-other"


async def test_stop_is_idempotent(fake_redis_client: RedisClient) -> None:
    peer = _StubPeerClient()
    node = FailoverNode(
        _config(is_primary=False),
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    await node.start()
    await node.stop()
    # Second stop must not raise.
    await node.stop()


# =========================================================================
# End-to-end-ish: heartbeat emit ticks while PRIMARY (sanity check)
# =========================================================================


async def test_emitter_writes_heartbeat_after_start_when_primary(
    fake_redis_client: RedisClient,
) -> None:
    """After ``start()`` the emitter loop should produce at least one heartbeat
    while we're PRIMARY. Uses a tight interval so the test runs quickly.
    """
    peer = _StubPeerClient()
    # Build a config with a tight interval directly via the constructor —
    # avoids relying on whether pydantic-settings allows post-init mutation.
    cfg = NodeConfig(  # type: ignore[call-arg]
        node_id="node-1",
        is_primary=True,
        port=8001,
        redis_host="localhost",
        redis_port=6379,
        heartbeat_interval=0.05,
        heartbeat_timeout=6.0,
        election_timeout=2.0,
        state_sync_interval=5.0,
        lock_ttl=6,
        peer_nodes="node-2:8002,node-3:8003",
    )
    node = FailoverNode(
        cfg,
        redis_client=fake_redis_client,
        peer_client=peer,  # type: ignore[arg-type]
    )
    try:
        await node.start()
        # Wait for a couple of ticks.
        for _ in range(40):
            if node.heartbeat_emitter.heartbeats_emitted_total >= 2:
                break
            await asyncio.sleep(0.02)
        assert node.heartbeat_emitter.heartbeats_emitted_total >= 1
    finally:
        await node.stop()
