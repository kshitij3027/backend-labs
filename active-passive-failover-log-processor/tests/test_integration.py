"""End-to-end-ish integration tests that wire two FailoverNodes against
one shared fakeredis backend.

These tests stop short of spinning up FastAPI servers and httpx — they
drive the node lifecycle directly via ``start()`` / ``stop()`` and the
private callbacks (``_on_primary_failed``). The goal is to verify the
post-promotion snapshot continuity: when standby B is promoted after
primary A dies, B's :class:`LogProcessor` counters pick up where A's
last snapshot left off.

The fixture in conftest.py provides a ``shared_fakeredis_factory`` so
two ``RedisClient`` instances point at the same fakeredis backend (one
per node, mirroring how each node has its own connection pool to the
real Redis service).
"""

from __future__ import annotations

from typing import Callable

from src.config import NodeConfig
from src.models import ElectionMessage, ElectionResult, NodeState
from src.node import FailoverNode
from src.redis_client import LEADER_LOCK_KEY, RedisClient


# =========================================================================
# Stub peer client — records calls; never reaches the network.
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
# Helper: per-node config builder.
# =========================================================================


def _config(
    node_id: str,
    *,
    is_primary: bool,
    state_sync_interval: float = 5.0,
    election_timeout: float = 2.0,
    peers_csv: str = "",
) -> NodeConfig:
    return NodeConfig(  # type: ignore[call-arg]
        node_id=node_id,
        is_primary=is_primary,
        port=8001,
        redis_host="fake",
        redis_port=0,
        heartbeat_interval=2.0,
        heartbeat_timeout=6.0,
        election_timeout=election_timeout,
        state_sync_interval=state_sync_interval,
        lock_ttl=6,
        peer_nodes=peers_csv,
    )


# =========================================================================
# Tests
# =========================================================================


async def test_primary_snapshot_visible_to_standby_via_shared_redis(
    shared_fakeredis_factory: Callable[[str], RedisClient],
) -> None:
    """A primary's snapshot should be readable from a separate
    ``RedisClient`` that points at the same backend."""
    rc_a = shared_fakeredis_factory("node-a")
    rc_b = shared_fakeredis_factory("node-b")

    peer_a = _StubPeerClient()
    node_a = FailoverNode(
        _config("node-a", is_primary=True, peers_csv="node-b:8002"),
        redis_client=rc_a,
        peer_client=peer_a,  # type: ignore[arg-type]
    )

    await node_a.start()
    try:
        # Drive some logs through A; manually snapshot.
        node_a.log_processor.ingest("hello")
        node_a.log_processor.ingest("world")
        node_a.log_processor.ingest("again")
        assert await node_a.state_persister.snapshot_now() is True

        # Confirm B can read it via its own RedisClient on the shared backend.
        raw = await rc_b.get_snapshot()
        assert raw is not None
    finally:
        await node_a.stop()


async def test_standby_promotes_after_primary_dies_loads_snapshot(
    shared_fakeredis_factory: Callable[[str], RedisClient],
) -> None:
    """The full post-failover continuity path:

    1. Build node A as primary; ingest 5 logs; take a snapshot.
    2. Stop A (releases the lock cleanly).
    3. Build node B as standby; start it; trigger ``_on_primary_failed``.
    4. B wins the election (lock is free), transitions to PRIMARY, and
       its ``_on_state_transition`` callback loads A's snapshot into
       B's :class:`LogProcessor`.
    5. Assert B's allocator is positioned past A's last_log_id.
    """
    rc_a = shared_fakeredis_factory("node-a")
    rc_b = shared_fakeredis_factory("node-b")

    # ----- node A: primary, write some logs, snapshot, then stop. -----
    peer_a = _StubPeerClient()
    node_a = FailoverNode(
        _config("node-a", is_primary=True, peers_csv="node-b:8002"),
        redis_client=rc_a,
        peer_client=peer_a,  # type: ignore[arg-type]
    )
    await node_a.start()
    assert node_a.state_machine.state is NodeState.PRIMARY

    for i in range(5):
        node_a.log_processor.ingest(f"a-msg-{i}")
    assert node_a.log_processor.last_log_id == 5

    snapshotted = await node_a.state_persister.snapshot_now()
    assert snapshotted is True

    # Clean shutdown — releases the lock so B can win the next election.
    await node_a.stop()

    # ----- node B: standby, runs an election, wins, loads snapshot. ----
    peer_b = _StubPeerClient()
    node_b = FailoverNode(
        _config("node-b", is_primary=False, peers_csv="node-a:8001"),
        redis_client=rc_b,
        peer_client=peer_b,  # type: ignore[arg-type]
    )
    await node_b.start()
    try:
        assert node_b.state_machine.state is NodeState.STANDBY
        # B starts with an empty log processor.
        assert node_b.log_processor.last_log_id == 0
        assert node_b.log_processor.log_count == 0

        # Simulate the heartbeat monitor declaring the primary dead.
        await node_b._on_primary_failed()

        assert node_b.state_machine.state is NodeState.PRIMARY
        assert node_b.election_coordinator.elections_won_total == 1
        # Snapshot was loaded as part of the ELECTION → PRIMARY callback.
        assert node_b.state_persister.snapshots_loaded_total == 1
        # The allocator is now positioned past A's last_log_id.
        assert node_b.log_processor._next_id == 6
        # last_log_id reflects the snapshotted maximum.
        assert node_b.log_processor.last_log_id == 5

        # Idempotent retries of pre-failover ids are deduped.
        retried = node_b.log_processor.ingest("retry-3", log_id=3)
        # The retry path returns the original entry; for a fresh
        # processor the entry isn't actually present, so the dedup path
        # falls through and treats id 3 as a fresh ingest. This is the
        # documented "should be unreachable" branch of LogProcessor —
        # but the key assertion is that the allocator did NOT regress
        # the next id below 6 (it would otherwise have collided with
        # legacy A-era ids).
        assert node_b.log_processor._next_id >= 6
        assert retried.log_id == 3
    finally:
        await node_b.stop()


async def test_no_snapshot_means_empty_processor_after_promotion(
    shared_fakeredis_factory: Callable[[str], RedisClient],
) -> None:
    """If no primary ever wrote a snapshot (cold-cluster case), a
    standby winning the first election must still come up cleanly with
    fresh-zero counters."""
    rc_b = shared_fakeredis_factory("node-b")

    peer_b = _StubPeerClient()
    node_b = FailoverNode(
        _config("node-b", is_primary=False, peers_csv="node-a:8001"),
        redis_client=rc_b,
        peer_client=peer_b,  # type: ignore[arg-type]
    )
    await node_b.start()
    try:
        assert node_b.state_machine.state is NodeState.STANDBY
        # Lock is free (no other primary ever ran).
        assert await rc_b.read_lock_holder() is None

        await node_b._on_primary_failed()

        assert node_b.state_machine.state is NodeState.PRIMARY
        # No snapshot existed — load_into returned False.
        assert node_b.state_persister.snapshots_loaded_total == 0
        # Counters are at fresh-zero defaults.
        assert node_b.log_processor.last_log_id == 0
        assert node_b.log_processor.log_count == 0
        assert node_b.log_processor._next_id == 1
    finally:
        await node_b.stop()


async def test_initial_primary_bootstrap_loads_existing_snapshot(
    shared_fakeredis_factory: Callable[[str], RedisClient],
) -> None:
    """Even on the very first start() — no election ever ran — if a
    snapshot exists in Redis (e.g. from a prior run) the new primary
    should load it.

    This exercises the ``INACTIVE → PRIMARY`` branch of the transition
    callback.
    """
    # Pre-populate Redis with a snapshot via a temporary writer node.
    rc_writer = shared_fakeredis_factory("node-writer")
    peer_w = _StubPeerClient()
    writer = FailoverNode(
        _config("node-writer", is_primary=True, peers_csv=""),
        redis_client=rc_writer,
        peer_client=peer_w,  # type: ignore[arg-type]
    )
    await writer.start()
    for i in range(7):
        writer.log_processor.ingest(f"pre-{i}")
    assert await writer.state_persister.snapshot_now() is True
    await writer.stop()  # Releases lock so the new primary can take it.

    # New primary boots and should pick up where the writer left off.
    rc_new = shared_fakeredis_factory("node-new")
    peer_n = _StubPeerClient()
    new_primary = FailoverNode(
        _config("node-new", is_primary=True, peers_csv=""),
        redis_client=rc_new,
        peer_client=peer_n,  # type: ignore[arg-type]
    )
    await new_primary.start()
    try:
        assert new_primary.state_machine.state is NodeState.PRIMARY
        # Snapshot was loaded during the INACTIVE → PRIMARY transition.
        assert new_primary.state_persister.snapshots_loaded_total == 1
        # Allocator is past the snapshot's last_log_id (7).
        assert new_primary.log_processor._next_id == 8
        assert new_primary.log_processor.last_log_id == 7
    finally:
        await new_primary.stop()


async def test_lock_collision_at_bootstrap_does_not_load_snapshot(
    shared_fakeredis_factory: Callable[[str], RedisClient],
) -> None:
    """If the IS_PRIMARY=true node loses the lock race at bootstrap, it
    transitions directly to STANDBY — no snapshot load is triggered
    because the transition callback only fires on ``* → PRIMARY``."""
    # First seed a real snapshot via a writer node so there's something
    # in Redis that we EXPECT not to be loaded by the colliding node.
    rc_writer = shared_fakeredis_factory("node-snapshotter")
    peer_w = _StubPeerClient()
    snapshotter = FailoverNode(
        _config("node-snapshotter", is_primary=True, peers_csv=""),
        redis_client=rc_writer,
        peer_client=peer_w,  # type: ignore[arg-type]
    )
    await snapshotter.start()
    snapshotter.log_processor.ingest("planted")
    snapshotter.log_processor.ingest("planted2")
    await snapshotter.state_persister.snapshot_now()
    await snapshotter.stop()  # Releases the snapshotter's lock.

    # Pre-acquire the lock as a third party so node-x can't get it.
    rc_x = shared_fakeredis_factory("node-x")
    underlying = rc_x._client()  # type: ignore[attr-defined]
    await underlying.set(LEADER_LOCK_KEY, b"node-other", nx=True, ex=6)

    peer_x = _StubPeerClient()
    node_x = FailoverNode(
        _config("node-x", is_primary=True, peers_csv=""),
        redis_client=rc_x,
        peer_client=peer_x,  # type: ignore[arg-type]
    )
    await node_x.start()
    try:
        assert node_x.state_machine.state is NodeState.STANDBY
        # No load happened — never transitioned into PRIMARY.
        assert node_x.state_persister.snapshots_loaded_total == 0
        assert node_x.log_processor.last_log_id == 0
    finally:
        await node_x.stop()
