"""Tests for src/election.py — ElectionCoordinator full protocol coverage.

Uses the ``fake_redis_client`` fixture from conftest. The peer client is
a hand-rolled stub (``_StubPeerClient``) that records every call so we
can assert broadcast fan-out, parallelism, and per-peer payloads.
"""

from __future__ import annotations

import asyncio
import time
from typing import Optional

import fakeredis.aioredis as fake_aioredis

from src.election import ElectionCoordinator
from src.models import ElectionMessage, ElectionResult, NodeState
from src.redis_client import RedisClient


# =========================================================================
# Test helpers
# =========================================================================


class _StubPeerClient:
    """Records every call so tests can inspect what the coordinator did.

    Records start times so we can assert that broadcast is parallel
    rather than sequential. ``send_*`` returns ``True`` by default; tests
    can subclass / monkeypatch to return False or sleep.
    """

    def __init__(self) -> None:
        self.candidacy_calls: list[tuple[tuple[str, int], ElectionMessage]] = []
        self.result_calls: list[tuple[tuple[str, int], ElectionResult]] = []
        self.candidacy_start_times: list[float] = []
        self.result_start_times: list[float] = []
        self.closed: bool = False

    async def send_candidacy(
        self,
        peer: tuple[str, int],
        msg: ElectionMessage,
    ) -> bool:
        self.candidacy_start_times.append(time.monotonic())
        self.candidacy_calls.append((peer, msg))
        return True

    async def send_election_result(
        self,
        peer: tuple[str, int],
        result: ElectionResult,
    ) -> bool:
        self.result_start_times.append(time.monotonic())
        self.result_calls.append((peer, result))
        return True

    async def close(self) -> None:
        self.closed = True


class _SlowPeerClient(_StubPeerClient):
    """Like the stub, but ``send_candidacy`` sleeps before returning."""

    def __init__(self, sleep_for: float) -> None:
        super().__init__()
        self._sleep_for = sleep_for

    async def send_candidacy(
        self,
        peer: tuple[str, int],
        msg: ElectionMessage,
    ) -> bool:
        self.candidacy_start_times.append(time.monotonic())
        self.candidacy_calls.append((peer, msg))
        await asyncio.sleep(self._sleep_for)
        return True


def _peers() -> list[tuple[str, int]]:
    return [("node-2", 8002), ("node-3", 8003)]


def _make_coordinator(
    redis_client: RedisClient,
    peer_client: _StubPeerClient,
    *,
    node_id: str = "node-test",
    priority: int = 0,
    peers: Optional[list[tuple[str, int]]] = None,
    lock_ttl: int = 6,
    election_timeout: float = 10.0,
    jitter_per_priority_unit: float = 0.0,
) -> ElectionCoordinator:
    """Construct a coordinator with sensible defaults for unit tests.

    Default ``jitter_per_priority_unit=0.0`` so tests don't pay the
    real-time jitter sleep unless they explicitly want it.
    """
    return ElectionCoordinator(
        node_id=node_id,
        priority=priority,
        peers=_peers() if peers is None else peers,
        redis_client=redis_client,
        peer_client=peer_client,
        lock_ttl=lock_ttl,
        election_timeout=election_timeout,
        jitter_per_priority_unit=jitter_per_priority_unit,
    )


# =========================================================================
# Smoke / sanity
# =========================================================================


async def test_priority_is_in_expected_range() -> None:
    """Sanity check: priority passed into the coordinator is honoured.

    The deterministic md5-based priority is unit-tested in test_config.py;
    here we just assert the value the caller supplies is exposed
    unchanged.
    """
    coord = ElectionCoordinator(
        node_id="node-x",
        priority=42,
        peers=[],
        redis_client=None,  # type: ignore[arg-type]
        peer_client=None,  # type: ignore[arg-type]
    )
    assert coord.priority == 42
    assert 0 <= coord.priority < 1000


async def test_initial_metrics_are_all_zero() -> None:
    coord = ElectionCoordinator(
        node_id="node-x",
        priority=0,
        peers=[],
        redis_client=None,  # type: ignore[arg-type]
        peer_client=None,  # type: ignore[arg-type]
    )
    assert coord.metrics == {
        "elections_run_total": 0,
        "elections_won_total": 0,
        "elections_lost_total": 0,
        "elections_timed_out_total": 0,
        "candidacies_received_total": 0,
        "results_received_total": 0,
    }
    assert coord.current_term == 0
    assert coord.known_winner is None


# =========================================================================
# Win path
# =========================================================================


async def test_run_election_wins_and_returns_primary(
    fake_redis_client: RedisClient,
) -> None:
    """When the lock is free, we win — counters, broadcasts, and state all match."""
    peer = _StubPeerClient()
    coord = _make_coordinator(fake_redis_client, peer)

    result = await coord.run_election()

    assert result is NodeState.PRIMARY
    assert coord.elections_run_total == 1
    assert coord.elections_won_total == 1
    assert coord.elections_lost_total == 0
    assert coord.current_term == 1
    assert coord.known_winner == "node-test"

    # Candidacy was broadcast to every peer with the right payload.
    assert len(peer.candidacy_calls) == len(_peers())
    sent_peers = {p for p, _ in peer.candidacy_calls}
    assert sent_peers == set(_peers())
    for _, msg in peer.candidacy_calls:
        assert msg.candidate == "node-test"
        assert msg.priority == 0
        assert msg.term == 1

    # Result was broadcast with winner == us.
    assert len(peer.result_calls) == len(_peers())
    for _, res in peer.result_calls:
        assert res.winner == "node-test"
        assert res.term == 1


async def test_winning_actually_holds_the_lock(
    fake_redis_client: RedisClient,
) -> None:
    peer = _StubPeerClient()
    coord = _make_coordinator(fake_redis_client, peer)

    await coord.run_election()
    holder = await fake_redis_client.read_lock_holder()
    assert holder == "node-test"


# =========================================================================
# Lose path
# =========================================================================


async def test_run_election_loses_and_returns_standby(
    fake_redis_client: RedisClient,
) -> None:
    """If another node already holds the lock, we lose cleanly."""
    # Pre-set the lock as a different owner. We can't use
    # ``acquire_lock`` here because that would grab it as ``node-test``;
    # write the bytes directly through the underlying fakeredis.
    underlying = fake_redis_client._client()  # type: ignore[attr-defined]
    await underlying.set("leader:lock", b"other-node", nx=True, ex=6)

    peer = _StubPeerClient()
    coord = _make_coordinator(fake_redis_client, peer)

    result = await coord.run_election()

    assert result is NodeState.STANDBY
    assert coord.elections_run_total == 1
    assert coord.elections_won_total == 0
    assert coord.elections_lost_total == 1
    assert coord.known_winner == "other-node"

    # Candidacy still broadcast to all peers.
    assert len(peer.candidacy_calls) == len(_peers())

    # Result broadcast with the actual winner so peers converge.
    assert len(peer.result_calls) == len(_peers())
    for _, res in peer.result_calls:
        assert res.winner == "other-node"


async def test_lose_with_disappearing_lock_skips_result_broadcast(
    fake_redis_client: RedisClient,
) -> None:
    """If the lock is held when we try SET NX but vanishes before our GET,
    we have no winner to advertise and must NOT broadcast a phantom result.
    """
    peer = _StubPeerClient()

    # Pre-acquire the lock as a foreign node so SET NX fails.
    underlying = fake_redis_client._client()  # type: ignore[attr-defined]
    await underlying.set("leader:lock", b"ghost", nx=True, ex=6)

    # Monkeypatch read_lock_holder to simulate the TTL-race condition
    # where the lock is gone by the time we try to read it.
    async def _gone() -> Optional[str]:
        return None

    fake_redis_client.read_lock_holder = _gone  # type: ignore[method-assign]

    coord = _make_coordinator(fake_redis_client, peer)
    result = await coord.run_election()

    assert result is NodeState.STANDBY
    assert coord.elections_lost_total == 1
    # Candidacy was sent (happens before the lock attempt).
    assert len(peer.candidacy_calls) == len(_peers())
    # Result was NOT sent — we have no winner to advertise.
    assert len(peer.result_calls) == 0


# =========================================================================
# Empty peer list
# =========================================================================


async def test_no_peers_win_path(fake_redis_client: RedisClient) -> None:
    """Single-node-mode win: empty peer list, no broadcasts, still wins."""
    peer = _StubPeerClient()
    coord = _make_coordinator(fake_redis_client, peer, peers=[])

    result = await coord.run_election()

    assert result is NodeState.PRIMARY
    assert coord.elections_won_total == 1
    assert peer.candidacy_calls == []
    assert peer.result_calls == []


async def test_no_peers_lose_path(fake_redis_client: RedisClient) -> None:
    """Single-node-mode loss: empty peer list, no broadcasts, still loses."""
    underlying = fake_redis_client._client()  # type: ignore[attr-defined]
    await underlying.set("leader:lock", b"other-node", nx=True, ex=6)

    peer = _StubPeerClient()
    coord = _make_coordinator(fake_redis_client, peer, peers=[])

    result = await coord.run_election()

    assert result is NodeState.STANDBY
    assert coord.elections_lost_total == 1
    assert peer.candidacy_calls == []
    assert peer.result_calls == []


# =========================================================================
# Concurrent contention — exactly one winner
# =========================================================================


async def test_concurrent_elections_only_one_winner() -> None:
    """Two coordinators sharing the same Redis race for the lock.

    Both call ``run_election`` simultaneously; exactly one returns
    ``PRIMARY`` and the other returns ``STANDBY``. This is the central
    correctness property of the whole protocol.
    """
    fake = fake_aioredis.FakeRedis(decode_responses=False)

    rc_a = RedisClient(host="localhost", port=6379, node_id="node-a")
    rc_a._redis = fake  # type: ignore[attr-defined]
    rc_b = RedisClient(host="localhost", port=6379, node_id="node-b")
    rc_b._redis = fake  # type: ignore[attr-defined]

    peer_a = _StubPeerClient()
    peer_b = _StubPeerClient()

    coord_a = ElectionCoordinator(
        node_id="node-a",
        priority=0,
        peers=[],
        redis_client=rc_a,
        peer_client=peer_a,
        lock_ttl=6,
        election_timeout=5.0,
        jitter_per_priority_unit=0.0,
    )
    coord_b = ElectionCoordinator(
        node_id="node-b",
        priority=0,
        peers=[],
        redis_client=rc_b,
        peer_client=peer_b,
        lock_ttl=6,
        election_timeout=5.0,
        jitter_per_priority_unit=0.0,
    )

    try:
        result_a, result_b = await asyncio.gather(
            coord_a.run_election(),
            coord_b.run_election(),
        )

        outcomes = {result_a, result_b}
        # Exactly one PRIMARY and one STANDBY; never two of the same.
        assert outcomes == {NodeState.PRIMARY, NodeState.STANDBY}

        # Counters: exactly one win and one loss across the pair.
        wins = coord_a.elections_won_total + coord_b.elections_won_total
        losses = coord_a.elections_lost_total + coord_b.elections_lost_total
        assert wins == 1
        assert losses == 1

        # Both coordinators agree on who the winner is.
        assert coord_a.known_winner == coord_b.known_winner
        assert coord_a.known_winner in {"node-a", "node-b"}
    finally:
        await fake.flushall()
        await fake.aclose()


# =========================================================================
# Election timeout
# =========================================================================


async def test_run_election_times_out(
    fake_redis_client: RedisClient,
) -> None:
    """A peer client that sleeps longer than the timeout forces wait_for to fire."""
    peer = _SlowPeerClient(sleep_for=0.5)
    coord = _make_coordinator(
        fake_redis_client,
        peer,
        election_timeout=0.1,
    )

    result = await coord.run_election()

    assert result is NodeState.STANDBY
    assert coord.elections_timed_out_total == 1
    assert coord.elections_won_total == 0
    assert coord.elections_lost_total == 0
    # The run was started (term incremented, run counter bumped) before
    # the timeout fired.
    assert coord.elections_run_total == 1
    assert coord.current_term == 1


# =========================================================================
# Receive-side handlers
# =========================================================================


async def test_handle_candidacy_increments_counter(
    fake_redis_client: RedisClient,
) -> None:
    coord = _make_coordinator(fake_redis_client, _StubPeerClient())

    msg = ElectionMessage(
        candidate="node-other",
        priority=10,
        term=5,
        timestamp=time.time(),
    )
    await coord.handle_candidacy(msg)
    await coord.handle_candidacy(msg)

    assert coord.candidacies_received_total == 2
    # handle_candidacy is informational only; it must not change
    # known_winner or trigger a vote.
    assert coord.known_winner is None


async def test_handle_election_result_updates_known_winner(
    fake_redis_client: RedisClient,
) -> None:
    coord = _make_coordinator(fake_redis_client, _StubPeerClient())

    result = ElectionResult(winner="node-z", term=3, timestamp=time.time())
    await coord.handle_election_result(result)

    assert coord.results_received_total == 1
    assert coord.known_winner == "node-z"

    # A second result for a different winner overwrites known_winner.
    result2 = ElectionResult(winner="node-y", term=4, timestamp=time.time())
    await coord.handle_election_result(result2)
    assert coord.known_winner == "node-y"
    assert coord.results_received_total == 2


# =========================================================================
# Jitter scales with priority
# =========================================================================


async def test_jitter_scales_with_priority(
    fake_redis_client: RedisClient,
) -> None:
    """Higher-priority coordinator should sleep longer before its lock attempt."""
    # Use a tiny scale so the test stays fast (~50ms vs ~0ms).
    scale = 0.001  # 1ms per priority unit

    peer_low = _StubPeerClient()
    coord_low = _make_coordinator(
        fake_redis_client,
        peer_low,
        priority=0,
        peers=[],
        jitter_per_priority_unit=scale,
    )

    t0 = time.monotonic()
    await coord_low.run_election()
    low_elapsed = time.monotonic() - t0
    assert coord_low.elections_won_total == 1

    # Free the lock for the next coordinator.
    await fake_redis_client.release_lock()

    peer_high = _StubPeerClient()
    coord_high = _make_coordinator(
        fake_redis_client,
        peer_high,
        priority=500,
        peers=[],
        jitter_per_priority_unit=scale,
    )

    t1 = time.monotonic()
    await coord_high.run_election()
    high_elapsed = time.monotonic() - t1
    assert coord_high.elections_won_total == 1

    # priority=500 with scale=0.001 means ~0.5s of jitter; priority=0
    # means ~0s of jitter. The high-priority run must take noticeably
    # longer than the low-priority run.
    delta = high_elapsed - low_elapsed
    assert delta >= 0.3, (
        f"expected higher-priority run to sleep ~0.5s longer, got delta={delta:.3f}s"
    )


# =========================================================================
# Broadcast is parallel, not sequential
# =========================================================================


async def test_candidacy_broadcast_is_parallel(
    fake_redis_client: RedisClient,
) -> None:
    """All peer ``send_candidacy`` calls must start nearly simultaneously."""
    # 5 peers, each call sleeps 50ms. If serial, total = 250ms+. If
    # parallel, total ≈ 50ms and the start times cluster within a few ms.
    peers = [(f"node-{i}", 8000 + i) for i in range(5)]
    peer = _SlowPeerClient(sleep_for=0.05)

    coord = _make_coordinator(
        fake_redis_client,
        peer,
        peers=peers,
        # Skip jitter so we measure broadcast timing only.
        jitter_per_priority_unit=0.0,
    )

    await coord.run_election()

    # Every peer was called.
    assert len(peer.candidacy_start_times) == len(peers)

    # All starts happened within a small window — i.e. they ran
    # concurrently, not back-to-back.
    spread = max(peer.candidacy_start_times) - min(peer.candidacy_start_times)
    assert spread < 0.05, (
        f"candidacy broadcast was serial, not parallel (spread={spread:.4f}s)"
    )


async def test_result_broadcast_is_parallel(
    fake_redis_client: RedisClient,
) -> None:
    """Same parallelism guarantee for the result broadcast on the win path."""

    class _SlowResult(_StubPeerClient):
        async def send_election_result(
            self,
            peer: tuple[str, int],
            result: ElectionResult,
        ) -> bool:
            self.result_start_times.append(time.monotonic())
            self.result_calls.append((peer, result))
            await asyncio.sleep(0.05)
            return True

    peers = [(f"node-{i}", 8000 + i) for i in range(5)]
    peer = _SlowResult()
    coord = _make_coordinator(fake_redis_client, peer, peers=peers)

    await coord.run_election()

    assert len(peer.result_start_times) == len(peers)
    spread = max(peer.result_start_times) - min(peer.result_start_times)
    assert spread < 0.05, (
        f"result broadcast was serial, not parallel (spread={spread:.4f}s)"
    )


# =========================================================================
# Term increments on every run
# =========================================================================


async def test_term_increments_per_run(
    fake_redis_client: RedisClient,
) -> None:
    """Term bumps by 1 every time run_election is called, regardless of outcome."""
    peer = _StubPeerClient()
    coord = _make_coordinator(fake_redis_client, peer)

    assert coord.current_term == 0
    await coord.run_election()
    assert coord.current_term == 1

    # Free the lock so the next run can win again.
    await fake_redis_client.release_lock()
    await coord.run_election()
    assert coord.current_term == 2


# =========================================================================
# Peer failures don't break the coordinator
# =========================================================================


async def test_peer_send_candidacy_returning_false_does_not_break_election(
    fake_redis_client: RedisClient,
) -> None:
    """A peer client that returns False (peer down) is the normal failover case."""

    class _FailingPeer(_StubPeerClient):
        async def send_candidacy(self, peer, msg):
            self.candidacy_calls.append((peer, msg))
            return False

        async def send_election_result(self, peer, result):
            self.result_calls.append((peer, result))
            return False

    peer = _FailingPeer()
    coord = _make_coordinator(fake_redis_client, peer)

    result = await coord.run_election()

    # Election still wins because the lock acquire still works.
    assert result is NodeState.PRIMARY
    assert coord.elections_won_total == 1
    # Calls were attempted to every peer.
    assert len(peer.candidacy_calls) == len(_peers())
    assert len(peer.result_calls) == len(_peers())


async def test_peer_send_candidacy_raising_does_not_break_election(
    fake_redis_client: RedisClient,
) -> None:
    """An exception from the peer client must not propagate.

    ``asyncio.gather(..., return_exceptions=True)`` should catch it.
    """

    class _RaisingPeer(_StubPeerClient):
        async def send_candidacy(self, peer, msg):
            self.candidacy_calls.append((peer, msg))
            raise RuntimeError("boom")

        async def send_election_result(self, peer, result):
            self.result_calls.append((peer, result))
            raise RuntimeError("boom")

    peer = _RaisingPeer()
    coord = _make_coordinator(fake_redis_client, peer)

    result = await coord.run_election()
    assert result is NodeState.PRIMARY
    assert coord.elections_won_total == 1
