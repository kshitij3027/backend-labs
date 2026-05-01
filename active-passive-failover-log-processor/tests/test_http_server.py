"""Tests for src/http_server.py — endpoint contracts via FastAPI's TestClient.

Tests are ``async def`` so the pytest_asyncio "auto" mode picks them up;
this lets us mutate the state machine via ``await transition_to`` in the
SAME event loop as the fakeredis fixture before handing the app to a
``TestClient``. The TestClient internally serves each request in a
separate thread with its own event loop — that's a deliberate Starlette
design choice and is the supported way to exercise an async ASGI app
synchronously from a test.
"""

from __future__ import annotations

import time
from typing import Any, Awaitable, Callable

import pytest
from fastapi.testclient import TestClient

from src.config import NodeConfig
from src.election import ElectionCoordinator
from src.heartbeat import HeartbeatEmitter, HeartbeatMonitor
from src.http_server import create_app
from src.log_processor import LogProcessor
from src.models import (
    ElectionMessage,
    ElectionResult,
    HeartbeatMessage,
    NodeState,
    to_json,
)
from src.peer_client import PeerClient
from src.redis_client import RedisClient
from src.state_machine import NodeStateMachine


# =========================================================================
# Stubs / helpers
# =========================================================================


class _StubPeerClient:
    """Trivially-successful PeerClient for unit tests."""

    async def send_candidacy(self, peer: tuple[str, int], msg: ElectionMessage) -> bool:
        return True

    async def send_election_result(
        self, peer: tuple[str, int], result: ElectionResult
    ) -> bool:
        return True

    async def close(self) -> None:
        return None


def _build_components(
    fake_redis_client: RedisClient,
    *,
    node_id: str = "node-test",
    on_manual_failover: Callable[[], Awaitable[None]] | None = None,
    peers: list[tuple[str, int]] | None = None,
) -> dict[str, Any]:
    """Build a complete set of dependencies for ``create_app``.

    Returns a dict of components so individual tests can reach in and
    inspect counters / call records on the same instances the FastAPI
    handlers see.
    """
    config = NodeConfig(node_id=node_id, port=8001)  # type: ignore[call-arg]
    state_machine = NodeStateMachine(NodeState.INACTIVE, node_id=node_id)
    log_processor = LogProcessor()
    peer_client: PeerClient = _StubPeerClient()  # type: ignore[assignment]
    election_coordinator = ElectionCoordinator(
        node_id=node_id,
        priority=0,
        peers=peers if peers is not None else [],
        redis_client=fake_redis_client,
        peer_client=peer_client,
        lock_ttl=6,
        election_timeout=5.0,
        jitter_per_priority_unit=0.0,
    )
    heartbeat_emitter = HeartbeatEmitter(
        redis_client=fake_redis_client,
        state_provider=lambda: state_machine.state,
        metrics_provider=lambda: {"logs_per_sec": 0.0, "log_count": 0.0, "last_log_id": 0.0},
        node_id=node_id,
        interval=2.0,
        lock_ttl=6,
    )
    heartbeat_monitor = HeartbeatMonitor(
        redis_client=fake_redis_client,
        state_provider=lambda: state_machine.state,
        node_id=node_id,
        poll_interval=1.0,
        failure_timeout=6.0,
    )

    if on_manual_failover is None:

        async def _noop() -> None:
            return None

        on_manual_failover = _noop

    app = create_app(
        config=config,
        state_machine=state_machine,
        log_processor=log_processor,
        election_coordinator=election_coordinator,
        heartbeat_emitter=heartbeat_emitter,
        heartbeat_monitor=heartbeat_monitor,
        redis_client=fake_redis_client,
        on_manual_failover=on_manual_failover,
    )
    return {
        "app": app,
        "config": config,
        "state_machine": state_machine,
        "log_processor": log_processor,
        "election_coordinator": election_coordinator,
        "heartbeat_emitter": heartbeat_emitter,
        "heartbeat_monitor": heartbeat_monitor,
        "peer_client": peer_client,
    }


async def _set_state(state_machine: NodeStateMachine, target: NodeState) -> None:
    """Drive a NodeStateMachine to ``target`` via valid transitions only.

    The transition table is validated by the state machine itself, so we
    just walk the shortest valid path.
    """
    if state_machine.state is target:
        return
    if state_machine.state is NodeState.INACTIVE:
        if target is NodeState.STANDBY:
            await state_machine.transition_to(NodeState.STANDBY)
        elif target is NodeState.PRIMARY:
            await state_machine.transition_to(NodeState.PRIMARY)
        elif target is NodeState.FAILED:
            await state_machine.transition_to(NodeState.FAILED)
        elif target is NodeState.ELECTION:
            await state_machine.transition_to(NodeState.STANDBY)
            await state_machine.transition_to(NodeState.ELECTION)
        return
    if state_machine.state is NodeState.STANDBY:
        if target is NodeState.PRIMARY:
            await state_machine.transition_to(NodeState.PRIMARY)
        elif target is NodeState.ELECTION:
            await state_machine.transition_to(NodeState.ELECTION)
        elif target is NodeState.FAILED:
            await state_machine.transition_to(NodeState.FAILED)


# =========================================================================
# /health
# =========================================================================


@pytest.mark.parametrize(
    "state,expected_code",
    [
        (NodeState.INACTIVE, 503),
        (NodeState.STANDBY, 503),
        (NodeState.ELECTION, 503),
        (NodeState.FAILED, 503),
        (NodeState.PRIMARY, 200),
    ],
)
async def test_health_returns_correct_code_for_state(
    fake_redis_client: RedisClient, state: NodeState, expected_code: int
) -> None:
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], state)

    with TestClient(components["app"]) as client:
        resp = client.get("/health")

    assert resp.status_code == expected_code
    body = resp.json()
    assert body["state"] == state.value
    assert body["node_id"] == "node-test"
    assert body["status"] in {"healthy", "unhealthy"}


# =========================================================================
# /role
# =========================================================================


async def test_role_returns_full_shape(fake_redis_client: RedisClient) -> None:
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], NodeState.STANDBY)

    with TestClient(components["app"]) as client:
        resp = client.get("/role")

    assert resp.status_code == 200
    body = resp.json()
    for key in ("node_id", "state", "role", "lock_holder", "known_winner", "term"):
        assert key in body, f"missing key {key!r} in /role body: {body}"
    assert body["node_id"] == "node-test"
    assert body["state"] == "STANDBY"
    assert body["role"] == "standby"
    assert body["term"] == 0


@pytest.mark.parametrize(
    "state",
    [NodeState.INACTIVE, NodeState.STANDBY, NodeState.PRIMARY, NodeState.FAILED],
)
async def test_role_returns_200_on_every_state(
    fake_redis_client: RedisClient, state: NodeState
) -> None:
    """``/role`` must NEVER 503; it's the introspection-of-last-resort."""
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], state)
    with TestClient(components["app"]) as client:
        resp = client.get("/role")
    assert resp.status_code == 200, f"state={state} returned {resp.status_code}"


async def test_role_reflects_lock_holder(fake_redis_client: RedisClient) -> None:
    """If the lock is held in Redis, ``/role`` surfaces the holder."""
    # Pre-acquire as a different node by writing the underlying key.
    underlying = fake_redis_client._client()  # type: ignore[attr-defined]
    await underlying.set("leader:lock", b"node-other", nx=True, ex=6)

    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], NodeState.STANDBY)

    with TestClient(components["app"]) as client:
        resp = client.get("/role")
    body = resp.json()
    assert body["lock_holder"] == "node-other"


# =========================================================================
# /metrics
# =========================================================================


async def test_metrics_returns_plain_text(fake_redis_client: RedisClient) -> None:
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], NodeState.STANDBY)

    with TestClient(components["app"]) as client:
        resp = client.get("/metrics")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")


async def test_metrics_contains_every_documented_counter(
    fake_redis_client: RedisClient,
) -> None:
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], NodeState.PRIMARY)

    with TestClient(components["app"]) as client:
        resp = client.get("/metrics")

    body = resp.text
    expected = [
        "heartbeats_emitted_total",
        "lock_renewal_failures_total",
        "primary_failures_detected_total",
        "elections_run_total",
        "elections_won_total",
        "elections_lost_total",
        "elections_timed_out_total",
        "candidacies_received_total",
        "results_received_total",
        "logs_ingested_total",
        "logs_rejected_total",
    ]
    for name in expected:
        assert f"# HELP {name}" in body, f"missing HELP for {name!r}"
        assert f"# TYPE {name} counter" in body, f"missing TYPE for {name!r}"
        # ``name{node_id="node-test"} <int>`` value line.
        assert f'{name}{{node_id="node-test"}}' in body, f"missing value line for {name!r}"

    # node_state gauge — one line per state.
    assert "# TYPE node_state gauge" in body
    for state in NodeState:
        assert f'state="{state.value}"' in body
    # The matching state has value 1.
    assert 'node_state{node_id="node-test",state="PRIMARY"} 1' in body
    # And the others have value 0.
    assert 'node_state{node_id="node-test",state="STANDBY"} 0' in body


async def test_metrics_reflects_log_counters(fake_redis_client: RedisClient) -> None:
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], NodeState.PRIMARY)
    components["log_processor"].ingest("first")
    components["log_processor"].ingest("second")
    components["log_processor"].reject()

    with TestClient(components["app"]) as client:
        resp = client.get("/metrics")
    body = resp.text
    assert 'logs_ingested_total{node_id="node-test"} 2' in body
    assert 'logs_rejected_total{node_id="node-test"} 1' in body


# =========================================================================
# POST /logs
# =========================================================================


async def test_post_logs_primary_returns_201_and_increments(
    fake_redis_client: RedisClient,
) -> None:
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], NodeState.PRIMARY)

    with TestClient(components["app"]) as client:
        r1 = client.post("/logs", json={"message": "hello", "level": "INFO"})
        r2 = client.post("/logs", json={"message": "world"})

    assert r1.status_code == 201
    assert r1.json() == {"status": "accepted", "log_id": 1}
    assert r2.status_code == 201
    assert r2.json() == {"status": "accepted", "log_id": 2}
    assert components["log_processor"].log_count == 2


async def test_post_logs_standby_returns_503_and_bumps_rejected(
    fake_redis_client: RedisClient,
) -> None:
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], NodeState.STANDBY)

    with TestClient(components["app"]) as client:
        resp = client.post("/logs", json={"message": "hello"})

    assert resp.status_code == 503
    assert resp.json() == {"status": "rejected", "reason": "not_primary"}
    assert components["log_processor"].logs_rejected_total == 1
    assert components["log_processor"].log_count == 0


async def test_post_logs_idempotent_on_supplied_log_id(
    fake_redis_client: RedisClient,
) -> None:
    """Posting the same client-supplied ``log_id`` twice returns the same
    id and the count does not double."""
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], NodeState.PRIMARY)

    with TestClient(components["app"]) as client:
        r1 = client.post("/logs", json={"message": "hi", "log_id": 99})
        r2 = client.post("/logs", json={"message": "hi", "log_id": 99})

    assert r1.status_code == 201
    assert r1.json()["log_id"] == 99
    assert r2.status_code == 201
    assert r2.json()["log_id"] == 99
    assert components["log_processor"].log_count == 1


async def test_post_logs_invalid_json_returns_400(
    fake_redis_client: RedisClient,
) -> None:
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], NodeState.PRIMARY)

    with TestClient(components["app"]) as client:
        resp = client.post(
            "/logs",
            content=b"not-json",
            headers={"content-type": "application/json"},
        )

    assert resp.status_code == 400


async def test_post_logs_missing_message_returns_400(
    fake_redis_client: RedisClient,
) -> None:
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], NodeState.PRIMARY)

    with TestClient(components["app"]) as client:
        resp = client.post("/logs", json={"level": "INFO"})

    assert resp.status_code == 400


# =========================================================================
# GET /logs
# =========================================================================


async def test_get_logs_primary_returns_recent(
    fake_redis_client: RedisClient,
) -> None:
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], NodeState.PRIMARY)
    lp: LogProcessor = components["log_processor"]
    for i in range(5):
        lp.ingest(f"msg-{i}")

    with TestClient(components["app"]) as client:
        resp = client.get("/logs?limit=3")

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 5
    assert body["last_log_id"] == 5
    assert len(body["logs"]) == 3
    assert [e["log_id"] for e in body["logs"]] == [3, 4, 5]


async def test_get_logs_standby_returns_503(fake_redis_client: RedisClient) -> None:
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], NodeState.STANDBY)

    with TestClient(components["app"]) as client:
        resp = client.get("/logs")

    assert resp.status_code == 503
    assert resp.json() == {"status": "rejected", "reason": "not_primary"}


async def test_get_logs_default_limit_is_100(fake_redis_client: RedisClient) -> None:
    components = _build_components(fake_redis_client)
    await _set_state(components["state_machine"], NodeState.PRIMARY)
    for i in range(150):
        components["log_processor"].ingest(f"msg-{i}")

    with TestClient(components["app"]) as client:
        resp = client.get("/logs")

    body = resp.json()
    assert len(body["logs"]) == 100  # default limit


# =========================================================================
# /admin/trigger-failover
# =========================================================================


async def test_trigger_failover_primary_calls_callback_and_returns_202(
    fake_redis_client: RedisClient,
) -> None:
    call_count = {"n": 0}

    async def cb() -> None:
        call_count["n"] += 1

    components = _build_components(fake_redis_client, on_manual_failover=cb)
    await _set_state(components["state_machine"], NodeState.PRIMARY)

    with TestClient(components["app"]) as client:
        resp = client.post("/admin/trigger-failover")

    assert resp.status_code == 202
    assert resp.json() == {"status": "failover_triggered"}
    assert call_count["n"] == 1


async def test_trigger_failover_standby_returns_503_and_skips_callback(
    fake_redis_client: RedisClient,
) -> None:
    call_count = {"n": 0}

    async def cb() -> None:
        call_count["n"] += 1

    components = _build_components(fake_redis_client, on_manual_failover=cb)
    await _set_state(components["state_machine"], NodeState.STANDBY)

    with TestClient(components["app"]) as client:
        resp = client.post("/admin/trigger-failover")

    assert resp.status_code == 503
    assert resp.json() == {"status": "rejected", "reason": "not_primary"}
    assert call_count["n"] == 0


# =========================================================================
# POST /heartbeat
# =========================================================================


async def test_post_heartbeat_accepts_valid_message(
    fake_redis_client: RedisClient,
) -> None:
    components = _build_components(fake_redis_client)

    msg = HeartbeatMessage(
        node_id="node-other",
        timestamp=time.time(),
        state=NodeState.PRIMARY,
        role="primary",
        metrics={"logs_per_sec": 1.0, "last_log_id": 5.0, "log_count": 5.0},
    )

    with TestClient(components["app"]) as client:
        resp = client.post(
            "/heartbeat",
            content=to_json(msg),
            headers={"content-type": "application/json"},
        )

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_post_heartbeat_invalid_body_returns_400(
    fake_redis_client: RedisClient,
) -> None:
    components = _build_components(fake_redis_client)
    with TestClient(components["app"]) as client:
        resp = client.post(
            "/heartbeat",
            content=b"not-json",
            headers={"content-type": "application/json"},
        )
    assert resp.status_code == 400


# =========================================================================
# /election/candidacy
# =========================================================================


async def test_post_candidacy_invokes_handler_exactly_once(
    fake_redis_client: RedisClient,
) -> None:
    components = _build_components(fake_redis_client)
    coord: ElectionCoordinator = components["election_coordinator"]

    msg = ElectionMessage(
        candidate="node-other",
        priority=10,
        term=2,
        timestamp=time.time(),
    )

    before = coord.candidacies_received_total
    with TestClient(components["app"]) as client:
        resp = client.post(
            "/election/candidacy",
            content=to_json(msg),
            headers={"content-type": "application/json"},
        )

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
    assert coord.candidacies_received_total == before + 1


async def test_post_candidacy_invalid_body_returns_400(
    fake_redis_client: RedisClient,
) -> None:
    components = _build_components(fake_redis_client)
    coord: ElectionCoordinator = components["election_coordinator"]
    before = coord.candidacies_received_total
    with TestClient(components["app"]) as client:
        resp = client.post(
            "/election/candidacy",
            content=b"not-json",
            headers={"content-type": "application/json"},
        )
    assert resp.status_code == 400
    # Counter must not move on a parse failure.
    assert coord.candidacies_received_total == before


# =========================================================================
# /election/result
# =========================================================================


async def test_post_election_result_invokes_handler_and_updates_known_winner(
    fake_redis_client: RedisClient,
) -> None:
    components = _build_components(fake_redis_client)
    coord: ElectionCoordinator = components["election_coordinator"]

    result = ElectionResult(winner="node-z", term=7, timestamp=time.time())

    before_results = coord.results_received_total
    with TestClient(components["app"]) as client:
        resp = client.post(
            "/election/result",
            content=to_json(result),
            headers={"content-type": "application/json"},
        )

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
    assert coord.results_received_total == before_results + 1
    assert coord.known_winner == "node-z"


async def test_post_election_result_invalid_body_returns_400(
    fake_redis_client: RedisClient,
) -> None:
    components = _build_components(fake_redis_client)
    coord: ElectionCoordinator = components["election_coordinator"]
    before = coord.results_received_total
    with TestClient(components["app"]) as client:
        resp = client.post(
            "/election/result",
            content=b"not-json",
            headers={"content-type": "application/json"},
        )
    assert resp.status_code == 400
    assert coord.results_received_total == before
