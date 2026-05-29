"""WebSocket integration tests for the ``/ws/metrics`` live stream (C8).

These drive the *real* application (``src.main.app``) through its lifespan using
Starlette/FastAPI's :class:`~fastapi.testclient.TestClient`, which speaks the ASGI
WebSocket protocol *and* runs the app's ``lifespan`` — so the background
:func:`~src.main.optimization_loop` is actually running and broadcasting a fresh
``_ws_payload`` on every tick. No network, no Docker; the cross-container probe
lives in the e2e step.

Determinism / timing note
-------------------------
The broadcast cadence is ``OPTIMIZATION_INTERVAL`` seconds. To keep these tests
fast and non-flaky the suite is *run* with ``OPTIMIZATION_INTERVAL=0.1`` (see the
C8 run command), so ticks fire ~10x/sec and a handful of ``receive_json`` calls
return within well under a second. The very first message a client gets is the
immediate personal push sent right after ``connect`` (see ``ws_metrics`` in
``src.main``); its ``snapshot`` may legitimately be ``null`` if the loop has not
recorded a tick yet, so we tolerate that.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from src.main import app

# Keys every tick envelope must carry (see src.main._ws_payload).
_ENVELOPE_KEYS = {"type", "snapshot", "status", "series"}
# Parallel chart series the dashboard relies on (see AdaptiveBatcher.metrics_series).
_SERIES_KEYS = {
    "throughput",
    "batch_size",
    "cpu_percent",
    "memory_percent",
    "queue_depth",
    "state",
}


def _assert_well_formed_tick(msg: dict) -> None:
    """Assert a single broadcast message matches the ``_ws_payload`` contract."""
    assert isinstance(msg, dict), f"payload is not a dict: {msg!r}"
    assert _ENVELOPE_KEYS.issubset(msg), (
        f"missing envelope keys; got {sorted(msg)}"
    )
    assert msg["type"] == "tick", f"unexpected type: {msg['type']!r}"

    status = msg["status"]
    assert isinstance(status, dict), "status must be a dict"
    assert "state" in status, "status missing 'state'"
    assert "batch_size" in status, "status missing 'batch_size'"
    # Batch size must stay inside the configured safety bounds [50, 5000].
    assert 50 <= status["batch_size"] <= 5000, (
        f"batch_size out of bounds: {status['batch_size']}"
    )

    series = msg["series"]
    assert isinstance(series, dict), "series must be a dict"
    assert _SERIES_KEYS.issubset(series), (
        f"series missing keys; got {sorted(series)}"
    )

    # snapshot may be null on the very first (immediate) message; if present it
    # must be a dict carrying a batch_size.
    snap = msg["snapshot"]
    assert snap is None or isinstance(snap, dict), "snapshot must be dict or null"
    if isinstance(snap, dict):
        assert "batch_size" in snap, "snapshot missing 'batch_size'"


def test_immediate_payload_on_connect() -> None:
    """A fresh client receives a well-formed tick envelope immediately on connect.

    ``ws_metrics`` pushes the current state right after registering the socket, so
    the dashboard paints without waiting a full control-loop interval.
    """
    with TestClient(app) as client:
        with client.websocket_connect("/ws/metrics") as ws:
            msg = ws.receive_json()

    _assert_well_formed_tick(msg)
    assert msg["status"]["state"], "state should be a non-empty value"


def test_live_streaming_updates() -> None:
    """The stream is live: several well-formed ticks arrive and the data advances.

    We pull a handful of messages and assert (a) every one is well-formed and
    (b) the stream is genuinely progressing — at least one observable signal moves
    across the batch: the throughput series grows, the snapshot transitions from
    null to populated, or a snapshot metric / status changes. We stay tolerant and
    do not pin exact values (the optimizer + simulated load make them vary).
    """
    received: list[dict] = []
    with TestClient(app) as client:
        with client.websocket_connect("/ws/metrics") as ws:
            for _ in range(6):
                received.append(ws.receive_json())

    # Every message is well-formed; we expect at least 3 (run interval is 0.1s).
    assert len(received) >= 3, f"expected >=3 messages, got {len(received)}"
    for msg in received:
        _assert_well_formed_tick(msg)

    # --- Liveness: at least one signal must advance across the received batch. ---
    throughput_lens = [len(m["series"]["throughput"]) for m in received]
    series_grew = throughput_lens[-1] > throughput_lens[0]

    snaps = [m["snapshot"] for m in received]
    snapshot_appeared = snaps[0] is None and any(s is not None for s in snaps)

    populated = [s for s in snaps if isinstance(s, dict)]
    snapshot_changed = False
    if len(populated) >= 2:
        bs = {s["batch_size"] for s in populated}
        tp = {round(float(s["throughput"]), 6) for s in populated}
        snapshot_changed = len(bs) > 1 or len(tp) > 1

    statuses = [
        (m["status"]["batch_size"], m["status"].get("reason")) for m in received
    ]
    status_changed = len(set(statuses)) > 1

    assert series_grew or snapshot_appeared or snapshot_changed or status_changed, (
        "stream did not advance across received messages: "
        f"throughput_lens={throughput_lens}, "
        f"snapshot_appeared={snapshot_appeared}, "
        f"snapshot_changed={snapshot_changed}, status_changed={status_changed}"
    )


def test_two_clients_both_receive() -> None:
    """Broadcast fan-out: two concurrent clients each receive a well-formed tick."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws/metrics") as ws_a:
            with client.websocket_connect("/ws/metrics") as ws_b:
                msg_a = ws_a.receive_json()
                msg_b = ws_b.receive_json()
                # Both should also see a subsequent broadcast (fan-out, not just
                # the per-client immediate push).
                next_a = ws_a.receive_json()
                next_b = ws_b.receive_json()

    for msg in (msg_a, msg_b, next_a, next_b):
        _assert_well_formed_tick(msg)


def test_disconnect_is_clean() -> None:
    """Losing a client does not crash the server; remaining clients still stream.

    We open a survivor, then open-and-drop a second client inside a nested block.
    The connection manager prunes the dropped socket on its next broadcast send;
    the survivor must keep receiving well-formed ticks afterwards.
    """
    with TestClient(app) as client:
        with client.websocket_connect("/ws/metrics") as survivor:
            # First payload for the survivor.
            _assert_well_formed_tick(survivor.receive_json())

            # Open a second client and let it go away when this block exits.
            with client.websocket_connect("/ws/metrics") as doomed:
                _assert_well_formed_tick(doomed.receive_json())
            # `doomed` is now closed/pruned.

            # The survivor must continue to receive well-formed broadcasts.
            post = [survivor.receive_json() for _ in range(3)]

    assert len(post) == 3
    for msg in post:
        _assert_well_formed_tick(msg)
