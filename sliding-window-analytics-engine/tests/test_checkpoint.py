"""Unit tests for the Redis-backed checkpoint path.

These tests never touch a real Redis — they drop an
``unittest.mock.AsyncMock`` in place of ``CheckpointStore._client`` so
they can inspect the exact ``set`` / ``get`` calls and craft custom
return values for edge cases.

Coverage:
* ``SlidingWindow.state_dict`` / ``load_state`` round-trip.
* ``WindowManager.state_dict`` / ``load_state`` round-trip.
* ``CheckpointStore.save`` writes a JSON payload containing all windows.
* ``CheckpointStore.load`` rehydrates windows end-to-end.
* ``CheckpointStore.load`` drops stale checkpoints past ``max_age_seconds``.
* ``checkpoint_loop`` saves periodically and exits when the stop event fires.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from unittest.mock import AsyncMock

import pytest

from src.checkpoint import CHECKPOINT_KEY, CheckpointStore, checkpoint_loop
from src.config import Config
from src.models import Event, WindowConfig
from src.sliding_window import SlidingWindow
from src.window_manager import WindowManager, build_default_manager

# ``asyncio_mode = auto`` in pytest.ini takes care of awaiting async
# test functions, so we don't need per-test ``@pytest.mark.asyncio``.


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _make_event(ts: float, value: float, metric: str = "response_time") -> Event:
    return Event(
        event_id=str(uuid.uuid4()),
        timestamp=ts,
        value=value,
        metric=metric,
        metadata={},
    )


def _make_window(
    *,
    name: str = "response_time_1m",
    resolution: str = "1m",
    window_size: float = 60.0,
    slide_interval: float = 5.0,
    max_size: int = 100,
) -> SlidingWindow:
    return SlidingWindow(
        name=name,
        resolution=resolution,
        window_size=window_size,
        slide_interval=slide_interval,
        max_size=max_size,
    )


def _fresh_manager() -> WindowManager:
    """A small two-window manager reused by most round-trip tests."""
    manager = WindowManager()
    manager.add_window(
        WindowConfig(
            metric="response_time",
            resolution="1m",
            window_size=60.0,
            slide_interval=5.0,
            max_size=100,
        )
    )
    manager.add_window(
        WindowConfig(
            metric="throughput",
            resolution="1m",
            window_size=60.0,
            slide_interval=5.0,
            max_size=100,
        )
    )
    return manager


def _store_with_mock_client(
    max_age: float = 3600.0,
) -> tuple[CheckpointStore, AsyncMock]:
    """Build a CheckpointStore whose client is an :class:`AsyncMock`."""
    store = CheckpointStore(host="ignored", port=0, max_age_seconds=max_age)
    mock = AsyncMock()
    store.client = mock
    return store, mock


# --------------------------------------------------------------------------- #
# SlidingWindow state_dict <-> load_state round-trip
# --------------------------------------------------------------------------- #

async def test_sliding_window_state_dict_roundtrip() -> None:
    """Serializing and reloading a window preserves count/sum/min/max/std_dev."""
    window = _make_window()
    base_ts = 1_000_000.0
    values = [10.0, 20.0, 30.0, 40.0, 50.0]
    for i, v in enumerate(values):
        window.add(_make_event(ts=base_ts + i, value=v))

    state = window.state_dict()

    # state_dict should capture config + events, no derived stats.
    assert state["name"] == "response_time_1m"
    assert state["resolution"] == "1m"
    assert state["window_size"] == 60.0
    assert state["slide_interval"] == 5.0
    assert state["max_size"] == 100
    assert len(state["events"]) == len(values)
    for ev, v in zip(state["events"], values):
        assert ev["value"] == v
        assert ev["metric"] == "response_time"

    restored = _make_window()
    restored.load_state(state)

    # Snapshot the original vs restored window at the same wall-clock time;
    # the restored window should match byte-for-byte on the stat fields.
    now = base_ts + len(values)
    original = window.snapshot(now=now)
    replay = restored.snapshot(now=now)

    assert replay.count == original.count
    assert replay.sum == pytest.approx(original.sum)
    assert replay.average == pytest.approx(original.average)
    assert replay.min == pytest.approx(original.min)
    assert replay.max == pytest.approx(original.max)
    assert replay.std_dev == pytest.approx(original.std_dev)
    assert restored.size() == len(values)


async def test_sliding_window_load_state_clears_previous_buffer() -> None:
    """load_state should wipe any pre-existing buffer/stats first."""
    window = _make_window()
    # Seed with 3 events that should be discarded on load_state.
    for v in (99.0, 88.0, 77.0):
        window.add(_make_event(ts=1_000.0, value=v))
    assert window.size() == 3

    fresh_state = {
        "name": "response_time_1m",
        "resolution": "1m",
        "window_size": 60.0,
        "slide_interval": 5.0,
        "max_size": 100,
        "events": [
            {
                "event_id": "a",
                "timestamp": 2_000.0,
                "value": 5.0,
                "metric": "response_time",
                "metadata": {},
            },
        ],
    }
    window.load_state(fresh_state)

    snap = window.snapshot(now=2_000.0)
    assert snap.count == 1
    assert snap.sum == pytest.approx(5.0)


# --------------------------------------------------------------------------- #
# WindowManager state_dict <-> load_state round-trip
# --------------------------------------------------------------------------- #

async def test_window_manager_state_dict_roundtrip() -> None:
    """A WindowManager should survive a full serialize/deserialize cycle."""
    manager = _fresh_manager()
    base_ts = 2_000_000.0
    # 5 response_time events + 3 throughput events.
    for i in range(5):
        manager.dispatch(_make_event(ts=base_ts + i, value=float(i + 1)))
    for i in range(3):
        manager.dispatch(
            _make_event(ts=base_ts + i, value=100.0 * (i + 1), metric="throughput")
        )

    state = manager.state_dict()
    # Keys should be "<metric>__<resolution>".
    assert "response_time__1m" in state
    assert "throughput__1m" in state

    # Serialise to JSON and back to prove it's JSON-safe.
    reloaded = json.loads(json.dumps(state))

    fresh = _fresh_manager()
    restored = fresh.load_state(reloaded)
    assert restored == 2

    now = base_ts + 10
    original_snap = manager.snapshot_all(now)
    replay_snap = fresh.snapshot_all(now)

    for metric in ("response_time", "throughput"):
        orig = original_snap[metric]["1m"]
        rep = replay_snap[metric]["1m"]
        assert rep.count == orig.count
        assert rep.sum == pytest.approx(orig.sum)
        assert rep.min == pytest.approx(orig.min)
        assert rep.max == pytest.approx(orig.max)


async def test_window_manager_load_state_ignores_unknown_keys() -> None:
    """Keys that don't match a registered window should be silently dropped."""
    manager = _fresh_manager()
    bogus_state = {
        "nonexistent__1m": {"events": []},
        "response_time__1m": {
            "name": "response_time_1m",
            "resolution": "1m",
            "window_size": 60.0,
            "slide_interval": 5.0,
            "max_size": 100,
            "events": [
                {
                    "event_id": "x",
                    "timestamp": 1.0,
                    "value": 42.0,
                    "metric": "response_time",
                    "metadata": {},
                }
            ],
        },
        "badkey-no-delimiter": {"events": []},
        "response_time__4h": {"events": []},  # valid key, missing window
    }

    restored = manager.load_state(bogus_state)
    assert restored == 1

    window = manager.get_window("response_time", "1m")
    assert window is not None
    snap = window.snapshot(now=1.0)
    assert snap.count == 1
    assert snap.sum == pytest.approx(42.0)


# --------------------------------------------------------------------------- #
# CheckpointStore.save / load with mocked Redis
# --------------------------------------------------------------------------- #

async def test_checkpoint_save_roundtrip_via_mocked_redis() -> None:
    """save() must serialise every window into a single JSON payload."""
    manager = _fresh_manager()
    base_ts = 3_000_000.0
    for i in range(5):
        manager.dispatch(_make_event(ts=base_ts + i, value=float(i + 1)))

    store, mock_client = _store_with_mock_client()

    ok = await store.save(manager)
    assert ok is True
    mock_client.set.assert_awaited_once()

    args, kwargs = mock_client.set.call_args
    # Positional args are (key, value) — both forms are supported.
    if len(args) == 2:
        key, raw = args
    else:
        key, raw = kwargs["name"], kwargs["value"]
    assert key == CHECKPOINT_KEY

    payload = json.loads(raw)
    assert "saved_at" in payload
    assert "state" in payload
    assert set(payload["state"].keys()) == {"response_time__1m", "throughput__1m"}

    rt_state = payload["state"]["response_time__1m"]
    assert len(rt_state["events"]) == 5


async def test_checkpoint_save_returns_false_without_client() -> None:
    """A store with no client should skip the write and return False."""
    store = CheckpointStore(host="ignored", port=0)
    assert store.client is None
    assert await store.save(_fresh_manager()) is False


async def test_checkpoint_load_restores_events() -> None:
    """load() must replay the stored events into a fresh WindowManager."""
    # Prepare a saved payload using a "producer" manager.
    producer = _fresh_manager()
    base_ts = 4_000_000.0
    for i in range(7):
        producer.dispatch(_make_event(ts=base_ts + i, value=float(i * 2)))

    payload = {
        "saved_at": time.time() - 5.0,  # fresh
        "state": producer.state_dict(),
    }

    # Prepare a fresh consumer manager + a store whose mock client
    # returns the payload from GET.
    consumer = _fresh_manager()
    store, mock_client = _store_with_mock_client()
    mock_client.get.return_value = json.dumps(payload)

    restored = await store.load(consumer)
    assert restored == 2

    snap = consumer.snapshot_all(now=base_ts + 10)
    rt = snap["response_time"]["1m"]
    assert rt.count == 7
    assert rt.sum == pytest.approx(sum(float(i * 2) for i in range(7)))


async def test_checkpoint_load_ignores_stale() -> None:
    """A checkpoint older than max_age_seconds must be discarded."""
    producer = _fresh_manager()
    base_ts = 5_000_000.0
    for i in range(3):
        producer.dispatch(_make_event(ts=base_ts + i, value=1.0))

    # saved_at is 2h in the past, but max_age is 1h → stale.
    payload = {
        "saved_at": time.time() - 7200.0,
        "state": producer.state_dict(),
    }

    consumer = _fresh_manager()
    store, mock_client = _store_with_mock_client(max_age=3600.0)
    mock_client.get.return_value = json.dumps(payload)

    restored = await store.load(consumer)
    assert restored == 0

    snap = consumer.snapshot_all(now=base_ts + 5)
    assert snap["response_time"]["1m"].count == 0
    assert snap["throughput"]["1m"].count == 0


async def test_checkpoint_load_returns_zero_when_key_absent() -> None:
    """A missing Redis key is not an error — just a zero-restore."""
    store, mock_client = _store_with_mock_client()
    mock_client.get.return_value = None

    manager = _fresh_manager()
    restored = await store.load(manager)
    assert restored == 0


async def test_checkpoint_load_returns_zero_on_invalid_json() -> None:
    """Corrupt JSON should not crash the loader."""
    store, mock_client = _store_with_mock_client()
    mock_client.get.return_value = "not-json-at-all"

    manager = _fresh_manager()
    restored = await store.load(manager)
    assert restored == 0


async def test_checkpoint_load_returns_zero_without_client() -> None:
    """load() without a connected client is a silent no-op."""
    store = CheckpointStore(host="ignored", port=0)
    assert await store.load(_fresh_manager()) == 0


# --------------------------------------------------------------------------- #
# checkpoint_loop behaviour
# --------------------------------------------------------------------------- #

async def test_checkpoint_loop_runs_until_stop() -> None:
    """The loop must call save periodically and exit when stop_event is set."""
    manager = _fresh_manager()
    store = CheckpointStore(host="ignored", port=0)
    # Patch the save method directly so we don't need a client at all.
    save_mock = AsyncMock(return_value=True)
    store.save = save_mock  # type: ignore[assignment]

    stop_event = asyncio.Event()

    async def stopper() -> None:
        # Let the loop make at least one save call, then signal stop.
        await asyncio.sleep(0.15)
        stop_event.set()

    await asyncio.gather(
        checkpoint_loop(
            store, manager, interval_seconds=0.05, stop_event=stop_event
        ),
        stopper(),
    )

    assert save_mock.await_count >= 1


async def test_checkpoint_loop_swallows_save_errors() -> None:
    """A raising save() must not kill the loop — it should just keep trying."""
    manager = _fresh_manager()
    store = CheckpointStore(host="ignored", port=0)

    calls = {"count": 0}

    async def flaky_save(_wm: WindowManager) -> bool:
        calls["count"] += 1
        raise RuntimeError("boom")

    store.save = flaky_save  # type: ignore[assignment]

    stop_event = asyncio.Event()

    async def stopper() -> None:
        await asyncio.sleep(0.15)
        stop_event.set()

    await asyncio.gather(
        checkpoint_loop(
            store, manager, interval_seconds=0.05, stop_event=stop_event
        ),
        stopper(),
    )
    # The loop should have tried at least once despite the exception.
    assert calls["count"] >= 1


async def test_checkpoint_save_load_against_default_manager() -> None:
    """Smoke test over the canonical 7-window layout."""
    manager = build_default_manager(Config())
    base_ts = 6_000_000.0
    for i in range(4):
        manager.dispatch(_make_event(ts=base_ts + i, value=10.0 * (i + 1)))
        manager.dispatch(
            _make_event(ts=base_ts + i, value=float(i), metric="throughput")
        )
        manager.dispatch(
            _make_event(ts=base_ts + i, value=float(i) / 100.0, metric="error_rate")
        )

    store, mock_client = _store_with_mock_client()
    await store.save(manager)
    mock_client.set.assert_awaited_once()

    args, _ = mock_client.set.call_args
    _, raw = args
    payload = json.loads(raw)
    # 7 windows in the canonical layout.
    assert len(payload["state"]) == 7

    # Round-trip through a fresh manager.
    fresh = build_default_manager(Config())
    mock_client.get.return_value = raw
    restored = await store.load(fresh)
    assert restored == 7

    now = base_ts + 10
    original = manager.snapshot_all(now)
    replay = fresh.snapshot_all(now)
    assert original["response_time"]["1m"].count == replay["response_time"]["1m"].count
    assert original["throughput"]["1m"].sum == pytest.approx(
        replay["throughput"]["1m"].sum
    )
