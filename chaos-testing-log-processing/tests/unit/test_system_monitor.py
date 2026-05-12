"""Unit tests for C6 — :class:`SystemMonitor` continuous metrics collector.

Covers the loop lifecycle, deque eviction, listener fan-out, per-tick
resilience, Docker stats math, and stop semantics. Everything I/O-related
is mocked:

- ``psutil`` host metrics are monkeypatched to deterministic values.
- ``DockerClient`` is a plain ``MagicMock`` whose ``list_chaos_targets``
  returns fake container mocks with ``.name`` + ``.stats(stream=False)``.
- ``httpx.AsyncClient`` is injected via the ``http_client`` constructor
  param using :class:`httpx.MockTransport` so probes resolve synchronously
  with controlled status codes.

The success-criteria test from the C6 brief is named exactly
``test_metrics_collection`` and lives at module-top-level so it can be
selected by name.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from src.models.metrics import ServiceHealth, SystemMetrics
from src.monitoring.system_monitor import SystemMonitor


# --------------------------------------------------------------------------- #
# Fixed values used by the psutil monkeypatches so assertions can be precise.
# --------------------------------------------------------------------------- #
FAKE_CPU_PCT = 42.5
FAKE_MEM_PCT = 33.3
FAKE_DISK_PCT = 27.7

# Standard synthetic Docker stats sample. ``cpu_delta/system_delta * 4 * 100``
# = (1e9 / 10e9) * 4 * 100 = 40.0%. Mem usage / limit = 512MB / 1024MB = 50%.
SYNTHETIC_STATS: dict[str, Any] = {
    "cpu_stats": {
        "cpu_usage": {"total_usage": 2_000_000_000},
        "system_cpu_usage": 20_000_000_000,
        "online_cpus": 4,
    },
    "precpu_stats": {
        "cpu_usage": {"total_usage": 1_000_000_000},
        "system_cpu_usage": 10_000_000_000,
    },
    "memory_stats": {
        "usage": 512 * 1024 * 1024,
        "limit": 1024 * 1024 * 1024,
    },
}


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _patch_psutil(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force psutil to return our deterministic FAKE_* values."""
    import psutil

    monkeypatch.setattr(
        psutil, "cpu_percent", lambda interval=None: FAKE_CPU_PCT
    )
    fake_vmem = MagicMock(percent=FAKE_MEM_PCT)
    monkeypatch.setattr(psutil, "virtual_memory", lambda: fake_vmem)
    fake_disk = MagicMock(percent=FAKE_DISK_PCT)
    monkeypatch.setattr(psutil, "disk_usage", lambda _path: fake_disk)


def _make_container_mock(name: str, stats: dict[str, Any] | None = None) -> MagicMock:
    """Build a fake ``docker.models.containers.Container`` lookalike."""
    container = MagicMock(name=f"container[{name}]")
    container.name = name
    container.stats = MagicMock(return_value=stats or SYNTHETIC_STATS)
    return container


def _make_docker_client(target_names: list[str]) -> MagicMock:
    """Build a fake DockerClient whose list_chaos_targets returns our mocks."""
    docker_client = MagicMock(name="docker_client")
    docker_client.list_chaos_targets = MagicMock(
        return_value=[_make_container_mock(n) for n in target_names]
    )
    return docker_client


def _make_http_client_200(target_names: list[str]) -> httpx.AsyncClient:
    """Return an ``AsyncClient`` whose every probe answers 200 OK."""

    def _handler(request: httpx.Request) -> httpx.Response:
        # Sanity-check the URL pattern: ``http://{name}:8000/health``.
        host = request.url.host
        assert host in target_names, f"unexpected host: {host}"
        assert request.url.path == "/health"
        return httpx.Response(200, json={"status": "ok"})

    return httpx.AsyncClient(transport=httpx.MockTransport(_handler))


async def _wait_for_ticks(monitor: SystemMonitor, n: int, timeout: float = 2.0) -> None:
    """Spin until the monitor's history has at least ``n`` entries."""
    deadline = asyncio.get_event_loop().time() + timeout
    while monitor.history_size() < n:
        if asyncio.get_event_loop().time() > deadline:
            return
        await asyncio.sleep(0.01)


# --------------------------------------------------------------------------- #
# A. Success-criteria test (must be named exactly ``test_metrics_collection``)
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_metrics_collection(monkeypatch: pytest.MonkeyPatch) -> None:
    """C6 success-criteria: continuous collection populates snapshot + history.

    Build the monitor with a fast tick, run it long enough for >=3 ticks,
    then assert every field is wired through end-to-end.
    """
    _patch_psutil(monkeypatch)

    targets = ["log-producer", "log-consumer"]
    docker_client = _make_docker_client(targets)
    http_client = _make_http_client_200(targets)

    monitor = SystemMonitor(
        docker_client=docker_client,
        interval_seconds=0.05,
        history_size=5,
        target_health_paths={name: "/health" for name in targets},
        http_client=http_client,
    )

    try:
        await monitor.start()
        # Need 3+ ticks; 0.3s / 0.05s = 6 expected ticks. Generous margin.
        await _wait_for_ticks(monitor, 3, timeout=2.0)
        await asyncio.sleep(0.05)
    finally:
        await monitor.stop()
        await http_client.aclose()

    # --- Deque bookkeeping --------------------------------------------------
    size = monitor.history_size()
    assert 3 <= size <= 5, f"expected 3..5 entries, got {size}"

    snap = monitor.snapshot()
    assert snap is not None, "snapshot must be populated after >=1 tick"
    assert isinstance(snap, SystemMetrics)

    # --- Timestamp is timezone-aware UTC -----------------------------------
    assert snap.timestamp.tzinfo is not None
    assert snap.timestamp.utcoffset() == timezone.utc.utcoffset(snap.timestamp)

    # --- Host metrics straight from the psutil patches ---------------------
    assert snap.cpu_pct == pytest.approx(FAKE_CPU_PCT)
    assert snap.mem_pct == pytest.approx(FAKE_MEM_PCT)
    assert snap.disk_pct == pytest.approx(FAKE_DISK_PCT)

    # --- Container stats: both targets present, non-negative ---------------
    assert set(snap.container_stats.keys()) == set(targets)
    for name in targets:
        stats = snap.container_stats[name]
        assert stats["cpu_pct"] >= 0.0
        assert stats["mem_pct"] >= 0.0

    # --- Service health: both targets healthy (200 from MockTransport) -----
    health_by_name = {sh.name: sh for sh in snap.service_health}
    assert set(health_by_name.keys()) == set(targets)
    for name in targets:
        assert health_by_name[name].is_healthy is True

    # --- History ordered ascending by timestamp ----------------------------
    history = monitor.history()
    timestamps = [s.timestamp for s in history]
    assert timestamps == sorted(timestamps)


# --------------------------------------------------------------------------- #
# B. Deque eviction
# --------------------------------------------------------------------------- #


class TestHistoryEviction:
    """The ``deque(maxlen=history_size)`` must cap the buffer."""

    @pytest.mark.asyncio
    async def test_only_last_n_retained(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_psutil(monkeypatch)
        targets = ["log-producer"]
        docker_client = _make_docker_client(targets)
        http_client = _make_http_client_200(targets)

        monitor = SystemMonitor(
            docker_client=docker_client,
            interval_seconds=0.02,
            history_size=2,
            target_health_paths={"log-producer": "/health"},
            http_client=http_client,
        )

        try:
            await monitor.start()
            # 5 ticks of headroom: 0.02s * 5 = 0.10s plus margin.
            await _wait_for_ticks(monitor, 5, timeout=2.0)
        finally:
            await monitor.stop()
            await http_client.aclose()

        assert monitor.history_size() == 2
        history = monitor.history()
        assert len(history) == 2
        # Surviving entries are the two most recent (timestamps ascending).
        assert history[0].timestamp <= history[1].timestamp


# --------------------------------------------------------------------------- #
# C. Listener invocation
# --------------------------------------------------------------------------- #


class TestListeners:
    """Sync + async listeners both fire every tick and isolate failures."""

    @pytest.mark.asyncio
    async def test_sync_and_async_listeners_both_called(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_psutil(monkeypatch)
        targets = ["log-producer"]
        docker_client = _make_docker_client(targets)
        http_client = _make_http_client_200(targets)

        sync_calls: list[SystemMetrics] = []
        async_calls: list[SystemMetrics] = []

        def sync_listener(sample: SystemMetrics) -> None:
            sync_calls.append(sample)

        async def async_listener(sample: SystemMetrics) -> None:
            async_calls.append(sample)

        monitor = SystemMonitor(
            docker_client=docker_client,
            interval_seconds=0.02,
            history_size=10,
            target_health_paths={"log-producer": "/health"},
            http_client=http_client,
        )
        monitor.add_listener(sync_listener)
        monitor.add_listener(async_listener)

        try:
            await monitor.start()
            await _wait_for_ticks(monitor, 3, timeout=2.0)
        finally:
            await monitor.stop()
            await http_client.aclose()

        ticks_observed = monitor.history_size()
        assert ticks_observed >= 3
        # Listeners run on the same tick as the snapshot, so call counts
        # must equal the number of completed ticks.
        assert len(sync_calls) == ticks_observed
        assert len(async_calls) == ticks_observed
        for sample in sync_calls + async_calls:
            assert isinstance(sample, SystemMetrics)

    @pytest.mark.asyncio
    async def test_raising_listener_does_not_break_loop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One listener blowing up must not stop other listeners or ticks."""
        _patch_psutil(monkeypatch)
        targets = ["log-producer"]
        docker_client = _make_docker_client(targets)
        http_client = _make_http_client_200(targets)

        bad_calls: list[int] = []
        good_calls: list[SystemMetrics] = []

        def bad_listener(sample: SystemMetrics) -> None:
            bad_calls.append(1)
            raise RuntimeError("boom")

        async def good_listener(sample: SystemMetrics) -> None:
            good_calls.append(sample)

        monitor = SystemMonitor(
            docker_client=docker_client,
            interval_seconds=0.02,
            history_size=10,
            target_health_paths={"log-producer": "/health"},
            http_client=http_client,
        )
        monitor.add_listener(bad_listener)
        monitor.add_listener(good_listener)

        try:
            await monitor.start()
            await _wait_for_ticks(monitor, 3, timeout=2.0)
        finally:
            await monitor.stop()
            await http_client.aclose()

        ticks_observed = monitor.history_size()
        assert ticks_observed >= 3
        # The bad listener was called each tick (didn't get pulled).
        assert len(bad_calls) == ticks_observed
        # The good listener was still called every tick.
        assert len(good_calls) == ticks_observed


# --------------------------------------------------------------------------- #
# D. Tick failure resilience
# --------------------------------------------------------------------------- #


class TestTickResilience:
    """A single failing ``_collect_once`` must not kill the loop."""

    @pytest.mark.asyncio
    async def test_collect_failure_logged_and_loop_continues(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_psutil(monkeypatch)
        targets = ["log-producer"]
        docker_client = _make_docker_client(targets)
        http_client = _make_http_client_200(targets)

        monitor = SystemMonitor(
            docker_client=docker_client,
            interval_seconds=0.02,
            history_size=10,
            target_health_paths={"log-producer": "/health"},
            http_client=http_client,
        )

        call_count = {"n": 0}
        good_sample = SystemMetrics(
            timestamp=datetime.now(timezone.utc),
            cpu_pct=1.0,
            mem_pct=2.0,
            disk_pct=3.0,
            service_health=[],
            container_stats={},
        )

        async def flaky_collect_once() -> SystemMetrics:
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("first tick blew up")
            return good_sample

        monkeypatch.setattr(monitor, "_collect_once", flaky_collect_once)

        try:
            await monitor.start()
            # Wait until at least one successful tick has been recorded.
            await _wait_for_ticks(monitor, 1, timeout=2.0)
        finally:
            await monitor.stop()
            await http_client.aclose()

        # The first call raised so history is empty after tick 1; tick 2
        # succeeded so we should see at least one entry.
        assert call_count["n"] >= 2, (
            f"loop should have retried after a failure, only called "
            f"{call_count['n']} time(s)"
        )
        assert monitor.history_size() >= 1
        snap = monitor.snapshot()
        assert snap is not None
        assert snap.cpu_pct == 1.0  # i.e., from the good sample, not psutil


# --------------------------------------------------------------------------- #
# E. Docker stats math
# --------------------------------------------------------------------------- #


class TestDockerStatsMath:
    """Per-container ``cpu_pct`` + ``mem_pct`` come from raw Docker stats."""

    @pytest.mark.asyncio
    async def test_cpu_and_mem_percent_computed_from_stats(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cpu_delta/system_delta * online_cpus * 100; mem usage/limit * 100."""
        _patch_psutil(monkeypatch)
        targets = ["log-producer"]
        # SYNTHETIC_STATS already encodes cpu_delta=1e9, system_delta=1e10,
        # online_cpus=4 -> 40.0%, mem=512/1024 -> 50.0%.
        docker_client = _make_docker_client(targets)
        http_client = _make_http_client_200(targets)

        monitor = SystemMonitor(
            docker_client=docker_client,
            interval_seconds=0.02,
            history_size=5,
            target_health_paths={"log-producer": "/health"},
            http_client=http_client,
        )

        try:
            await monitor.start()
            await _wait_for_ticks(monitor, 1, timeout=2.0)
        finally:
            await monitor.stop()
            await http_client.aclose()

        snap = monitor.snapshot()
        assert snap is not None
        stats = snap.container_stats["log-producer"]
        assert stats["cpu_pct"] == pytest.approx(40.0, abs=0.5)
        assert stats["mem_pct"] == pytest.approx(50.0, abs=0.5)

    @pytest.mark.asyncio
    async def test_zero_system_delta_yields_zero_cpu_pct(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Division by zero must be guarded — first-sample case."""
        _patch_psutil(monkeypatch)

        zero_system_stats = {
            "cpu_stats": {
                "cpu_usage": {"total_usage": 1_000_000_000},
                "system_cpu_usage": 10_000_000_000,
                "online_cpus": 4,
            },
            "precpu_stats": {
                "cpu_usage": {"total_usage": 0},
                "system_cpu_usage": 10_000_000_000,  # == cpu_stats -> delta 0
            },
            "memory_stats": {
                "usage": 100 * 1024 * 1024,
                "limit": 1024 * 1024 * 1024,
            },
        }

        targets = ["log-producer"]
        docker_client = MagicMock(name="docker_client")
        docker_client.list_chaos_targets = MagicMock(
            return_value=[_make_container_mock("log-producer", zero_system_stats)]
        )
        http_client = _make_http_client_200(targets)

        monitor = SystemMonitor(
            docker_client=docker_client,
            interval_seconds=0.02,
            history_size=5,
            target_health_paths={"log-producer": "/health"},
            http_client=http_client,
        )

        try:
            await monitor.start()
            await _wait_for_ticks(monitor, 1, timeout=2.0)
        finally:
            await monitor.stop()
            await http_client.aclose()

        snap = monitor.snapshot()
        assert snap is not None
        stats = snap.container_stats["log-producer"]
        # No crash; cpu_pct gracefully degrades to 0.0 on zero delta.
        assert stats["cpu_pct"] == 0.0
        # mem_pct is still computed normally.
        assert stats["mem_pct"] == pytest.approx(100 / 1024 * 100, abs=0.5)


# --------------------------------------------------------------------------- #
# F. Stop semantics
# --------------------------------------------------------------------------- #


class TestStopSemantics:
    """Stop must be idempotent and cancel the background task cleanly."""

    @pytest.mark.asyncio
    async def test_task_done_after_stop_and_double_stop_is_noop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _patch_psutil(monkeypatch)
        targets = ["log-producer"]
        docker_client = _make_docker_client(targets)
        http_client = _make_http_client_200(targets)

        monitor = SystemMonitor(
            docker_client=docker_client,
            interval_seconds=0.02,
            history_size=5,
            target_health_paths={"log-producer": "/health"},
            http_client=http_client,
        )

        try:
            await monitor.start()
            captured_task = monitor._task
            assert captured_task is not None
            await _wait_for_ticks(monitor, 1, timeout=2.0)

            await monitor.stop()

            # The captured task should now be in a done state (cancelled
            # or finished), even though the attribute has been cleared.
            assert captured_task.done()
            assert monitor._task is None

            # Second stop is a no-op: must not raise.
            await monitor.stop()
            assert monitor._task is None
        finally:
            await http_client.aclose()
