"""Continuous system metrics collector.

Runs as a single asyncio task started in the FastAPI lifespan. On each
tick it samples:

- Host (i.e., framework container) CPU%, memory%, disk% via psutil.
- Per-target Docker container stats (CPU%, memory%) via the Docker SDK.
- Optional service health: HTTP GET on each target's :8000/health, capturing
  ``latency_ms`` and an ``is_healthy`` boolean.

The latest snapshot is exposed via :meth:`snapshot`; the rolling history
(deque, ``maxlen=metrics_history_size``) via :meth:`history`. Listeners
subscribed with :meth:`add_listener` are awaited on every tick so the
WebSocket broadcaster (C14) and SafetySupervisor (C15) can react in
real time.

Failures during a single tick (Docker socket hiccup, target unreachable,
psutil glitch) are caught and logged --- the monitor keeps running. The
``snapshot``/``history`` API never raises.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Optional

import httpx
import psutil

from ..docker_client.client import DockerClient
from ..models.metrics import ServiceHealth, SystemMetrics


logger = logging.getLogger(__name__)


# Listener callbacks may be either plain (sync) callables or coroutine
# functions. Both shapes are supported; coroutine results are awaited.
ListenerCallback = Callable[[SystemMetrics], Optional[Awaitable[None]]]


class SystemMonitor:
    """Background metrics collector with bounded rolling history.

    A single asyncio task drives the loop; each tick collects host metrics
    (via psutil), per-target Docker stats (via the injected
    :class:`DockerClient`), and optional HTTP health probes. The latest
    sample is exposed via :meth:`snapshot` and the rolling history via
    :meth:`history`. Listeners registered with :meth:`add_listener` are
    invoked on each tick.

    Parameters:
        docker_client: Allowlisted Docker SDK wrapper from C5.
        interval_seconds: Polling cadence in seconds.
        history_size: ``maxlen`` for the internal ``deque`` of snapshots.
        target_health_paths: Mapping of container name -> HTTP path to
            probe for ``/health``. The probe URL is built as
            ``http://{container_name}:8000{path}`` since the framework
            shares ``chaos-net`` with the targets. Defaults to ``{}``.
        http_client: Optional injected :class:`httpx.AsyncClient` (test
            seam for ``httpx.MockTransport``). If ``None``, the monitor
            constructs (and later closes) its own client with a 2s timeout.
        clock: Monotonic time source for measuring health-probe latency.
            Defaults to :func:`time.monotonic`. Exposed as a test seam.
    """

    def __init__(
        self,
        docker_client: DockerClient,
        interval_seconds: float = 5.0,
        history_size: int = 1000,
        target_health_paths: dict[str, str] | None = None,
        http_client: httpx.AsyncClient | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._docker = docker_client
        self._interval = float(interval_seconds)
        self._target_health_paths: dict[str, str] = dict(target_health_paths or {})
        self._clock = clock

        # HTTP client lifecycle: if the caller injected one, we don't own it
        # and we must NOT close it on stop(). If we built our own, we close.
        self._owns_http_client = http_client is None
        self._http_client = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(2.0)
        )

        self._history: deque[SystemMetrics] = deque(maxlen=history_size)
        self._latest: SystemMetrics | None = None
        self._listeners: list[ListenerCallback] = []

        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    # ------------------------------------------------------------------ #
    # Listener pub/sub
    # ------------------------------------------------------------------ #

    def add_listener(self, callback: ListenerCallback) -> None:
        """Register a callback invoked on every tick with the new snapshot.

        Callbacks can be either sync or ``async`` callables. Exceptions
        raised by one listener never break others --- they are logged and
        the loop moves on.
        """
        if callback not in self._listeners:
            self._listeners.append(callback)

    def remove_listener(self, callback: ListenerCallback) -> None:
        """Unregister a previously added listener (no-op if absent)."""
        try:
            self._listeners.remove(callback)
        except ValueError:
            pass

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    async def start(self) -> None:
        """Spawn the background collector task if not already running."""
        if self._task is not None and not self._task.done():
            return
        self._stop.clear()
        self._task = asyncio.create_task(
            self._loop(), name="chaos.system-monitor"
        )

    async def stop(self) -> None:
        """Signal the loop to stop, await the task, and release resources."""
        self._stop.set()
        task = self._task
        if task is not None:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                # CancelledError is expected on stop; any other exception
                # has already been logged inside the loop. Swallow either
                # way --- stop() must be best-effort.
                pass
        self._task = None

        if self._owns_http_client:
            try:
                await self._http_client.aclose()
            except Exception:  # noqa: BLE001
                logger.exception("error closing monitor http client")

    # ------------------------------------------------------------------ #
    # Public accessors (never raise)
    # ------------------------------------------------------------------ #

    def snapshot(self) -> SystemMetrics | None:
        """Return the most recent snapshot, or ``None`` before the first tick."""
        return self._latest

    def history(self) -> list[SystemMetrics]:
        """Return a copy of the rolling history (oldest -> newest)."""
        return list(self._history)

    def history_size(self) -> int:
        """Return how many snapshots are currently buffered."""
        return len(self._history)

    # ------------------------------------------------------------------ #
    # Internal loop
    # ------------------------------------------------------------------ #

    async def _loop(self) -> None:
        """Main collector loop --- runs until :meth:`stop` is called."""
        while not self._stop.is_set():
            try:
                sample = await self._collect_once()
                self._latest = sample
                self._history.append(sample)
                await self._notify_listeners(sample)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("metrics tick failed")

            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=self._interval
                )
            except asyncio.TimeoutError:
                # Normal: the interval elapsed without a stop signal.
                pass

    # ------------------------------------------------------------------ #
    # Per-tick collection
    # ------------------------------------------------------------------ #

    async def _collect_once(self) -> SystemMetrics:
        """Build and return a fresh :class:`SystemMetrics` snapshot."""
        cpu_pct = self._safe_host_cpu()
        mem_pct = self._safe_host_mem()
        disk_pct = self._safe_host_disk()

        container_stats = await asyncio.to_thread(self._collect_container_stats)
        service_health = await self._collect_service_health()

        healthy_latencies = [
            sh.latency_ms
            for sh in service_health
            if sh.is_healthy and sh.latency_ms is not None
        ]
        network_latency_ms: float | None
        if healthy_latencies:
            network_latency_ms = sum(healthy_latencies) / len(healthy_latencies)
        else:
            network_latency_ms = None

        return SystemMetrics(
            timestamp=datetime.now(timezone.utc),
            cpu_pct=cpu_pct,
            mem_pct=mem_pct,
            disk_pct=disk_pct,
            network_latency_ms=network_latency_ms,
            service_health=service_health,
            container_stats=container_stats,
        )

    # -- Host metrics ---------------------------------------------------- #

    @staticmethod
    def _safe_host_cpu() -> float:
        """Return host CPU% (0..100). Falls back to 0.0 on any psutil glitch."""
        try:
            # ``interval=None`` -> non-blocking; compares to last cached read.
            return float(psutil.cpu_percent(interval=None))
        except Exception:  # noqa: BLE001
            logger.exception("psutil.cpu_percent failed")
            return 0.0

    @staticmethod
    def _safe_host_mem() -> float:
        try:
            return float(psutil.virtual_memory().percent)
        except Exception:  # noqa: BLE001
            logger.exception("psutil.virtual_memory failed")
            return 0.0

    @staticmethod
    def _safe_host_disk() -> float:
        try:
            return float(psutil.disk_usage("/").percent)
        except Exception:  # noqa: BLE001
            logger.exception("psutil.disk_usage failed")
            return 0.0

    # -- Container stats ------------------------------------------------- #

    def _collect_container_stats(self) -> dict[str, dict[str, float]]:
        """Sample CPU% + memory% for every allowlisted chaos target.

        Runs in a worker thread (Docker SDK is blocking). Exceptions for
        individual containers are logged and skipped; one bad container
        must not break the snapshot.
        """
        out: dict[str, dict[str, float]] = {}
        try:
            targets = self._docker.list_chaos_targets()
        except Exception:  # noqa: BLE001
            logger.exception("docker.list_chaos_targets failed")
            return out

        for container in targets:
            try:
                stats = container.stats(stream=False)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "container.stats failed for %s", getattr(container, "name", "?")
                )
                continue

            try:
                cpu_pct = self._compute_cpu_pct(stats)
                mem_pct = self._compute_mem_pct(stats)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "stats parse failed for %s", getattr(container, "name", "?")
                )
                continue

            out[container.name] = {"cpu_pct": cpu_pct, "mem_pct": mem_pct}
        return out

    @staticmethod
    def _compute_cpu_pct(stats: dict) -> float:
        """Compute container CPU% from a single docker stats sample.

        Docker stats schema:
            cpu_delta = cpu_stats.cpu_usage.total_usage
                      - precpu_stats.cpu_usage.total_usage
            system_delta = cpu_stats.system_cpu_usage
                         - precpu_stats.system_cpu_usage
            cpu_pct = (cpu_delta / system_delta) * num_cpus * 100

        Returns 0.0 when ``system_delta <= 0`` (first sample, container
        just started, or the kernel reported a non-monotonic value).
        """
        cpu_stats = stats.get("cpu_stats") or {}
        precpu_stats = stats.get("precpu_stats") or {}

        cpu_usage = (cpu_stats.get("cpu_usage") or {}).get("total_usage", 0)
        pre_cpu_usage = (precpu_stats.get("cpu_usage") or {}).get("total_usage", 0)
        cpu_delta = float(cpu_usage) - float(pre_cpu_usage)

        system_usage = float(cpu_stats.get("system_cpu_usage", 0) or 0)
        pre_system_usage = float(precpu_stats.get("system_cpu_usage", 0) or 0)
        system_delta = system_usage - pre_system_usage

        if system_delta <= 0 or cpu_delta < 0:
            return 0.0

        num_cpus = cpu_stats.get("online_cpus")
        if not num_cpus:
            percpu = (cpu_stats.get("cpu_usage") or {}).get("percpu_usage") or []
            num_cpus = len(percpu) or 1

        return (cpu_delta / system_delta) * float(num_cpus) * 100.0

    @staticmethod
    def _compute_mem_pct(stats: dict) -> float:
        """Compute container memory% from a single docker stats sample."""
        mem = stats.get("memory_stats") or {}
        usage = float(mem.get("usage", 0) or 0)
        limit = float(mem.get("limit", 0) or 0)
        if limit <= 0:
            return 0.0
        return (usage / limit) * 100.0

    # -- Service health probes ------------------------------------------ #

    async def _collect_service_health(self) -> list[ServiceHealth]:
        """Probe ``/health`` on each target in ``target_health_paths``.

        URL pattern: ``http://{container_name}:8000{path}``. The framework
        sits on the same ``chaos-net`` bridge so container names resolve
        as DNS. ``latency_ms`` is recorded for successful probes; failed
        probes record ``latency_ms=None`` and ``is_healthy=False``.
        """
        out: list[ServiceHealth] = []
        for name, path in self._target_health_paths.items():
            url = f"http://{name}:8000{path}"
            start = self._clock()
            is_healthy = False
            latency_ms: float | None = None
            try:
                response = await self._http_client.get(url)
                end = self._clock()
                latency_ms = max((end - start) * 1000.0, 0.0)
                is_healthy = 200 <= response.status_code < 300
            except Exception as exc:  # noqa: BLE001
                # Probe never connected (DNS, TCP, timeout, etc.) ->
                # latency_ms stays None per the ServiceHealth contract.
                logger.debug(
                    "health probe failed name=%s url=%s err=%s",
                    name,
                    url,
                    exc.__class__.__name__,
                )

            out.append(
                ServiceHealth(
                    name=name,
                    is_healthy=is_healthy,
                    last_check_at=datetime.now(timezone.utc),
                    latency_ms=latency_ms,
                )
            )
        return out

    # ------------------------------------------------------------------ #
    # Listeners
    # ------------------------------------------------------------------ #

    async def _notify_listeners(self, sample: SystemMetrics) -> None:
        """Invoke every listener; isolate per-listener failures."""
        for listener in list(self._listeners):
            try:
                result = listener(sample)
                if inspect.isawaitable(result):
                    await result
            except Exception:  # noqa: BLE001
                logger.exception(
                    "metrics listener failed listener=%s",
                    getattr(listener, "__qualname__", repr(listener)),
                )


# --------------------------------------------------------------------------- #
# Module-level singleton accessors
# --------------------------------------------------------------------------- #
#
# ``src/main.py`` populates this on startup so the WebSocket broadcaster (C14)
# and SafetySupervisor (C15) can reach the live monitor without having to
# thread it through every call site. Reset to ``None`` on shutdown.

_MONITOR_SINGLETON: SystemMonitor | None = None


def set_monitor(monitor: SystemMonitor | None) -> None:
    """Install (or clear) the module-level :class:`SystemMonitor` singleton."""
    global _MONITOR_SINGLETON
    _MONITOR_SINGLETON = monitor


def get_monitor() -> SystemMonitor | None:
    """Return the installed :class:`SystemMonitor`, or ``None`` if not set."""
    return _MONITOR_SINGLETON
