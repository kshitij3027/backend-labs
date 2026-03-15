"""Worker pool with dynamic scaling for processing prioritized log messages."""

import logging
import threading
import time

from src.config import Settings
from src.metrics import MetricsTracker
from src.models import Priority
from src.priority_queue import ThreadSafePriorityQueue

logger = logging.getLogger(__name__)


class WorkerPool:
    """A pool of worker threads that drain a priority queue.

    Workers simulate processing by sleeping for a priority-dependent duration.
    The pool supports dynamic scaling: the built-in monitor thread periodically
    checks queue utilization and adjusts the worker count up or down.
    """

    def __init__(
        self,
        queue: ThreadSafePriorityQueue,
        metrics: MetricsTracker,
        settings: Settings,
    ) -> None:
        self._queue = queue
        self._metrics = metrics
        self._settings = settings

        self._workers: list[threading.Thread] = []
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._worker_id_counter: int = 0
        self._target_workers: int = 0
        self._worker_stop_events: dict[int, threading.Event] = {}
        self._monitor_thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Processing-time lookup
    # ------------------------------------------------------------------

    def _get_process_time(self, priority: Priority) -> float:
        """Return the configured processing time in seconds for *priority*."""
        ms = {
            Priority.CRITICAL: self._settings.critical_process_time_ms,
            Priority.HIGH: self._settings.high_process_time_ms,
            Priority.MEDIUM: self._settings.medium_process_time_ms,
            Priority.LOW: self._settings.low_process_time_ms,
        }[priority]
        return ms / 1000.0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, num_workers: int | None = None) -> None:
        """Create and start worker threads plus a monitor thread."""
        if num_workers is None:
            num_workers = self._settings.num_workers

        self._stop_event.clear()
        self._target_workers = num_workers

        for _ in range(num_workers):
            self._add_worker()

        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="worker-pool-monitor",
            daemon=True,
        )
        self._monitor_thread.start()

    def stop(self) -> None:
        """Signal all threads to stop and wait for them to finish."""
        self._stop_event.set()

        # Signal all per-worker stop events too
        for evt in self._worker_stop_events.values():
            evt.set()

        for t in self._workers:
            t.join(timeout=5)

        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=5)

    # ------------------------------------------------------------------
    # Scaling
    # ------------------------------------------------------------------

    def scale_to(self, target: int) -> None:
        """Adjust the number of worker threads toward *target*."""
        with self._lock:
            target = max(self._settings.min_workers, min(target, self._settings.max_workers))
            self._target_workers = target

            current = self._alive_worker_count()

            if target > current:
                for _ in range(target - current):
                    self._add_worker()
            elif target < current:
                # Gracefully stop excess workers via their individual stop events
                alive_ids = [
                    wid
                    for wid, evt in self._worker_stop_events.items()
                    if not evt.is_set()
                ]
                excess = current - target
                for wid in alive_ids[:excess]:
                    self._worker_stop_events[wid].set()

    def _add_worker(self) -> None:
        """Spin up a single new worker thread (caller must hold _lock or be in start())."""
        wid = self._worker_id_counter
        self._worker_id_counter += 1
        stop_evt = threading.Event()
        self._worker_stop_events[wid] = stop_evt

        t = threading.Thread(
            target=self._worker_loop,
            args=(wid, stop_evt),
            name=f"worker-{wid}",
            daemon=True,
        )
        self._workers.append(t)
        t.start()

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    def _worker_loop(self, worker_id: int, my_stop: threading.Event) -> None:
        """Continuously pop and process messages until told to stop."""
        while not self._stop_event.is_set() and not my_stop.is_set():
            msg = self._queue.pop()
            if msg is None:
                # Queue empty -- back off briefly
                self._stop_event.wait(0.01)
                continue

            start = time.monotonic()
            process_time = self._get_process_time(msg.priority)
            time.sleep(process_time)
            duration = time.monotonic() - start

            self._metrics.record_processed(msg.priority, duration, msg)
            logger.info(
                "[worker-%d] Processed %s message in %.1fms",
                worker_id,
                msg.priority.name,
                duration * 1000,
            )

    # ------------------------------------------------------------------
    # Monitor loop
    # ------------------------------------------------------------------

    def _monitor_loop(self) -> None:
        """Periodically check queue utilization and auto-scale workers."""
        consecutive_low: int = 0

        while not self._stop_event.is_set():
            if self._stop_event.wait(timeout=5):
                break

            utilization = self._queue.utilization
            current = self._alive_worker_count()

            if utilization > self._settings.scale_up_threshold and current < self._settings.max_workers:
                new_target = min(current * 2, self._settings.max_workers)
                self.scale_to(new_target)
                consecutive_low = 0
            elif utilization < self._settings.scale_down_threshold:
                consecutive_low += 1
                if consecutive_low >= 3:
                    new_target = max(current // 2, self._settings.min_workers)
                    self.scale_to(new_target)
                    consecutive_low = 0
            else:
                consecutive_low = 0

            self._metrics.update_active_workers(self._alive_worker_count())
            self._metrics.update_queue_depth(self._queue.priority_counts)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def worker_count(self) -> int:
        """Number of worker threads currently alive (excludes the monitor)."""
        return self._alive_worker_count()

    @property
    def is_running(self) -> bool:
        return not self._stop_event.is_set() and self._alive_worker_count() > 0

    def _alive_worker_count(self) -> int:
        return sum(1 for t in self._workers if t.is_alive())
