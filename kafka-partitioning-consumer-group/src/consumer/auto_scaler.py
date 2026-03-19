"""Auto-scaling consumer group based on consumer lag."""
import logging
import threading
import time
from src.config import Settings
from src.monitoring.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class AutoScaler:
    """Monitors consumer lag and scales consumers up/down."""

    def __init__(self, settings: Settings, metrics: MetricsCollector, add_fn, remove_fn, count_fn) -> None:
        self._settings = settings
        self._metrics = metrics
        self._add_consumer = add_fn
        self._remove_consumer = remove_fn
        self._get_count = count_fn
        self._shutdown = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_scale_time = 0.0
        self._scaling_history: list[dict] = []
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start the auto-scaler monitoring thread."""
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True, name="auto-scaler")
        self._thread.start()
        logger.info("AutoScaler started (threshold=%d, cooldown=%ds, max=%d)",
                     self._settings.lag_threshold, self._settings.scale_cooldown_s,
                     self._settings.max_consumers)

    def stop(self) -> None:
        """Stop the auto-scaler."""
        self._shutdown.set()
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("AutoScaler stopped")

    @property
    def scaling_history(self) -> list[dict]:
        with self._lock:
            return list(self._scaling_history)

    def _monitor_loop(self) -> None:
        """Check lag every 5 seconds and make scaling decisions."""
        while not self._shutdown.is_set():
            self._shutdown.wait(5.0)
            if self._shutdown.is_set():
                break

            try:
                self._check_and_scale()
            except Exception as e:
                logger.error("AutoScaler error: %s", e)

    def _check_and_scale(self) -> None:
        """Evaluate lag and decide whether to scale."""
        snap = self._metrics.snapshot()
        total_lag = sum(snap.get("lag", {}).values())
        current_count = self._get_count()
        now = time.time()

        # Cooldown check
        if (now - self._last_scale_time) < self._settings.scale_cooldown_s:
            return

        # Get throughput for history
        throughput = snap.get("throughput", [])
        current_throughput = throughput[-1]["mps"] if throughput else 0

        if total_lag > self._settings.lag_threshold and current_count < self._settings.max_consumers:
            # Scale up
            new_count = min(current_count + 1, self._settings.max_consumers)
            self._add_consumer()
            self._record_scaling("scale_up", current_count, new_count,
                                f"lag={total_lag} > threshold={self._settings.lag_threshold}",
                                current_throughput)
            self._last_scale_time = now
            logger.info("Scaled UP: %d -> %d (lag=%d)", current_count, new_count, total_lag)

        elif total_lag == 0 and current_count > 1:
            # Scale down when completely idle
            snap_consumed = snap.get("total_consumed", 0)
            # Only scale down if we've been idle (no throughput recently)
            if current_throughput == 0 and snap_consumed > 0:
                new_count = current_count - 1
                self._remove_consumer()
                self._record_scaling("scale_down", current_count, new_count,
                                    "zero lag and zero throughput",
                                    current_throughput)
                self._last_scale_time = now
                logger.info("Scaled DOWN: %d -> %d (idle)", current_count, new_count)

    def _record_scaling(self, action: str, from_count: int, to_count: int,
                        reason: str, throughput: float) -> None:
        with self._lock:
            self._scaling_history.append({
                "timestamp": time.time(),
                "action": action,
                "from_count": from_count,
                "to_count": to_count,
                "reason": reason,
                "throughput_before": throughput,
            })
        self._metrics.record_rebalance(
            f"auto-{action}", [], f"{from_count}->{to_count}"
        )
