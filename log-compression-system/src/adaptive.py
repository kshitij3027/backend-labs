"""Adaptive compression — adjusts level based on CPU usage via psutil."""

import logging
import threading

import psutil

logger = logging.getLogger(__name__)


class AdaptiveCompression:
    """Monitors CPU usage and adjusts compression level accordingly.

    High CPU → lower compression level (less CPU work).
    Low CPU → higher compression level (better compression ratio).

    Linear mapping: level = max_level - (cpu_percent / 100) * (max_level - min_level)
    """

    def __init__(
        self,
        compression_handler,
        min_level: int = 1,
        max_level: int = 9,
        check_interval: float = 5.0,
    ):
        self._handler = compression_handler
        self._min_level = min_level
        self._max_level = max_level
        self._check_interval = check_interval
        self._shutdown = threading.Event()
        self._thread = None
        self._current_cpu = 0.0

    @property
    def current_cpu(self) -> float:
        return self._current_cpu

    def start(self):
        """Start the adaptive monitoring thread."""
        self._thread = threading.Thread(target=self._monitor, daemon=True)
        self._thread.start()
        logger.info(
            "Adaptive compression started (level range: %d-%d, interval: %.1fs)",
            self._min_level, self._max_level, self._check_interval,
        )

    def stop(self):
        """Stop the monitoring thread."""
        self._shutdown.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _monitor(self):
        """Periodically check CPU and adjust compression level."""
        # Initial CPU read to prime psutil
        psutil.cpu_percent(interval=None)

        while not self._shutdown.is_set():
            self._shutdown.wait(timeout=self._check_interval)
            if self._shutdown.is_set():
                break

            self._current_cpu = psutil.cpu_percent(interval=None)
            new_level = self._calculate_level(self._current_cpu)
            old_level = self._handler.level
            self._handler.level = new_level

            if new_level != old_level:
                logger.info(
                    "CPU: %.1f%% → compression level %d → %d",
                    self._current_cpu, old_level, new_level,
                )

    def _calculate_level(self, cpu_percent: float) -> int:
        """Map CPU percentage to compression level (linear, clamped)."""
        level_range = self._max_level - self._min_level
        level = self._max_level - (cpu_percent / 100.0) * level_range
        return max(self._min_level, min(self._max_level, round(level)))
