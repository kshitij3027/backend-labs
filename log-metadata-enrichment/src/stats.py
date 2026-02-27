"""Thread-safe statistics tracker for the enrichment pipeline."""

from __future__ import annotations

import threading
import time

from src.models import EnrichmentStats


class StatsTracker:
    """Tracks processed counts, errors, and throughput in a thread-safe manner."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._processed = 0
        self._errors = 0
        self._start_time = time.time()

    def record_success(self) -> None:
        """Record a successfully processed log entry."""
        with self._lock:
            self._processed += 1

    def record_error(self) -> None:
        """Record a log entry that encountered errors during enrichment."""
        with self._lock:
            self._processed += 1
            self._errors += 1

    def snapshot(self) -> EnrichmentStats:
        """Return a point-in-time snapshot of enrichment statistics."""
        with self._lock:
            processed = self._processed
            errors = self._errors
            runtime = time.time() - self._start_time

        success_rate = (processed - errors) / processed if processed > 0 else 0.0
        average_throughput = processed / runtime if runtime > 0 else 0.0

        return EnrichmentStats(
            processed_count=processed,
            error_count=errors,
            success_rate=success_rate,
            runtime_seconds=runtime,
            average_throughput=average_throughput,
        )
