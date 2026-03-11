"""In-memory metrics aggregation with sliding window."""

from __future__ import annotations

import asyncio
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field

import structlog

from src.models import DashboardStats, EndpointMetrics, LogEntry

logger = structlog.get_logger(__name__)


@dataclass
class _TimestampedEntry:
    """An entry tagged with the wall-clock time it was recorded."""
    entry: LogEntry
    recorded_at: float


class MetricsAggregator:
    """Thread-safe, async-safe metrics aggregator with a sliding time window."""

    def __init__(self, window_sec: int = 300) -> None:
        self._window_sec = window_sec
        self._lock = asyncio.Lock()

        # Sliding window of recent entries
        self._entries: deque[_TimestampedEntry] = deque()

        # Per-endpoint aggregations
        self._endpoint_stats: dict[str, dict] = defaultdict(
            lambda: {
                "response_times": [],
                "status_codes": Counter(),
                "request_count": 0,
            }
        )

        # Global aggregations
        self._status_codes: Counter = Counter()
        self._ip_counts: Counter = Counter()
        self._total_processed: int = 0
        self._total_errors: int = 0
        self._start_time: float = time.time()

    def _purge_expired(self, now: float) -> None:
        """Remove entries older than the sliding window."""
        cutoff = now - self._window_sec
        while self._entries and self._entries[0].recorded_at < cutoff:
            self._entries.popleft()

    async def record(self, entry: LogEntry) -> None:
        """Record a parsed log entry into all aggregations."""
        now = time.time()
        async with self._lock:
            self._purge_expired(now)

            self._entries.append(_TimestampedEntry(entry=entry, recorded_at=now))
            self._total_processed += 1

            # Track errors (status >= 400)
            if entry.status_code >= 400:
                self._total_errors += 1

            # Global status code distribution
            self._status_codes[str(entry.status_code)] += 1

            # IP frequency
            self._ip_counts[entry.ip] += 1

            # Per-endpoint stats
            ep = self._endpoint_stats[entry.path]
            ep["request_count"] += 1
            ep["status_codes"][str(entry.status_code)] += 1
            if entry.response_time_ms is not None:
                ep["response_times"].append(entry.response_time_ms)

    async def snapshot(self) -> DashboardStats:
        """Compute and return a point-in-time dashboard stats snapshot."""
        async with self._lock:
            now = time.time()
            self._purge_expired(now)

            elapsed = now - self._start_time
            rps = self._total_processed / elapsed if elapsed > 0 else 0.0

            # Per-endpoint metrics
            endpoints: dict[str, EndpointMetrics] = {}
            for path, stats in self._endpoint_stats.items():
                req_count = stats["request_count"]
                rts = stats["response_times"]
                status_counts = stats["status_codes"]

                error_count = sum(
                    count for code, count in status_counts.items()
                    if int(code) >= 400
                )
                error_rate = error_count / req_count if req_count > 0 else 0.0

                avg_rt = sum(rts) / len(rts) if rts else 0.0
                p50, p95, p99 = _percentiles(rts, [0.50, 0.95, 0.99])

                endpoints[path] = EndpointMetrics(
                    path=path,
                    request_count=req_count,
                    avg_response_time=avg_rt,
                    error_rate=error_rate,
                    p50=p50,
                    p95=p95,
                    p99=p99,
                )

            # Top 10 paths by request count
            top_paths = [
                {"path": path, "count": stats["request_count"]}
                for path, stats in sorted(
                    self._endpoint_stats.items(),
                    key=lambda item: item[1]["request_count"],
                    reverse=True,
                )[:10]
            ]

            # Top 10 IPs
            top_ips = [
                {"ip": ip, "count": count}
                for ip, count in self._ip_counts.most_common(10)
            ]

            # Overall latency percentiles (from all entries in the window)
            all_rts = [
                te.entry.response_time_ms
                for te in self._entries
                if te.entry.response_time_ms is not None
            ]
            p50, p95, p99 = _percentiles(all_rts, [0.50, 0.95, 0.99])
            latency_percentiles = {"p50": p50, "p95": p95, "p99": p99}

            return DashboardStats(
                total_processed=self._total_processed,
                total_errors=self._total_errors,
                requests_per_second=rps,
                endpoints=endpoints,
                status_code_distribution=dict(self._status_codes),
                top_paths=top_paths,
                top_ips=top_ips,
                latency_percentiles=latency_percentiles,
                uptime_seconds=elapsed,
            )


def _percentiles(values: list[float], quantiles: list[float]) -> tuple[float, ...]:
    """Index-based percentile calculation.

    Returns a tuple of percentile values corresponding to the given quantiles.
    If *values* is empty every percentile is 0.0.
    """
    if not values:
        return tuple(0.0 for _ in quantiles)

    sorted_vals = sorted(values)
    n = len(sorted_vals)
    result = []
    for q in quantiles:
        idx = int(n * q)
        idx = min(idx, n - 1)  # clamp to valid range
        result.append(sorted_vals[idx])
    return tuple(result)
