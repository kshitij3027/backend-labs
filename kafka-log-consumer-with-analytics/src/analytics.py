"""Real-time analytics engine with sliding window metrics."""
import logging
import threading
import time
from collections import defaultdict, deque

from src.models import WebAccessLog, AppLog, ErrorLog, LogMessage

logger = logging.getLogger(__name__)


class AnalyticsEngine:
    """Computes real-time analytics over sliding time windows.

    Tracks:
    - Throughput (messages/sec) via pre-aggregated second buckets
    - Response time percentiles (P50, P95, P99) via bounded deque
    - Per-endpoint stats (request count, error count, response times)
    - Error rates by endpoint
    - Geographic distribution
    """

    def __init__(self, window_seconds: int = 60) -> None:
        self._window = window_seconds
        self._lock = threading.Lock()

        # Throughput: deque of (epoch_second, count) tuples
        self._throughput_buckets: deque[tuple[int, int]] = deque()
        self._current_second: int = 0
        self._current_count: int = 0

        # Response times: bounded deque for percentile computation
        self._response_times: deque[float] = deque(maxlen=10000)

        # Per-endpoint stats
        self._endpoint_stats: dict[str, dict] = defaultdict(
            lambda: {"request_count": 0, "error_count": 0, "response_times": []}
        )

        # Geographic distribution
        self._geo_distribution: dict[str, int] = defaultdict(int)

        # Totals
        self._total_messages = 0
        self._total_errors = 0

    def record_message(self, message: LogMessage) -> None:
        """Record a single parsed log message into analytics."""
        with self._lock:
            self._total_messages += 1
            now = int(time.time())

            # Update throughput bucket
            if now == self._current_second:
                self._current_count += 1
            else:
                if self._current_second > 0:
                    self._throughput_buckets.append(
                        (self._current_second, self._current_count)
                    )
                self._current_second = now
                self._current_count = 1
                self._expire_buckets(now)

            if isinstance(message, WebAccessLog):
                self._record_web_log(message)
            elif isinstance(message, ErrorLog):
                self._record_error_log(message)

    def _record_web_log(self, log: WebAccessLog) -> None:
        """Record web access log metrics (called under lock)."""
        endpoint = log.endpoint or "unknown"

        # Response time
        if log.response_time_ms > 0:
            self._response_times.append(log.response_time_ms)
            self._endpoint_stats[endpoint]["response_times"].append(
                log.response_time_ms
            )
            # Keep per-endpoint response_times bounded
            if len(self._endpoint_stats[endpoint]["response_times"]) > 1000:
                self._endpoint_stats[endpoint]["response_times"] = \
                    self._endpoint_stats[endpoint]["response_times"][-1000:]

        # Request count
        self._endpoint_stats[endpoint]["request_count"] += 1

        # Error tracking (4xx/5xx)
        if log.status_code >= 400:
            self._endpoint_stats[endpoint]["error_count"] += 1
            self._total_errors += 1

        # Geographic distribution
        if log.geo:
            self._geo_distribution[log.geo] += 1

    def _record_error_log(self, log: ErrorLog) -> None:
        """Record error log metrics (called under lock)."""
        self._total_errors += 1
        endpoint = log.endpoint or "unknown"
        self._endpoint_stats[endpoint]["error_count"] += 1

    def _expire_buckets(self, now: int) -> None:
        """Remove throughput buckets outside the sliding window."""
        cutoff = now - self._window
        while self._throughput_buckets and self._throughput_buckets[0][0] < cutoff:
            self._throughput_buckets.popleft()

    # ------------------------------------------------------------------
    # Snapshot methods
    # ------------------------------------------------------------------

    def get_stats(self) -> dict:
        """High-level stats: total messages, throughput, error rate."""
        with self._lock:
            throughput = self._compute_throughput()
            total = self._total_messages
            errors = self._total_errors
            return {
                "total_messages": total,
                "total_errors": errors,
                "error_rate": round(errors / total * 100, 2) if total > 0 else 0.0,
                "throughput_per_sec": throughput,
                "window_seconds": self._window,
            }

    def get_analytics(self) -> dict:
        """Detailed analytics: percentiles, per-endpoint, geo distribution."""
        with self._lock:
            percentiles = self._compute_percentiles()
            endpoints = {}
            for ep, data in self._endpoint_stats.items():
                req = data["request_count"]
                err = data["error_count"]
                rts = data["response_times"]
                ep_p95 = self._percentile(rts, 95) if rts else 0.0
                endpoints[ep] = {
                    "request_count": req,
                    "error_count": err,
                    "error_rate": round(err / req * 100, 2) if req > 0 else 0.0,
                    "avg_response_time_ms": round(sum(rts) / len(rts), 2) if rts else 0.0,
                    "p95_response_time_ms": ep_p95,
                }
            return {
                "percentiles": percentiles,
                "endpoints": endpoints,
                "geo_distribution": dict(self._geo_distribution),
            }

    def get_metrics(self) -> dict:
        """Metrics for dashboard: throughput history, processing stats."""
        with self._lock:
            throughput_history = [
                {"timestamp": ts, "count": count}
                for ts, count in self._throughput_buckets
            ]
            # Add current partial bucket
            if self._current_count > 0:
                throughput_history.append(
                    {"timestamp": self._current_second, "count": self._current_count}
                )
            return {
                "throughput_history": throughput_history[-60:],  # last 60 data points
                "total_messages": self._total_messages,
                "total_errors": self._total_errors,
                "response_time_percentiles": self._compute_percentiles(),
            }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _compute_throughput(self) -> float:
        """Compute messages/sec over the sliding window."""
        now = int(time.time())
        self._expire_buckets(now)
        total = sum(count for _, count in self._throughput_buckets)
        total += self._current_count
        window = min(self._window, max(now - (self._throughput_buckets[0][0] if self._throughput_buckets else now), 1))
        return round(total / window, 2) if window > 0 else 0.0

    def _compute_percentiles(self) -> dict:
        """Compute P50, P95, P99 from the response time deque."""
        if not self._response_times:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0}
        sorted_times = sorted(self._response_times)
        return {
            "p50": self._percentile(sorted_times, 50),
            "p95": self._percentile(sorted_times, 95),
            "p99": self._percentile(sorted_times, 99),
        }

    @staticmethod
    def _percentile(sorted_data: list[float], pct: int) -> float:
        """Compute the given percentile from pre-sorted data."""
        if not sorted_data:
            return 0.0
        # If not sorted, sort it
        data = sorted(sorted_data)
        idx = int(len(data) * pct / 100)
        idx = min(idx, len(data) - 1)
        return round(data[idx], 2)
