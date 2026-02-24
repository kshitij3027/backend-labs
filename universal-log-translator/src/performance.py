"""Performance tracking with EWMA and adaptive handler ordering."""
import time
from dataclasses import dataclass, field

from src.base_handler import BaseHandler
from src.detector import FormatDetector
from src.models import LogEntry, UnsupportedFormatError
from src.normalizer import LogNormalizer


@dataclass
class HandlerStats:
    """Per-handler performance statistics using EWMA."""
    format_name: str
    total_calls: int = 0
    successes: int = 0
    failures: int = 0
    avg_time_ms: float = 0.0  # EWMA of parse time
    avg_detect_time_ms: float = 0.0  # EWMA of can_handle time
    _alpha: float = 0.1  # EWMA smoothing factor

    def record_detect(self, time_ms: float, matched: bool):
        """Record a can_handle() call."""
        self.avg_detect_time_ms = self._ewma(self.avg_detect_time_ms, time_ms)
        if not matched:
            return

    def record_parse(self, time_ms: float, success: bool):
        """Record a parse() call."""
        self.total_calls += 1
        if success:
            self.successes += 1
            self.avg_time_ms = self._ewma(self.avg_time_ms, time_ms)
        else:
            self.failures += 1

    def _ewma(self, current: float, new_value: float) -> float:
        """Exponentially weighted moving average."""
        if current == 0.0:
            return new_value
        return self._alpha * new_value + (1 - self._alpha) * current

    @property
    def success_rate(self) -> float:
        """Success rate as a fraction."""
        if self.total_calls == 0:
            return 1.0
        return self.successes / self.total_calls

    @property
    def score(self) -> float:
        """Composite score for ordering: lower is better.

        Combines: avg parse time / success rate.
        Handlers with high success and low latency score best.
        """
        if self.success_rate == 0:
            return float('inf')
        return self.avg_time_ms / self.success_rate


class PerformanceTracker:
    """Tracks per-handler performance metrics."""

    def __init__(self):
        self._stats: dict[str, HandlerStats] = {}

    def get_stats(self, format_name: str) -> HandlerStats:
        """Get or create stats for a handler."""
        if format_name not in self._stats:
            self._stats[format_name] = HandlerStats(format_name=format_name)
        return self._stats[format_name]

    @property
    def all_stats(self) -> dict[str, HandlerStats]:
        """All handler statistics."""
        return dict(self._stats)

    def optimal_order(self) -> list[str]:
        """Return handler names sorted by score (best first)."""
        return sorted(
            self._stats.keys(),
            key=lambda name: self._stats[name].score
        )

    def report(self) -> str:
        """Generate a human-readable performance report."""
        lines = ["=== Performance Report ==="]
        for name, stats in sorted(self._stats.items(), key=lambda x: x[1].score):
            lines.append(
                f"  {name}: calls={stats.total_calls}, "
                f"success_rate={stats.success_rate:.1%}, "
                f"avg_time={stats.avg_time_ms:.3f}ms, "
                f"score={stats.score:.3f}"
            )
        lines.append("=" * 30)
        return "\n".join(lines)


class PerformanceAwareNormalizer(LogNormalizer):
    """LogNormalizer with adaptive performance tracking.

    Tracks handler timing and success rates using EWMA.
    Reorders handler detection every `reorder_interval` calls.
    Reports stats every `report_interval` calls.
    """

    def __init__(
        self,
        handler_order: list[str] | None = None,
        reorder_interval: int = 100,
        report_interval: int = 1000,
    ):
        super().__init__(handler_order=handler_order)
        self.tracker = PerformanceTracker()
        self._reorder_interval = reorder_interval
        self._report_interval = report_interval
        self._call_count = 0
        self._last_report: str = ""

    def normalize(self, raw_data: bytes, source_format: str | None = None) -> LogEntry:
        """Normalize with performance tracking."""
        self._call_count += 1

        if source_format:
            return self._timed_parse(raw_data, source_format)

        # Auto-detect with timing
        handler = None
        for h in self._detector.handlers:
            start = time.perf_counter()
            matched = h.can_handle(raw_data)
            detect_ms = (time.perf_counter() - start) * 1000
            self.tracker.get_stats(h.format_name).record_detect(detect_ms, matched)
            if matched:
                handler = h
                break

        if handler is None:
            raise UnsupportedFormatError(
                f"No handler can parse the given data ({len(raw_data)} bytes)"
            )

        # Parse with timing
        start = time.perf_counter()
        try:
            entry = handler.parse(raw_data)
            parse_ms = (time.perf_counter() - start) * 1000
            self.tracker.get_stats(handler.format_name).record_parse(parse_ms, True)
        except Exception:
            parse_ms = (time.perf_counter() - start) * 1000
            self.tracker.get_stats(handler.format_name).record_parse(parse_ms, False)
            raise

        # Periodic reorder
        if self._call_count % self._reorder_interval == 0:
            self._reorder_handlers()

        # Periodic report
        if self._call_count % self._report_interval == 0:
            self._last_report = self.tracker.report()

        return entry

    def _timed_parse(self, raw_data: bytes, source_format: str) -> LogEntry:
        """Parse with explicit format, tracking timing."""
        handler = BaseHandler.get_handler(source_format)
        start = time.perf_counter()
        try:
            entry = handler.parse(raw_data)
            parse_ms = (time.perf_counter() - start) * 1000
            self.tracker.get_stats(source_format).record_parse(parse_ms, True)
            return entry
        except Exception:
            parse_ms = (time.perf_counter() - start) * 1000
            self.tracker.get_stats(source_format).record_parse(parse_ms, False)
            raise

    def _reorder_handlers(self):
        """Reorder handlers based on performance scores."""
        new_order = self.tracker.optimal_order()
        if new_order:
            self._detector = FormatDetector(handler_order=new_order)

    @property
    def stats_report(self) -> str:
        """Get the latest performance report."""
        return self.tracker.report()

    @property
    def call_count(self) -> int:
        return self._call_count
