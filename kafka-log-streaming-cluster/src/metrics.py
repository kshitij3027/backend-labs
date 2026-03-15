"""Metrics tracker — records throughput and per-topic message counts."""

import threading
import time
from collections import defaultdict, deque

# Throughput is measured over this rolling window (in seconds).
_THROUGHPUT_WINDOW = 10


class MetricsTracker:
    """Tracks throughput and message counts.  Thread-safe."""

    def __init__(self):
        self._lock = threading.Lock()
        self._message_timestamps: deque[float] = deque(maxlen=100000)
        self._counts: defaultdict[str, int] = defaultdict(int)
        self._total: int = 0

    def record_consumed(self, topic: str):
        """Record that one message was consumed from *topic*."""
        with self._lock:
            self._message_timestamps.append(time.time())
            self._counts[topic] += 1
            self._total += 1

    @property
    def throughput(self) -> dict:
        """Return throughput stats over the last 10 seconds."""
        with self._lock:
            now = time.time()
            cutoff = now - _THROUGHPUT_WINDOW
            recent = sum(1 for t in self._message_timestamps if t > cutoff)
            return {
                "messages_per_second": round(recent / _THROUGHPUT_WINDOW, 1),
                "total_messages": self._total,
                "by_topic": dict(self._counts),
            }
