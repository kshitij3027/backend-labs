from __future__ import annotations

import threading
import time
from collections import deque
from typing import Iterable

from src.metrics.sample import MetricSample, StageName


class RingBuffer:
    """Thread-safe bounded deque of MetricSample.

    Drops the oldest sample when maxlen exceeded (deque maxlen semantics).
    """

    def __init__(self, maxlen: int) -> None:
        if maxlen <= 0:
            raise ValueError("maxlen must be positive")
        self._dq: deque[MetricSample] = deque(maxlen=maxlen)
        self._lock = threading.Lock()
        self._maxlen = maxlen

    def add(self, sample: MetricSample) -> None:
        with self._lock:
            self._dq.append(sample)

    def extend(self, samples: Iterable[MetricSample]) -> None:
        with self._lock:
            self._dq.extend(samples)

    def snapshot(self, window_sec: float | None = None) -> list[MetricSample]:
        with self._lock:
            items = list(self._dq)
        if window_sec is None:
            return items
        cutoff = time.time() - window_sec
        return [s for s in items if s.ts >= cutoff]

    def by_stage(self, stage: StageName, window_sec: float) -> list[MetricSample]:
        return [s for s in self.snapshot(window_sec) if s.stage == stage]

    def clear(self) -> None:
        with self._lock:
            self._dq.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._dq)

    @property
    def maxlen(self) -> int:
        return self._maxlen
