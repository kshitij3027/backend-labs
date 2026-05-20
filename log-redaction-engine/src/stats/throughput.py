"""Sliding-window throughput counter.

One bucket per epoch second, bounded by ``window_seconds`` via a
``deque(maxlen=window_seconds)``. ``record()`` either increments the
current second's bucket or pushes a fresh one; the deque automatically
drops the oldest bucket when the window slides forward.

``ops_per_second()`` is O(window) — we sum all bucket counts that fall
within the window and divide by ``window_seconds`` to get a stable
rate. The "fall within the window" check is necessary because the
deque only bounds COUNT of buckets, not their AGE: if no ops were
recorded for 10 seconds, the last bucket inside the deque could be 10
seconds stale and would inflate the rate without the cutoff.

Thread safety
-------------
A single coarse lock guards the bucket deque. The critical section is
nanoseconds (one deque op + a tuple update), so contention is
negligible even under heavy parallel writes from the C5 processor.
"""
from __future__ import annotations

import threading
import time
from collections import deque


class ThroughputCounter:
    """Sliding-window operations-per-second counter.

    Parameters
    ----------
    window_seconds : int, default 60
        How many seconds of history to retain. The reported rate is the
        sum of all bucket counts in the window divided by this number.

    Notes
    -----
    The deque has ``maxlen=window_seconds`` so it can hold at most one
    bucket per second of the window. The actual age-cutoff is applied
    in :meth:`ops_per_second`, where we drop buckets older than the
    current window even if they're still in the deque (which happens
    after a gap in traffic).
    """

    def __init__(self, window_seconds: int = 60) -> None:
        self._window_seconds = window_seconds
        # Each bucket is (epoch_second, count). The deque bound matches
        # the window size so a steady stream can never grow past it.
        self._buckets: deque[tuple[int, int]] = deque(maxlen=window_seconds)
        self._lock = threading.Lock()

    def _now_sec(self) -> int:
        """Return the current epoch second as int.

        Wrapped in a method so tests can monkeypatch ``_now_sec`` to
        advance time deterministically without mocking ``time.time``
        globally (which would affect the rest of the test process).
        """
        return int(time.time())

    def record(self) -> None:
        """Increment the current second's bucket; create one if needed.

        Thread-safe. The lock is held only for the bucket lookup +
        update so contention is negligible.
        """
        with self._lock:
            now = self._now_sec()
            # If the last bucket is for the current second, increment
            # its count in place. Deque supports __setitem__ even
            # though tuples don't, so we replace the trailing tuple
            # with an incremented copy.
            if self._buckets and self._buckets[-1][0] == now:
                last_ts, last_count = self._buckets[-1]
                self._buckets[-1] = (last_ts, last_count + 1)
            else:
                # Fresh second — start a new bucket. The deque's
                # maxlen handles dropping the oldest bucket if we're
                # at capacity.
                self._buckets.append((now, 1))

    def ops_per_second(self) -> float:
        """Return the sliding-window ops-per-second rate.

        Returns
        -------
        float
            ``sum(counts_in_window) / window_seconds``. Returns 0.0 if
            no buckets fall within the current window (i.e., no ops
            recorded recently).

        Notes
        -----
        The cutoff is ``now - window_seconds + 1`` (inclusive) so a
        60-second window covers the buckets at seconds ``[now-59,
        now]`` — exactly ``window_seconds`` buckets. Older buckets
        (which may still be in the deque if traffic was bursty) are
        ignored at this layer.
        """
        with self._lock:
            now = self._now_sec()
            # Inclusive lower bound: a 60s window at now=t covers
            # seconds [t-59, t], which is 60 seconds of history.
            cutoff = now - self._window_seconds + 1
            total = 0
            for ts, count in self._buckets:
                if ts >= cutoff:
                    total += count
            # Divide by the window size, not by the number of populated
            # buckets — empty seconds count as zero ops, which is what
            # an operator graphing a smooth rate expects.
            return total / self._window_seconds

    def total_count(self) -> int:
        """Return the total count across every retained bucket.

        Notes
        -----
        Counts buckets even if they're stale relative to the current
        window. Used by tests to assert the counter saw every record()
        call irrespective of timing. In production, prefer
        :meth:`ops_per_second` for rate observation.
        """
        with self._lock:
            return sum(count for _, count in self._buckets)
