"""Per-pattern hit counters.

A single :class:`PatternCounters` instance is shared between the C5
processor and the C7 stats endpoint. Each call to :meth:`incr` bumps a
named counter (typically the pattern name — ``ssn``, ``credit_card``,
``email``, ...); the snapshot returns a copy that downstream code can
serialize freely.

Thread safety
-------------
A single coarse lock guards the dict. Every operation touches one
key, the lock is held for nanoseconds, and the dict copy in
:meth:`snapshot` is also under the lock. There's no per-key lock
because the contention model is "many writers, occasional reader" and
a coarse lock is faster than a striped lock at small dict sizes.
"""
from __future__ import annotations

import threading


class PatternCounters:
    """Thread-safe ``{pattern_name → count}`` map.

    The counter dict is created lazily — ``incr("ssn")`` on a fresh
    instance simply creates the ``"ssn"`` entry at zero before
    incrementing. There is no eager pre-population because the set of
    pattern names is determined by the active configuration, which
    isn't known at construction time.

    Notes
    -----
    Uses ``dict[str, int]`` directly rather than
    :class:`collections.Counter` because the latter is not thread-safe
    and silently allows non-int values. Explicit integer semantics
    enforced at the boundary is more defensible for a security-sensitive
    component.
    """

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def incr(self, pattern: str, n: int = 1) -> None:
        """Atomically increment ``pattern`` by ``n``.

        Parameters
        ----------
        pattern : str
            Counter name (typically a detection pattern name).
        n : int, default 1
            Increment amount. Negative values are accepted (the lock
            still makes the operation atomic), but the C5 processor
            never decrements — counts grow monotonically over the
            process lifetime.

        Notes
        -----
        Uses ``dict.get(pattern, 0)`` so the first increment of a new
        pattern transparently creates the entry at zero before adding
        ``n``. The read-modify-write is atomic under the lock.
        """
        with self._lock:
            self._counts[pattern] = self._counts.get(pattern, 0) + n

    def snapshot(self) -> dict[str, int]:
        """Return a copy of the counter map.

        The returned dict is a copy — callers can mutate it without
        affecting the live state. Materialized under the lock so a
        concurrent :meth:`incr` can't shift the view mid-snapshot.
        """
        with self._lock:
            return dict(self._counts)

    def total(self) -> int:
        """Return the sum of every counter value.

        Used by the dashboard's "total redactions" tile. Computed
        under the lock so the sum reflects a consistent point-in-time
        snapshot of the counters (no half-applied increments).
        """
        with self._lock:
            return sum(self._counts.values())
