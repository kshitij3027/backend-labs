"""Thread-safe bounded ring buffer.

A thin lock-guarded wrapper around :class:`collections.deque` configured
with ``maxlen``. The deque already drops the oldest item on append when
full — we just need to add a lock so concurrent producers (the C5 parallel
encrypt path, the C7 rotation background task) don't trip over each
other when appending or snapshotting.

Why not use ``deque`` directly?
-------------------------------
``deque.append`` is itself atomic under CPython's GIL, but ``snapshot()``
needs to materialize a list while no producer is mid-append, otherwise
we can observe a half-mutated state. Wrapping the operations behind a
lock makes the contract explicit and easy to reason about. The lock is
held only for the duration of a single deque op — nanoseconds — so the
performance cost is negligible.

Generic over ``T`` so type checkers can preserve the element type — in
practice this is :class:`~src.audit.audit_logger.AuditEvent`, but the
buffer is fully type-agnostic.
"""
from __future__ import annotations

import collections
import threading
from typing import Generic, TypeVar

T = TypeVar("T")


class RingBuffer(Generic[T]):
    """Bounded thread-safe FIFO buffer with O(1) append and snapshot.

    Parameters
    ----------
    maxlen : int, default 1000
        Maximum number of elements retained. Once full, every new
        :meth:`append` drops the oldest element. A bounded buffer is
        a mandatory safety property: the audit channel must never be
        able to consume unbounded memory.

    Notes
    -----
    All public methods take ``self._lock``. Holding the lock during
    :meth:`snapshot` is intentional — we materialize a list copy
    inside the critical section so concurrent appends can't change
    the structure mid-iteration.
    """

    def __init__(self, maxlen: int = 1000) -> None:
        # deque(maxlen=N) already drops oldest on append-when-full, which
        # is exactly the ring-buffer contract. We do not need any custom
        # overflow handling.
        self._buf: collections.deque[T] = collections.deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def append(self, item: T) -> None:
        """Append one item; if at capacity, the oldest is silently dropped.

        Thread-safe — multiple producers can call :meth:`append`
        concurrently and each call appears atomically to a concurrent
        :meth:`snapshot`.
        """
        with self._lock:
            self._buf.append(item)

    def snapshot(self) -> list[T]:
        """Return a list copy of the buffer's current contents.

        The list is oldest-first (deque iteration order). The copy is
        materialized under the lock so concurrent appends can't shift
        the view; the returned list is therefore a stable point-in-time
        snapshot that the caller can iterate freely.
        """
        with self._lock:
            # list(deque) walks deque-front-to-back, i.e. oldest-first.
            return list(self._buf)

    def __len__(self) -> int:
        """Current number of items in the buffer (under lock)."""
        with self._lock:
            return len(self._buf)

    def clear(self) -> None:
        """Drop every item. Used by tests; never by production code."""
        with self._lock:
            self._buf.clear()
