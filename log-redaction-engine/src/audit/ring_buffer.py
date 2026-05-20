"""Thread-safe bounded ring buffer with audit-aware filtering.

A thin lock-guarded wrapper around :class:`collections.deque` configured
with ``maxlen``. The deque already drops the oldest item on append when
full — the lock just ensures concurrent producers don't trip over each
other when appending or snapshotting.

The :meth:`filter` method exists specifically for the C8 compliance
report: it lets operators query the buffer by time window, event type,
and compliance tag without leaking a reference to the underlying
deque (which would defeat the lock guarantee).
"""
from __future__ import annotations

import threading
from collections import deque
from datetime import datetime
from typing import Optional

from .events import AuditEvent


class RingBuffer:
    """Bounded thread-safe FIFO buffer of :class:`AuditEvent` records.

    Parameters
    ----------
    maxlen : int
        Maximum number of events retained. Once full, every new
        :meth:`append` drops the oldest event (deque ``maxlen`` does
        this automatically). A bounded buffer is mandatory: the audit
        channel must never be able to consume unbounded memory.

    Notes
    -----
    Every public method takes ``self._lock``. Holding the lock during
    :meth:`snapshot` and :meth:`filter` is intentional — we materialize
    a list copy inside the critical section so concurrent appends can't
    change the structure mid-iteration.
    """

    def __init__(self, maxlen: int) -> None:
        # deque(maxlen=N) already drops oldest on append-when-full,
        # which is exactly the ring-buffer contract. No custom overflow
        # handling required.
        self._buf: deque[AuditEvent] = deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def append(self, event: AuditEvent) -> None:
        """Append one event; if at capacity, the oldest is silently dropped.

        Thread-safe — multiple producers can call :meth:`append`
        concurrently and each call appears atomically to a concurrent
        :meth:`snapshot` / :meth:`filter`.
        """
        with self._lock:
            self._buf.append(event)

    def snapshot(self) -> list[AuditEvent]:
        """Return a list copy of the buffer's current contents.

        The list is oldest-first (deque iteration order). The copy is
        materialized under the lock so concurrent appends can't shift
        the view; the returned list is therefore a stable point-in-time
        snapshot the caller can iterate freely.
        """
        with self._lock:
            # list(deque) walks deque-front-to-back, i.e. oldest-first.
            return list(self._buf)

    def filter(
        self,
        *,
        since: Optional[datetime] = None,
        event_type: Optional[str] = None,
        compliance_tag: Optional[str] = None,
    ) -> list[AuditEvent]:
        """Return events matching all supplied criteria.

        Parameters
        ----------
        since : datetime | None
            If provided, drop events with ``timestamp_utc < since``.
            Used by the dashboard "last 5 minutes" query.
        event_type : str | None
            If provided, keep only events with this exact ``event_type``.
        compliance_tag : str | None
            If provided, keep only events whose ``compliance_tags`` list
            contains this tag. Drives the C8 "show me all HIPAA-tagged
            redactions" report.

        Returns
        -------
        list[AuditEvent]
            A fresh list of events in oldest-first order. Empty list
            if no events match.

        Notes
        -----
        Implementation: snapshot under the lock, then filter outside
        the critical section so the lock is held for the minimum time.
        Each criterion uses ``is not None`` as the gate so an explicit
        empty string would still be applied as a filter (defensive
        against caller bugs).
        """
        with self._lock:
            # Materialize a copy inside the lock so a concurrent append
            # can't change the view we're about to filter.
            events = list(self._buf)

        # Filter outside the critical section — the snapshot is ours.
        result: list[AuditEvent] = []
        for event in events:
            if since is not None and event.timestamp_utc < since:
                continue
            if event_type is not None and event.event_type != event_type:
                continue
            if compliance_tag is not None and compliance_tag not in event.compliance_tags:
                continue
            result.append(event)
        return result

    def __len__(self) -> int:
        """Current number of events in the buffer (under lock)."""
        with self._lock:
            return len(self._buf)
