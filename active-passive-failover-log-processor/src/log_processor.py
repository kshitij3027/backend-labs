"""In-memory log store used by the PRIMARY node.

The :class:`LogProcessor` is intentionally state-agnostic — it knows
nothing about :class:`NodeState` and never refuses an ingest itself.
The HTTP layer is responsible for routing: only PRIMARY nodes call
:py:meth:`ingest`; non-PRIMARY nodes call :py:meth:`reject` and return
503 to the client.

Idempotency
-----------
Clients may supply a ``log_id`` along with their message. If we've
already accepted that id we simply return the existing entry — the
client's retry is a no-op. If ``log_id`` is omitted, the processor
allocates the next monotonically-increasing id and uses that.

This is important for two reasons:

1. After a failover, the new primary may load a snapshot containing
   ids the client also retried; idempotency dedups those.
2. The dashboard / load tester can replay the same batch without
   inflating ``log_count``.
"""

from __future__ import annotations

import time

from src.models import LogEntry


class LogProcessor:
    """Append-only, in-memory log store with idempotent ingest.

    All state lives on the instance — there is no shared singleton.
    A node holds exactly one ``LogProcessor`` for its lifetime;
    :py:class:`src.node.FailoverNode` constructs and owns it.
    """

    def __init__(self) -> None:
        self._logs: list[LogEntry] = []
        # Next id to allocate when the client doesn't supply one.
        # Starts at 1 so ``last_log_id`` cleanly reports 0 when empty.
        self._next_id: int = 1
        # Set of accepted ids — used purely for idempotent dedup.
        self._seen_ids: set[int] = set()

        # --- counters (read directly by the /metrics endpoint) ----------
        self.logs_ingested_total: int = 0
        self.logs_rejected_total: int = 0

    # --- public read-only accessors -------------------------------------

    @property
    def log_count(self) -> int:
        """Number of accepted entries currently in memory."""
        return len(self._logs)

    @property
    def last_log_id(self) -> int:
        """Highest id ever allocated, or 0 if no logs have been ingested."""
        return self._next_id - 1

    # --- mutation -------------------------------------------------------

    def ingest(
        self,
        message: str,
        level: str = "INFO",
        log_id: int | None = None,
        timestamp: float | None = None,
    ) -> LogEntry:
        """Append a new log entry; idempotent on client-supplied ``log_id``.

        Behaviour:

        * If ``log_id`` is supplied AND already known, return the original
          entry untouched. ``logs_ingested_total`` does NOT increment.
        * If ``log_id`` is supplied and new, accept it as-is and bump
          ``_next_id`` past it so future server-allocated ids never
          collide with already-seen client ids.
        * If ``log_id`` is omitted, allocate the next server-side id.

        ``timestamp`` defaults to ``time.time()`` if not provided.
        """
        if log_id is not None and log_id in self._seen_ids:
            # Idempotent path: return the original entry.
            for existing in self._logs:
                if existing.log_id == log_id:
                    return existing
            # Should be unreachable: _seen_ids is always populated alongside
            # _logs. Fall through and treat as a new entry to be safe.

        if log_id is None:
            allocated_id = self._next_id
            self._next_id += 1
        else:
            allocated_id = log_id
            # Keep _next_id ahead of every id we've ever seen so the next
            # server-allocated id never collides with a future client id.
            if allocated_id >= self._next_id:
                self._next_id = allocated_id + 1

        ts = timestamp if timestamp is not None else time.time()
        entry = LogEntry(
            log_id=allocated_id,
            message=message,
            level=level,
            timestamp=ts,
        )
        self._logs.append(entry)
        self._seen_ids.add(allocated_id)
        self.logs_ingested_total += 1
        return entry

    def reject(self) -> None:
        """Bump the rejected counter.

        Called by the HTTP layer when a non-PRIMARY node receives a
        ``POST /logs`` (and returns 503 to the client). Exists as a
        method rather than direct attribute mutation so the HTTP layer
        doesn't need to reach into private state to keep metrics honest.
        """
        self.logs_rejected_total += 1

    # --- read APIs ------------------------------------------------------

    def get_recent(self, limit: int = 100) -> list[LogEntry]:
        """Return at most the last ``limit`` entries, oldest-first within
        the slice.

        ``limit`` is clamped to be non-negative; passing 0 returns an
        empty list, passing more than ``log_count`` returns everything.
        """
        if limit <= 0:
            return []
        return self._logs[-limit:]
