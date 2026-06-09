"""Sqlite-backed "expensive storage" tier for the two-tier pipeline (C10).

Extended C asks for a bloom filter *in front of* expensive storage, so this
module supplies the expensive half: a real database whose lookups cost a SQL
parse, a B-tree descent, and (cold) actual disk I/O — milliseconds-ish under
load versus the filters' microseconds. Sqlite is the honest minimal stand-in
for "the slow authoritative tier" (Postgres, a log archive, S3 listings...):
the two-tier *pattern* is the point of C10, not sqlite tuning, which is why
the schema is a bare presence table and the tuning stops at two pragmas.

Schema
------
``logs (log_type, log_key, added_at)`` with ``PRIMARY KEY (log_type,
log_key) WITHOUT ROWID``: the table *is* its primary-key B-tree, so the
pipeline's two operations — presence INSERT and presence SELECT — are each a
single keyed descent, and ``INSERT OR IGNORE`` gives the exact-dedup
semantics the probabilistic tier gets judged against. ``added_at`` (epoch
seconds) records when a key was first ingested; nothing queries it yet, but
a presence store without a timestamp answers "have we seen it?" while
forever losing "since when?" for the price of one REAL per row.

Concurrency
-----------
One shared connection (``check_same_thread=False``) plus one
``threading.Lock`` around every statement. Callers are AnyIO threadpool
workers (the ``sync def`` /pipeline handlers), so statements genuinely race;
the lock serializes them through the single connection — far simpler than a
connection pool and plenty for this demo tier's rates. ``journal_mode=WAL``
lets independent readers (tests, a sqlite3 CLI poking the bind mount)
proceed while we write, and ``synchronous=NORMAL`` is the standard WAL
pairing: fsync at checkpoint boundaries rather than per-commit — a power cut
can cost the last instants of ingests, never consistency, the same stance
the filter snapshots already take.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path


class LogStore:
    """Presence store: has ``(log_type, log_key)`` ever been ingested?

    All methods are thread-safe via the single internal lock (module
    docstring). Construction creates the parent directory, the database
    file, and the schema as needed; after :meth:`close`, every operation
    raises ``sqlite3.ProgrammingError`` — loud, because a lookup silently
    answering against a closed store would corrupt the pipeline's "sqlite is
    ground truth" contract.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        # isolation_level=None → autocommit: every statement is its own
        # transaction, so no dangling write transaction can sit on the WAL
        # between calls and there is no commit() bookkeeping to forget.
        self._conn = sqlite3.connect(
            str(self._path), check_same_thread=False, isolation_level=None
        )
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS logs (
                    log_type TEXT NOT NULL,
                    log_key  TEXT NOT NULL,
                    added_at REAL NOT NULL,
                    PRIMARY KEY (log_type, log_key)
                ) WITHOUT ROWID
                """
            )

    @property
    def path(self) -> Path:
        """Filesystem location of the database file."""
        return self._path

    def insert(self, log_type: str, log_key: str) -> bool:
        """Record ``(log_type, log_key)``; return True iff the row is new.

        ``INSERT OR IGNORE`` against the primary key makes re-ingest a
        no-op: ``rowcount`` is 1 only when a row was actually written, which
        is the exact-dedup signal the pipeline surfaces as ``duplicate``.
        ``added_at`` is the ingest wall-clock epoch.
        """
        with self._lock:
            cursor = self._conn.execute(
                "INSERT OR IGNORE INTO logs (log_type, log_key, added_at) "
                "VALUES (?, ?, ?)",
                (log_type, log_key, time.time()),
            )
            return cursor.rowcount == 1

    def exists(self, log_type: str, log_key: str) -> bool:
        """Authoritative membership: was this key ever ingested for this type?

        The expensive call the bloom tier exists to avoid — and the oracle
        that verifies the bloom tier's "probably" answers.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM logs WHERE log_type = ? AND log_key = ?",
                (log_type, log_key),
            ).fetchone()
        return row is not None

    def count(self, log_type: str | None = None) -> int:
        """Row count for one ``log_type``, or for the whole table when None."""
        with self._lock:
            if log_type is None:
                row = self._conn.execute("SELECT COUNT(*) FROM logs").fetchone()
            else:
                row = self._conn.execute(
                    "SELECT COUNT(*) FROM logs WHERE log_type = ?", (log_type,)
                ).fetchone()
        return int(row[0])

    def close(self) -> None:
        """Close the shared connection (idempotent).

        Later operations raise ``sqlite3.ProgrammingError`` by design — see
        the class docstring.
        """
        with self._lock:
            self._conn.close()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return f"LogStore(path={str(self._path)!r})"
