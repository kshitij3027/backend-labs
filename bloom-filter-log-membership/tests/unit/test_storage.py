"""Unit tests for src.storage.LogStore — the sqlite "expensive storage" tier.

LogStore is deliberately tiny (presence INSERT / presence SELECT / COUNT on
one PK-deduped table), so these tests pin its contract edges: exact dedup
via INSERT OR IGNORE, per-type isolation, thread safety of the shared
locked connection, the persistent WAL journal mode, and the fail-loudly
behaviour after close().
"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from src.storage import LogStore


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """A database path whose parent directories do not exist yet."""
    return tmp_path / "nested" / "data" / "logs.db"


@pytest.fixture
def store(db_path: Path) -> Iterator[LogStore]:
    store = LogStore(db_path)
    yield store
    store.close()  # close() is idempotent, so in-test closes are fine too


# ---------------------------------------------------------------------- #
# basic contract                                                         #
# ---------------------------------------------------------------------- #


def test_insert_then_exists_roundtrip(store: LogStore) -> None:
    assert store.exists("error_logs", "req-1") is False
    assert store.insert("error_logs", "req-1") is True
    assert store.exists("error_logs", "req-1") is True


def test_construction_creates_parent_directories(
    store: LogStore, db_path: Path
) -> None:
    """The nested parent dirs from the fixture were created, db file exists."""
    assert db_path.exists()


def test_insert_or_ignore_dedup(store: LogStore) -> None:
    """Second insert of the same row reports False and writes nothing."""
    assert store.insert("error_logs", "dup-key") is True
    assert store.insert("error_logs", "dup-key") is False
    assert store.count("error_logs") == 1


def test_per_type_isolation(store: LogStore) -> None:
    """The same key under different log types is two independent rows."""
    assert store.insert("error_logs", "shared-key") is True
    assert store.exists("error_logs", "shared-key") is True
    assert store.exists("access_logs", "shared-key") is False
    # Not a duplicate: (log_type, log_key) is the identity, not log_key alone.
    assert store.insert("access_logs", "shared-key") is True
    assert store.count("error_logs") == 1
    assert store.count("access_logs") == 1


def test_count_per_type_and_total(store: LogStore) -> None:
    for i in range(3):
        store.insert("error_logs", f"e-{i}")
    for i in range(2):
        store.insert("security_logs", f"s-{i}")
    assert store.count("error_logs") == 3
    assert store.count("security_logs") == 2
    assert store.count("access_logs") == 0
    assert store.count() == 5
    assert store.count(None) == 5


# ---------------------------------------------------------------------- #
# concurrency / pragmas / lifecycle                                      #
# ---------------------------------------------------------------------- #


def test_threaded_inserts_are_safe_and_exact(store: LogStore) -> None:
    """8 workers x 200 distinct keys: no exceptions, count lands exactly.

    Exercises the one-shared-connection-plus-lock design under real thread
    contention (the same shape as AnyIO threadpool workers hitting the
    /pipeline handlers concurrently).
    """

    def worker(worker_id: int) -> None:
        for i in range(200):
            assert store.insert("error_logs", f"w{worker_id}-key-{i}") is True

    with ThreadPoolExecutor(max_workers=8) as pool:
        # list() drains the iterator and re-raises any worker exception.
        list(pool.map(worker, range(8)))

    assert store.count("error_logs") == 8 * 200


def test_wal_mode_is_active(store: LogStore, db_path: Path) -> None:
    """journal_mode=WAL is persistent in the db file: a second, completely
    independent connection reads it back (and can read while we hold ours
    open — the reader-friendliness WAL is there for)."""
    store.insert("error_logs", "wal-proof")
    probe = sqlite3.connect(str(db_path))
    try:
        assert probe.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        row = probe.execute(
            "SELECT COUNT(*) FROM logs WHERE log_type = 'error_logs'"
        ).fetchone()
        assert row[0] == 1
    finally:
        probe.close()


def test_operations_after_close_raise(db_path: Path) -> None:
    """A closed store fails loudly instead of silently answering wrong."""
    store = LogStore(db_path)
    store.insert("error_logs", "pre-close")
    store.close()
    with pytest.raises(sqlite3.ProgrammingError):
        store.insert("error_logs", "post-close")
    with pytest.raises(sqlite3.ProgrammingError):
        store.exists("error_logs", "pre-close")
    with pytest.raises(sqlite3.ProgrammingError):
        store.count()
    store.close()  # idempotent: double close is a no-op, never an error
