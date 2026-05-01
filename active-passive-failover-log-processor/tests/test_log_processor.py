"""Tests for src/log_processor.py — append, idempotency, counters, paging."""

from __future__ import annotations

from src.log_processor import LogProcessor
from src.models import LogEntry


# --- empty-state invariants ----------------------------------------------


def test_initial_state_is_empty() -> None:
    lp = LogProcessor()
    assert lp.log_count == 0
    assert lp.last_log_id == 0  # zero when nothing ingested
    assert lp.logs_ingested_total == 0
    assert lp.logs_rejected_total == 0
    assert lp.get_recent() == []


# --- ingest allocates server-side ids ------------------------------------


def test_ingest_without_log_id_allocates_sequentially() -> None:
    """When the client doesn't supply a ``log_id`` the processor allocates
    1, 2, 3, ... and bumps ``last_log_id`` accordingly.
    """
    lp = LogProcessor()
    e1 = lp.ingest("first")
    e2 = lp.ingest("second")
    e3 = lp.ingest("third")

    assert isinstance(e1, LogEntry)
    assert e1.log_id == 1
    assert e2.log_id == 2
    assert e3.log_id == 3

    assert lp.log_count == 3
    assert lp.last_log_id == 3
    assert lp.logs_ingested_total == 3


def test_ingest_uses_provided_level_and_default_to_INFO() -> None:
    lp = LogProcessor()
    a = lp.ingest("with default level")
    b = lp.ingest("with explicit level", level="WARN")
    assert a.level == "INFO"
    assert b.level == "WARN"


def test_ingest_uses_provided_timestamp() -> None:
    lp = LogProcessor()
    entry = lp.ingest("hello", timestamp=42.0)
    assert entry.timestamp == 42.0


# --- idempotency on client-supplied log_id -------------------------------


def test_ingest_with_supplied_log_id_is_idempotent() -> None:
    """Calling ``ingest(log_id=5)`` twice with the same id returns the
    SAME LogEntry, doesn't duplicate the entry, and doesn't double-count.
    """
    lp = LogProcessor()
    first = lp.ingest("hello", log_id=5)
    second = lp.ingest("hello", log_id=5)

    assert first is second  # same object
    assert lp.log_count == 1
    assert lp.logs_ingested_total == 1


def test_ingest_supplied_id_keeps_next_id_ahead() -> None:
    """A client-supplied id of 100 must shift the auto-allocator past 100
    so the very next server-allocated id is 101 — never 1, never 100.
    """
    lp = LogProcessor()
    lp.ingest("client-supplied", log_id=100)
    auto = lp.ingest("server-allocated")
    assert auto.log_id == 101
    assert lp.last_log_id == 101


def test_ingest_supplied_id_doesnt_collide_with_auto_id() -> None:
    """Auto-allocate then supply an id below the auto cursor — both must
    be retained as distinct entries; no overwrite.
    """
    lp = LogProcessor()
    lp.ingest("auto-1")  # id = 1
    lp.ingest("auto-2")  # id = 2
    explicit = lp.ingest("explicit", log_id=10)
    assert explicit.log_id == 10
    assert lp.log_count == 3
    auto3 = lp.ingest("auto-3")
    # Auto cursor jumped past 10, so the next auto id is 11.
    assert auto3.log_id == 11


# --- reject counter ------------------------------------------------------


def test_reject_increments_counter() -> None:
    lp = LogProcessor()
    assert lp.logs_rejected_total == 0
    lp.reject()
    assert lp.logs_rejected_total == 1
    lp.reject()
    lp.reject()
    assert lp.logs_rejected_total == 3
    # Rejecting MUST NOT touch the ingested counter or the log list.
    assert lp.logs_ingested_total == 0
    assert lp.log_count == 0


# --- get_recent ---------------------------------------------------------


def test_get_recent_returns_last_n_entries() -> None:
    lp = LogProcessor()
    for i in range(10):
        lp.ingest(f"msg-{i}")
    recent = lp.get_recent(limit=3)
    assert len(recent) == 3
    assert [e.log_id for e in recent] == [8, 9, 10]
    assert [e.message for e in recent] == ["msg-7", "msg-8", "msg-9"]


def test_get_recent_clamps_to_log_count() -> None:
    """Asking for more than we have returns everything; doesn't error."""
    lp = LogProcessor()
    for i in range(3):
        lp.ingest(f"msg-{i}")
    recent = lp.get_recent(limit=100)
    assert len(recent) == 3


def test_get_recent_zero_limit_returns_empty() -> None:
    lp = LogProcessor()
    lp.ingest("one")
    assert lp.get_recent(limit=0) == []


def test_get_recent_negative_limit_returns_empty() -> None:
    """Defensive: a negative limit must not silently slice in reverse."""
    lp = LogProcessor()
    lp.ingest("one")
    assert lp.get_recent(limit=-5) == []


# --- last_log_id tracking ------------------------------------------------


def test_last_log_id_is_zero_when_no_logs() -> None:
    lp = LogProcessor()
    assert lp.last_log_id == 0


def test_last_log_id_tracks_highest_allocated() -> None:
    lp = LogProcessor()
    lp.ingest("a")  # 1
    assert lp.last_log_id == 1
    lp.ingest("b", log_id=42)
    assert lp.last_log_id == 42
    lp.ingest("c")  # auto -> 43
    assert lp.last_log_id == 43
