"""Unit tests for ``src/audit/chain.py`` (C13).

The audit chain has three observable properties this test module pins
down:

  1. **Genesis**. :func:`ensure_genesis` inserts seq=0 with the right
     sentinel fields (actor='system', action='genesis',
     prev_hash='0'*64). Subsequent calls are no-ops — idempotency
     matters because ``init_db`` calls it on every boot.
  2. **Linkage**. Each :meth:`AuditAppender.append` produces strictly
     monotonic seq values and threads ``prev_hash = previous.entry_hash``
     through every row, so a verifier walk reconstructs the same chain
     deterministically.
  3. **Canonicalisation**. Two callers passing dicts with different key
     order produce the same entry_hash. This is the property the
     verifier later relies on when re-deriving hashes.
  4. **Concurrency**. Five concurrent ``append`` calls via
     :func:`asyncio.gather` serialize at the SQLite write lock and
     produce seqs 1..5 with no duplicates.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import datetime

import pytest
import sqlalchemy as sa

from src.audit.chain import (
    GENESIS_PREV_HASH,
    AuditAppender,
    _canonical_bytes,
    _compute_hash,
    ensure_genesis,
)
from src.persistence.models import AuditEntry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _all_entries_sorted(session_factory) -> list[AuditEntry]:
    """Return every audit_entries row ordered by seq ascending."""
    async with session_factory() as session:
        result = await session.execute(
            sa.select(AuditEntry).order_by(AuditEntry.seq.asc())
        )
        return list(result.scalars().all())


async def _count_entries(session_factory) -> int:
    """Return the total audit_entries row count."""
    async with session_factory() as session:
        result = await session.execute(sa.select(sa.func.count(AuditEntry.seq)))
        return int(result.scalar_one())


# ---------------------------------------------------------------------------
# Test setup notes:
#
# The shared ``session_factory`` fixture (tests/conftest.py) already runs
# ``init_db`` on a fresh in-memory engine. ``init_db`` in turn now calls
# ``ensure_genesis``, which means by the time a test function receives
# ``session_factory`` there's already one row (seq=0) in the table.
#
# The "empty DB" tests below therefore must clear the table explicitly
# before they exercise the genesis-insertion code path; the helper below
# does exactly that.
# ---------------------------------------------------------------------------


async def _clear_audit_entries(session_factory) -> None:
    """Drop every audit_entries row so a test starts from a truly empty chain."""
    async with session_factory() as session:
        await session.execute(sa.delete(AuditEntry))
        await session.commit()


# ---------------------------------------------------------------------------
# Genesis insertion
# ---------------------------------------------------------------------------


async def test_genesis_row_inserted_by_ensure_genesis(session_factory):
    """Empty DB -> ensure_genesis inserts exactly one row with the sentinel
    field values (seq=0, prev_hash='0'*64, actor='system', action='genesis')."""
    await _clear_audit_entries(session_factory)
    assert await _count_entries(session_factory) == 0

    await ensure_genesis(session_factory)

    rows = await _all_entries_sorted(session_factory)
    assert len(rows) == 1
    genesis = rows[0]
    assert genesis.seq == 0
    assert genesis.prev_hash == GENESIS_PREV_HASH
    assert genesis.prev_hash == "0" * 64
    assert genesis.actor == "system"
    assert genesis.action == "genesis"
    assert genesis.resource == "audit_chain"
    # metadata_json should be canonical empty JSON
    assert json.loads(genesis.metadata_json) == {}
    # Genesis hash must verify under the same compute_hash code path
    expected = _compute_hash(
        seq=0,
        ts_utc=genesis.ts_utc,
        actor="system",
        action="genesis",
        resource="audit_chain",
        metadata={},
        prev_hash=GENESIS_PREV_HASH,
    )
    assert genesis.entry_hash == expected


async def test_ensure_genesis_is_idempotent(session_factory):
    """Calling ensure_genesis twice on an empty DB still results in 1 row."""
    await _clear_audit_entries(session_factory)

    await ensure_genesis(session_factory)
    await ensure_genesis(session_factory)
    await ensure_genesis(session_factory)

    assert await _count_entries(session_factory) == 1
    rows = await _all_entries_sorted(session_factory)
    assert rows[0].seq == 0


async def test_ensure_genesis_no_op_when_chain_already_populated(session_factory):
    """ensure_genesis must not overwrite an existing chain (or duplicate seq=0)."""
    # Conftest already ran init_db -> ensure_genesis once. Append a few
    # entries so the chain looks like a live deployment.
    appender = AuditAppender(session_factory)
    await appender.append(actor="x", action="a", resource="r1", metadata={})
    await appender.append(actor="x", action="a", resource="r2", metadata={})

    before = await _count_entries(session_factory)
    await ensure_genesis(session_factory)  # should be a no-op
    after = await _count_entries(session_factory)

    assert before == after == 3  # genesis + 2 appended


# ---------------------------------------------------------------------------
# Append: seq + prev_hash linkage
# ---------------------------------------------------------------------------


async def test_append_increments_seq_and_links_prev_hash(session_factory):
    """3 appends -> seqs 1, 2, 3; each row's prev_hash == prior row's entry_hash."""
    appender = AuditAppender(session_factory)

    e1 = await appender.append(
        actor="applier",
        action="transition_applied",
        resource="file:1",
        metadata={"k": 1},
    )
    e2 = await appender.append(
        actor="applier",
        action="transition_applied",
        resource="file:2",
        metadata={"k": 2},
    )
    e3 = await appender.append(
        actor="sweeper",
        action="hard_delete",
        resource="pending_delete:7",
        metadata={"pending_delete_id": 7},
    )

    # Seqs are 1, 2, 3 (genesis = 0).
    assert e1.seq == 1
    assert e2.seq == 2
    assert e3.seq == 3

    # Each entry's prev_hash points back to its predecessor's entry_hash.
    rows = await _all_entries_sorted(session_factory)
    assert len(rows) == 4  # genesis + 3 appends
    assert rows[1].prev_hash == rows[0].entry_hash  # e1.prev == genesis.hash
    assert rows[2].prev_hash == rows[1].entry_hash  # e2.prev == e1.hash
    assert rows[3].prev_hash == rows[2].entry_hash  # e3.prev == e2.hash


async def test_append_returns_fully_populated_orm_row(session_factory):
    """The returned ORM instance carries seq, entry_hash, prev_hash."""
    appender = AuditAppender(session_factory)
    out = await appender.append(
        actor="applier",
        action="transition_applied",
        resource="file:42",
        metadata={"file_id": 42},
    )
    assert out.seq == 1
    assert out.entry_hash is not None and len(out.entry_hash) == 64
    assert out.prev_hash is not None and len(out.prev_hash) == 64
    assert out.actor == "applier"
    assert out.action == "transition_applied"
    assert out.resource == "file:42"
    # metadata_json round-trips through json.loads back to a dict
    assert json.loads(out.metadata_json) == {"file_id": 42}


async def test_append_raises_when_chain_is_empty(session_factory):
    """Appending against a genesis-less chain is a deployment bug — raise loudly."""
    await _clear_audit_entries(session_factory)
    appender = AuditAppender(session_factory)
    with pytest.raises(RuntimeError, match="audit chain is empty"):
        await appender.append(actor="x", action="a", resource="r", metadata={})


# ---------------------------------------------------------------------------
# Canonicalisation
# ---------------------------------------------------------------------------


async def test_append_metadata_canonicalized(session_factory):
    """Dict key order in metadata must not affect the entry_hash."""
    appender = AuditAppender(session_factory)
    ts = datetime(2026, 5, 23, 12, 0, 0)

    # Append with reverse-ordered keys.
    out = await appender.append(
        actor="applier",
        action="transition_applied",
        resource="file:1",
        metadata={"b": 1, "a": 2},
        ts_utc=ts,
    )

    # Recompute the same hash with sorted-key input and assert match.
    expected = _compute_hash(
        seq=out.seq,
        ts_utc=ts,
        actor="applier",
        action="transition_applied",
        resource="file:1",
        metadata={"a": 2, "b": 1},  # different python dict order
        prev_hash=out.prev_hash,
    )
    assert out.entry_hash == expected


def test_canonical_bytes_independent_of_dict_order():
    """Pure-function check: two equivalent dicts hash to the same bytes."""
    ts = datetime(2026, 5, 23, 12, 0, 0)
    a = _canonical_bytes(
        seq=1,
        ts_utc=ts,
        actor="x",
        action="y",
        resource="z",
        metadata={"a": 1, "b": 2},
        prev_hash="0" * 64,
    )
    b = _canonical_bytes(
        seq=1,
        ts_utc=ts,
        actor="x",
        action="y",
        resource="z",
        metadata={"b": 2, "a": 1},
        prev_hash="0" * 64,
    )
    assert a == b
    # And the digest matches the documented algorithm explicitly.
    expected_hash = hashlib.sha256(a).hexdigest()
    assert _compute_hash(
        seq=1,
        ts_utc=ts,
        actor="x",
        action="y",
        resource="z",
        metadata={"a": 1, "b": 2},
        prev_hash="0" * 64,
    ) == expected_hash


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


async def test_concurrent_appends_serialize(session_factory):
    """5 concurrent appends produce seqs 1..5 with no duplicates."""
    appender = AuditAppender(session_factory)

    async def _one(i: int):
        return await appender.append(
            actor="applier",
            action="transition_applied",
            resource=f"file:{i}",
            metadata={"i": i},
        )

    results = await asyncio.gather(*(_one(i) for i in range(5)))

    # Collect the assigned seqs — must be unique and cover {1, 2, 3, 4, 5}.
    seqs = sorted(r.seq for r in results)
    assert seqs == [1, 2, 3, 4, 5]

    # The DB now has genesis + 5 = 6 rows, and every row's prev_hash
    # chains correctly to its predecessor.
    rows = await _all_entries_sorted(session_factory)
    assert len(rows) == 6
    for i in range(1, len(rows)):
        assert rows[i].prev_hash == rows[i - 1].entry_hash
