"""Unit tests for the audit hash-chain primitives, append, and verifier.

Covers:

* ``compute_entry_hash`` determinism + sensitivity to every input field.
* ``canonical_payload_json`` produces sorted, whitespace-free JSON.
* ``append_audit_entry`` links to the genesis row and chains correctly
  across multiple appends.
* ``verify_chain`` reports a clean chain after init and after appends,
  and detects payload tampering by returning the first broken sequence.
"""
from __future__ import annotations

import pytest
from sqlalchemy import select

from src.audit.chain import (
    GENESIS_PREV_HASH,
    GENESIS_SEQUENCE,
    append_audit_entry,
    canonical_payload_json,
    compute_entry_hash,
)
from src.audit.verifier import verify_chain
from src.persistence.models import ErasureAuditLog


def test_compute_entry_hash_is_deterministic():
    """Same inputs → same hash, and the output is a 64-char hex SHA-256."""
    h1 = compute_entry_hash("p", 1, "E", "{}", "2026-01-01T00:00:00")
    h2 = compute_entry_hash("p", 1, "E", "{}", "2026-01-01T00:00:00")
    assert h1 == h2
    assert len(h1) == 64


def test_compute_entry_hash_sensitive_to_each_field():
    """Flipping any single field bit must change the hash."""
    base = ("p", 1, "E", "{}", "2026-01-01T00:00:00")
    h0 = compute_entry_hash(*base)
    assert compute_entry_hash("p2", 1, "E", "{}", "2026-01-01T00:00:00") != h0
    assert compute_entry_hash("p", 2, "E", "{}", "2026-01-01T00:00:00") != h0
    assert compute_entry_hash("p", 1, "X", "{}", "2026-01-01T00:00:00") != h0
    assert compute_entry_hash("p", 1, "E", "{\"a\":1}", "2026-01-01T00:00:00") != h0
    assert compute_entry_hash("p", 1, "E", "{}", "2026-01-02T00:00:00") != h0


def test_canonical_payload_json_sorted_compact():
    """Keys are sorted, separators are whitespace-free."""
    assert canonical_payload_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'


@pytest.mark.asyncio
async def test_append_single_entry_links_to_genesis(session_factory):
    """First append after init_db gets sequence=1 chained off the genesis hash."""
    async with session_factory() as s:
        genesis = (
            await s.execute(
                select(ErasureAuditLog).where(
                    ErasureAuditLog.sequence == GENESIS_SEQUENCE
                )
            )
        ).scalar_one()
        entry = await append_audit_entry(
            s, request_id=None, event_type="X", payload={"k": "v"}
        )
        await s.commit()

    assert entry.sequence == 1
    assert entry.prev_hash == genesis.entry_hash
    expected_hash = compute_entry_hash(
        prev_hash=genesis.entry_hash,
        sequence=1,
        event_type="X",
        payload_json_str=canonical_payload_json({"k": "v"}),
        created_at_iso=entry.created_at.isoformat(),
    )
    assert entry.entry_hash == expected_hash


@pytest.mark.asyncio
async def test_append_five_entries_chain_correctly(session_factory):
    """Five sequential appends produce sequences 1..5 with linked prev_hashes."""
    async with session_factory() as s:
        prev_hash = (
            await s.execute(
                select(ErasureAuditLog).where(
                    ErasureAuditLog.sequence == GENESIS_SEQUENCE
                )
            )
        ).scalar_one().entry_hash
        entries = []
        for i in range(1, 6):
            e = await append_audit_entry(
                s, request_id=None, event_type=f"E{i}", payload={"i": i}
            )
            entries.append(e)
        await s.commit()

    for idx, e in enumerate(entries, start=1):
        assert e.sequence == idx
        assert e.prev_hash == prev_hash
        prev_hash = e.entry_hash


@pytest.mark.asyncio
async def test_verify_chain_clean_after_init(session_factory):
    """Just the genesis row → verifier returns ``(True, None)``."""
    async with session_factory() as s:
        ok, bad = await verify_chain(s)
    assert ok is True
    assert bad is None


@pytest.mark.asyncio
async def test_verify_chain_detects_tamper(session_factory):
    """Mutating a row's payload_json after commit must surface as a break."""
    async with session_factory() as s:
        for i in range(1, 4):
            await append_audit_entry(
                s, request_id=None, event_type=f"E{i}", payload={"i": i}
            )
        await s.commit()

        # Tamper: rewrite sequence=2's payload without recomputing the hash.
        target = (
            await s.execute(
                select(ErasureAuditLog).where(ErasureAuditLog.sequence == 2)
            )
        ).scalar_one()
        target.payload_json = {"i": 999}
        await s.commit()

        ok, bad = await verify_chain(s)

    assert ok is False
    assert bad == 2


@pytest.mark.asyncio
async def test_verify_chain_passes_after_appends(session_factory):
    """Untouched appends keep the chain intact end-to-end."""
    async with session_factory() as s:
        for i in range(1, 4):
            await append_audit_entry(
                s, request_id=None, event_type=f"E{i}", payload={"i": i}
            )
        await s.commit()
        ok, bad = await verify_chain(s)

    assert ok is True
    assert bad is None
