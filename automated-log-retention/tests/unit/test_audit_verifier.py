"""Unit tests for ``src/audit/verifier.py`` (C13).

The verifier replays the chain and surfaces the first break. These tests
cover the three break categories:

  * ``"genesis_mismatch"`` — seq=0 row's prev_hash mutated.
  * ``"prev_hash_mismatch"`` — a row's prev_hash no longer links to the
    previous row's entry_hash.
  * ``"hash_mismatch"`` — payload field (metadata_json, actor, etc.)
    mutated, so the recomputed entry_hash differs from the stored value.

Plus the happy-path cases:

  * empty chain (no genesis) -> ok=True, head_seq=None
  * genesis-only -> ok=True, head_seq=0
  * full walk over N appended rows -> ok=True, head_seq=N
"""
from __future__ import annotations

import sqlalchemy as sa

from src.audit.chain import AuditAppender, GENESIS_PREV_HASH
from src.audit.verifier import ChainVerifier, VerifyResult
from src.persistence.models import AuditEntry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _clear_audit_entries(session_factory) -> None:
    """Drop every audit_entries row so a test starts from a truly empty chain."""
    async with session_factory() as session:
        await session.execute(sa.delete(AuditEntry))
        await session.commit()


async def _mutate_row(
    session_factory,
    seq: int,
    *,
    metadata_json: str | None = None,
    prev_hash: str | None = None,
    actor: str | None = None,
) -> None:
    """Apply a raw UPDATE to a single audit_entries row.

    Used to simulate tamper events the verifier must catch.
    """
    values: dict = {}
    if metadata_json is not None:
        values["metadata_json"] = metadata_json
    if prev_hash is not None:
        values["prev_hash"] = prev_hash
    if actor is not None:
        values["actor"] = actor
    async with session_factory() as session:
        await session.execute(
            sa.update(AuditEntry).where(AuditEntry.seq == seq).values(**values)
        )
        await session.commit()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_verify_empty_chain_returns_ok_with_none_head(session_factory):
    """A DB with no audit_entries rows verifies vacuously."""
    await _clear_audit_entries(session_factory)
    verifier = ChainVerifier(session_factory)
    result = await verifier.verify_full()
    assert isinstance(result, VerifyResult)
    assert result.ok is True
    assert result.head_seq is None
    assert result.first_break_seq is None
    assert result.first_break_reason is None


async def test_verify_genesis_only_returns_ok(session_factory):
    """The genesis row alone is a valid chain at head_seq=0."""
    # conftest already ran ensure_genesis via init_db. Don't append anything.
    verifier = ChainVerifier(session_factory)
    result = await verifier.verify_full()
    assert result.ok is True
    assert result.head_seq == 0
    assert result.first_break_seq is None


async def test_verify_full_walk_chain_ok(session_factory):
    """5 appended entries -> ok=True, head_seq=5 (genesis is seq=0)."""
    appender = AuditAppender(session_factory)
    for i in range(5):
        await appender.append(
            actor="applier",
            action="transition_applied",
            resource=f"file:{i}",
            metadata={"i": i},
        )

    verifier = ChainVerifier(session_factory)
    result = await verifier.verify_full()
    assert result.ok is True
    assert result.head_seq == 5
    assert result.first_break_seq is None
    assert result.first_break_reason is None


# ---------------------------------------------------------------------------
# Break detection: hash_mismatch (payload mutation)
# ---------------------------------------------------------------------------


async def test_verify_detects_mutated_metadata(session_factory):
    """Mutating metadata_json on seq=2 -> first_break_seq=2, reason='hash_mismatch'."""
    appender = AuditAppender(session_factory)
    await appender.append(actor="applier", action="t1", resource="r1", metadata={})
    await appender.append(actor="applier", action="t2", resource="r2", metadata={})
    await appender.append(actor="applier", action="t3", resource="r3", metadata={})

    # Tamper: change metadata_json on seq=2 directly via SQL.
    await _mutate_row(session_factory, seq=2, metadata_json='{"hacked":1}')

    verifier = ChainVerifier(session_factory)
    result = await verifier.verify_full()
    assert result.ok is False
    assert result.first_break_seq == 2
    assert result.first_break_reason == "hash_mismatch"


async def test_verify_detects_mutated_actor(session_factory):
    """Mutating actor on any row trips hash_mismatch at that seq."""
    appender = AuditAppender(session_factory)
    await appender.append(actor="applier", action="t1", resource="r1", metadata={})
    await appender.append(actor="applier", action="t2", resource="r2", metadata={})

    await _mutate_row(session_factory, seq=1, actor="evil_actor")

    result = await ChainVerifier(session_factory).verify_full()
    assert result.ok is False
    assert result.first_break_seq == 1
    assert result.first_break_reason == "hash_mismatch"


# ---------------------------------------------------------------------------
# Break detection: prev_hash_mismatch (chain linkage)
# ---------------------------------------------------------------------------


async def test_verify_detects_broken_prev_hash(session_factory):
    """Mutating prev_hash on seq=2 -> first_break_seq=2, reason='prev_hash_mismatch'."""
    appender = AuditAppender(session_factory)
    await appender.append(actor="applier", action="t1", resource="r1", metadata={})
    await appender.append(actor="applier", action="t2", resource="r2", metadata={})
    await appender.append(actor="applier", action="t3", resource="r3", metadata={})

    # Tamper: replace seq=2's prev_hash with a bogus value.
    bogus = "f" * 64
    await _mutate_row(session_factory, seq=2, prev_hash=bogus)

    result = await ChainVerifier(session_factory).verify_full()
    assert result.ok is False
    assert result.first_break_seq == 2
    assert result.first_break_reason == "prev_hash_mismatch"


# ---------------------------------------------------------------------------
# Break detection: genesis_mismatch
# ---------------------------------------------------------------------------


async def test_verify_detects_mutated_genesis_prev_hash(session_factory):
    """Mutating seq=0's prev_hash away from '0'*64 -> genesis_mismatch."""
    # No appends needed — just tamper with seq=0 (which conftest already created).
    bogus = "1" * 64
    await _mutate_row(session_factory, seq=0, prev_hash=bogus)

    result = await ChainVerifier(session_factory).verify_full()
    assert result.ok is False
    assert result.first_break_seq == 0
    assert result.first_break_reason == "genesis_mismatch"


async def test_verify_genesis_prev_hash_sentinel_is_zeros():
    """Sanity check on the documented genesis sentinel value."""
    assert GENESIS_PREV_HASH == "0" * 64
