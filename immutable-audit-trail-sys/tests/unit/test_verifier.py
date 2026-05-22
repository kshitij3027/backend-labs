"""Unit tests for ChainVerifier.

We bypass the immutability triggers in some tests to simulate post-hoc
tampering — that's the whole point of the verifier. To do this we
temporarily disable triggers via PRAGMA writable_schema, mutate, then
restore. THIS IS TEST-ONLY: production code never touches triggers.
"""
import base64
import os

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from src.chain.appender import ChainAppender
from src.chain.verifier import ChainVerifier, VerifyResult
from src.crypto.signer import Ed25519Signer, Ed25519Verifier
from src.persistence.db import init_db, make_engine, make_session_factory
from src.persistence.models import AuditRecord as AuditRecordORM


# --- Helpers -----------------------------------------------------------------

async def _seed_chain(appender: ChainAppender, n: int) -> None:
    """Append n records (seq 1..n) with stable, distinguishable fields."""
    for i in range(1, n + 1):
        await appender.append(
            actor=f"user_{i}",
            action="read",
            resource=f"resource_{i}",
            success=True,
            args_digest="0" * 64,
            result_digest="0" * 64,
            processing_ms=1.0,
        )


async def _tamper_row(engine: AsyncEngine, sql: str) -> None:
    """Execute a raw mutation bypassing the immutability triggers.

    We disable the triggers via PRAGMA writable_schema=1, replay the
    DDL with `enabled=0` (disabling them by name), then restore. The
    chain verifier should then detect the tamper on the next scan.
    """
    # Simpler approach: just DROP the triggers, do the mutation, recreate them.
    async with engine.begin() as conn:
        await conn.exec_driver_sql("DROP TRIGGER IF EXISTS audit_records_no_update;")
        await conn.exec_driver_sql("DROP TRIGGER IF EXISTS audit_records_no_delete;")
        await conn.exec_driver_sql(sql)


# --- Fixtures ----------------------------------------------------------------

@pytest.fixture
def signer():
    return Ed25519Signer(base64.b64encode(os.urandom(32)).decode())


@pytest.fixture
async def chain_components(tmp_path, signer):
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    engine = make_engine(url)
    await init_db(engine, signer, "test")
    factory = make_session_factory(engine)
    appender = ChainAppender(factory, signer)
    chain_verifier = ChainVerifier(factory, Ed25519Verifier(signer.public_key_b64()))
    yield engine, appender, chain_verifier
    await engine.dispose()


# --- Clean chain ---------------------------------------------------------

@pytest.mark.asyncio
async def test_clean_chain_verifies(chain_components):
    _engine, appender, chain_verifier = chain_components
    await _seed_chain(appender, 20)
    result = await chain_verifier.verify_full()
    assert result.ok is True
    assert result.integrity_status == "VALID"
    assert result.total_records == 21  # genesis + 20
    assert result.verified_records == 21
    assert result.failed_records == 0
    assert result.head_seq == 20
    assert result.first_break_seq is None
    assert result.first_break_reason is None
    assert result.signature_failures == []
    assert result.seq_gaps == []


@pytest.mark.asyncio
async def test_empty_chain_after_genesis(chain_components):
    """Verifier on just-init'd DB (only genesis): should be VALID."""
    _engine, _appender, chain_verifier = chain_components
    result = await chain_verifier.verify_full()
    assert result.ok is True
    assert result.total_records == 1
    assert result.head_seq == 0


# --- Tampered chain ------------------------------------------------------

@pytest.mark.asyncio
async def test_tampered_field_detected_at_first_break(chain_components):
    engine, appender, chain_verifier = chain_components
    await _seed_chain(appender, 10)
    # Tamper with seq=5 — change actor.
    await _tamper_row(
        engine,
        "UPDATE audit_records SET actor='evil' WHERE seq=5",
    )
    result = await chain_verifier.verify_full()
    assert result.ok is False
    assert result.integrity_status == "BROKEN"
    assert result.first_break_seq == 5
    assert result.first_break_reason == "hash_mismatch"
    # head_seq still reflects the on-disk view.
    assert result.head_seq == 10


@pytest.mark.asyncio
async def test_signature_forgery_detected(chain_components):
    engine, appender, chain_verifier = chain_components
    await _seed_chain(appender, 10)
    # Forge the signature on seq=7 with a syntactically-valid but wrong b64.
    fake_sig = base64.b64encode(b"\x00" * 64).decode()
    await _tamper_row(
        engine,
        f"UPDATE audit_records SET signature='{fake_sig}' WHERE seq=7",
    )
    result = await chain_verifier.verify_full()
    assert result.ok is False
    # The hash itself is still valid (we only changed signature), so the
    # first break is logged as signature_invalid at seq=7.
    assert result.first_break_seq == 7
    assert result.first_break_reason == "signature_invalid"
    assert 7 in result.signature_failures


@pytest.mark.asyncio
async def test_seq_gap_detected(chain_components):
    engine, appender, chain_verifier = chain_components
    await _seed_chain(appender, 10)
    # Delete seq=6, creating a gap.
    await _tamper_row(engine, "DELETE FROM audit_records WHERE seq=6")
    result = await chain_verifier.verify_full()
    assert result.ok is False
    # First break should be at seq=6 (the missing one). The scanner sees
    # expected_seq=6 but the next row in iteration is seq=7 — that's the gap.
    assert result.first_break_seq == 6
    assert result.first_break_reason in ("seq_gap", "hash_mismatch")
    # At least one gap entry.
    assert len(result.seq_gaps) >= 1


@pytest.mark.asyncio
async def test_continues_collecting_signature_failures_after_first_break(chain_components):
    engine, appender, chain_verifier = chain_components
    await _seed_chain(appender, 10)
    fake_sig = base64.b64encode(b"\x00" * 64).decode()
    # Forge two signatures.
    await _tamper_row(
        engine,
        f"UPDATE audit_records SET signature='{fake_sig}' WHERE seq=3",
    )
    await _tamper_row(
        engine,
        f"UPDATE audit_records SET signature='{fake_sig}' WHERE seq=8",
    )
    result = await chain_verifier.verify_full()
    assert result.first_break_seq == 3  # canonical smallest
    assert sorted(result.signature_failures) == [3, 8]


# --- Range mode ----------------------------------------------------------

@pytest.mark.asyncio
async def test_verify_range_scoped(chain_components):
    _engine, appender, chain_verifier = chain_components
    await _seed_chain(appender, 10)
    result = await chain_verifier.verify_range(3, 7)
    assert result.ok is True
    assert result.total_records == 5  # 3,4,5,6,7
    assert result.first_break_seq is None


@pytest.mark.asyncio
async def test_verify_range_detects_break_inside_slice(chain_components):
    engine, appender, chain_verifier = chain_components
    await _seed_chain(appender, 10)
    await _tamper_row(
        engine,
        "UPDATE audit_records SET actor='x' WHERE seq=5",
    )
    result = await chain_verifier.verify_range(3, 7)
    assert result.ok is False
    assert result.first_break_seq == 5


@pytest.mark.asyncio
async def test_verify_range_invalid_args(chain_components):
    _engine, _appender, chain_verifier = chain_components
    with pytest.raises(ValueError):
        await chain_verifier.verify_range(-1, 5)
    with pytest.raises(ValueError):
        await chain_verifier.verify_range(5, 3)
