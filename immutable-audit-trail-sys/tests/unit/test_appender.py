"""Unit tests for ChainAppender.

Uses a tmp-path SQLite DB and a fresh Ed25519 keypair per test for full
isolation. init_db() is called at the start of each test to lay down
the genesis row that ChainAppender requires.
"""
import asyncio
import base64
import os

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine

from src.chain.appender import ChainAppender
from src.chain.schema import compute_self_hash, AuditRecordPayload
from src.crypto.signer import Ed25519Signer, Ed25519Verifier
from src.persistence.db import init_db, make_engine, make_session_factory
from src.persistence.models import AuditRecord as AuditRecordORM


# --- Fixtures ----------------------------------------------------------------

@pytest.fixture
def signer():
    return Ed25519Signer(base64.b64encode(os.urandom(32)).decode())


@pytest.fixture
async def appender_and_engine(tmp_path, signer):
    """Create a tmp DB, run init_db, return (appender, engine, signer)."""
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    engine = make_engine(url)
    await init_db(engine, signer, "test-genesis")
    factory = make_session_factory(engine)
    appender = ChainAppender(factory, signer)
    yield appender, engine, signer
    await engine.dispose()


# --- Basic chain linkage ----------------------------------------------------

@pytest.mark.asyncio
async def test_single_append_returns_sealed_record(appender_and_engine):
    appender, _engine, _signer = appender_and_engine
    record = await appender.append(
        actor="alice",
        action="read",
        resource="/logs/app",
        success=True,
        args_digest="a" * 64,
        result_digest="b" * 64,
        processing_ms=5.0,
    )
    assert record.seq == 1
    assert record.actor == "alice"
    assert len(record.self_hash) == 64
    assert record.signature  # non-empty


@pytest.mark.asyncio
async def test_seq_is_monotonic_across_ten_appends(appender_and_engine):
    appender, _, _ = appender_and_engine
    records = []
    for i in range(10):
        records.append(await appender.append(
            actor=f"user_{i}",
            action="read",
            resource="x",
            success=True,
            args_digest="0" * 64,
            result_digest="0" * 64,
            processing_ms=1.0,
        ))
    seqs = [r.seq for r in records]
    assert seqs == list(range(1, 11))


@pytest.mark.asyncio
async def test_prev_hash_links_to_previous_self_hash(appender_and_engine):
    appender, engine, _ = appender_and_engine
    # Two appends
    r1 = await appender.append(actor="a", action="r", resource="x",
                               success=True, args_digest="0"*64,
                               result_digest="0"*64, processing_ms=1.0)
    r2 = await appender.append(actor="a", action="r", resource="x",
                               success=True, args_digest="0"*64,
                               result_digest="0"*64, processing_ms=1.0)
    # r1's prev_hash should match genesis (seq=0) self_hash.
    factory = make_session_factory(engine)
    async with factory() as session:
        rows = (await session.execute(
            sa.select(AuditRecordORM).order_by(AuditRecordORM.seq)
        )).scalars().all()
    assert len(rows) == 3  # genesis + 2
    assert rows[1].prev_hash == rows[0].self_hash
    assert rows[2].prev_hash == rows[1].self_hash
    assert r2.prev_hash == r1.self_hash


# --- Signature integrity ---------------------------------------------------

@pytest.mark.asyncio
async def test_signatures_verify_with_matching_verifier(appender_and_engine):
    appender, _, signer = appender_and_engine
    verifier = Ed25519Verifier(signer.public_key_b64())
    for i in range(5):
        rec = await appender.append(actor="a", action="r", resource="x",
                                    success=True, args_digest="0"*64,
                                    result_digest="0"*64, processing_ms=1.0)
        assert verifier.verify(rec.signature, rec.self_hash) is True


@pytest.mark.asyncio
async def test_self_hash_matches_compute_self_hash(appender_and_engine):
    """Re-derive self_hash from the returned record's fields and confirm match."""
    appender, _, _ = appender_and_engine
    rec = await appender.append(actor="a", action="r", resource="x",
                                success=True, args_digest="0"*64,
                                result_digest="0"*64, processing_ms=2.5)
    payload = AuditRecordPayload(**{k: getattr(rec, k) for k in (
        "seq", "timestamp_utc", "actor", "action", "resource",
        "success", "error_message", "processing_ms",
        "args_digest", "result_digest", "prev_hash",
    )})
    assert compute_self_hash(payload) == rec.self_hash


# --- Failure path ----------------------------------------------------------

@pytest.mark.asyncio
async def test_failure_record_persists_error_message(appender_and_engine):
    appender, engine, _ = appender_and_engine
    rec = await appender.append(
        actor="a", action="r", resource="x",
        success=False, error_message="permission denied",
        args_digest="0"*64, result_digest="",
        processing_ms=0.5,
    )
    assert rec.success is False
    assert rec.error_message == "permission denied"
    # Re-read from DB to confirm persistence shape.
    factory = make_session_factory(engine)
    async with factory() as session:
        rows = (await session.execute(
            sa.select(AuditRecordORM).where(AuditRecordORM.seq == rec.seq)
        )).scalars().all()
    assert rows[0].error_message == "permission denied"
    assert rows[0].success is False


# --- Concurrency: BEGIN IMMEDIATE serialises writers -----------------------

@pytest.mark.asyncio
async def test_concurrent_appends_produce_valid_chain(appender_and_engine):
    """Fire 20 appends concurrently; chain must remain unbroken.

    With BEGIN IMMEDIATE, sqlite serialises writers — every record's
    prev_hash should match the previous record's self_hash, with no
    duplicate seqs.
    """
    appender, engine, _ = appender_and_engine
    async def one():
        return await appender.append(
            actor="a", action="r", resource="x",
            success=True, args_digest="0"*64,
            result_digest="0"*64, processing_ms=1.0,
        )

    # Note: aiosqlite serialises at the engine level; 20 is enough to
    # exercise the lock without taking forever.
    results = await asyncio.gather(*[one() for _ in range(20)])
    seqs = sorted(r.seq for r in results)
    assert seqs == list(range(1, 21)), f"duplicate or missing seqs: {seqs}"

    # Re-read all rows in seq order; confirm chain linkage is intact.
    factory = make_session_factory(engine)
    async with factory() as session:
        rows = (await session.execute(
            sa.select(AuditRecordORM).order_by(AuditRecordORM.seq)
        )).scalars().all()
    for i in range(1, len(rows)):
        assert rows[i].prev_hash == rows[i-1].self_hash, (
            f"chain break at seq={rows[i].seq}"
        )


# --- Error: appender on empty DB ------------------------------------------

@pytest.mark.asyncio
async def test_append_on_empty_db_raises(tmp_path, signer):
    """Without init_db (so no genesis), append() must refuse."""
    url = f"sqlite+aiosqlite:///{tmp_path / 'empty.db'}"
    engine = make_engine(url)
    # Create the schema but skip the genesis insert by importing models + create_all only.
    from src.persistence.models import Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    factory = make_session_factory(engine)
    appender = ChainAppender(factory, signer)
    with pytest.raises(RuntimeError, match="init_db"):
        await appender.append(
            actor="a", action="r", resource="x",
            success=True, args_digest="0"*64,
            result_digest="0"*64, processing_ms=1.0,
        )
    await engine.dispose()
