"""Unit tests for src/persistence/db.py.

These use file-backed SQLite (tempfile) — NOT :memory: — because WAL
mode and the trigger semantics behave differently on in-memory DBs.
"""
import base64
import os
import tempfile
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DatabaseError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from src.crypto.hasher import GENESIS_PREV_HASH, sha256_hex
from src.crypto.signer import Ed25519Signer, Ed25519Verifier
from src.persistence.db import _build_genesis_record, init_db, make_engine, make_session_factory
from src.persistence.models import AuditRecord


# --- Fixtures ----------------------------------------------------------------

@pytest.fixture
def signer() -> Ed25519Signer:
    seed_b64 = base64.b64encode(os.urandom(32)).decode()
    return Ed25519Signer(seed_b64)


@pytest.fixture
def tmp_db_url(tmp_path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"


# --- Tests -------------------------------------------------------------------

@pytest.mark.asyncio
async def test_init_db_creates_genesis(tmp_db_url, signer):
    engine = make_engine(tmp_db_url)
    try:
        await init_db(engine, signer, "test-genesis-note")
        factory = make_session_factory(engine)
        async with factory() as session:
            result = await session.execute(sa.select(AuditRecord).where(AuditRecord.seq == 0))
            row = result.scalar_one()
        assert row.seq == 0
        assert row.actor == "system"
        assert row.action == "genesis"
        assert row.resource == "test-genesis-note"
        assert row.prev_hash == GENESIS_PREV_HASH
        assert row.self_hash is not None and len(row.self_hash) == 64
        assert row.signature is not None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_init_db_is_idempotent(tmp_db_url, signer):
    engine = make_engine(tmp_db_url)
    try:
        await init_db(engine, signer, "note")
        await init_db(engine, signer, "note")  # second run
        factory = make_session_factory(engine)
        async with factory() as session:
            result = await session.execute(sa.select(sa.func.count()).select_from(AuditRecord))
            count = result.scalar_one()
        assert count == 1  # exactly one genesis row, not two
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_genesis_signature_verifies(tmp_db_url, signer):
    engine = make_engine(tmp_db_url)
    try:
        await init_db(engine, signer, "note")
        verifier = Ed25519Verifier(signer.public_key_b64())
        factory = make_session_factory(engine)
        async with factory() as session:
            result = await session.execute(sa.select(AuditRecord).where(AuditRecord.seq == 0))
            row = result.scalar_one()
        assert verifier.verify(row.signature, row.self_hash) is True
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_wal_journal_mode_active(tmp_db_url, signer):
    engine = make_engine(tmp_db_url)
    try:
        await init_db(engine, signer, "note")
        async with engine.connect() as conn:
            result = await conn.exec_driver_sql("PRAGMA journal_mode;")
            mode = result.scalar_one()
        assert mode.lower() == "wal", f"expected wal journal_mode, got {mode}"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_update_trigger_blocks_mutation(tmp_db_url, signer):
    engine = make_engine(tmp_db_url)
    try:
        await init_db(engine, signer, "note")
        # SQLAlchemy wraps the underlying sqlite3 error as a DatabaseError
        # subclass; depending on driver/version it may surface as either
        # OperationalError or IntegrityError. Both inherit from DatabaseError.
        with pytest.raises(DatabaseError) as excinfo:
            async with engine.begin() as conn:
                await conn.exec_driver_sql(
                    "UPDATE audit_records SET actor='evil' WHERE seq=0"
                )
        assert "append-only" in str(excinfo.value).lower()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_delete_trigger_blocks_mutation(tmp_db_url, signer):
    engine = make_engine(tmp_db_url)
    try:
        await init_db(engine, signer, "note")
        with pytest.raises(DatabaseError) as excinfo:
            async with engine.begin() as conn:
                await conn.exec_driver_sql("DELETE FROM audit_records WHERE seq=0")
        assert "append-only" in str(excinfo.value).lower()
    finally:
        await engine.dispose()


def test_build_genesis_record_is_deterministic(signer):
    r1 = _build_genesis_record("note", signer)
    r2 = _build_genesis_record("note", signer)
    # Timestamp is a fixed constant in db.py (not utcnow()), so the records
    # should match byte-for-byte. If this fails, _build_genesis_record is
    # using a non-deterministic source — call it out.
    assert r1["seq"] == r2["seq"] == 0
    assert r1["self_hash"] == r2["self_hash"], "_build_genesis_record must be deterministic"
    assert r1["signature"] == r2["signature"], "Ed25519 signatures are deterministic"
    assert r1["prev_hash"] == GENESIS_PREV_HASH
