"""Async SQLite engine factory, session factory, and DB initialization.

The engine boots in WAL journal mode (concurrent readers + single writer,
which matches our chain semantics) and applies the append-only triggers
on every init — `CREATE TRIGGER IF NOT EXISTS` makes this idempotent.

init_db() also inserts the genesis row (seq=0) on first run. The row
is the chain's anchor; its prev_hash is the canonical 64 zero-chars and
its self_hash is whatever the canonicalisation produces over the genesis
fields. Subsequent appends link to seq=0's self_hash.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from src.crypto.hasher import GENESIS_PREV_HASH, sha256_hex
from src.crypto.signer import Ed25519Signer
from src.persistence.models import IMMUTABILITY_TRIGGERS_SQL, AuditRecord, Base

log = logging.getLogger(__name__)


def make_engine(database_url: str) -> AsyncEngine:
    """Create the async engine and register a PRAGMA listener.

    The listener fires on every new SQLite connection and sets WAL +
    NORMAL synchronous + foreign-keys ON. These are the SQLite-specific
    settings that give us concurrent reads, sane durability, and FK
    enforcement (even though we don't currently use FKs, defensive default).
    """
    engine = create_async_engine(
        database_url,
        future=True,
        # check_same_thread=False is required because the async greenlet may
        # cross threads even though we don't share connections across awaits.
        connect_args={"check_same_thread": False},
    )

    @event.listens_for(engine.sync_engine, "connect")
    def _apply_pragmas(dbapi_conn: Any, _conn_record: Any) -> None:
        cursor = dbapi_conn.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("PRAGMA synchronous=NORMAL;")
            cursor.execute("PRAGMA foreign_keys=ON;")
        finally:
            cursor.close()

    return engine


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Bound async session factory. Use as ``async with factory() as session:``."""
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


def _build_genesis_record(genesis_note: str, signer: Ed25519Signer) -> dict[str, Any]:
    """Compose the genesis row deterministically.

    Used by init_db to insert seq=0 on a fresh DB. The fields here are
    the contract the verifier will replay on every full check — change
    them and you invalidate every existing genesis.
    """
    empty_digest = sha256_hex({})  # canonical hash of empty payload
    timestamp_utc = datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat()
    payload = {
        "seq": 0,
        "timestamp_utc": timestamp_utc,
        "actor": "system",
        "action": "genesis",
        "resource": genesis_note,
        "success": True,
        "error_message": None,
        "processing_ms": None,
        "args_digest": empty_digest,
        "result_digest": empty_digest,
        "prev_hash": GENESIS_PREV_HASH,
    }
    self_hash = sha256_hex(payload)
    signature = signer.sign(self_hash)
    return {**payload, "self_hash": self_hash, "signature": signature}


async def init_db(
    engine: AsyncEngine,
    signer: Ed25519Signer,
    genesis_note: str,
) -> None:
    """Create tables, install triggers, insert the genesis row idempotently.

    Safe to call on every app startup — schema and triggers use IF NOT
    EXISTS semantics; the genesis insert is guarded by a SELECT for seq=0.
    """
    # 1. Schema.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # 2. Triggers. Done in a separate transaction so a trigger-creation
    #    failure doesn't roll back schema creation.
    async with engine.begin() as conn:
        for sql in IMMUTABILITY_TRIGGERS_SQL:
            await conn.exec_driver_sql(sql)

    # 3. Genesis row, only if seq=0 isn't there yet. The select happens
    #    before any insert so we never trip the no-update trigger by
    #    racing two boots — at worst two boots both decide "seq=0 absent"
    #    and the second insert fails on the seq PK uniqueness, which is
    #    a clean idempotency signal.
    factory = make_session_factory(engine)
    async with factory() as session:
        existing = await session.execute(select(AuditRecord).where(AuditRecord.seq == 0))
        if existing.scalar_one_or_none() is not None:
            log.debug("genesis row already present; skipping insert")
            return
        row = _build_genesis_record(genesis_note, signer)
        session.add(AuditRecord(**row))
        try:
            await session.commit()
            log.info("genesis row inserted (seq=0, resource=%s)", genesis_note)
        except Exception:
            await session.rollback()
            # Re-check: if some other boot won the race, we're good.
            existing = await session.execute(
                select(AuditRecord).where(AuditRecord.seq == 0)
            )
            if existing.scalar_one_or_none() is not None:
                log.debug("genesis insert lost race but row present; continuing")
                return
            raise
