"""Hash-chain primitives + concurrent-safe append.

``GENESIS_SEQUENCE`` is ``0`` and ``GENESIS_PREV_HASH`` is the 64-char
all-zero string. Every subsequent entry's ``prev_hash`` is the previous
row's ``entry_hash``.

The chain hash for entry N is::

    sha256(prev_hash:sequence:event_type:payload_json_str:created_at_iso)

where ``prev_hash`` is entry ``(N-1)``'s ``entry_hash``, ``sequence`` is
``N``, ``payload_json_str`` is the canonical JSON encoding produced by
:func:`canonical_payload_json` (``json.dumps(..., sort_keys=True,
separators=(",", ":"))``), and ``created_at_iso`` is the entry's
``created_at`` timestamp in ISO-8601 format (no microseconds — the
model truncates).

The field order in the hash input is locked: changing it would silently
invalidate every existing chain, so don't reorder without a migration
story.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from typing import Any, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


GENESIS_SEQUENCE: int = 0
GENESIS_PREV_HASH: str = "0" * 64

# Postgres advisory-lock key for the audit chain. A single fixed int means
# every appender serialises on the same lock — globally ordered.
AUDIT_CHAIN_LOCK_KEY: int = 0x6764_7072_4348_4149  # ascii "gdprCHAI" approx


def compute_entry_hash(
    prev_hash: str,
    sequence: int,
    event_type: str,
    payload_json_str: str,
    created_at_iso: str,
) -> str:
    """SHA-256 of a canonical ``":"`` joined string of the entry's fields.

    Field order is locked: ``prev_hash:sequence:event_type:payload_json_str:created_at_iso``.
    Changing the order would silently invalidate every existing chain, so
    don't reorder without a migration story.
    """
    payload = ":".join(
        [prev_hash, str(sequence), event_type, payload_json_str, created_at_iso]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def canonical_payload_json(payload: dict[str, Any]) -> str:
    """Canonical JSON encoding used for hashing.

    ``sort_keys`` ensures stability across Python runs and dict-insertion
    orders; ``separators=(",", ":")`` strips the default whitespace so
    two equivalent dicts can never disagree about their hashed bytes.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _utcnow() -> dt.datetime:
    """Naive UTC timestamp with microseconds truncated.

    Matches the model's ``_utcnow`` convention so the ISO string round-trips
    cleanly through SQLite (which doesn't carry tz info) and Postgres.
    """
    return dt.datetime.utcnow().replace(microsecond=0)


async def append_audit_entry(
    session: AsyncSession,
    *,
    request_id: Optional[str],
    event_type: str,
    payload: dict[str, Any],
) -> "ErasureAuditLog":  # type: ignore[name-defined]
    """Append a new audit entry transactionally and concurrent-safely.

    Concurrency strategy:
    - On Postgres: acquire pg_advisory_xact_lock(AUDIT_CHAIN_LOCK_KEY) so all
      appenders serialise on a single key for the duration of the transaction.
      Released automatically on COMMIT or ROLLBACK.
    - On SQLite (tests): the dialect doesn't have advisory locks, but SQLite
      serialises all writes globally so the same invariant holds without
      additional locking.

    The caller is responsible for `await session.commit()` (or rollback).
    """
    from src.persistence.models import ErasureAuditLog  # local to avoid cycles
    from sqlalchemy import text

    # Postgres-only: serialise appenders via advisory xact lock
    bind = session.get_bind()
    if bind.dialect.name == "postgresql":
        await session.execute(text("SELECT pg_advisory_xact_lock(:k)").bindparams(k=AUDIT_CHAIN_LOCK_KEY))

    stmt = (
        select(ErasureAuditLog)
        .order_by(ErasureAuditLog.sequence.desc())
        .limit(1)
    )
    last = (await session.execute(stmt)).scalar_one_or_none()
    if last is None:
        raise RuntimeError(
            "audit chain has no genesis row — init_db must run before append_audit_entry"
        )
    next_sequence = last.sequence + 1
    prev_hash = last.entry_hash
    created_at = _utcnow()
    payload_json_str = canonical_payload_json(payload)
    entry_hash = compute_entry_hash(
        prev_hash=prev_hash,
        sequence=next_sequence,
        event_type=event_type,
        payload_json_str=payload_json_str,
        created_at_iso=created_at.isoformat(),
    )

    entry = ErasureAuditLog(
        request_id=request_id,
        sequence=next_sequence,
        event_type=event_type,
        payload_json=payload,
        prev_hash=prev_hash,
        entry_hash=entry_hash,
        created_at=created_at,
    )
    session.add(entry)
    await session.flush()
    return entry
