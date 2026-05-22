"""ChainAppender — atomic, signed appends to the audit chain.

A single instance per process. Holds the signer + session factory and
exposes one async method, ``append``, that takes the call-site facts
(actor, action, resource, success, digests, timing) and returns the
freshly-sealed ``AuditRecord``.

Concurrency model: SQLite serialises writers via ``BEGIN IMMEDIATE``,
so two coroutines calling ``append`` concurrently never produce a torn
chain — they queue at the engine. Throughput is therefore single-stream
for writes, multi-stream for reads. Acceptable trade for an audit log.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from src.chain.schema import AuditRecord, AuditRecordPayload, compute_self_hash
from src.crypto.hasher import GENESIS_PREV_HASH
from src.crypto.signer import Ed25519Signer
from src.persistence.models import AuditRecord as AuditRecordORM

log = logging.getLogger(__name__)


class ChainAppender:
    """Singleton-per-process writer for the audit chain."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        signer: Ed25519Signer,
    ) -> None:
        self._sessions = session_factory
        self._signer = signer

    async def append(
        self,
        *,
        actor: str,
        action: str,
        resource: str,
        success: bool,
        args_digest: str,
        result_digest: str,
        processing_ms: float | None = None,
        error_message: Optional[str] = None,
        timestamp_utc: Optional[str] = None,
    ) -> AuditRecord:
        """Build, seal, persist a new audit record. Returns the sealed record.

        The write path:
          1. Open a session and BEGIN IMMEDIATE (write lock).
          2. SELECT the row with the highest seq for prev_hash + next_seq.
          3. Construct an AuditRecordPayload with seq = prev_seq + 1.
          4. Compute self_hash, sign it.
          5. INSERT the sealed row.
          6. COMMIT.

        If no rows exist (shouldn't happen — init_db inserts genesis), we
        fail loud rather than silently bootstrap; init_db is the only thing
        allowed to write seq=0.
        """
        ts = timestamp_utc or datetime.now(timezone.utc).isoformat()
        async with self._sessions() as session:
            # Take the SQLite write lock immediately. This is what serialises
            # competing appenders — without it, two coroutines could both read
            # the same prev_seq and produce a duplicate seq (which would then
            # fail on the PK uniqueness, but with a confusing error).
            await session.execute(sa.text("BEGIN IMMEDIATE"))
            prev = (
                await session.execute(
                    sa.select(AuditRecordORM)
                    .order_by(AuditRecordORM.seq.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            if prev is None:
                # init_db should have inserted seq=0 already. If it didn't,
                # we refuse to bootstrap from here — that's a deployment bug.
                raise RuntimeError(
                    "audit chain is empty; init_db must run before append()"
                )

            payload = AuditRecordPayload(
                seq=prev.seq + 1,
                timestamp_utc=ts,
                actor=actor,
                action=action,
                resource=resource,
                success=success,
                error_message=error_message,
                processing_ms=processing_ms,
                args_digest=args_digest,
                result_digest=result_digest,
                prev_hash=prev.self_hash,
            )
            self_hash = compute_self_hash(payload)
            signature = self._signer.sign(self_hash)

            row = AuditRecordORM(
                **payload.model_dump(),
                self_hash=self_hash,
                signature=signature,
            )
            session.add(row)
            await session.commit()

        return AuditRecord(
            **payload.model_dump(),
            self_hash=self_hash,
            signature=signature,
        )
