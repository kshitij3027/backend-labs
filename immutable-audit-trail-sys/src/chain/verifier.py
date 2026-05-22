"""ChainVerifier — replays the audit chain and reports tamper-evidence.

Two modes:
- ``verify_full()`` checks the entire chain from seq=0.
- ``verify_range(from_seq, to_seq)`` checks a contiguous slice (faster
  for spot checks; the first record in the range is anchored against
  the previous row's self_hash if present).

The verifier returns a structured ``VerifyResult`` rather than a bool:
``first_break_seq`` is the canonical answer (the smallest seq where the
chain broke), and the auxiliary fields (``signature_failures``,
``seq_gaps``) help an auditor diagnose what kind of tampering happened.

If a row's ``self_hash`` doesn't match the recomputed value, the chain
breaks at that seq (hash_mismatch). If a row's signature doesn't verify,
the seq is added to ``signature_failures`` but the chain continues
(signature failure on row N doesn't necessarily corrupt row N+1's link).
If a seq is missing, the gap is recorded and treated as a break.
"""
from __future__ import annotations

import logging
from typing import Literal, Optional

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.chain.schema import AuditRecordPayload, compute_self_hash
from src.crypto.hasher import GENESIS_PREV_HASH
from src.crypto.signer import Ed25519Verifier
from src.persistence.models import AuditRecord as AuditRecordORM

log = logging.getLogger(__name__)


IntegrityStatus = Literal["VALID", "BROKEN"]


class VerifyResult(BaseModel):
    """Structured outcome of a chain verification pass."""

    ok: bool
    integrity_status: IntegrityStatus
    head_seq: int
    total_records: int
    verified_records: int
    failed_records: int
    first_break_seq: Optional[int] = None
    first_break_reason: Optional[str] = None  # "hash_mismatch" | "signature_invalid" | "seq_gap"
    signature_failures: list[int] = []
    seq_gaps: list[tuple[int, int]] = []  # (expected_seq, found_seq)


class ChainVerifier:
    """Streams rows from the DB and replays the chain."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        verifier: Ed25519Verifier,
    ) -> None:
        self._sessions = session_factory
        self._verifier = verifier

    async def verify_full(self) -> VerifyResult:
        """Verify from seq=0 to the current head."""
        return await self._verify(from_seq=None, to_seq=None)

    async def verify_range(self, from_seq: int, to_seq: int) -> VerifyResult:
        """Verify the inclusive [from_seq, to_seq] slice.

        The first row in the slice is anchored against the previous row's
        self_hash (if from_seq > 0); seq=0 anchors against GENESIS_PREV_HASH.
        """
        if from_seq < 0 or to_seq < from_seq:
            raise ValueError(f"invalid range: [{from_seq}, {to_seq}]")
        return await self._verify(from_seq=from_seq, to_seq=to_seq)

    async def _verify(
        self,
        *,
        from_seq: Optional[int],
        to_seq: Optional[int],
    ) -> VerifyResult:
        async with self._sessions() as session:
            # Get the head seq up front so we report it even on an early break.
            head_row = (await session.execute(
                sa.select(AuditRecordORM)
                .order_by(AuditRecordORM.seq.desc())
                .limit(1)
            )).scalar_one_or_none()
            head_seq = head_row.seq if head_row is not None else -1

            # Build the query: full chain or a range slice.
            stmt = sa.select(AuditRecordORM).order_by(AuditRecordORM.seq)
            if from_seq is not None:
                stmt = stmt.where(AuditRecordORM.seq >= from_seq)
            if to_seq is not None:
                stmt = stmt.where(AuditRecordORM.seq <= to_seq)
            rows = (await session.execute(stmt)).scalars().all()

            # If we're doing a range starting > 0, seed expected_prev_hash
            # from the row at from_seq - 1 (so we can detect tampering at
            # the boundary of the range).
            expected_prev_hash: str
            if from_seq is not None and from_seq > 0:
                anchor = (await session.execute(
                    sa.select(AuditRecordORM).where(
                        AuditRecordORM.seq == from_seq - 1
                    )
                )).scalar_one_or_none()
                expected_prev_hash = (
                    anchor.self_hash if anchor is not None else GENESIS_PREV_HASH
                )
            else:
                expected_prev_hash = GENESIS_PREV_HASH

            # Expected next seq for gap detection — starts at from_seq (or 0).
            expected_seq = from_seq if from_seq is not None else 0

        total_records = len(rows)
        verified_records = 0
        failed_records = 0
        first_break_seq: Optional[int] = None
        first_break_reason: Optional[str] = None
        signature_failures: list[int] = []
        seq_gaps: list[tuple[int, int]] = []

        for row in rows:
            # 1. Sequence gap check.
            if row.seq != expected_seq:
                seq_gaps.append((expected_seq, row.seq))
                if first_break_seq is None:
                    first_break_seq = expected_seq
                    first_break_reason = "seq_gap"
                # Re-sync to the row we actually saw and continue.
                expected_seq = row.seq

            # 2. Re-derive self_hash from the row's payload.
            payload = AuditRecordPayload(
                seq=row.seq,
                timestamp_utc=row.timestamp_utc,
                actor=row.actor,
                action=row.action,
                resource=row.resource,
                success=row.success,
                error_message=row.error_message,
                processing_ms=row.processing_ms,
                args_digest=row.args_digest,
                result_digest=row.result_digest,
                prev_hash=row.prev_hash,
            )
            recomputed = compute_self_hash(payload)

            hash_ok = recomputed == row.self_hash
            link_ok = row.prev_hash == expected_prev_hash
            sig_ok = self._verifier.verify(row.signature, row.self_hash)

            if not hash_ok or not link_ok:
                failed_records += 1
                if first_break_seq is None:
                    first_break_seq = row.seq
                    first_break_reason = "hash_mismatch"
            else:
                verified_records += 1

            if not sig_ok:
                signature_failures.append(row.seq)
                # Signature failure alone is not necessarily a chain break
                # — the prev_hash chain can still be intact — but record it.
                if first_break_seq is None:
                    first_break_seq = row.seq
                    first_break_reason = "signature_invalid"

            # Advance the linker to this row's self_hash (whatever we found,
            # so subsequent rows are still checked against the on-disk view).
            expected_prev_hash = row.self_hash
            expected_seq = row.seq + 1

        ok = (
            first_break_seq is None
            and failed_records == 0
            and not signature_failures
            and not seq_gaps
        )

        return VerifyResult(
            ok=ok,
            integrity_status="VALID" if ok else "BROKEN",
            head_seq=head_seq,
            total_records=total_records,
            verified_records=verified_records,
            failed_records=failed_records,
            first_break_seq=first_break_seq,
            first_break_reason=first_break_reason,
            signature_failures=signature_failures,
            seq_gaps=seq_gaps,
        )
