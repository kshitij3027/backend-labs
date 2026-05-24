"""Audit-chain integrity verification.

The verifier walks the full ``ErasureAuditLog`` table in ``sequence``
ascending order, replaying each row's hash from the stored fields and
comparing against the persisted ``entry_hash``. Any divergence — bad
genesis anchor, missing sequence, wrong ``prev_hash``, mutated payload
or event_type, tampered ``created_at`` — surfaces as a returned
``(False, sequence_of_first_break)`` so the caller knows exactly where
the chain broke.

The verifier MUST re-encode ``row.payload_json`` with
:func:`src.audit.chain.canonical_payload_json` (not ``str(...)``) so the
bytes it hashes match what :func:`src.audit.chain.append_audit_entry`
originally hashed.
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.audit.chain import (
    GENESIS_PREV_HASH,
    GENESIS_SEQUENCE,
    canonical_payload_json,
    compute_entry_hash,
)
from src.persistence.models import ErasureAuditLog


async def verify_chain(
    session: AsyncSession,
    request_id: Optional[str] = None,
) -> tuple[bool, Optional[int]]:
    """Replay the chain from genesis and re-compute each entry's hash.

    Returns ``(True, None)`` if intact, ``(False, sequence_of_first_break)``
    otherwise.

    ``request_id=None`` verifies the entire chain (from genesis).

    Even when ``request_id`` is provided, the verifier reads the FULL
    chain in sequence order (since ``prev_hash`` linkage is global),
    then optionally filters which sequence to *report*. For simplicity
    here we always verify the global chain and return the global
    first-break sequence; the ``request_id`` argument is accepted for
    API symmetry and future per-request scoping.
    """
    stmt = select(ErasureAuditLog).order_by(ErasureAuditLog.sequence.asc())
    rows = (await session.execute(stmt)).scalars().all()

    if not rows:
        # No genesis seeded → treat as a clean empty chain (caller decides if OK).
        return True, None

    # Genesis row sanity: sequence=0 anchored at the all-zero prev_hash.
    first = rows[0]
    if first.sequence != GENESIS_SEQUENCE or first.prev_hash != GENESIS_PREV_HASH:
        return False, first.sequence

    expected_prev_hash = GENESIS_PREV_HASH
    expected_sequence = GENESIS_SEQUENCE
    for row in rows:
        if row.sequence != expected_sequence:
            return False, row.sequence
        if row.prev_hash != expected_prev_hash:
            return False, row.sequence
        payload_json_str = canonical_payload_json(row.payload_json or {})
        recomputed = compute_entry_hash(
            prev_hash=row.prev_hash,
            sequence=row.sequence,
            event_type=row.event_type,
            payload_json_str=payload_json_str,
            created_at_iso=row.created_at.isoformat(),
        )
        if recomputed != row.entry_hash:
            return False, row.sequence
        expected_prev_hash = row.entry_hash
        expected_sequence = row.sequence + 1

    return True, None
