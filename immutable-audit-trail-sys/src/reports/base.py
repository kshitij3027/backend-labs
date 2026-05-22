"""Shared building blocks for compliance reports.

Every framework report has the same outer shape:
  framework, generated_at, time_range, filters, records,
  verify_result, attestation_signature.

The framework-specific bits (GDPR's lawful-basis tags, HIPAA's PHI
filter, SOC 2's anomaly indicators, PCI DSS's cardholder filter) live
in per-framework modules.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.chain.schema import AuditRecord
from src.chain.verifier import ChainVerifier, VerifyResult
from src.crypto.hasher import sha256_hex
from src.crypto.signer import Ed25519Signer
from src.persistence.models import AuditRecord as AuditRecordORM


class ReportBundle(BaseModel):
    """The common outer envelope every report shares."""
    framework: str
    generated_at: str
    time_range: tuple[str, str]
    filters: dict[str, Any]
    records: list[AuditRecord]
    verify_result: VerifyResult
    attestation_signature: str
    # Per-framework extras live here, free-form by design.
    extras: dict[str, Any] = {}


def _orm_to_record(row: AuditRecordORM) -> AuditRecord:
    return AuditRecord(
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
        self_hash=row.self_hash,
        signature=row.signature,
    )


async def fetch_records_in_range(
    session_factory: async_sessionmaker[AsyncSession],
    from_ts: str,
    to_ts: str,
    *,
    extra_filters: Optional[list[Any]] = None,
) -> list[AuditRecord]:
    """Fetch all records in [from_ts, to_ts] with optional extra WHERE filters."""
    stmt = sa.select(AuditRecordORM).where(
        AuditRecordORM.timestamp_utc >= from_ts,
        AuditRecordORM.timestamp_utc <= to_ts,
    )
    if extra_filters:
        stmt = stmt.where(*extra_filters)
    stmt = stmt.order_by(AuditRecordORM.seq)
    async with session_factory() as session:
        rows = (await session.execute(stmt)).scalars().all()
    return [_orm_to_record(r) for r in rows]


def sign_bundle(bundle_payload: dict[str, Any], signer: Ed25519Signer) -> str:
    """Compute a deterministic Ed25519 signature over the canonical bundle bytes."""
    digest = sha256_hex(bundle_payload)
    return signer.sign(digest)


async def build_base_bundle(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    chain_verifier: ChainVerifier,
    signer: Ed25519Signer,
    framework: str,
    from_ts: str,
    to_ts: str,
    filters: dict[str, Any],
    extra_filters: Optional[list[Any]] = None,
    extras: Optional[dict[str, Any]] = None,
) -> ReportBundle:
    """Compose the outer ReportBundle. Framework modules add to `extras`."""
    records = await fetch_records_in_range(
        session_factory, from_ts, to_ts, extra_filters=extra_filters
    )
    verify = await chain_verifier.verify_full()
    generated_at = datetime.now(timezone.utc).isoformat()

    # Sign over a deterministic dict of the auditable contents — records
    # are summarized as their seqs+self_hashes to keep the digest stable
    # without re-hashing the entire chain twice.
    sig_payload = {
        "framework": framework,
        "generated_at": generated_at,
        "time_range": [from_ts, to_ts],
        "filters": filters,
        "record_seqs": [r.seq for r in records],
        "record_self_hashes": [r.self_hash for r in records],
        "verify_ok": verify.ok,
        "verify_head_seq": verify.head_seq,
        "verify_first_break_seq": verify.first_break_seq,
        "extras": extras or {},
    }
    attestation_signature = sign_bundle(sig_payload, signer)

    return ReportBundle(
        framework=framework,
        generated_at=generated_at,
        time_range=(from_ts, to_ts),
        filters=filters,
        records=records,
        verify_result=verify,
        attestation_signature=attestation_signature,
        extras=extras or {},
    )
