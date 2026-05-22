"""HIPAA §164.312(b) 'Audit controls' report.

Required content: a list of PHI-access events plus an integrity
attestation. PHI is identified by resource prefix ``PATIENT_`` — this
is a deployment convention the calling service follows when emitting
audit events for protected health data.
"""
from __future__ import annotations

from src.chain.schema import AuditRecord
from src.chain.verifier import ChainVerifier
from src.crypto.signer import Ed25519Signer
from src.reports.base import ReportBundle, build_base_bundle
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


_PHI_RESOURCE_PREFIX = "PATIENT_"
_PHI_ACTIONS = {"read", "search", "export"}


async def render_hipaa_report(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    chain_verifier: ChainVerifier,
    signer: Ed25519Signer,
    from_ts: str,
    to_ts: str,
) -> ReportBundle:
    """HIPAA report — PHI-access subset only, with integrity attestation."""
    from src.persistence.models import AuditRecord as AuditRecordORM
    from src.reports.base import fetch_records_in_range

    extra_filters = [
        AuditRecordORM.resource.startswith(_PHI_RESOURCE_PREFIX),
        AuditRecordORM.action.in_(_PHI_ACTIONS),
    ]
    # Fetch once up front so the count is part of the signed extras.
    records = await fetch_records_in_range(
        session_factory, from_ts, to_ts, extra_filters=extra_filters,
    )
    phi_access_count = len(records)
    suspicious_access_count = sum(1 for r in records if not r.success)

    bundle = await build_base_bundle(
        session_factory=session_factory,
        chain_verifier=chain_verifier,
        signer=signer,
        framework="hipaa",
        from_ts=from_ts,
        to_ts=to_ts,
        filters={
            "resource_prefix": _PHI_RESOURCE_PREFIX,
            "actions": sorted(_PHI_ACTIONS),
        },
        extra_filters=extra_filters,
        extras={
            "regulation_reference": "HIPAA 45 CFR §164.312(b) Audit controls",
            "phi_access_count": phi_access_count,
            "suspicious_access_count": suspicious_access_count,
        },
    )
    return bundle
