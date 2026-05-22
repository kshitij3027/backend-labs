"""PCI DSS Requirement 10.2 'Audit trails' report.

Req 10.2 mandates logging all access to cardholder data (CHD). This
report filters the chain to records whose resource starts with
``CARDHOLDER_`` and bundles them with a tamper-evidence attestation
(the same verify_result + Ed25519 signature every report carries).
"""
from __future__ import annotations

from src.chain.verifier import ChainVerifier
from src.crypto.signer import Ed25519Signer
from src.reports.base import ReportBundle, build_base_bundle, fetch_records_in_range
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


_CARDHOLDER_RESOURCE_PREFIX = "CARDHOLDER_"


async def render_pci_dss_report(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    chain_verifier: ChainVerifier,
    signer: Ed25519Signer,
    from_ts: str,
    to_ts: str,
) -> ReportBundle:
    from src.persistence.models import AuditRecord as AuditRecordORM
    extra_filters = [
        AuditRecordORM.resource.startswith(_CARDHOLDER_RESOURCE_PREFIX),
    ]
    records = await fetch_records_in_range(
        session_factory, from_ts, to_ts, extra_filters=extra_filters,
    )
    chd_access_count = len(records)
    failed_attempt_count = sum(1 for r in records if not r.success)
    return await build_base_bundle(
        session_factory=session_factory,
        chain_verifier=chain_verifier,
        signer=signer,
        framework="pci_dss",
        from_ts=from_ts,
        to_ts=to_ts,
        filters={"resource_prefix": _CARDHOLDER_RESOURCE_PREFIX},
        extra_filters=extra_filters,
        extras={
            "regulation_reference": "PCI DSS Requirement 10.2 (Audit trails for cardholder data access)",
            "chd_access_count": chd_access_count,
            "failed_attempt_count": failed_attempt_count,
        },
    )
