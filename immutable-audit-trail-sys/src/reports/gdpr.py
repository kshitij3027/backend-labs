"""GDPR Art. 30 'Records of processing activities' report.

For each audit record, we attach a lawful-basis tag and a documented
processing purpose. The mapping from our internal action verbs to GDPR
lawful bases is conservative: read-style operations default to
``legitimate_interests`` (auditing access to logs is a legitimate-
interests use case under Art. 6(1)(f)) unless the action implies
something stronger.
"""
from __future__ import annotations

from typing import Any

from src.chain.schema import AuditRecord
from src.chain.verifier import ChainVerifier
from src.crypto.signer import Ed25519Signer
from src.reports.base import ReportBundle, build_base_bundle
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


_LAWFUL_BASIS_BY_ACTION: dict[str, str] = {
    "read": "legitimate_interests",
    "search": "legitimate_interests",
    "export": "legal_obligation",
    "redact": "legal_obligation",
    "delete": "legal_obligation",
    "genesis": "system_bootstrap",
}

_PROCESSING_PURPOSE_BY_ACTION: dict[str, str] = {
    "read": "audit and security monitoring",
    "search": "audit and security monitoring",
    "export": "regulatory disclosure",
    "redact": "data subject rights fulfilment",
    "delete": "data subject rights fulfilment / retention policy",
    "genesis": "system bootstrap (immutable anchor)",
}


def _annotate_for_gdpr(record: AuditRecord) -> dict[str, Any]:
    return {
        "seq": record.seq,
        "timestamp_utc": record.timestamp_utc,
        "actor": record.actor,
        "action": record.action,
        "resource": record.resource,
        "lawful_basis": _LAWFUL_BASIS_BY_ACTION.get(record.action, "legitimate_interests"),
        "processing_purpose": _PROCESSING_PURPOSE_BY_ACTION.get(record.action, "audit and security monitoring"),
        "retention_period_days": 1095,  # 3 years default — caller can override per deployment
    }


async def render_gdpr_report(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    chain_verifier: ChainVerifier,
    signer: Ed25519Signer,
    from_ts: str,
    to_ts: str,
    actor: str | None = None,
    resource: str | None = None,
) -> ReportBundle:
    """GDPR Art. 30 report — all records in range, annotated with lawful basis."""
    from src.persistence.models import AuditRecord as AuditRecordORM
    extra_filters = []
    if actor:
        extra_filters.append(AuditRecordORM.actor == actor)
    if resource:
        extra_filters.append(AuditRecordORM.resource == resource)
    bundle = await build_base_bundle(
        session_factory=session_factory,
        chain_verifier=chain_verifier,
        signer=signer,
        framework="gdpr",
        from_ts=from_ts,
        to_ts=to_ts,
        filters={"actor": actor, "resource": resource},
        extra_filters=extra_filters or None,
        extras={
            "regulation_reference": "GDPR Article 30",
            "annotations": [_annotate_for_gdpr(r) for r in await _records_for_extras(
                session_factory, from_ts, to_ts, extra_filters,
            )],
        },
    )
    return bundle


async def _records_for_extras(session_factory, from_ts, to_ts, extra_filters):
    """Re-fetch the same records used by the bundle, for annotation."""
    from src.reports.base import fetch_records_in_range
    return await fetch_records_in_range(
        session_factory, from_ts, to_ts, extra_filters=extra_filters or None,
    )
