"""SOC 2 CC7.2 'System monitoring' report.

CC7.2 requires the entity to monitor system components for anomalies
that could indicate a security event. This report packages the audit
records in range with two anomaly indicators:
  - failure_count: number of records with success=False (likely access
    denials or upstream errors).
  - off_hours_count: number of records timestamped between 22:00 and
    06:00 UTC (rough proxy for unusual access patterns; deployments
    that span time zones should override the window).
"""
from __future__ import annotations

from datetime import datetime

from src.chain.schema import AuditRecord
from src.chain.verifier import ChainVerifier
from src.crypto.signer import Ed25519Signer
from src.reports.base import ReportBundle, build_base_bundle, fetch_records_in_range
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


_OFF_HOURS_START = 22  # 22:00 UTC
_OFF_HOURS_END = 6     # 06:00 UTC


def _is_off_hours(timestamp_utc: str) -> bool:
    """True if the ISO timestamp's hour falls in [22:00, 24:00) ∪ [00:00, 06:00)."""
    try:
        dt = datetime.fromisoformat(timestamp_utc)
    except ValueError:
        return False  # malformed timestamps don't count as anomalies here
    hour = dt.hour
    return hour >= _OFF_HOURS_START or hour < _OFF_HOURS_END


async def render_soc2_report(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    chain_verifier: ChainVerifier,
    signer: Ed25519Signer,
    from_ts: str,
    to_ts: str,
) -> ReportBundle:
    records = await fetch_records_in_range(session_factory, from_ts, to_ts)
    failure_count = sum(1 for r in records if not r.success)
    off_hours_count = sum(1 for r in records if _is_off_hours(r.timestamp_utc))
    return await build_base_bundle(
        session_factory=session_factory,
        chain_verifier=chain_verifier,
        signer=signer,
        framework="soc2",
        from_ts=from_ts,
        to_ts=to_ts,
        filters={},
        extras={
            "regulation_reference": "SOC 2 Trust Services Criteria CC7.2 (System monitoring)",
            "anomaly_indicators": {
                "failure_count": failure_count,
                "off_hours_count": off_hours_count,
                "off_hours_window_utc": f"{_OFF_HOURS_START:02d}:00-{_OFF_HOURS_END:02d}:00",
            },
        },
    )
