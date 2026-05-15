"""In-memory mock log store. ~30 records spanning all 8 resources for demo flows."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List


@dataclass(frozen=True)
class LogRecord:
    """A mock log line with structured fields."""
    id: str
    resource: str          # e.g. "application.auth"
    timestamp: datetime
    level: str             # "info" | "warn" | "error"
    message: str
    fields: Dict[str, str] = field(default_factory=dict)


def _t(offset_minutes: int) -> datetime:
    base = datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc)
    return base + timedelta(minutes=offset_minutes)


# Seeded records — chosen to make each role's demo flow interesting.
MOCK_LOGS: List[LogRecord] = [
    # --- application.auth (5 records, some with PII) ---
    LogRecord("L0001", "application.auth", _t(-30), "info", "login success",
              {"username": "alice", "email": "alice@example.com", "ip": "10.0.0.4"}),
    LogRecord("L0002", "application.auth", _t(-29), "warn", "login failure (bad password)",
              {"username": "eve", "ip": "10.0.0.99"}),
    LogRecord("L0003", "application.auth", _t(-25), "info", "logout",
              {"username": "bob", "email": "bob@example.com"}),
    LogRecord("L0004", "application.auth", _t(-20), "error", "rate limit exceeded",
              {"ip": "10.0.0.99"}),
    LogRecord("L0005", "application.auth", _t(-18), "info", "session refreshed",
              {"username": "carol"}),

    # --- application.api ---
    LogRecord("L0010", "application.api", _t(-15), "info", "GET /users",
              {"user_id": "u-101", "latency_ms": "12"}),
    LogRecord("L0011", "application.api", _t(-13), "warn", "POST /orders slow",
              {"user_id": "u-202", "latency_ms": "850"}),
    LogRecord("L0012", "application.api", _t(-10), "error", "DB connection refused",
              {"db": "primary", "host": "10.0.0.10"}),

    # --- application.worker ---
    LogRecord("L0020", "application.worker", _t(-8), "info", "job complete",
              {"job_id": "j-555", "duration_s": "12.4"}),
    LogRecord("L0021", "application.worker", _t(-7), "warn", "retry queue depth high",
              {"queue": "default", "depth": "1834"}),

    # --- business.metrics (analyst-friendly aggregate-y data) ---
    LogRecord("L0030", "business.metrics", _t(-60), "info", "daily_signups",
              {"count": "142", "day": "2026-05-14"}),
    LogRecord("L0031", "business.metrics", _t(-59), "info", "daily_signups",
              {"count": "156", "day": "2026-05-15"}),
    LogRecord("L0032", "business.metrics", _t(-50), "info", "active_sessions",
              {"count": "8910"}),

    # --- business.financial (deny target for some roles) ---
    LogRecord("L0040", "business.financial", _t(-120), "info", "invoice issued",
              {"invoice_id": "I-700", "amount_usd": "129.00"}),
    LogRecord("L0041", "business.financial", _t(-90), "warn", "payment failed",
              {"invoice_id": "I-701", "amount_usd": "489.00", "reason": "insufficient_funds"}),

    # --- business.customer (PII heavy — masking target) ---
    LogRecord("L0050", "business.customer", _t(-45), "info", "support ticket opened",
              {"ticket_id": "T-9001", "email": "alice@example.com", "user_id": "u-101"}),
    LogRecord("L0051", "business.customer", _t(-40), "info", "address updated",
              {"user_id": "u-202", "phone": "+1-555-0123"}),
    LogRecord("L0052", "business.customer", _t(-35), "warn", "duplicate account suspected",
              {"email": "bob@example.com", "ip": "10.0.0.4"}),

    # --- system.kernel ---
    LogRecord("L0060", "system.kernel", _t(-5), "info", "oom-killer disabled"),
    LogRecord("L0061", "system.kernel", _t(-4), "warn", "high CPU pressure",
              {"load_avg_5m": "8.4"}),
    LogRecord("L0062", "system.kernel", _t(-3), "error", "disk i/o stall",
              {"device": "/dev/nvme0n1"}),

    # --- system.audit (admin only) ---
    LogRecord("L0070", "system.audit", _t(-2), "info", "policy reload",
              {"policy": "ROLE_POLICIES", "actor": "alice"}),
    LogRecord("L0071", "system.audit", _t(-1), "warn", "permissions changed",
              {"target_user": "bob", "actor": "alice"}),
]


KNOWN_RESOURCES: tuple[str, ...] = (
    "application.auth", "application.api", "application.worker",
    "business.metrics", "business.financial", "business.customer",
    "system.kernel", "system.audit",
)


# --- PII masking helpers ------------------------------------------------- #
_PII_KEYS = {"email", "ip", "phone", "user_id", "username"}


def _mask_value(_value: str) -> str:
    return "***"


def mask_pii(record: LogRecord) -> LogRecord:
    """Return a copy of record with PII fields replaced by ***. Does not mutate input."""
    masked_fields = {k: (_mask_value(v) if k in _PII_KEYS else v) for k, v in record.fields.items()}
    return LogRecord(
        id=record.id,
        resource=record.resource,
        timestamp=record.timestamp,
        level=record.level,
        message=record.message,
        fields=masked_fields,
    )


def aggregate(records: List[LogRecord]) -> dict:
    """Reduce records to grouped counts by level — what an analyst gets under aggregated_only."""
    counts_by_level: Dict[str, int] = {}
    for r in records:
        counts_by_level[r.level] = counts_by_level.get(r.level, 0) + 1
    return {
        "total": len(records),
        "by_level": counts_by_level,
        "earliest": min((r.timestamp.isoformat() for r in records), default=None),
        "latest": max((r.timestamp.isoformat() for r in records), default=None),
    }


def search(resource: str, *, limit: int = 100) -> List[LogRecord]:
    """Return records for a specific resource leaf, newest first."""
    matches = [r for r in MOCK_LOGS if r.resource == resource]
    matches.sort(key=lambda r: r.timestamp, reverse=True)
    return matches[:limit]
