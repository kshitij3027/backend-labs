"""Per-framework compliance report renderers.

C14 introduces report renderers for GDPR / SOX / HIPAA. C15 will
append the PCI DSS and SOC 2 renderers to the same dispatch table.

Architecture choice — one module, three renderers — instead of
``reports/gdpr.py`` + ``reports/hipaa.py`` etc. The framework-specific
logic is small (a handful of policy + file checks per framework) and
keeping it co-located makes diff review trivial: a change to the
report contract touches exactly one file, and the three renderers can
visibly diverge or converge over time without import gymnastics.

Each renderer:

  1. Selects the in-scope policies (``compliance_tag == framework``).
  2. Selects the in-scope files (``files.compliance_tag == framework``).
  3. Loads the audit entries inside the requested time window.
  4. Walks per-framework rules and accumulates violation strings.
  5. Returns a :class:`ReportBundle` with a coarse ``compliance_score``
     derived from the violation count.

The bundle is a Pydantic ``extra='forbid'`` model so a typo in any
renderer fails at construction time rather than silently shipping an
ill-formed JSON response. Fields use ``mode='json'`` serialization at
the route layer so datetimes become ISO strings on the wire.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.compliance.rules import MIN_RETENTION_DAYS
from src.persistence.models import AuditEntry, File
from src.policy.schema import PolicySet


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class FileSummary(BaseModel):
    """Projection of an ORM ``File`` row inside a compliance report.

    Subset of columns the auditor needs to answer "what's in scope, on
    which tier, how big, and which window of records does it cover?".
    The ``model_config`` allows ORM attribute access so callers can
    feed an ORM row directly via ``model_validate(f, from_attributes=True)``.
    """

    model_config = ConfigDict(from_attributes=True)

    id: int
    source: str
    segment_path: str
    tier: str
    size_bytes: int
    compliance_tag: str | None
    immutable: bool
    oldest_record_ts: datetime
    newest_record_ts: datetime


class TransitionSummary(BaseModel):
    """Placeholder transition projection.

    The plan reserves a slot for transition data in the report bundle
    but the v1 renderers do not query transitions (the audit chain is
    the load-bearing artifact for auditors). Kept here for future
    expansion so the wire shape can grow without renaming the field.
    """


class AuditSummary(BaseModel):
    """Projection of an ORM ``AuditEntry`` row inside a compliance report.

    Includes only the fields auditors care about during a windowed
    walkthrough — the entry hash and prev-hash linkage live on the
    integrity surface (``GET /partials/...`` in C18) and would bloat
    the report payload unnecessarily here.
    """

    seq: int
    ts_utc: datetime
    actor: str
    action: str
    resource: str


class ReportBundle(BaseModel):
    """Outer envelope for every framework's compliance report.

    ``extra='forbid'`` so any renderer that accidentally adds an
    unmodelled field fails loudly at construction time — protects the
    wire shape from drift as more renderers land in C15.
    """

    model_config = ConfigDict(extra="forbid")

    framework: str
    generated_at: datetime
    time_range: dict  # {"from": iso, "to": iso}
    policies_in_scope: list[dict]
    files_in_scope: list[FileSummary]
    audit_in_range: list[AuditSummary]
    violations: list[str]
    compliance_score: float
    extras: dict = {}


# ---------------------------------------------------------------------------
# Shared scope-gathering helper
# ---------------------------------------------------------------------------


async def _gather_scope(
    session_factory: async_sessionmaker[AsyncSession],
    policy_set: PolicySet,
    framework: str,
    time_from: datetime,
    time_to: datetime,
) -> tuple[list[Any], list[File], list[AuditEntry]]:
    """Return (policies, files, audit entries) scoped to ``framework`` and the time window.

    The policies are filtered in memory (``policy_set`` is small — five
    or ten policies, not thousands), while the files and audit entries
    are filtered at the SQL layer so a large catalog or audit table
    doesn't bloat the response.

    Audit entries are ordered by ``seq`` ascending so a renderer that
    walks the list sees them in the same order they were appended.
    """
    policies = [p for p in policy_set.policies if p.compliance_tag == framework]
    async with session_factory() as session:
        files_result = await session.execute(
            select(File).where(File.compliance_tag == framework)
        )
        files = list(files_result.scalars().all())
        audit_result = await session.execute(
            select(AuditEntry)
            .where(
                and_(
                    AuditEntry.ts_utc >= time_from,
                    AuditEntry.ts_utc <= time_to,
                )
            )
            .order_by(AuditEntry.seq.asc())
        )
        audit = list(audit_result.scalars().all())
    return policies, files, audit


def _audit_summaries(audit: list[AuditEntry]) -> list[AuditSummary]:
    """Convert ORM ``AuditEntry`` rows to the wire projection."""
    return [
        AuditSummary(
            seq=a.seq,
            ts_utc=a.ts_utc,
            actor=a.actor,
            action=a.action,
            resource=a.resource,
        )
        for a in audit
    ]


# ---------------------------------------------------------------------------
# GDPR
# ---------------------------------------------------------------------------


async def render_gdpr_report(
    session_factory: async_sessionmaker[AsyncSession],
    policy_set: PolicySet,
    time_from: datetime,
    time_to: datetime,
) -> ReportBundle:
    """Render the GDPR compliance report.

    Rules enforced:

      1. **Right to erasure (Art. 17).** Every GDPR-tagged policy must
         have at least one ``delete`` phase. Personal data must be
         erased once it is no longer necessary for the purpose for
         which it was collected.
      2. **Minimum retention.** Each delete phase must fire on or after
         ``MIN_RETENTION_DAYS['gdpr']`` (1095 d — the project's
         conservative security-log baseline). Anything shorter is
         suspect and surfaced as a violation, mirroring the boot-time
         validator so a policy that slipped past startup gets flagged
         here too.

    The ``extras`` block includes file scope and a coarse count of
    ``hard_delete`` audit events in-window — useful as a sanity check
    when reconciling the audit chain against policy expectations.
    """
    framework = "gdpr"
    policies, files, audit = await _gather_scope(
        session_factory, policy_set, framework, time_from, time_to
    )

    violations: list[str] = []
    for p in policies:
        delete_phases = [ph for ph in p.phases if ph.action == "delete"]
        if not delete_phases:
            violations.append(
                f"GDPR policy '{p.name}' has no delete phase "
                f"(data must be deleted when no longer necessary)"
            )
        else:
            for dp in delete_phases:
                if dp.after_days < MIN_RETENTION_DAYS[framework]:
                    violations.append(
                        f"GDPR policy '{p.name}' delete fires at "
                        f"{dp.after_days}d < required "
                        f"{MIN_RETENTION_DAYS[framework]}d"
                    )

    delete_entries = [a for a in audit if a.action == "hard_delete"]

    # Score: fraction of policies with no violation messages mentioning
    # their name. Empty policy set => 100.0 (nothing to fail).
    total_checks = max(len(policies), 1)
    failing_policies = len({v.split("'")[1] for v in violations if "'" in v})
    compliance_score = (
        100.0 * (total_checks - failing_policies) / total_checks
        if total_checks > 0
        else 100.0
    )

    return ReportBundle(
        framework=framework,
        generated_at=datetime.utcnow(),
        time_range={"from": time_from.isoformat(), "to": time_to.isoformat()},
        policies_in_scope=[p.model_dump(mode="json") for p in policies],
        files_in_scope=[
            FileSummary.model_validate(f, from_attributes=True) for f in files
        ],
        audit_in_range=_audit_summaries(audit),
        violations=violations,
        compliance_score=round(compliance_score, 2),
        extras={
            "in_scope_file_count": len(files),
            "delete_audit_events": len(delete_entries),
        },
    )


# ---------------------------------------------------------------------------
# SOX
# ---------------------------------------------------------------------------


async def render_sox_report(
    session_factory: async_sessionmaker[AsyncSession],
    policy_set: PolicySet,
    time_from: datetime,
    time_to: datetime,
) -> ReportBundle:
    """Render the SOX compliance report.

    Rules enforced:

      1. **Immutable policy.** Every SOX-tagged policy must have
         ``immutable=True``. The boot-time validator already enforces
         this — we re-check here so an auditor reading just the report
         sees the same answer without consulting the loader logs.
      2. **Immutable files.** Every in-scope ``File`` row must have
         ``immutable=True``. A mutable archive segment in SOX scope is
         a SOX 17 CFR 210.2-06 violation regardless of the policy
         posture.
      3. **Minimum retention.** Any ``delete`` phase must fire on or
         after ``MIN_RETENTION_DAYS['sox']`` (2555 d / 7 yr).

    The score denominator is policies + files in scope: a SOX
    deployment with 0 in-scope policies and 0 files reports 100.0
    (vacuously compliant); 1 violation against 4 checks reports 75.0.
    """
    framework = "sox"
    policies, files, audit = await _gather_scope(
        session_factory, policy_set, framework, time_from, time_to
    )

    violations: list[str] = []
    for p in policies:
        if not p.immutable:
            violations.append(
                f"SOX policy '{p.name}' is not immutable "
                f"(required for SOX compliance)"
            )
    for f in files:
        if not f.immutable:
            violations.append(
                f"SOX file '{f.segment_path}' (id={f.id}) is not marked immutable"
            )
    for p in policies:
        for ph in p.phases:
            if ph.action == "delete" and ph.after_days < MIN_RETENTION_DAYS[framework]:
                violations.append(
                    f"SOX policy '{p.name}' delete fires at "
                    f"{ph.after_days}d < required "
                    f"{MIN_RETENTION_DAYS[framework]}d"
                )

    total_checks = max(len(policies) + len(files), 1)
    failing = len(violations)
    compliance_score = (
        100.0 * max(total_checks - failing, 0) / total_checks
        if total_checks > 0
        else 100.0
    )

    return ReportBundle(
        framework=framework,
        generated_at=datetime.utcnow(),
        time_range={"from": time_from.isoformat(), "to": time_to.isoformat()},
        policies_in_scope=[p.model_dump(mode="json") for p in policies],
        files_in_scope=[
            FileSummary.model_validate(f, from_attributes=True) for f in files
        ],
        audit_in_range=_audit_summaries(audit),
        violations=violations,
        compliance_score=round(compliance_score, 2),
        extras={"in_scope_file_count": len(files)},
    )


# ---------------------------------------------------------------------------
# HIPAA
# ---------------------------------------------------------------------------


async def render_hipaa_report(
    session_factory: async_sessionmaker[AsyncSession],
    policy_set: PolicySet,
    time_from: datetime,
    time_to: datetime,
) -> ReportBundle:
    """Render the HIPAA compliance report.

    Rules enforced:

      1. **Immutable policy.** HIPAA-tagged policies must have
         ``immutable=True`` (45 CFR 164.312(c)(1) — integrity of
         ePHI). Mirrors the boot-time validator.
      2. **Immutable archive files.** Every HIPAA-tagged ``File`` on
         the ``archive`` tier must have ``immutable=True``. We
         intentionally scope this to ``archive`` (not all tiers)
         because the operational hot/warm tiers may legitimately be
         mutable as records flow through them — only the
         long-retention copy needs WORM protection.
      3. **Minimum retention.** Delete phases fire on or after
         ``MIN_RETENTION_DAYS['hipaa']`` (2190 d / 6 yr) per
         45 CFR 164.316(b)(2).

    The score denominator is policies + archive-tier files: empty
    deployments report 100.0; mixed deployments lose 1 point per
    violation against the combined denominator.
    """
    framework = "hipaa"
    policies, files, audit = await _gather_scope(
        session_factory, policy_set, framework, time_from, time_to
    )

    violations: list[str] = []
    for p in policies:
        if not p.immutable:
            violations.append(
                f"HIPAA policy '{p.name}' is not immutable"
            )
    for f in files:
        if f.tier == "archive" and not f.immutable:
            violations.append(
                f"HIPAA archive file '{f.segment_path}' "
                f"(id={f.id}) is not marked immutable"
            )
    for p in policies:
        for ph in p.phases:
            if ph.action == "delete" and ph.after_days < MIN_RETENTION_DAYS[framework]:
                violations.append(
                    f"HIPAA policy '{p.name}' delete at "
                    f"{ph.after_days}d < required "
                    f"{MIN_RETENTION_DAYS[framework]}d"
                )

    archive_files = [f for f in files if f.tier == "archive"]
    total_checks = max(len(policies) + len(archive_files), 1)
    failing = len(violations)
    compliance_score = (
        100.0 * max(total_checks - failing, 0) / total_checks
        if total_checks > 0
        else 100.0
    )

    return ReportBundle(
        framework=framework,
        generated_at=datetime.utcnow(),
        time_range={"from": time_from.isoformat(), "to": time_to.isoformat()},
        policies_in_scope=[p.model_dump(mode="json") for p in policies],
        files_in_scope=[
            FileSummary.model_validate(f, from_attributes=True) for f in files
        ],
        audit_in_range=_audit_summaries(audit),
        violations=violations,
        compliance_score=round(compliance_score, 2),
        extras={
            "in_scope_file_count": len(files),
            "archive_file_count": len(archive_files),
        },
    )


# ---------------------------------------------------------------------------
# PCI DSS
# ---------------------------------------------------------------------------


async def render_pci_dss_report(
    session_factory: async_sessionmaker[AsyncSession],
    policy_set: PolicySet,
    time_from: datetime,
    time_to: datetime,
) -> ReportBundle:
    """Render the PCI DSS compliance report.

    Rules enforced:

      1. **Immutable policy (Req. 10.5.2).** PCI DSS requires that audit
         trail files be protected from modification. Every PCI-tagged
         policy must therefore have ``immutable=True``; we mirror the
         boot-time validator so the report stands alone for an auditor.
      2. **Immutable files (Req. 10.5.2).** Every in-scope ``File`` row
         (not just archive — PCI considers the entire cardholder-data
         lifecycle in scope) must have ``immutable=True``.
      3. **Minimum retention (Req. 10.5.1).** Any ``delete`` phase must
         fire on or after ``MIN_RETENTION_DAYS['pci_dss']`` (365 d / 12 mo).
         PCI explicitly mandates a 12-month online window for audit logs.

    Indefinite retention (no delete phase) is *allowed* — PCI's Req.
    10.5.1 is a floor, not a ceiling. So unlike GDPR, the absence of a
    delete phase is not a violation here.

    The ``extras`` block exposes ``cardholder_data_segments`` (count of
    in-scope ``File`` rows) — a coarse measure of the cardholder data
    estate's footprint that auditors typically ask about first.
    """
    framework = "pci_dss"
    policies, files, audit = await _gather_scope(
        session_factory, policy_set, framework, time_from, time_to
    )

    violations: list[str] = []
    # Rule 1: every PCI policy must be immutable=True.
    for p in policies:
        if not p.immutable:
            violations.append(
                f"PCI DSS policy '{p.name}' is not immutable (Req. 10.5.2)"
            )
    # Rule 2: every in-scope file must be immutable.
    for f in files:
        if not f.immutable:
            violations.append(
                f"PCI DSS file '{f.segment_path}' (id={f.id}) is not marked immutable"
            )
    # Rule 3: delete phases must fire >= 365d (Req. 10.5.1 — 12 months).
    for p in policies:
        for ph in p.phases:
            if ph.action == "delete" and ph.after_days < MIN_RETENTION_DAYS[framework]:
                violations.append(
                    f"PCI DSS policy '{p.name}' delete fires at "
                    f"{ph.after_days}d < required "
                    f"{MIN_RETENTION_DAYS[framework]}d"
                )

    # Rule 4 (rule of thumb): PCI doesn't require an indefinite-retention
    # ceiling — kept forever is fine, so a missing delete phase is NOT a
    # violation. (Contrast GDPR's Art. 17 right-to-erasure stance.)

    cardholder_count = len(files)
    total_checks = max(len(policies) + len(files), 1)
    failing = len(violations)
    compliance_score = (
        100.0 * max(total_checks - failing, 0) / total_checks
        if total_checks > 0
        else 100.0
    )

    return ReportBundle(
        framework=framework,
        generated_at=datetime.utcnow(),
        time_range={"from": time_from.isoformat(), "to": time_to.isoformat()},
        policies_in_scope=[p.model_dump(mode="json") for p in policies],
        files_in_scope=[
            FileSummary.model_validate(f, from_attributes=True) for f in files
        ],
        audit_in_range=_audit_summaries(audit),
        violations=violations,
        compliance_score=round(compliance_score, 2),
        extras={"cardholder_data_segments": cardholder_count},
    )


# ---------------------------------------------------------------------------
# SOC 2
# ---------------------------------------------------------------------------


async def render_soc2_report(
    session_factory: async_sessionmaker[AsyncSession],
    policy_set: PolicySet,
    time_from: datetime,
    time_to: datetime,
) -> ReportBundle:
    """Render the SOC 2 compliance report.

    SOC 2 (Type II) doesn't prescribe a single rule set the way SOX or
    HIPAA do — it's a set of Trust Service Criteria, of which CC7.2
    ("monitoring of system components") and CC7.3 ("evaluation of
    security events") map most directly to a retention engine's audit
    trail. The check we run here is therefore the most load-bearing
    one for SOC 2: **the audit chain must verify end-to-end**.

    Rules enforced:

      1. **Chain integrity.** Run :class:`ChainVerifier.verify_full` and
         surface a violation if any break is detected. This is the
         binary CC7.2 signal — either the audit trail is tamper-evident
         and intact, or it is not.

    Observations (no violation):

      * Counts of ``transition_applied`` and ``hard_delete`` audit
        events in-window are reported in ``extras`` as a coverage
        signal. A fresh DB legitimately has zero — absence of coverage
        is not a violation here, just a data point.

    ``compliance_score`` is 100.0 when the chain is valid, 50.0 when
    only the chain is broken (the report is still a half-credit pass —
    policies were correctly authored, but the integrity surface failed).
    """
    framework = "soc2"
    policies, files, audit = await _gather_scope(
        session_factory, policy_set, framework, time_from, time_to
    )

    # SOC2-specific: re-run chain integrity check.
    # Import inline to avoid a top-of-module cycle with audit/verifier
    # (which currently doesn't import this module, but keeping the
    # import inside the function makes the dependency localised).
    from src.audit.verifier import ChainVerifier

    verifier = ChainVerifier(session_factory)
    chain_result = await verifier.verify_full()

    violations: list[str] = []
    if not chain_result.ok:
        violations.append(
            f"SOC 2: audit chain integrity broken at seq={chain_result.first_break_seq} "
            f"(reason: {chain_result.first_break_reason})"
        )

    # Coverage signal: count transition_applied / hard_delete entries in
    # the window. These are the events CC7.2 expects the engine to log
    # for every retention action. Absent on a fresh DB — that's fine.
    transition_audit_count = sum(1 for a in audit if a.action == "transition_applied")
    delete_audit_count = sum(1 for a in audit if a.action == "hard_delete")

    # Score: chain integrity is binary. SOC2 allows mutable storage so
    # policy-immutability is not a check here — the score is essentially
    # "did the chain verify?". Half-credit when only the chain broke.
    if chain_result.ok:
        compliance_score = 100.0
    else:
        compliance_score = 50.0

    return ReportBundle(
        framework=framework,
        generated_at=datetime.utcnow(),
        time_range={"from": time_from.isoformat(), "to": time_to.isoformat()},
        policies_in_scope=[p.model_dump(mode="json") for p in policies],
        files_in_scope=[
            FileSummary.model_validate(f, from_attributes=True) for f in files
        ],
        audit_in_range=_audit_summaries(audit),
        violations=violations,
        compliance_score=round(compliance_score, 2),
        extras={
            "chain_integrity_status": "VALID" if chain_result.ok else "BROKEN",
            "chain_head_seq": chain_result.head_seq,
            "chain_first_break_seq": chain_result.first_break_seq,
            "transition_audit_events": transition_audit_count,
            "hard_delete_audit_events": delete_audit_count,
        },
    )


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


# Per-framework dispatch table. C15 adds ``pci_dss`` and ``soc2`` so the
# route layer can dispatch all five frameworks by name without further
# wiring; unknown slugs raise ``KeyError`` and the route translates to 400.
_RENDERERS: dict[str, Any] = {
    "gdpr": render_gdpr_report,
    "sox": render_sox_report,
    "hipaa": render_hipaa_report,
    "pci_dss": render_pci_dss_report,
    "soc2": render_soc2_report,
}


async def render_report(
    framework: str,
    session_factory: async_sessionmaker[AsyncSession],
    policy_set: PolicySet,
    time_from: datetime,
    time_to: datetime,
) -> ReportBundle:
    """Dispatch to the per-framework renderer.

    Raises ``KeyError`` for unknown frameworks so the route layer can
    translate to HTTP 400 with a list of supported framework slugs.
    """
    if framework not in _RENDERERS:
        raise KeyError(f"unknown framework: {framework}")
    return await _RENDERERS[framework](
        session_factory, policy_set, time_from, time_to
    )


__all__ = [
    "AuditSummary",
    "FileSummary",
    "ReportBundle",
    "TransitionSummary",
    "render_gdpr_report",
    "render_hipaa_report",
    "render_pci_dss_report",
    "render_report",
    "render_soc2_report",
    "render_sox_report",
]
