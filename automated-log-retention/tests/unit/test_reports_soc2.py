"""Unit tests for ``render_soc2_report`` in ``src/compliance/reports.py``.

The SOC 2 renderer's load-bearing check is **chain integrity** — the
report re-runs :class:`ChainVerifier.verify_full` and surfaces a
violation if any break is detected. These tests cover:

  * Clean DB (only genesis row) -> chain VALID, 0 violations, score=100.0
  * Tampered chain (metadata mutated post-append) -> chain BROKEN,
    violation lists the break seq + reason, score=50.0
  * Framework filtering: SOC2 + GDPR policies in the set,
    ``policies_in_scope`` has only the SOC2 entry.
  * Chain head_seq advances as entries are appended.

The genesis row is inserted by ``init_db`` in the shared ``session_factory``
fixture, so the "clean DB" case already has seq=0 visible.
"""
from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa

from src.audit.chain import AuditAppender
from src.compliance.reports import render_soc2_report
from src.persistence.models import AuditEntry
from src.policy.schema import Phase, Policy, PolicySet, Selector


def _soc2_policy(name: str = "service_logs_soc2") -> Policy:
    """Build a SOC2 policy. SOC2 allows mutable storage, so immutable=False is fine."""
    return Policy(
        name=name,
        selector=Selector(category="service"),
        priority=100,
        compliance_tag="soc2",
        immutable=False,
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(after_days=30, action="promote", target_tier="warm"),
            Phase(
                after_days=90,
                action="compress",
                target_tier="cold",
                compression_level=3,
            ),
            Phase(after_days=365, action="delete"),
        ],
    )


def _gdpr_policy() -> Policy:
    """A GDPR policy used to verify the framework filter excludes other tags."""
    return Policy(
        name="user_gdpr",
        selector=Selector(category="user_activity"),
        priority=100,
        compliance_tag="gdpr",
        immutable=False,
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(after_days=1095, action="delete"),
        ],
    )


_DEFAULT_WINDOW = (
    datetime(2026, 1, 1, 0, 0, 0),
    datetime(2026, 12, 31, 23, 59, 59),
)


async def _mutate_metadata(session_factory, seq: int, new_json: str) -> None:
    """Raw-SQL tamper helper — bypasses the AuditAppender contract.

    Used to simulate the in-the-wild tamper case the SOC2 chain
    integrity check must detect: a row whose metadata_json was modified
    after the row was sealed, leaving the stored entry_hash stale.
    """
    async with session_factory() as session:
        await session.execute(
            sa.update(AuditEntry)
            .where(AuditEntry.seq == seq)
            .values(metadata_json=new_json)
        )
        await session.commit()


async def test_soc2_chain_intact_valid(session_factory):
    """A clean DB (genesis only) verifies cleanly: VALID, 0 violations, score=100."""
    policy_set = PolicySet(policies=[_soc2_policy()])
    bundle = await render_soc2_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert bundle.framework == "soc2"
    assert bundle.extras["chain_integrity_status"] == "VALID"
    assert bundle.violations == []
    assert bundle.compliance_score == 100.0


async def test_soc2_chain_break_detected(session_factory):
    """Mutating a sealed entry's metadata trips the chain integrity check.

    Appends two entries, then rewrites seq=1's ``metadata_json`` via raw
    SQL. The verifier walks seq=0 (genesis, ok), then seq=1 — recomputes
    the hash from the now-mutated metadata and finds it no longer
    matches the stored ``entry_hash``. SOC2 renderer surfaces this as
    a violation, downgrades the score to 50.0, and reports
    ``chain_integrity_status == "BROKEN"``.
    """
    policy_set = PolicySet(policies=[_soc2_policy()])
    appender = AuditAppender(session_factory)
    await appender.append(
        actor="applier", action="transition_applied", resource="file:1", metadata={"k": "v1"}
    )
    await appender.append(
        actor="applier", action="transition_applied", resource="file:2", metadata={"k": "v2"}
    )

    # Tamper: rewrite seq=1's metadata_json (recomputed hash will not
    # match stored entry_hash).
    await _mutate_metadata(session_factory, seq=1, new_json='{"hacked":true}')

    bundle = await render_soc2_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert bundle.extras["chain_integrity_status"] == "BROKEN"
    assert bundle.extras["chain_first_break_seq"] == 1
    assert any(
        "chain integrity" in v.lower() and "seq=1" in v
        for v in bundle.violations
    ), bundle.violations
    assert bundle.compliance_score == 50.0


async def test_soc2_in_scope_filtering(session_factory):
    """GDPR-tagged policies are filtered out of the SOC2 report."""
    policy_set = PolicySet(policies=[_soc2_policy(), _gdpr_policy()])
    bundle = await render_soc2_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert len(bundle.policies_in_scope) == 1
    assert bundle.policies_in_scope[0]["compliance_tag"] == "soc2"


async def test_soc2_extras_chain_head_seq(session_factory):
    """``chain_head_seq`` matches the highest appended seq (3 after 3 appends)."""
    policy_set = PolicySet(policies=[_soc2_policy()])
    appender = AuditAppender(session_factory)
    for i in range(3):
        await appender.append(
            actor="applier",
            action="transition_applied",
            resource=f"file:{i}",
            metadata={"i": i},
        )

    bundle = await render_soc2_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    # genesis is seq=0; three appends => head_seq=3
    assert bundle.extras["chain_head_seq"] == 3
    assert bundle.extras["chain_integrity_status"] == "VALID"
