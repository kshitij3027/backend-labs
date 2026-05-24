"""Unit tests for ``render_hipaa_report`` in ``src/compliance/reports.py``.

Builds synthetic ``PolicySet`` instances directly via Pydantic and
seeds the per-test SQLite engine with ``File`` rows through
``CatalogRepo``. The HIPAA rules in C14 scope file immutability to
the ``archive`` tier specifically — earlier tiers may legitimately be
mutable as records flow through them, and the unit tests pin that
distinction down so a future refactor cannot silently broaden the
check.
"""
from __future__ import annotations

from datetime import datetime

from src.compliance.reports import render_hipaa_report
from src.policy.schema import Phase, Policy, PolicySet, Selector
from src.storage.catalog import CatalogRepo


def _hipaa_compliant_policy() -> Policy:
    """A compliant HIPAA policy — immutable + 6 yr delete."""
    return Policy(
        name="health_records_hipaa",
        selector=Selector(category="health"),
        priority=1000,
        compliance_tag="hipaa",
        immutable=True,
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(after_days=30, action="promote", target_tier="warm"),
            Phase(
                after_days=90,
                action="archive",
                target_tier="archive",
                compression_level=19,
            ),
            Phase(after_days=2190, action="delete"),
        ],
    )


def _hipaa_mutable_policy() -> Policy:
    """A HIPAA policy with immutable=False (violation)."""
    return Policy(
        name="hipaa_mutable",
        selector=Selector(category="health"),
        priority=10,
        compliance_tag="hipaa",
        immutable=False,
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(
                after_days=90,
                action="archive",
                target_tier="archive",
                compression_level=19,
            ),
        ],
    )


def _gdpr_policy() -> Policy:
    """A GDPR policy used as a non-HIPAA contrast."""
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


async def test_hipaa_archive_must_be_immutable(session_factory):
    """An archive-tier HIPAA file without immutable=True is a violation."""
    policy_set = PolicySet(policies=[_hipaa_compliant_policy()])

    repo = CatalogRepo(session_factory)
    base = datetime(2026, 1, 1)
    # Archive-tier file WITHOUT immutable=True => violation.
    await repo.add_file(
        source="health-svc",
        segment_path="/tiers/archive/health-mutable.jsonl",
        tier="archive",
        size_bytes=1024,
        oldest_record_ts=base,
        newest_record_ts=base,
        compliance_tag="hipaa",
        immutable=False,
    )

    bundle = await render_hipaa_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert any(
        "health-mutable.jsonl" in v and "not marked immutable" in v
        for v in bundle.violations
    ), bundle.violations
    assert bundle.compliance_score < 100.0


async def test_hipaa_non_archive_mutability_not_flagged(session_factory):
    """A HOT-tier HIPAA file with immutable=False is NOT flagged.

    HIPAA's immutability check is scoped to archive tier only — hot
    and warm are flow-through tiers, not retention destinations. This
    test pins that contract down so future tightening is intentional.
    """
    policy_set = PolicySet(policies=[_hipaa_compliant_policy()])

    repo = CatalogRepo(session_factory)
    base = datetime(2026, 1, 1)
    await repo.add_file(
        source="health-svc",
        segment_path="/tiers/hot/health-fresh.jsonl",
        tier="hot",
        size_bytes=1024,
        oldest_record_ts=base,
        newest_record_ts=base,
        compliance_tag="hipaa",
        immutable=False,  # mutable in hot is fine
    )

    bundle = await render_hipaa_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert bundle.violations == []
    assert bundle.compliance_score == 100.0


async def test_hipaa_short_delete_flagged(session_factory):
    """A HIPAA delete firing before 2190 d is reported as a violation."""
    policy_set = PolicySet(
        policies=[
            Policy(
                name="hipaa_short_delete",
                selector=Selector(category="health"),
                priority=10,
                compliance_tag="hipaa",
                immutable=True,
                phases=[
                    Phase(after_days=0, action="promote", target_tier="hot"),
                    # HIPAA min is 2190 d; 100 d is clearly under
                    Phase(after_days=100, action="delete"),
                ],
            )
        ]
    )
    bundle = await render_hipaa_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert any(
        "hipaa_short_delete" in v and "100d" in v and "2190" in v
        for v in bundle.violations
    ), bundle.violations
    assert bundle.compliance_score < 100.0


async def test_hipaa_mutable_policy_flagged(session_factory):
    """A HIPAA policy with immutable=False is reported as a violation."""
    policy_set = PolicySet(policies=[_hipaa_mutable_policy()])
    bundle = await render_hipaa_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert any(
        "hipaa_mutable" in v and "not immutable" in v.lower()
        for v in bundle.violations
    ), bundle.violations
    assert bundle.compliance_score < 100.0


async def test_hipaa_compliant_no_violations(session_factory):
    """A compliant HIPAA policy + immutable archive file yield zero violations."""
    policy_set = PolicySet(policies=[_hipaa_compliant_policy()])

    repo = CatalogRepo(session_factory)
    base = datetime(2026, 1, 1)
    await repo.add_file(
        source="health-svc",
        segment_path="/tiers/archive/health-ok.jsonl",
        tier="archive",
        size_bytes=1024,
        oldest_record_ts=base,
        newest_record_ts=base,
        compliance_tag="hipaa",
        immutable=True,
    )

    bundle = await render_hipaa_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert bundle.violations == []
    assert bundle.compliance_score == 100.0
    assert bundle.extras["archive_file_count"] == 1
    assert bundle.extras["in_scope_file_count"] == 1


async def test_hipaa_in_scope_filtering(session_factory):
    """GDPR-tagged policies and files don't appear in the HIPAA scope."""
    policy_set = PolicySet(
        policies=[_hipaa_compliant_policy(), _gdpr_policy()]
    )

    repo = CatalogRepo(session_factory)
    base = datetime(2026, 1, 1)
    await repo.add_file(
        source="health-svc",
        segment_path="/tiers/archive/h-1.jsonl",
        tier="archive",
        size_bytes=100,
        oldest_record_ts=base,
        newest_record_ts=base,
        compliance_tag="hipaa",
        immutable=True,
    )
    await repo.add_file(
        source="user-svc",
        segment_path="/tiers/hot/u-1.jsonl",
        tier="hot",
        size_bytes=100,
        oldest_record_ts=base,
        newest_record_ts=base,
        compliance_tag="gdpr",
    )

    bundle = await render_hipaa_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )

    assert len(bundle.policies_in_scope) == 1
    assert bundle.policies_in_scope[0]["compliance_tag"] == "hipaa"
    assert len(bundle.files_in_scope) == 1
    assert bundle.files_in_scope[0].compliance_tag == "hipaa"
    assert bundle.violations == []
    assert bundle.compliance_score == 100.0
