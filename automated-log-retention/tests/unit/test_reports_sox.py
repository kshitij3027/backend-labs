"""Unit tests for ``render_sox_report`` in ``src/compliance/reports.py``.

Builds synthetic ``PolicySet`` instances directly and seeds the
per-test SQLite engine with ``File`` rows through ``CatalogRepo``.
The SOX rules in C14 enforce both policy-level immutability and
per-file immutability — the tests below cover each surface in
isolation and then together.

Note: ``PolicySet`` is built directly via Pydantic constructors,
bypassing the boot-time validator. That's intentional — the report
renderer is the audit-time view of the same property and must report
the violation even if a mutable SOX policy somehow slipped past
startup (e.g., direct config tampering).
"""
from __future__ import annotations

from datetime import datetime

from src.compliance.reports import render_sox_report
from src.policy.schema import Phase, Policy, PolicySet, Selector
from src.storage.catalog import CatalogRepo


def _sox_compliant_policy(name: str = "payment_sox") -> Policy:
    """Build a SOX policy with immutable=True and no early delete."""
    return Policy(
        name=name,
        selector=Selector(category="payment"),
        priority=1000,
        compliance_tag="sox",
        immutable=True,
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(after_days=30, action="promote", target_tier="warm"),
            Phase(
                after_days=90,
                action="compress",
                target_tier="cold",
                compression_level=3,
            ),
            Phase(
                after_days=365,
                action="archive",
                target_tier="archive",
                compression_level=19,
            ),
            # No delete phase — SOX is 7 yr, we keep indefinitely.
        ],
    )


def _sox_mutable_policy() -> Policy:
    """A SOX policy with immutable=False (violation)."""
    return Policy(
        name="sox_mutable",
        selector=Selector(category="payment"),
        priority=10,
        compliance_tag="sox",
        immutable=False,
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(
                after_days=365,
                action="archive",
                target_tier="archive",
                compression_level=19,
            ),
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


async def test_sox_mutable_policy_flagged(session_factory):
    """A SOX policy with immutable=False is reported as a violation."""
    policy_set = PolicySet(policies=[_sox_mutable_policy()])
    bundle = await render_sox_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert any(
        "sox_mutable" in v and "not immutable" in v.lower()
        for v in bundle.violations
    ), bundle.violations
    assert bundle.compliance_score < 100.0


async def test_sox_mutable_file_flagged(session_factory):
    """A SOX-tagged file with immutable=False is reported as a violation."""
    policy_set = PolicySet(policies=[_sox_compliant_policy()])

    repo = CatalogRepo(session_factory)
    base = datetime(2026, 1, 1)
    await repo.add_file(
        source="payment-svc",
        segment_path="/tiers/archive/pay-mutable.jsonl",
        tier="archive",
        size_bytes=1024,
        oldest_record_ts=base,
        newest_record_ts=base,
        compliance_tag="sox",
        immutable=False,  # violation
    )

    bundle = await render_sox_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )

    assert any(
        "pay-mutable.jsonl" in v and "not marked immutable" in v
        for v in bundle.violations
    ), bundle.violations
    assert bundle.compliance_score < 100.0


async def test_sox_compliant_no_violations(session_factory):
    """A compliant SOX policy + immutable files yield zero violations."""
    policy_set = PolicySet(policies=[_sox_compliant_policy()])

    repo = CatalogRepo(session_factory)
    base = datetime(2026, 1, 1)
    await repo.add_file(
        source="payment-svc",
        segment_path="/tiers/archive/pay-ok.jsonl",
        tier="archive",
        size_bytes=1024,
        oldest_record_ts=base,
        newest_record_ts=base,
        compliance_tag="sox",
        immutable=True,
    )

    bundle = await render_sox_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert bundle.violations == []
    assert bundle.compliance_score == 100.0
    assert len(bundle.policies_in_scope) == 1
    assert len(bundle.files_in_scope) == 1
    assert bundle.files_in_scope[0].immutable is True


async def test_sox_in_scope_filtering(session_factory):
    """GDPR-tagged policies and files are filtered out of the SOX report."""
    policy_set = PolicySet(
        policies=[_sox_compliant_policy(), _gdpr_policy()]
    )

    repo = CatalogRepo(session_factory)
    base = datetime(2026, 1, 1)
    await repo.add_file(
        source="payment-svc",
        segment_path="/tiers/archive/pay-1.jsonl",
        tier="archive",
        size_bytes=100,
        oldest_record_ts=base,
        newest_record_ts=base,
        compliance_tag="sox",
        immutable=True,
    )
    await repo.add_file(
        source="user-svc",
        segment_path="/tiers/hot/user-1.jsonl",
        tier="hot",
        size_bytes=100,
        oldest_record_ts=base,
        newest_record_ts=base,
        compliance_tag="gdpr",
    )

    bundle = await render_sox_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )

    assert len(bundle.policies_in_scope) == 1
    assert bundle.policies_in_scope[0]["compliance_tag"] == "sox"
    assert len(bundle.files_in_scope) == 1
    assert bundle.files_in_scope[0].compliance_tag == "sox"
    assert bundle.violations == []
    assert bundle.compliance_score == 100.0


async def test_sox_short_delete_flagged(session_factory):
    """A SOX delete firing before 2555 d is reported as a violation."""
    policy_set = PolicySet(
        policies=[
            Policy(
                name="sox_short_delete",
                selector=Selector(category="payment"),
                priority=10,
                compliance_tag="sox",
                immutable=True,
                phases=[
                    Phase(after_days=0, action="promote", target_tier="hot"),
                    # SOX min is 2555 d; 30 d is clearly under
                    Phase(after_days=30, action="delete"),
                ],
            )
        ]
    )
    bundle = await render_sox_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert any(
        "sox_short_delete" in v and "30d" in v and "2555" in v
        for v in bundle.violations
    ), bundle.violations
    assert bundle.compliance_score < 100.0


async def test_sox_empty_scope_returns_perfect_score(session_factory):
    """No SOX policies / no SOX files => 100.0 vacuously compliant."""
    policy_set = PolicySet(policies=[_gdpr_policy()])
    bundle = await render_sox_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert bundle.policies_in_scope == []
    assert bundle.files_in_scope == []
    assert bundle.violations == []
    assert bundle.compliance_score == 100.0
