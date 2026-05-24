"""Unit tests for ``render_gdpr_report`` in ``src/compliance/reports.py``.

Builds synthetic ``PolicySet`` instances directly via Pydantic and
seeds the per-test SQLite engine with ``File`` rows through
``CatalogRepo``. The audit chain genesis row is inserted by
``init_db`` in the shared fixture, but no further audit entries are
appended — the GDPR rules in C14 don't depend on the audit walk; they
fault on policy shape alone.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from src.compliance.reports import render_gdpr_report
from src.policy.schema import Phase, Policy, PolicySet, Selector
from src.storage.catalog import CatalogRepo


def _gdpr_valid_policy() -> Policy:
    """Build a GDPR policy whose delete fires past the 1095 d floor."""
    return Policy(
        name="user_activity_gdpr",
        selector=Selector(category="user_activity"),
        priority=100,
        compliance_tag="gdpr",
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
            Phase(
                after_days=365,
                action="archive",
                target_tier="archive",
                compression_level=19,
            ),
            Phase(after_days=1095, action="delete"),
        ],
    )


def _gdpr_short_delete_policy() -> Policy:
    """A GDPR policy whose delete fires too early (30 d)."""
    return Policy(
        name="gdpr_short",
        selector=Selector(category="user_activity"),
        priority=10,
        compliance_tag="gdpr",
        immutable=False,
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(after_days=30, action="delete"),
        ],
    )


def _gdpr_no_delete_policy() -> Policy:
    """A GDPR policy with no delete phase at all."""
    return Policy(
        name="gdpr_keep_forever",
        selector=Selector(category="user_activity"),
        priority=10,
        compliance_tag="gdpr",
        immutable=False,
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(after_days=30, action="promote", target_tier="warm"),
        ],
    )


def _sox_policy() -> Policy:
    """A SOX policy used to verify the framework filter excludes other tags."""
    return Policy(
        name="payment_sox",
        selector=Selector(category="payment"),
        priority=1000,
        compliance_tag="sox",
        immutable=True,
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


_DEFAULT_WINDOW = (
    datetime(2026, 1, 1, 0, 0, 0),
    datetime(2026, 12, 31, 23, 59, 59),
)


async def test_gdpr_valid_policy_no_violations(session_factory):
    """A compliant GDPR policy yields zero violations and score 100.0."""
    policy_set = PolicySet(policies=[_gdpr_valid_policy()])
    bundle = await render_gdpr_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert bundle.framework == "gdpr"
    assert bundle.violations == []
    assert bundle.compliance_score == 100.0
    assert len(bundle.policies_in_scope) == 1
    assert bundle.policies_in_scope[0]["name"] == "user_activity_gdpr"


async def test_gdpr_short_delete_flagged(session_factory):
    """A GDPR delete firing at 30 d is reported as a violation."""
    policy_set = PolicySet(policies=[_gdpr_short_delete_policy()])
    bundle = await render_gdpr_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert len(bundle.violations) == 1
    msg = bundle.violations[0]
    assert "GDPR" in msg
    assert "gdpr_short" in msg
    assert "30d" in msg
    assert "1095" in msg
    # One policy, one failing => score is 0.0
    assert bundle.compliance_score == 0.0


async def test_gdpr_no_delete_phase_flagged(session_factory):
    """A GDPR policy without a delete phase is reported as a violation."""
    policy_set = PolicySet(policies=[_gdpr_no_delete_policy()])
    bundle = await render_gdpr_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert len(bundle.violations) == 1
    msg = bundle.violations[0]
    assert "GDPR" in msg
    assert "gdpr_keep_forever" in msg
    assert "no delete phase" in msg.lower()
    assert bundle.compliance_score == 0.0


async def test_gdpr_in_scope_filtering_excludes_other_tags(session_factory):
    """SOX/HIPAA-tagged policies and files don't appear in the GDPR scope."""
    policy_set = PolicySet(policies=[_gdpr_valid_policy(), _sox_policy()])

    repo = CatalogRepo(session_factory)
    base = datetime(2026, 1, 1)
    await repo.add_file(
        source="user-svc",
        segment_path="/tiers/hot/user-1.jsonl",
        tier="hot",
        size_bytes=100,
        oldest_record_ts=base,
        newest_record_ts=base,
        compliance_tag="gdpr",
    )
    await repo.add_file(
        source="payments-svc",
        segment_path="/tiers/archive/pay-1.jsonl",
        tier="archive",
        size_bytes=200,
        oldest_record_ts=base,
        newest_record_ts=base,
        compliance_tag="sox",
        immutable=True,
    )

    bundle = await render_gdpr_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )

    # Only the GDPR policy is in scope.
    assert len(bundle.policies_in_scope) == 1
    assert bundle.policies_in_scope[0]["compliance_tag"] == "gdpr"

    # Only the GDPR-tagged file is in scope.
    assert len(bundle.files_in_scope) == 1
    assert bundle.files_in_scope[0].compliance_tag == "gdpr"
    assert bundle.files_in_scope[0].source == "user-svc"

    # No violations because the in-scope policy is valid and the SOX
    # policy/file are filtered out.
    assert bundle.violations == []
    assert bundle.compliance_score == 100.0
    assert bundle.extras["in_scope_file_count"] == 1


async def test_gdpr_empty_scope_returns_perfect_score(session_factory):
    """A policy set with no GDPR policies still returns 100.0."""
    policy_set = PolicySet(policies=[_sox_policy()])
    bundle = await render_gdpr_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert bundle.policies_in_scope == []
    assert bundle.files_in_scope == []
    assert bundle.violations == []
    assert bundle.compliance_score == 100.0


async def test_gdpr_time_range_propagates_to_bundle(session_factory):
    """The requested time window is echoed back as ISO strings."""
    policy_set = PolicySet(policies=[_gdpr_valid_policy()])
    time_from = datetime(2026, 1, 15, 0, 0, 0)
    time_to = datetime(2026, 6, 30, 23, 59, 59)
    bundle = await render_gdpr_report(
        session_factory, policy_set, time_from, time_to
    )
    assert bundle.time_range == {
        "from": time_from.isoformat(),
        "to": time_to.isoformat(),
    }
