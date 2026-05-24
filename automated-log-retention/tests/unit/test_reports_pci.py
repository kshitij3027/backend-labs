"""Unit tests for ``render_pci_dss_report`` in ``src/compliance/reports.py``.

Builds synthetic ``PolicySet`` instances directly via Pydantic and seeds
the per-test SQLite engine with ``File`` rows through ``CatalogRepo``.
The PCI DSS rules in C15 enforce three things on the in-scope surface:

  * policy must be ``immutable=True`` (Req. 10.5.2)
  * every in-scope file must be ``immutable=True`` (Req. 10.5.2)
  * any ``delete`` phase must fire at >= 365 d (Req. 10.5.1)

Unlike GDPR's right-to-erasure stance, PCI explicitly *allows*
indefinite retention — kept-forever is not a violation. The
``test_pci_compliant_policy_no_violations`` case pins that contract by
constructing a policy with a 365 d delete that lands exactly on the
floor.

Note: ``PolicySet`` is built directly via Pydantic constructors,
bypassing the boot-time validator. That's intentional — the report
renderer is the audit-time view of the same property and must report
the violation even if a mutable PCI policy somehow slipped past
startup (e.g., direct config tampering).
"""
from __future__ import annotations

from datetime import datetime

from src.compliance.reports import render_pci_dss_report
from src.policy.schema import Phase, Policy, PolicySet, Selector
from src.storage.catalog import CatalogRepo


def _pci_compliant_policy(name: str = "card_pci") -> Policy:
    """Build a PCI policy with immutable=True and delete >= 365 d."""
    return Policy(
        name=name,
        selector=Selector(category="cardholder"),
        priority=1000,
        compliance_tag="pci_dss",
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
                after_days=180,
                action="archive",
                target_tier="archive",
                compression_level=19,
            ),
            # PCI floor is 365 d; landing exactly on the floor must NOT
            # violate the rule.
            Phase(after_days=365, action="delete"),
        ],
    )


def _pci_mutable_policy() -> Policy:
    """A PCI policy with immutable=False (violation)."""
    return Policy(
        name="pci_mutable",
        selector=Selector(category="cardholder"),
        priority=10,
        compliance_tag="pci_dss",
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


def _pci_short_delete_policy() -> Policy:
    """A PCI policy whose delete fires too early (30 d)."""
    return Policy(
        name="pci_short_delete",
        selector=Selector(category="cardholder"),
        priority=10,
        compliance_tag="pci_dss",
        immutable=True,
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            # PCI min is 365 d; 30 d is clearly under
            Phase(after_days=30, action="delete"),
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


async def test_pci_compliant_policy_no_violations(session_factory):
    """Compliant PCI policy + immutable file => zero violations, score 100.0."""
    policy_set = PolicySet(policies=[_pci_compliant_policy()])

    repo = CatalogRepo(session_factory)
    base = datetime(2026, 1, 1)
    await repo.add_file(
        source="card-svc",
        segment_path="/tiers/archive/card-ok.jsonl",
        tier="archive",
        size_bytes=1024,
        oldest_record_ts=base,
        newest_record_ts=base,
        compliance_tag="pci_dss",
        immutable=True,
    )

    bundle = await render_pci_dss_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert bundle.framework == "pci_dss"
    assert bundle.violations == []
    assert bundle.compliance_score == 100.0
    assert len(bundle.policies_in_scope) == 1
    assert len(bundle.files_in_scope) == 1
    assert bundle.files_in_scope[0].immutable is True


async def test_pci_mutable_policy_flagged(session_factory):
    """A PCI policy with immutable=False is reported as a violation."""
    policy_set = PolicySet(policies=[_pci_mutable_policy()])
    bundle = await render_pci_dss_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert any(
        "pci_mutable" in v and "not immutable" in v.lower()
        for v in bundle.violations
    ), bundle.violations
    assert bundle.compliance_score < 100.0


async def test_pci_mutable_file_flagged(session_factory):
    """A PCI-tagged file with immutable=False is reported as a violation."""
    policy_set = PolicySet(policies=[_pci_compliant_policy()])

    repo = CatalogRepo(session_factory)
    base = datetime(2026, 1, 1)
    await repo.add_file(
        source="card-svc",
        segment_path="/tiers/archive/card-mutable.jsonl",
        tier="archive",
        size_bytes=1024,
        oldest_record_ts=base,
        newest_record_ts=base,
        compliance_tag="pci_dss",
        immutable=False,  # violation
    )

    bundle = await render_pci_dss_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert any(
        "card-mutable.jsonl" in v and "not marked immutable" in v
        for v in bundle.violations
    ), bundle.violations
    assert bundle.compliance_score < 100.0


async def test_pci_short_delete_flagged(session_factory):
    """A PCI delete firing before 365 d is reported as a violation."""
    policy_set = PolicySet(policies=[_pci_short_delete_policy()])
    bundle = await render_pci_dss_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert any(
        "pci_short_delete" in v and "30d" in v and "365" in v
        for v in bundle.violations
    ), bundle.violations
    assert bundle.compliance_score < 100.0


async def test_pci_extras_cardholder_count(session_factory):
    """``extras['cardholder_data_segments']`` reports the in-scope file count."""
    policy_set = PolicySet(policies=[_pci_compliant_policy()])

    repo = CatalogRepo(session_factory)
    base = datetime(2026, 1, 1)
    for i in range(3):
        await repo.add_file(
            source="card-svc",
            segment_path=f"/tiers/archive/card-{i}.jsonl",
            tier="archive",
            size_bytes=1024,
            oldest_record_ts=base,
            newest_record_ts=base,
            compliance_tag="pci_dss",
            immutable=True,
        )

    bundle = await render_pci_dss_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert bundle.extras["cardholder_data_segments"] == 3


async def test_pci_in_scope_filtering(session_factory):
    """GDPR-tagged policies are filtered out of the PCI report."""
    policy_set = PolicySet(
        policies=[_pci_compliant_policy(), _gdpr_policy()]
    )

    bundle = await render_pci_dss_report(
        session_factory, policy_set, *_DEFAULT_WINDOW
    )
    assert len(bundle.policies_in_scope) == 1
    assert bundle.policies_in_scope[0]["compliance_tag"] == "pci_dss"
