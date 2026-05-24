"""Unit tests for ``src.compliance.validator.validate_policy_set``.

Builds ``PolicySet`` instances directly via Pydantic constructors so
tests don't depend on YAML loading. Covers all five framework slugs,
the aggregation requirement (no fail-fast), and the empty-set + no-tag
short-circuit paths. The final test re-uses the demo config file to
confirm the shipping defaults pass cleanly.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from src.compliance.rules import MIN_RETENTION_DAYS, REQUIRES_IMMUTABLE
from src.compliance.validator import (
    ComplianceValidationError,
    validate_policy_set,
)
from src.policy.loader import load_policy_set
from src.policy.schema import Phase, Policy, PolicySet, Selector

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_YAML = REPO_ROOT / "config" / "retention_config.yaml"


def _make_policy(
    name: str,
    *,
    compliance_tag: str | None,
    immutable: bool,
    phases: list[Phase],
    selector: Selector | None = None,
) -> Policy:
    """Test-only constructor — fills in selector/priority defaults."""
    return Policy(
        name=name,
        selector=selector if selector is not None else Selector(),
        priority=0,
        compliance_tag=compliance_tag,
        immutable=immutable,
        phases=phases,
    )


def test_no_compliance_tag_passes() -> None:
    """Untagged debug policy deleting after 7 d is allowed."""
    policy = _make_policy(
        "debug",
        compliance_tag=None,
        immutable=False,
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(after_days=7, action="delete"),
        ],
    )

    # Should not raise.
    validate_policy_set(PolicySet(policies=[policy]))


def test_sox_delete_under_minimum_raises() -> None:
    """SOX policy deleting before 2555 d is rejected."""
    policy = _make_policy(
        "sox_short",
        compliance_tag="sox",
        immutable=True,
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(after_days=30, action="delete"),
        ],
    )

    with pytest.raises(ComplianceValidationError) as exc_info:
        validate_policy_set(PolicySet(policies=[policy]))

    assert len(exc_info.value.violations) == 1
    msg = exc_info.value.violations[0]
    assert "SOX" in msg
    assert "2555" in msg
    assert "sox_short" in msg


def test_sox_without_immutable_raises() -> None:
    """SOX policy with ``immutable=False`` is rejected."""
    policy = _make_policy(
        "sox_mutable",
        compliance_tag="sox",
        immutable=False,
        # No delete phase → no retention-window violation, so the
        # only finding should be the immutability one.
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

    with pytest.raises(ComplianceValidationError) as exc_info:
        validate_policy_set(PolicySet(policies=[policy]))

    assert len(exc_info.value.violations) == 1
    msg = exc_info.value.violations[0]
    assert "SOX" in msg
    assert "immutable" in msg


def test_pci_without_immutable_raises() -> None:
    """PCI DSS policy with ``immutable=False`` is rejected."""
    policy = _make_policy(
        "pci_mutable",
        compliance_tag="pci_dss",
        immutable=False,
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(after_days=365, action="delete"),
        ],
    )

    with pytest.raises(ComplianceValidationError) as exc_info:
        validate_policy_set(PolicySet(policies=[policy]))

    assert len(exc_info.value.violations) == 1
    msg = exc_info.value.violations[0]
    assert "PCI_DSS" in msg
    assert "immutable" in msg


def test_gdpr_mutable_is_allowed() -> None:
    """GDPR does not require immutable=True (hash chain is the proof)."""
    assert REQUIRES_IMMUTABLE["gdpr"] is False

    policy = _make_policy(
        "gdpr_mutable",
        compliance_tag="gdpr",
        immutable=False,
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(after_days=1095, action="delete"),
        ],
    )

    # Should not raise.
    validate_policy_set(PolicySet(policies=[policy]))


def test_aggregates_all_violations() -> None:
    """Two bad policies produce >= 2 distinct violation strings."""
    sox_bad = _make_policy(
        "sox_short",
        compliance_tag="sox",
        immutable=True,  # immutability OK
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(after_days=30, action="delete"),  # < 2555 d
        ],
    )
    pci_bad = _make_policy(
        "pci_mutable",
        compliance_tag="pci_dss",
        immutable=False,  # NOT immutable
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(after_days=365, action="delete"),  # retention OK
        ],
    )

    with pytest.raises(ComplianceValidationError) as exc_info:
        validate_policy_set(PolicySet(policies=[sox_bad, pci_bad]))

    violations = exc_info.value.violations
    assert len(violations) == 2

    joined = " | ".join(violations)
    assert "sox_short" in joined
    assert "pci_mutable" in joined
    assert "2555" in joined  # SOX retention message
    assert "immutable" in joined  # PCI immutability message


def test_aggregates_multiple_violations_per_policy() -> None:
    """A single policy violating both rules produces two entries."""
    really_bad = _make_policy(
        "all_wrong",
        compliance_tag="hipaa",
        immutable=False,  # HIPAA wants immutable=True
        phases=[
            Phase(after_days=0, action="promote", target_tier="hot"),
            Phase(after_days=30, action="delete"),  # HIPAA wants 2190 d
        ],
    )

    with pytest.raises(ComplianceValidationError) as exc_info:
        validate_policy_set(PolicySet(policies=[really_bad]))

    assert len(exc_info.value.violations) == 2


def test_empty_policy_set_passes() -> None:
    """An empty PolicySet is trivially compliant."""
    validate_policy_set(PolicySet(policies=[]))


def test_passing_config_yaml_full() -> None:
    """The shipped demo config passes the validator end-to-end."""
    # load_policy_set already runs the validator; if it returns, we're good.
    result = load_policy_set(CONFIG_YAML)

    # Belt-and-braces: re-run the validator directly on the loaded set.
    validate_policy_set(result)

    # Sanity: every framework slug we ship a policy for is also a key
    # in both constants dicts (catches typos in either side).
    tagged = [p for p in result.policies if p.compliance_tag is not None]
    for policy in tagged:
        assert policy.compliance_tag in MIN_RETENTION_DAYS
        assert policy.compliance_tag in REQUIRES_IMMUTABLE


def test_violations_attribute_is_a_list_copy() -> None:
    """ComplianceValidationError stores its violations as a list."""
    err = ComplianceValidationError(["a", "b"])
    assert err.violations == ["a", "b"]
    # Mutating the attribute must not mutate the caller's source list.
    src = ["x", "y"]
    err2 = ComplianceValidationError(src)
    src.append("z")
    assert err2.violations == ["x", "y"]
