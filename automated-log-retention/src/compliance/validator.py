"""Compliance validator for loaded ``PolicySet`` instances.

Called by ``policy/loader.py`` immediately after Pydantic parsing
succeeds. Walks every policy with a ``compliance_tag`` and enforces
two per-framework rules sourced from ``compliance/rules.py``:

  1. **Minimum retention.** Any phase with ``action="delete"`` must
     fire on or after ``MIN_RETENTION_DAYS[tag]`` days. A SOX policy
     that deletes after 30 d violates SEC 17 CFR 210.2-06 (7 yr).
  2. **Immutable archive.** If ``REQUIRES_IMMUTABLE[tag]`` is True,
     the policy's ``immutable`` flag must also be True. A PCI policy
     marked ``immutable=False`` would let an operator (or attacker)
     rewrite cardholder logs in-place.

The validator **aggregates** all violations across all policies into a
single ``ComplianceValidationError`` rather than failing fast — auditors
asked for "the full list of findings", not the first one. Empty
policy sets pass trivially (the service can boot with no policies and
have them loaded later).
"""
from __future__ import annotations

from src.compliance.rules import MIN_RETENTION_DAYS, REQUIRES_IMMUTABLE
from src.policy.schema import PolicySet


class ComplianceValidationError(ValueError):
    """Raised when a ``PolicySet`` violates one or more framework rules.

    ``violations`` exposes the full list of human-readable strings so
    callers can render them in startup logs, dashboards, or audit
    reports without re-parsing the exception message.
    """

    def __init__(self, violations: list[str]) -> None:
        self.violations: list[str] = list(violations)
        super().__init__("; ".join(violations) or "compliance violations")


def validate_policy_set(policy_set: PolicySet) -> None:
    """Validate every tagged policy against its framework's rules.

    Iterates all policies, collecting every violation it finds. Raises
    a single ``ComplianceValidationError`` carrying the full list iff
    the list is non-empty; otherwise returns ``None``. Policies with
    ``compliance_tag is None`` are skipped — debug/ops policies are
    allowed to delete after 7 d and stay mutable.
    """
    violations: list[str] = []

    for policy in policy_set.policies:
        tag = policy.compliance_tag
        if tag is None:
            continue

        min_days = MIN_RETENTION_DAYS[tag]
        for phase in policy.phases:
            if phase.action == "delete" and phase.after_days < min_days:
                violations.append(
                    f"policy '{policy.name}': delete fires at "
                    f"{phase.after_days}d but {tag.upper()} requires "
                    f"≥ {min_days}d"
                )

        if REQUIRES_IMMUTABLE[tag] and not policy.immutable:
            violations.append(
                f"policy '{policy.name}': {tag.upper()} requires "
                f"immutable=True but policy is mutable"
            )

    if violations:
        raise ComplianceValidationError(violations)
