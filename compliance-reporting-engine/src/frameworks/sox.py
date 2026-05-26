"""SOX (Sarbanes-Oxley) framework rules.

SOX evidence for the reporting engine breaks into five categories:

  * ``admin_access``         — privileged login activity
  * ``financial_transactions`` — ledger / billing posts, reversals
  * ``system_changes``       — config / infra mutations
  * ``approval_workflows``   — review-and-approve events
  * ``sod_violations``       — segregation-of-duty breaches (e.g. self-approval)

The ``findings`` rule emits three kinds of human-readable strings:

  1. If any segregation-of-duty violations exist, surface the count.
  2. If admin logins fail in the window, surface how many — failed
     privileged auth is a SOX red flag.
  3. If the number of system changes exceeds the number of approval
     workflows recorded against them, surface the delta — a basic
     "every change must have an approval" sanity check.

These thresholds intentionally err toward sensitivity: even a single
SoD violation or failed admin login is worth surfacing in a SOX report.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from . import register_framework
from .base import FrameworkRules

if TYPE_CHECKING:
    from src.persistence.models import LogEvent


@register_framework("SOX")
class SOXRules(FrameworkRules):
    """Concrete ``FrameworkRules`` for SOX (Sarbanes-Oxley).

    Categories track the five buckets a SOX auditor expects to see in
    evidence: privileged-access activity, financial postings, system
    mutations, change-approval records, and SoD violations.
    """

    name = "SOX"

    categories = [
        "admin_access",
        "financial_transactions",
        "system_changes",
        "approval_workflows",
        "sod_violations",
    ]

    event_type_to_category = {
        "admin_login": "admin_access",
        "financial_transaction": "financial_transactions",
        "system_config_change": "system_changes",
        "approval_workflow": "approval_workflows",
        "sod_violation": "sod_violations",
    }

    @classmethod
    def findings(cls, events: list["LogEvent"]) -> list[str]:
        """Emit SOX-specific human-readable findings.

        Three rules:
          * any SoD violations -> surface the count
          * any failed admin logins -> surface the count
          * system_config_change count > approval_workflow count -> surface the delta
        """
        results: list[str] = []

        sod_violation_count = sum(
            1 for event in events if event.event_type == "sod_violation"
        )
        if sod_violation_count > 0:
            results.append(
                f"{sod_violation_count} SoD violations detected in period"
            )

        failed_admin_logins = sum(
            1
            for event in events
            if event.event_type == "admin_login" and event.outcome == "failure"
        )
        if failed_admin_logins > 0:
            results.append(
                f"{failed_admin_logins} admin access events with outcome=failure"
            )

        system_changes_count = sum(
            1 for event in events if event.event_type == "system_config_change"
        )
        approvals_count = sum(
            1 for event in events if event.event_type == "approval_workflow"
        )
        if system_changes_count > approvals_count:
            delta = system_changes_count - approvals_count
            results.append(
                f"{delta} system changes without an associated approval workflow"
            )

        return results
