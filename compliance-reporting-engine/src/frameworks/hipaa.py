"""HIPAA (Health Insurance Portability and Accountability Act) framework rules.

HIPAA evidence for the reporting engine breaks into five categories:

  * ``phi_access``         — protected health information read / view / export
  * ``auth_failures``      — failed authentication attempts against PHI systems
  * ``phi_modifications``  — creates / updates / deletes against PHI
  * ``breach_events``      — detected, reported, or contained breaches
  * ``user_audit``         — periodic user-activity snapshots / reviews

The ``findings`` rule emits three kinds of human-readable strings:

  1. If any ``phi_access`` events are recorded with ``outcome == "denied"``,
     surface the count — unauthorized PHI access is a HIPAA red flag and
     OCR investigators look at these first.
  2. If any ``breach_event`` rows exist in the window, surface the count
     and remind the operator that HIPAA's breach-notification workflow
     (60 days for affected individuals, sooner for HHS in larger breaches)
     must be triggered.
  3. If failed auth attempts exceed 50 in the window, surface the count
     as a hint that identity controls may be misconfigured or under attack.

The thresholds intentionally err toward sensitivity for the first two
rules (any single event is worth surfacing) and only the broad-volume
auth-failure check uses a numeric threshold to avoid noisy reports on
small test datasets.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from . import register_framework
from .base import FrameworkRules

if TYPE_CHECKING:
    from src.persistence.models import LogEvent


@register_framework("HIPAA")
class HIPAARules(FrameworkRules):
    """Concrete ``FrameworkRules`` for HIPAA.

    Categories track the five buckets a HIPAA auditor expects to see in
    evidence: PHI-access activity, authentication failures, PHI
    modifications, breach events, and periodic user-activity audits.
    """

    name = "HIPAA"

    categories = [
        "phi_access",
        "auth_failures",
        "phi_modifications",
        "breach_events",
        "user_audit",
    ]

    event_type_to_category = {
        "phi_access": "phi_access",
        "auth_failure": "auth_failures",
        "phi_modification": "phi_modifications",
        "breach_event": "breach_events",
        "user_audit": "user_audit",
    }

    @classmethod
    def findings(cls, events: list["LogEvent"]) -> list[str]:
        """Emit HIPAA-specific human-readable findings.

        Three rules:
          * any ``phi_access`` with ``outcome=denied`` -> surface the count
          * any ``breach_event`` rows -> surface the count + remind about
            the notification workflow
          * ``auth_failure`` count > 50 -> surface the count as a hint
            to review identity controls
        """
        results: list[str] = []

        unauthorized_phi_access_count = sum(
            1
            for event in events
            if event.event_type == "phi_access" and event.outcome == "denied"
        )
        if unauthorized_phi_access_count > 0:
            results.append(
                f"{unauthorized_phi_access_count} unauthorized PHI access events (outcome=denied)"
            )

        breach_event_count = sum(
            1 for event in events if event.event_type == "breach_event"
        )
        if breach_event_count > 0:
            results.append(
                f"Breach events detected ({breach_event_count}) — notification workflow required"
            )

        auth_failure_count = sum(
            1 for event in events if event.event_type == "auth_failure"
        )
        if auth_failure_count > 50:
            results.append(
                f"High auth failure volume ({auth_failure_count}) — review identity controls"
            )

        return results
