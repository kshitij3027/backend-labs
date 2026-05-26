"""GDPR (General Data Protection Regulation) framework rules.

GDPR evidence for the reporting engine breaks into five categories:

  * ``personal_data_processing`` — process / store / transmit operations
    on personal data of EU/UK data subjects
  * ``consent_records``          — granted / withdrawn / renewed consent
    records from data subjects
  * ``dsr_requests``             — data-subject requests (access, erasure,
    portability, rectification under Articles 15-22)
  * ``breach_notifications``     — breach disclosure events (Article 33's
    72-hour notification window)
  * ``cross_border_transfers``   — transfers of personal data outside the
    EU/UK (require SCCs / adequacy decisions under Chapter V)

The ``findings`` rule emits four kinds of human-readable strings:

  1. A gauge of how many data-subject requests (DSRs) were processed in
     the window. Auditors track DSR throughput against the GDPR
     statutory response window (one month, extendable to three).
  2. If any breach notifications exist, surface the count and remind the
     operator that GDPR Article 33 requires disclosure within 72 hours.
  3. If any cross-border transfers exist, surface the count and remind
     the operator to confirm Standard Contractual Clauses (or another
     valid Chapter V mechanism) are in place.
  4. A consent heuristic: count the ``personal_data_processing`` events
     whose actor has no associated ``consent_record`` event in the same
     window — these are processing operations without a lawful-basis
     trail and warrant manual review under Article 6.

The consent heuristic intentionally uses presence-of-record-by-actor as
a coarse proxy; tightening it (e.g. matching on subject_email_hash from
the payload or comparing timestamps) is left to a future iteration.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from . import register_framework
from .base import FrameworkRules

if TYPE_CHECKING:
    from src.persistence.models import LogEvent


@register_framework("GDPR")
class GDPRRules(FrameworkRules):
    """Concrete ``FrameworkRules`` for GDPR.

    Categories track the five buckets a GDPR DPO expects to see in
    evidence: personal-data processing operations, consent records,
    data-subject requests, breach notifications, and cross-border
    transfers.
    """

    name = "GDPR"

    categories = [
        "personal_data_processing",
        "consent_records",
        "dsr_requests",
        "breach_notifications",
        "cross_border_transfers",
    ]

    event_type_to_category = {
        "personal_data_processing": "personal_data_processing",
        "consent_record": "consent_records",
        "dsr_request": "dsr_requests",
        "breach_notification": "breach_notifications",
        "cross_border_transfer": "cross_border_transfers",
    }

    @classmethod
    def findings(cls, events: list["LogEvent"]) -> list[str]:
        """Emit GDPR-specific human-readable findings.

        Four rules:
          * any ``dsr_request`` events -> surface the count
          * any ``breach_notification`` events -> surface the count and
            remind about the 72-hour disclosure timing (Article 33)
          * any ``cross_border_transfer`` events -> surface the count
            and remind about Standard Contractual Clauses (Chapter V)
          * ``personal_data_processing`` events whose actor has no
            associated ``consent_record`` event in the same window ->
            surface the count as a lawful-basis review hint
        """
        results: list[str] = []

        # --- Rule 1: data-subject request gauge. ---
        dsr_count = sum(
            1 for event in events if event.event_type == "dsr_request"
        )
        if dsr_count > 0:
            results.append(
                f"{dsr_count} data-subject requests (DSRs) processed"
            )

        # --- Rule 2: breach-notification timing reminder. ---
        breach_notification_count = sum(
            1 for event in events if event.event_type == "breach_notification"
        )
        if breach_notification_count > 0:
            results.append(
                f"Breach notifications: {breach_notification_count}; "
                f"verify 72-hour disclosure timing"
            )

        # --- Rule 3: cross-border transfer SCC reminder. ---
        cross_border_count = sum(
            1 for event in events if event.event_type == "cross_border_transfer"
        )
        if cross_border_count > 0:
            results.append(
                f"{cross_border_count} cross-border transfers — "
                f"confirm SCCs in place"
            )

        # --- Rule 4: processing-without-consent heuristic by actor. ---
        # Coarse proxy: an actor with at least one ``consent_record`` event
        # in the window is treated as having a consent trail; any
        # ``personal_data_processing`` event from an actor outside that set
        # warrants manual lawful-basis review.
        actors_with_consent = {
            event.actor
            for event in events
            if event.event_type == "consent_record"
        }
        processing_without_consent = sum(
            1
            for event in events
            if event.event_type == "personal_data_processing"
            and event.actor not in actors_with_consent
        )
        if processing_without_consent > 0:
            results.append(
                f"{processing_without_consent} processing events "
                f"without an associated consent record"
            )

        return results
