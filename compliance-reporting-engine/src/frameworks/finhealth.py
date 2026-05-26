"""FinHealth — bonus composite framework for joined financial + clinical scope.

Combines a SOX-financial-controls subset (financial_transaction +
admin_login) with a HIPAA-PHI subset (phi_access + phi_modification).
Designed to surface "insider risk": actors who appear in both
financial and clinical event streams get flagged as composite_risk.

FinHealth is dual-signed: the report payload carries a primary HMAC
(SOX-scope key) plus a secondary HMAC (HIPAA-scope key), so two
independent compliance teams can attest separately. The coordinator
already handles the secondary signature when ``report.framework ==
"FINHEALTH"``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from . import register_framework
from .base import FrameworkRules

if TYPE_CHECKING:
    from src.persistence.models import LogEvent


# Event-type sets pulled from SOX's financial-controls subset and HIPAA's
# PHI subset. Declared at module scope so the classifier, summariser, and
# findings rule all reference the same canonical sets.
_SOX_EVENT_TYPES: frozenset[str] = frozenset(
    {"financial_transaction", "admin_login"}
)
_HIPAA_EVENT_TYPES: frozenset[str] = frozenset(
    {"phi_access", "phi_modification"}
)


@register_framework("FINHEALTH")
class FinHealthRules(FrameworkRules):
    """Concrete ``FrameworkRules`` for the FinHealth composite framework.

    Composites a SOX-financial-controls subset (``financial_transaction``,
    ``admin_login``) with a HIPAA-PHI subset (``phi_access``,
    ``phi_modification``). The ``composite_risk`` category fires when an
    actor straddles both event streams — a heuristic proxy for insider
    risk across financial + clinical scopes that wouldn't surface in
    either single-framework report.

    Reports are dual-signed in the coordinator: the primary HMAC plus a
    secondary HMAC under a HIPAA-scope key so two independent compliance
    teams can attest separately. The ``findings`` rule emits a meta line
    declaring the dual-sign attestation so the auditor reading the
    rendered report sees it in the report body, not just the signature
    columns on the row.
    """

    name = "FINHEALTH"

    categories = [
        "financial_transactions",
        "admin_access",
        "phi_access",
        "phi_modifications",
        "composite_risk",
    ]

    # SOX-financial subset + HIPAA-PHI subset. ``composite_risk`` is NOT
    # in the mapping because it isn't tied to a single event_type — it's
    # computed in ``summarize`` from the actor overlap between the two
    # sets.
    event_type_to_category = {
        # SOX financial controls
        "financial_transaction": "financial_transactions",
        "admin_login": "admin_access",
        # HIPAA PHI
        "phi_access": "phi_access",
        "phi_modification": "phi_modifications",
    }

    @classmethod
    def findings(cls, events: list["LogEvent"]) -> list[str]:
        """Emit composite_risk + dual-signature meta finding.

        Two rules:
          * Any actor appearing in BOTH the SOX-financial subset and
            the HIPAA-PHI subset -> emit a "Composite risk: N actor(s)..."
            finding. The set intersection is the cheapest signal we have
            for insider risk across financial + clinical scopes.
          * Always emit the dual-signature meta finding so the auditor
            reads it in the rendered report body, not just on the row.
        """
        findings: list[str] = []

        sox_actors = {
            event.actor for event in events if event.event_type in _SOX_EVENT_TYPES
        }
        hipaa_actors = {
            event.actor for event in events if event.event_type in _HIPAA_EVENT_TYPES
        }
        overlap = sox_actors & hipaa_actors

        if overlap:
            findings.append(
                f"Composite risk: {len(overlap)} actor(s) appear in both "
                f"SOX-financial and HIPAA-PHI events"
            )

        # Meta finding so the auditor sees the dual-sign attestation
        # reflected in the report body, not just the signature columns.
        findings.append(
            "DUAL-SIGNED REPORT: signed under SOX-scope key and HIPAA-scope key"
        )
        return findings

    @classmethod
    def summarize(cls, events: list["LogEvent"]) -> dict[str, int]:
        """Augment the base summary with a ``composite_risk`` count.

        The base implementation zero-fills every category from the
        ``event_type_to_category`` mapping; ``composite_risk`` isn't in
        that mapping so it stays at zero. We then overwrite it with the
        size of the SOX/HIPAA actor intersection — the same heuristic
        used by the ``findings`` rule.
        """
        base = super().summarize(events)
        sox_actors = {
            event.actor for event in events if event.event_type in _SOX_EVENT_TYPES
        }
        hipaa_actors = {
            event.actor for event in events if event.event_type in _HIPAA_EVENT_TYPES
        }
        base["composite_risk"] = len(sox_actors & hipaa_actors)
        return base
