"""``GET /frameworks`` — catalogue of supported compliance frameworks.

The endpoint reads :data:`src.frameworks.FRAMEWORK_REGISTRY` so any
framework that lands in a later commit (e.g. FinHealth in commit 17)
shows up automatically without changes here. Each entry carries the
framework's evidence categories and a short hardcoded description for
display on the dashboard's catalogue card.
"""
from __future__ import annotations

from fastapi import APIRouter

from ..frameworks import FRAMEWORK_REGISTRY
from .schemas import FrameworkInfo


router = APIRouter(tags=["frameworks"])


# Short hardcoded descriptions per framework code. Frameworks that
# aren't in the table fall back to a generic "<NAME> compliance rules"
# string, so new entries never crash the endpoint while their copy
# catches up.
_FRAMEWORK_DESCRIPTIONS: dict[str, str] = {
    "SOX": (
        "Sarbanes-Oxley: financial reporting controls — privileged "
        "access, financial transactions, system changes, approvals, "
        "and segregation-of-duty violations."
    ),
    "HIPAA": (
        "HIPAA: protected health information safeguards — PHI access, "
        "auth failures, modifications, breach events, and user audits."
    ),
    "PCI_DSS": (
        "PCI-DSS: cardholder data protection — cardholder access, "
        "payment processing, key rotation, failed auth, and config "
        "changes."
    ),
    "GDPR": (
        "GDPR: EU data protection — personal-data processing, consent "
        "records, data-subject requests, breach notifications, and "
        "cross-border transfers."
    ),
    "FINHEALTH": (
        "FinHealth (composite, dual-signed): SOX financial-controls "
        "subset plus HIPAA PHI-access subset for organisations that "
        "straddle both regimes."
    ),
}


@router.get("/frameworks", response_model=list[FrameworkInfo])
async def list_frameworks() -> list[FrameworkInfo]:
    """Return one :class:`FrameworkInfo` per registered framework.

    The order mirrors ``FRAMEWORK_REGISTRY``'s insertion order, which
    matches the order modules are imported from
    :mod:`src.frameworks.__init__`. Stable enough for the dashboard's
    sidebar without an explicit sort.
    """
    return [
        FrameworkInfo(
            name=name,
            categories=list(rules.categories),
            description=_FRAMEWORK_DESCRIPTIONS.get(
                name, f"{name} compliance rules"
            ),
        )
        for name, rules in FRAMEWORK_REGISTRY.items()
    ]
