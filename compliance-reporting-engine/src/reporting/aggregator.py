"""Report payload aggregator.

The aggregator is the seam between the persistence layer (log events
sitting in Postgres) and the rest of the pipeline (exporters, signer,
file writer). It does exactly one thing: take a framework code +
period bounds, fetch the matching events, run the framework's
classification / summary / findings logic, and shape the result into
a single canonical dict.

The output schema is small and stable so every downstream consumer
can rely on the same keys:

    {
        "framework": "<code>",
        "period":    {"start": "<iso>", "end": "<iso>"},
        "summary":   {<category>: <count>, ...},
        "findings":  ["...", "..."],
        "data":      {"events": [<event_dict>, ...]},
    }

PCI-DSS's ``findings`` widens the base contract with an optional
``period_end`` kwarg so the "key rotation overdue" rule can pin "now"
deterministically. The aggregator opts into that wider signature when
available (``try / except TypeError``), which keeps the call site
LSP-compatible across rule classes that do and don't accept the kwarg.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession

from ..frameworks import FRAMEWORK_REGISTRY
from ..logs.repository import query_logs_for_framework_in_window


async def build_report_payload(
    session: AsyncSession,
    framework: str,
    period_start: datetime,
    period_end: datetime,
) -> dict:
    """Fetch events, classify per the framework's rules, return the structured payload.

    Args:
        session: An open async session.
        framework: Framework code (must exist in ``FRAMEWORK_REGISTRY``).
        period_start: Inclusive window start (tz-aware UTC).
        period_end: Inclusive window end (tz-aware UTC).

    Returns:
        Dict with the canonical report shape (see module docstring).

    Raises:
        ValueError: If ``framework`` isn't a registered framework code.
    """
    if framework not in FRAMEWORK_REGISTRY:
        raise ValueError(f"Unknown framework: {framework}")

    rules = FRAMEWORK_REGISTRY[framework]
    events = await query_logs_for_framework_in_window(
        session, framework, period_start, period_end
    )

    # PCI-DSS's findings accepts an optional ``period_end`` kwarg to pin
    # "now" for deterministic regenerations; other frameworks don't.
    # Try the wider call first, fall back to the base signature on
    # ``TypeError`` so this stays LSP-friendly.
    try:
        finding_strings = rules.findings(events, period_end=period_end)
    except TypeError:
        finding_strings = rules.findings(events)

    summary = rules.summarize(events)

    return {
        "framework": framework,
        "period": {
            "start": period_start.isoformat(),
            "end": period_end.isoformat(),
        },
        "summary": dict(summary),
        "findings": list(finding_strings),
        "data": {"events": [event.to_dict() for event in events]},
    }
