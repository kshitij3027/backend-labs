"""JSON exporter — pretty-printed UTF-8 JSON of the aggregator payload.

The aggregator payload is already a plain ``dict`` of primitive
values, lists, and nested dicts, so this exporter is essentially a
thin ``json.dumps`` wrapper. Two non-default knobs matter:

  * ``indent=2`` — keeps the file human-readable so an auditor can eyeball
    the JSON without a formatter.
  * ``default=str`` — catches the occasional non-JSON-native value
    (``datetime``, ``UUID``, ``Decimal``) that might slip through if a
    framework's findings logic returns one. The aggregator already
    string-ifies most of these via ``LogEvent.to_dict()``, but
    ``default=str`` is cheap insurance.

Key ordering is left as-inserted (``sort_keys=False``) so the file
mirrors the canonical payload shape documented in
:mod:`src.reporting.aggregator`.
"""
from __future__ import annotations

import json

from . import register_exporter


@register_exporter("JSON")
def export_json(payload: dict) -> bytes:
    """Serialize the aggregator payload as pretty-printed UTF-8 JSON.

    Args:
        payload: Canonical aggregator payload (``framework``, ``period``,
            ``summary``, ``findings``, ``data``).

    Returns:
        UTF-8-encoded bytes ready to be written to disk or returned in
        an HTTP response.
    """
    return json.dumps(payload, indent=2, default=str, sort_keys=False).encode("utf-8")
