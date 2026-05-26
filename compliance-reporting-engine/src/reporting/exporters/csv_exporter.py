"""CSV exporter — pandas-driven sectioned CSV.

Single CSV file with three sections delimited by ``# SUMMARY``,
``# FINDINGS``, and ``# EVENTS`` header comments. Each section is a
self-contained block of CSV rows so an auditor can open the file
in any spreadsheet and ignore the sections they don't care about.

The events block flattens the canonical event dict — primitive
columns become CSV columns, the ``framework_tags`` list joins on
``|``, and the ``payload`` dict serializes as compact JSON so the
column stays auditable but doesn't explode into one column per key.
"""
from __future__ import annotations

import io
import json

import pandas as pd

from . import register_exporter


@register_exporter("CSV")
def export_csv(payload: dict) -> bytes:
    """Serialize the aggregator payload as a sectioned UTF-8 CSV.

    Args:
        payload: Canonical aggregator payload (``framework``, ``period``,
            ``summary``, ``findings``, ``data``).

    Returns:
        UTF-8-encoded CSV bytes with three sections demarcated by
        ``# SUMMARY``, ``# FINDINGS``, and ``# EVENTS`` comment headers.
    """
    buf = io.StringIO()

    # --- # SUMMARY ---
    buf.write("# SUMMARY\n")
    summary = payload.get("summary") or {}
    summary_df = pd.DataFrame([summary]) if summary else pd.DataFrame()
    summary_df.to_csv(buf, index=False)

    # --- # FINDINGS ---
    buf.write("\n# FINDINGS\n")
    findings = payload.get("findings") or []
    findings_df = (
        pd.DataFrame({"finding": findings}) if findings else pd.DataFrame({"finding": []})
    )
    findings_df.to_csv(buf, index=False)

    # --- # EVENTS ---
    buf.write("\n# EVENTS\n")
    events = ((payload.get("data") or {}).get("events")) or []
    if events:
        flat = []
        for e in events:
            row = {
                k: e.get(k)
                for k in (
                    "id",
                    "timestamp",
                    "event_type",
                    "actor",
                    "resource",
                    "action",
                    "outcome",
                    "sensitivity",
                )
            }
            row["framework_tags"] = "|".join(e.get("framework_tags") or [])
            row["payload"] = json.dumps(e.get("payload") or {}, default=str, sort_keys=True)
            flat.append(row)
        pd.DataFrame(flat).to_csv(buf, index=False)
    else:
        # Empty header still tells the auditor what columns to expect.
        pd.DataFrame(
            columns=[
                "id",
                "timestamp",
                "event_type",
                "actor",
                "resource",
                "action",
                "outcome",
                "sensitivity",
                "framework_tags",
                "payload",
            ]
        ).to_csv(buf, index=False)

    return buf.getvalue().encode("utf-8")
