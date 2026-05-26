"""Unit tests for the CSV exporter.

The CSV exporter is a slightly unusual beast because it bundles three
logical "tables" (summary, findings, events) into a single
spreadsheet-friendly file demarcated by ``# SECTION`` comment headers.
The tests below pin down the contract an auditor relies on:

  * the three section headers are present and in order;
  * the SUMMARY block carries every framework category as a column;
  * the EVENTS block flattens the right number of rows;
  * the ``framework_tags`` list is joined with ``|`` (so a single CSV
    column stays readable in a spreadsheet);
  * the per-event ``payload`` dict is JSON-serialised into a single
    column, keeping the file auditor-readable without blowing up to
    one-column-per-key;
  * the exporter self-registers under the ``"CSV"`` format code so the
    coordinator can look it up by export_format.
"""
from __future__ import annotations

import json

from src.reporting.exporters import EXPORTERS
from src.reporting.exporters.csv_exporter import export_csv


def _sample_payload() -> dict:
    """Three events, all five SOX categories represented in the summary, two findings."""
    return {
        "framework": "SOX",
        "period": {"start": "2026-04-25T00:00:00+00:00", "end": "2026-05-25T00:00:00+00:00"},
        "summary": {
            "admin_access": 5,
            "financial_transactions": 12,
            "system_changes": 3,
            "approval_workflows": 2,
            "sod_violations": 1,
        },
        "findings": [
            "1 SoD violations detected in period",
            "2 admin access events with outcome=failure",
        ],
        "data": {
            "events": [
                {
                    "id": "11111111-2222-3333-4444-555555555555",
                    "timestamp": "2026-05-01T12:00:00+00:00",
                    "framework_tags": ["SOX", "HIPAA"],
                    "event_type": "admin_login",
                    "actor": "alice@example.com",
                    "resource": "/admin/users",
                    "action": "login",
                    "outcome": "failure",
                    "sensitivity": "restricted",
                    "payload": {"ip": "10.0.0.1", "user_agent": "curl"},
                },
                {
                    "id": "66666666-7777-8888-9999-aaaaaaaaaaaa",
                    "timestamp": "2026-05-02T08:30:00+00:00",
                    "framework_tags": ["SOX", "HIPAA"],
                    "event_type": "financial_transaction",
                    "actor": "bob@example.com",
                    "resource": "/payments/123",
                    "action": "transfer",
                    "outcome": "success",
                    "sensitivity": "confidential",
                    "payload": {"amount_usd": 1500},
                },
                {
                    "id": "abcdef00-0000-0000-0000-000000000000",
                    "timestamp": "2026-05-03T16:45:00+00:00",
                    "framework_tags": ["SOX", "HIPAA"],
                    "event_type": "system_config_change",
                    "actor": "carol@example.com",
                    "resource": "/system/config",
                    "action": "update",
                    "outcome": "success",
                    "sensitivity": "internal",
                    "payload": {"key": "feature_flag", "value": "on"},
                },
            ],
        },
    }


def _decode(blob: bytes) -> str:
    """Helper — bytes -> text once, so the assertions read more naturally."""
    return blob.decode("utf-8")


def test_csv_contains_section_headers() -> None:
    """All three section markers must appear in the rendered CSV."""
    text = _decode(export_csv(_sample_payload()))
    assert "# SUMMARY" in text
    assert "# FINDINGS" in text
    assert "# EVENTS" in text


def test_csv_summary_columns_present() -> None:
    """The SUMMARY block carries every framework category as a header column."""
    text = _decode(export_csv(_sample_payload()))
    # Slice off the events block — those rows also contain headers but for events,
    # which would muddy a header-name search.
    summary_block = text.split("# EVENTS", 1)[0]
    for category in (
        "admin_access",
        "financial_transactions",
        "system_changes",
        "approval_workflows",
        "sod_violations",
    ):
        assert category in summary_block


def test_csv_event_rows_match_count() -> None:
    """Events section should have exactly one data row per input event after the header."""
    text = _decode(export_csv(_sample_payload()))
    events_block = text.split("# EVENTS", 1)[1].strip()
    lines = [ln for ln in events_block.splitlines() if ln.strip()]
    # First non-empty line is the header, the rest are data rows.
    assert len(lines) >= 2
    header = lines[0]
    data_rows = lines[1:]
    assert "event_type" in header
    assert len(data_rows) == 3


def test_csv_framework_tags_pipe_joined() -> None:
    """The framework_tags list joins on ``|`` so it stays a single CSV column."""
    text = _decode(export_csv(_sample_payload()))
    events_block = text.split("# EVENTS", 1)[1]
    assert "SOX|HIPAA" in events_block


def test_csv_payload_json_serialised() -> None:
    """Each event's payload column should be a JSON-parseable string.

    We don't pin the exact column position (pandas may quote the JSON or
    reorder columns by version), so instead we look for the JSON token
    we know was passed in and confirm a round-trip parse works.
    """
    text = _decode(export_csv(_sample_payload()))
    # Round-trip a known marker through json.loads to prove the column
    # stayed valid JSON in the cell. Any of the three payloads will do.
    assert '"amount_usd": 1500' in text or '"amount_usd":1500' in text or 'amount_usd' in text
    # Stronger check: pull out the substring we know is in there and parse it.
    # The compact JSON for the second event's payload starts with {"amount_usd"
    needle = '"amount_usd"'
    idx = text.find(needle)
    assert idx != -1
    # Walk back to the opening brace, forward to the closing brace, and parse.
    start = text.rfind("{", 0, idx)
    end = text.find("}", idx)
    assert start != -1 and end != -1
    # Strip any CSV-doubled quotes pandas inserts when quoting JSON cells.
    fragment = text[start : end + 1].replace('""', '"')
    parsed = json.loads(fragment)
    assert parsed["amount_usd"] == 1500


def test_registered_under_csv() -> None:
    """The decorator wires the exporter into the format-code registry."""
    assert "CSV" in EXPORTERS
    # And the registered callable is the one we imported.
    assert EXPORTERS["CSV"] is export_csv
