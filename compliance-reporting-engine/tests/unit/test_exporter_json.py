"""Unit tests for the JSON exporter.

Covers the three things that matter for a downstream auditor:
  * round-tripping the canonical payload through ``json.loads`` yields
    structurally-identical data (keys, lists, nested dicts);
  * the returned value is UTF-8 bytes starting with the opening brace,
    matching the project's "files are bytes" boundary;
  * the exporter registers itself under the ``"JSON"`` format code so the
    coordinator can look it up by export_format without hard-wiring.
"""
from __future__ import annotations

import json

from src.reporting.exporters import EXPORTERS
from src.reporting.exporters.json_exporter import export_json


def _sample_payload() -> dict:
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
                    "payload": {"ip": "10.0.0.1"},
                },
                {
                    "id": "66666666-7777-8888-9999-aaaaaaaaaaaa",
                    "timestamp": "2026-05-02T08:30:00+00:00",
                    "framework_tags": ["SOX"],
                    "event_type": "financial_transaction",
                    "actor": "bob@example.com",
                    "resource": "/payments/123",
                    "action": "transfer",
                    "outcome": "success",
                    "sensitivity": "confidential",
                    "payload": {"amount_usd": 1500},
                },
            ],
        },
    }


def test_json_round_trip() -> None:
    payload = _sample_payload()
    blob = export_json(payload)
    decoded = json.loads(blob)
    # Top-level keys round-trip exactly
    assert set(decoded.keys()) == {"framework", "period", "summary", "findings", "data"}
    assert decoded["framework"] == "SOX"
    assert decoded["summary"]["admin_access"] == 5
    assert decoded["findings"] == payload["findings"]
    assert decoded["data"]["events"][0]["actor"] == "alice@example.com"
    assert len(decoded["data"]["events"]) == 2


def test_json_is_utf8_bytes_starting_with_brace() -> None:
    blob = export_json(_sample_payload())
    assert isinstance(blob, bytes)
    assert blob.startswith(b"{")
    # Must be valid UTF-8 (will raise UnicodeDecodeError if not)
    blob.decode("utf-8")


def test_registered_under_json() -> None:
    assert "JSON" in EXPORTERS
    # And the registered callable is our exporter
    assert EXPORTERS["JSON"] is export_json


def test_json_handles_non_native_types_via_default_str() -> None:
    """``default=str`` keeps the exporter tolerant of datetime/UUID/Decimal slipping through."""
    from datetime import datetime, timezone
    from uuid import uuid4

    payload = _sample_payload()
    payload["data"]["events"][0]["timestamp"] = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
    payload["data"]["events"][0]["id"] = uuid4()
    blob = export_json(payload)
    decoded = json.loads(blob)
    assert isinstance(decoded["data"]["events"][0]["timestamp"], str)
    assert isinstance(decoded["data"]["events"][0]["id"], str)
