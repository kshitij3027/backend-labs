"""Unit tests for the PDF exporter.

The PDF format is opaque from a unit-test perspective — we deliberately
avoid pulling in PyPDF2 just to extract text. The guarantees we DO care
about and can check cheaply are:

  * the emitted bytes are a valid PDF (magic bytes ``%PDF-`` at byte 0),
  * the document is non-trivially large (``len > 1000``), proving ReportLab
    actually rendered the Platypus story and didn't return an empty stub;
  * the exporter is registered under the ``"PDF"`` format code so the
    coordinator can look it up by export_format without hard-wiring;
  * the rendering pipeline degrades gracefully for the common "no rows"
    edge cases — empty findings, empty events, and missing ``period``.
"""
from __future__ import annotations

from src.reporting.exporters import EXPORTERS
from src.reporting.exporters.pdf_exporter import export_pdf


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
            "3 system changes without approval workflow",
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


def test_pdf_magic_bytes() -> None:
    """A real PDF always starts with the ``%PDF-`` magic so readers can sniff it."""
    blob = export_pdf(_sample_payload())
    assert isinstance(blob, bytes)
    assert blob.startswith(b"%PDF-")


def test_pdf_non_trivial_size() -> None:
    """Rendering an actual Platypus story produces well over 1 KB; an empty stub would not."""
    blob = export_pdf(_sample_payload())
    assert len(blob) > 1000


def test_pdf_handles_empty_findings() -> None:
    """Empty findings list must not raise and must still produce a valid PDF."""
    payload = _sample_payload()
    payload["findings"] = []
    blob = export_pdf(payload)
    assert blob.startswith(b"%PDF-")
    assert len(blob) > 1000


def test_pdf_handles_empty_events() -> None:
    """Empty events list must not raise and must still produce a valid PDF."""
    payload = _sample_payload()
    payload["data"] = {"events": []}
    blob = export_pdf(payload)
    assert blob.startswith(b"%PDF-")
    assert len(blob) > 1000


def test_pdf_handles_missing_period() -> None:
    """A payload missing the ``period`` key must not raise — defensive defaults kick in."""
    payload = _sample_payload()
    del payload["period"]
    blob = export_pdf(payload)
    assert blob.startswith(b"%PDF-")
    assert len(blob) > 1000


def test_registered_under_pdf() -> None:
    """The decorator wires the function into the registry under ``"PDF"``."""
    assert "PDF" in EXPORTERS
    assert EXPORTERS["PDF"] is export_pdf
