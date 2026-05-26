"""Unit tests for the XML exporter.

Verifies the XML serializer produces a well-formed document that:
  * starts with an XML declaration and parses via ``ET.fromstring``;
  * carries the framework + period as root-level attributes for quick filter;
  * preserves every summary category, finding string, and event from the
    canonical aggregator payload;
  * registers under the ``"XML"`` format code in the exporter registry.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET

from src.reporting.exporters import EXPORTERS
from src.reporting.exporters.xml_exporter import export_xml


def _sample_payload() -> dict:
    return {
        "framework": "HIPAA",
        "period": {"start": "2026-04-25T00:00:00+00:00", "end": "2026-05-25T00:00:00+00:00"},
        "summary": {
            "phi_access": 7,
            "auth_failures": 4,
            "phi_modifications": 2,
            "breach_events": 1,
            "user_audit": 9,
        },
        "findings": [
            "2 unauthorized PHI access events (outcome=denied)",
            "Breach events detected (1) — notification workflow required",
        ],
        "data": {
            "events": [
                {
                    "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                    "timestamp": "2026-05-10T09:00:00+00:00",
                    "framework_tags": ["HIPAA", "SOX"],
                    "event_type": "phi_access",
                    "actor": "carol@hospital.example",
                    "resource": "/patient/123/record",
                    "action": "view",
                    "outcome": "denied",
                    "sensitivity": "restricted",
                    "payload": {"patient_id_hash": "abc123", "record_type": "lab"},
                },
                {
                    "id": "ffffffff-0000-1111-2222-333333333333",
                    "timestamp": "2026-05-11T10:30:00+00:00",
                    "framework_tags": ["HIPAA"],
                    "event_type": "breach_event",
                    "actor": "system",
                    "resource": "/breach/registry",
                    "action": "report",
                    "outcome": "success",
                    "sensitivity": "confidential",
                    "payload": {"records_affected": 42},
                },
            ],
        },
    }


def test_xml_starts_with_declaration_and_parses() -> None:
    blob = export_xml(_sample_payload())
    assert isinstance(blob, bytes)
    assert blob.startswith(b"<?xml")
    root = ET.fromstring(blob)
    assert root.tag == "report"


def test_xml_root_carries_framework_and_period_attributes() -> None:
    payload = _sample_payload()
    root = ET.fromstring(export_xml(payload))
    assert root.attrib["framework"] == "HIPAA"
    assert root.attrib["period_start"] == payload["period"]["start"]
    assert root.attrib["period_end"] == payload["period"]["end"]


def test_xml_contains_every_summary_category() -> None:
    payload = _sample_payload()
    root = ET.fromstring(export_xml(payload))
    summary_el = root.find("summary")
    assert summary_el is not None
    names = {cat.attrib["name"] for cat in summary_el.findall("category")}
    assert names == set(payload["summary"].keys())
    # Counts round-trip via the attribute value
    for cat_el in summary_el.findall("category"):
        assert int(cat_el.attrib["count"]) == payload["summary"][cat_el.attrib["name"]]


def test_xml_contains_every_finding_string() -> None:
    payload = _sample_payload()
    root = ET.fromstring(export_xml(payload))
    findings_el = root.find("findings")
    assert findings_el is not None
    texts = [f.text for f in findings_el.findall("finding")]
    assert texts == payload["findings"]


def test_xml_event_count_matches_input() -> None:
    payload = _sample_payload()
    root = ET.fromstring(export_xml(payload))
    data_el = root.find("data")
    assert data_el is not None
    events = data_el.findall("event")
    assert len(events) == len(payload["data"]["events"])
    # framework_tags + payload nested elements present on first event
    first = events[0]
    tags = first.find("framework_tags")
    assert tags is not None
    assert [t.text for t in tags.findall("tag")] == ["HIPAA", "SOX"]
    fields = first.find("payload")
    assert fields is not None
    field_keys = {f.attrib["key"] for f in fields.findall("field")}
    assert field_keys == {"patient_id_hash", "record_type"}


def test_registered_under_xml() -> None:
    assert "XML" in EXPORTERS
    assert EXPORTERS["XML"] is export_xml
