"""XML exporter — stdlib ``xml.etree.ElementTree`` serializer.

Produces a single ``<report>`` document whose shape mirrors the
canonical aggregator payload:

.. code-block:: xml

    <?xml version='1.0' encoding='utf-8'?>
    <report framework="SOX" period_start="..." period_end="...">
      <summary>
        <category name="admin_access" count="12"/>
        ...
      </summary>
      <findings>
        <finding>3 SoD violations detected in period</finding>
        ...
      </findings>
      <data>
        <event timestamp="..." event_type="..." actor="..." ...>
          <framework_tags>
            <tag>SOX</tag>
            <tag>HIPAA</tag>
          </framework_tags>
          <payload>
            <field key="amount">12345.67</field>
            ...
          </payload>
        </event>
        ...
      </data>
    </report>

Primitive scalar columns of each event land on the ``<event>`` tag as
attributes; the two complex columns (``framework_tags`` list and the
free-form ``payload`` dict) become nested elements so the XML stays
well-formed and re-parseable. Everything is stringified — XML has no
native numeric type and the auditor-facing format doesn't need one.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET

from . import register_exporter


# Event keys that round-trip cleanly as XML attributes. Anything not in
# this set is either complex (``framework_tags`` list, ``payload`` dict)
# or already covered (``id`` is omitted from attributes to keep the
# auditor-facing format clean — it stays available in the JSON export).
_EVENT_ATTR_KEYS = (
    "timestamp",
    "event_type",
    "actor",
    "resource",
    "action",
    "outcome",
    "sensitivity",
)


@register_exporter("XML")
def export_xml(payload: dict) -> bytes:
    """Serialize the aggregator payload as UTF-8 XML with a declaration.

    Args:
        payload: Canonical aggregator payload (``framework``, ``period``,
            ``summary``, ``findings``, ``data``).

    Returns:
        UTF-8-encoded XML bytes, starting with ``<?xml ...?>``. Safe to
        write straight to disk or stream as ``application/xml``.
    """
    period = payload.get("period") or {}
    root = ET.Element(
        "report",
        attrib={
            "framework": str(payload.get("framework", "")),
            "period_start": str(period.get("start", "")),
            "period_end": str(period.get("end", "")),
        },
    )

    # --- <summary> ---
    summary_el = ET.SubElement(root, "summary")
    for category, count in (payload.get("summary") or {}).items():
        ET.SubElement(
            summary_el,
            "category",
            attrib={"name": str(category), "count": str(count)},
        )

    # --- <findings> ---
    findings_el = ET.SubElement(root, "findings")
    for finding in payload.get("findings") or []:
        finding_el = ET.SubElement(findings_el, "finding")
        finding_el.text = str(finding)

    # --- <data> with nested <event> children ---
    data_el = ET.SubElement(root, "data")
    events = ((payload.get("data") or {}).get("events")) or []
    for event in events:
        attribs = {
            key: str(event.get(key, ""))
            for key in _EVENT_ATTR_KEYS
            if event.get(key) is not None
        }
        event_el = ET.SubElement(data_el, "event", attrib=attribs)

        # framework_tags -> <framework_tags><tag>...</tag></framework_tags>
        tags = event.get("framework_tags") or []
        tags_el = ET.SubElement(event_el, "framework_tags")
        for tag in tags:
            tag_el = ET.SubElement(tags_el, "tag")
            tag_el.text = str(tag)

        # payload -> <payload><field key="...">value</field></payload>
        payload_dict = event.get("payload") or {}
        payload_el = ET.SubElement(event_el, "payload")
        for key, value in payload_dict.items():
            field_el = ET.SubElement(payload_el, "field", attrib={"key": str(key)})
            field_el.text = str(value)

    return ET.tostring(root, encoding="utf-8", xml_declaration=True)
