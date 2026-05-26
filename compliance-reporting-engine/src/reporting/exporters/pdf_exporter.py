"""PDF exporter — ReportLab Platypus pipeline.

Builds a one-document PDF with:

  * a title line carrying the framework name
  * a sub-title with the period start/end
  * a summary table (one row per category, with count)
  * a findings bullet list
  * a total-events footer line

Renders to an in-memory BytesIO so the coordinator can take the
bytes and hand them straight to the at-rest encryptor without ever
touching the filesystem.
"""
from __future__ import annotations
import io
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.units import inch

from . import register_exporter


@register_exporter("PDF")
def export_pdf(payload: dict) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=0.75 * inch,
        rightMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
    )
    styles = getSampleStyleSheet()
    story = []

    framework = str(payload.get("framework", "Unknown"))
    period = payload.get("period") or {}
    story.append(Paragraph(f"{framework} Compliance Report", styles["Heading1"]))
    story.append(
        Paragraph(
            f"Period: {period.get('start', '')} &mdash; {period.get('end', '')}",
            styles["Normal"],
        )
    )
    story.append(Spacer(1, 0.2 * inch))

    # --- Summary table ---
    story.append(Paragraph("Summary", styles["Heading2"]))
    summary = payload.get("summary") or {}
    rows = [["Category", "Count"]]
    for category, count in summary.items():
        rows.append([str(category), str(count)])
    table = Table(rows, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (1, 1), (1, -1), "RIGHT"),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.whitesmoke, colors.white]),
            ]
        )
    )
    story.append(table)
    story.append(Spacer(1, 0.2 * inch))

    # --- Findings bullet list ---
    story.append(Paragraph("Findings", styles["Heading2"]))
    findings = payload.get("findings") or []
    if findings:
        for finding in findings:
            story.append(Paragraph(f"&bull; {finding}", styles["Normal"]))
    else:
        story.append(Paragraph("No findings for this period.", styles["Italic"]))
    story.append(Spacer(1, 0.2 * inch))

    # --- Footer with event count ---
    events = ((payload.get("data") or {}).get("events")) or []
    story.append(Paragraph(f"Total events analyzed: {len(events)}", styles["Normal"]))

    doc.build(story)
    return buf.getvalue()
