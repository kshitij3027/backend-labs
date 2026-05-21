"""Compliance reporting package (C8).

Exposes :class:`ComplianceReport` and :func:`generate_report` for the
``GET /api/compliance/{rule_set}`` endpoint. The report aggregates the
audit ring buffer's redaction events into a per-regime summary
(GDPR / HIPAA / PCI_DSS) — pattern counts, strategies used, and the
observed time window.

The package is intentionally a single module today; future commits may
add per-regime exporters (CSV/PDF) which would land here as siblings.
"""
