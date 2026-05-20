"""Audit subpackage: sealed audit events, ring buffer, and recorder.

The audit channel is intentionally append-only and schema-sealed. Every
event produced by the redaction engine flows through :class:`AuditLogger`
which builds an :class:`AuditEvent` via pydantic and appends it to a
bounded :class:`RingBuffer`. The pydantic schema is configured with
``extra="forbid"`` so there is no way — by accident or by mistake — to
add a ``plaintext`` / ``value`` / ``redacted_value`` field to an audit
record. That's the canonical leak-proof property the C8 compliance
report relies on.
"""
