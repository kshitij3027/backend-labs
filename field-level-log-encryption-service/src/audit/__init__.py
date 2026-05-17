"""Append-only audit logging for the field-level encryption service.

The audit subsystem records every security-relevant operation
(encrypt / decrypt / detect / key_rotate / key_destroy) into a bounded,
thread-safe ring buffer AND simultaneously emits a structured log line via
the stdlib :mod:`logging`. Two backends, one entry point — so operators
can scrape JSON lines from container stdout while the dashboard reads
the most recent N events directly out of the in-memory buffer.

Design invariants
-----------------
* **No plaintext / ciphertext / nonce / DEK material ever appears in an
  audit event.** This is enforced at the schema level: the
  :class:`AuditEvent` pydantic model has no field for any of those, and
  ``extra="forbid"`` makes adding one at the call site fail loudly.
* The ring buffer is bounded (default 1000 entries) so an attacker can't
  inflate memory by flooding the audit channel.
* All mutating operations are lock-guarded so the ring buffer is safe to
  call from the C5 parallel encryption path or from background rotation
  tasks.

Public surface:

* :class:`AuditEvent`     — the immutable record.
* :class:`AuditEventType` — Literal of legal event types.
* :class:`AuditOutcome`   — Literal of "success" / "failure".
* :class:`RingBuffer`     — generic thread-safe bounded deque wrapper.
* :class:`AuditLogger`    — the recorder.
"""
from __future__ import annotations

from .audit_logger import AuditEvent, AuditEventType, AuditLogger, AuditOutcome
from .ring_buffer import RingBuffer

__all__ = [
    "AuditEvent",
    "AuditEventType",
    "AuditLogger",
    "AuditOutcome",
    "RingBuffer",
]
