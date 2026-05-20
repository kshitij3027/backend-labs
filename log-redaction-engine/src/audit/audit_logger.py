"""Append-only audit logger.

The :class:`AuditLogger` is the single entry point used by the C5
processor (and future C7 HTTP layer) for recording security-relevant
events. Every call to :meth:`record` constructs a sealed
:class:`AuditEvent`, appends it to the injected :class:`RingBuffer`,
AND emits a structured stdlib log line so operators can ship audit
records via the same observability pipeline as application logs.

Sealed-schema guarantee
-----------------------
The :class:`AuditEvent` schema forbids ``plaintext`` / ``value`` /
``redacted_value`` fields via ``extra="forbid"``. ``model_dump_json``
on such an event therefore CANNOT serialize PII even if a caller
attempts to pass it as a kwarg — the construction itself raises
``ValidationError`` before any log line is emitted. This is the
load-bearing property that makes the structured log emission safe.
"""
from __future__ import annotations

import logging
from typing import Any

from .events import AuditEvent
from .ring_buffer import RingBuffer

# Module-level logger so callers can configure the "src.audit.audit_logger"
# channel separately from application logs (a common pattern when the
# audit trail is shipped to a SIEM rather than the standard log sink).
logger = logging.getLogger(__name__)


class AuditLogger:
    """Append-only audit recorder backed by a thread-safe ring buffer.

    Parameters
    ----------
    ring_buffer : RingBuffer
        Where validated events are appended. Construction is
        dependency-injection only so tests can ``snapshot()`` directly
        without going through this class.

    Notes
    -----
    The class deliberately does NOT own the ring buffer: a single
    process-wide buffer is typically constructed at startup and shared
    across the audit logger, the HTTP audit query endpoint, and the
    dashboard. Owning the buffer here would force every consumer to go
    through this class, which is the wrong coupling.
    """

    def __init__(self, ring_buffer: RingBuffer) -> None:
        self._buf = ring_buffer

    def record(
        self,
        *,
        event_type: str,
        outcome: str = "success",
        **kwargs: Any,
    ) -> AuditEvent:
        """Construct an :class:`AuditEvent`, append it, and emit a log line.

        Parameters
        ----------
        event_type : str
            One of the closed ``Literal`` values declared in
            :class:`AuditEvent.event_type`. Anything else raises a
            ``ValidationError`` at construction time.
        outcome : str, default ``"success"``
            ``"success"`` or ``"failure"``. Same closed-literal validation.
        **kwargs : Any
            Forwarded to the :class:`AuditEvent` constructor. Pydantic
            validates them — any unknown key (``plaintext``, ``value``,
            ``redacted_value``) raises ``ValidationError`` because of
            ``extra="forbid"`` on the model.

        Returns
        -------
        AuditEvent
            The validated, recorded event. Returned so callers can
            chain (e.g., attach the ``event_id`` to an HTTP response).

        Notes
        -----
        Order matters: we APPEND first, then log. If the structured log
        emission had a slow handler we still want the buffer to be the
        source of truth for the dashboard / API. The buffer is bounded
        so this can never grow unboundedly.
        """
        # Pydantic does the heavy lifting: closed-literal validation on
        # event_type / outcome, plus rejection of any unknown key
        # (extra="forbid"). Caller-side typos and plaintext attempts
        # surface here as ValidationError before any side effect.
        event = AuditEvent(event_type=event_type, outcome=outcome, **kwargs)

        # Buffer is the source of truth — append before logging.
        self._buf.append(event)

        # Structured emission. ``model_dump_json`` is safe because the
        # schema forbids plaintext fields — there are no PII slots in
        # the dump output. The "audit %s" prefix lets log shippers filter
        # the audit channel by literal prefix and JSON-parse the tail.
        logger.info("audit %s", event.model_dump_json())

        return event
