"""Append-only audit logger and event schema.

Every security-relevant operation in the service emits an
:class:`AuditEvent`:

* ``encrypt`` — one event per field successfully encrypted by the
  :class:`~src.processor.log_processor.LogProcessor` (C5 pipeline).
* ``decrypt`` — symmetric, one per field successfully decrypted.
* ``detect`` — emitted by the future C7 ``/v1/detect`` route (dry-run).
* ``key_rotate`` — emitted by :class:`~src.keystore.store.KeyStore` and
  the rotation manager when a fresh active DEK supersedes the old one.
* ``key_destroy`` — emitted on crypto-shred.

Each event ALSO produces a structured stdlib log line via
``logging.getLogger("audit").info(...)`` so operators can ship audit
records via the same observability pipeline as application logs (Filebeat,
Vector, Promtail, whatever) without bolting on a second exporter.

The :class:`AuditEvent` pydantic model is **sealed with**
``extra="forbid"`` — it is impossible to accidentally widen the schema
to include a plaintext field, a ciphertext blob, a nonce, or DEK
material. This is the load-bearing property that guarantees the audit
channel never leaks secrets: the schema has no slot for them at all.
Anything not in the explicit field list raises pydantic at validation
time.

Why we do NOT log plaintexts
----------------------------
A naïve audit logger might helpfully include a "value" or "before/after"
field for debugging. We deliberately do not, for three reasons:

1. The audit trail ends up in observability stores (SIEM, Splunk, ELK)
   with different access controls than the source logs — putting
   plaintext PII there defeats the entire point of field-level
   encryption.
2. The audit event is keyed by ``record_id`` and ``field_path`` — that
   pair is enough to correlate back to the source log if forensics
   ever needs to investigate.
3. Adding the field would be an irreversible policy decision and the
   sealed-schema design makes that a deliberate code change, not an
   accident.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field

from .ring_buffer import RingBuffer


AuditEventType = Literal[
    "encrypt",
    "decrypt",
    "detect",
    "key_rotate",
    "key_destroy",
]
"""Closed set of legal audit event types.

A ``Literal`` rather than a free-form ``str`` so pydantic validates the
value at construction time — a typo at the call site (e.g.
``event_type="encypt"``) fails loudly rather than slipping through into
the audit log under a never-queried name.
"""


AuditOutcome = Literal["success", "failure"]
"""``success`` for the happy path; ``failure`` when an exception was
raised during the operation.

We pair every ``failure`` event with a non-None
:attr:`AuditEvent.failure_reason` so operators can grep for "why did
that decrypt fail" without re-running the pipeline.
"""


class AuditEvent(BaseModel):
    """One immutable record of a security-relevant operation.

    Attributes
    ----------
    event_id : str
        Random hex UUID (no dashes). Stable identifier so operators can
        correlate this event with downstream tooling.
    timestamp_utc : datetime
        UTC instant the event was recorded. Set at construction time,
        not when the audited operation started — close enough for
        observability and avoids passing wall-clock state through the
        call chain.
    event_type : AuditEventType
        Which operation this event describes. See :data:`AuditEventType`.
    outcome : AuditOutcome
        ``"success"`` (default) or ``"failure"``.
    actor : str
        Caller identity. ``"system"`` for internal background tasks
        (rotation) and the default service operations until C7 wires in
        request-derived actors.
    request_id : str | None
        Optional HTTP / job correlator. Set by the C7 HTTP layer.
    record_id : str | None
        Per-log correlator used as part of the AES-GCM AAD (see
        :class:`~src.crypto.aesgcm.AESGCMEncryptor`). Lets operators
        join encrypt and decrypt audit entries for the same log entry.
    key_id : str | None
        DEK version under which the operation ran.
    field_path : str | None
        Dotted JSON path of the affected leaf — e.g.
        ``"shipping.postal_code"``.
    field_type : str | None
        Canonical PII type label from the detector (``"email"``,
        ``"ssn"``, ...).
    failure_reason : str | None
        Operator-readable description of the failure; only set when
        ``outcome == "failure"``. Crucially: this is ``str(exc)`` of
        the upstream exception, NOT a stacktrace or any field value —
        we never want to leak the plaintext that was being processed.
    byte_count : int | None
        Plaintext length in bytes (for encrypts) / ciphertext length
        (for decrypts). Useful for throughput stats.
    duration_us : int | None
        Wall-clock microseconds the operation took, measured via
        :func:`time.perf_counter_ns`.

    Notes
    -----
    There is no ``value``, ``plaintext``, ``ciphertext``, ``nonce``, or
    ``dek`` field. ``extra="forbid"`` means there never will be one
    unless this schema is explicitly widened by a code change — that
    deliberate friction is the entire point of the sealed-schema
    design (see module docstring).
    """

    event_id: str = Field(default_factory=lambda: uuid4().hex)
    timestamp_utc: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    event_type: AuditEventType
    outcome: AuditOutcome = "success"
    actor: str = "system"
    request_id: str | None = None
    record_id: str | None = None
    key_id: str | None = None
    field_path: str | None = None
    field_type: str | None = None
    failure_reason: str | None = None
    byte_count: int | None = None
    duration_us: int | None = None

    # The whole security argument of the audit channel is "this schema
    # cannot accidentally grow a plaintext slot". extra="forbid" is the
    # mechanism that enforces it — a downstream caller cannot smuggle
    # extra fields through to the underlying logger by mistake.
    model_config = ConfigDict(extra="forbid")


class AuditLogger:
    """Append-only audit recorder.

    Records every event into a :class:`RingBuffer` for the dashboard +
    HTTP ``/api/audit`` (future) endpoint and ALSO emits a structured
    log line via stdlib :mod:`logging`, so the audit channel is visible
    to whatever log shipper the deployment uses.

    Parameters
    ----------
    ring_buffer : RingBuffer[AuditEvent] | None
        Where events are appended. If ``None`` (default), a fresh
        :class:`RingBuffer` of size 1000 is constructed. Tests usually
        pass in their own buffer so they can ``snapshot()`` directly.
    logger_name : str
        Name of the stdlib logger used for the structured emission.
        Default ``"audit"`` so operators can filter that channel
        separately from the application's own log lines.
    """

    def __init__(
        self,
        ring_buffer: RingBuffer[AuditEvent] | None = None,
        *,
        logger_name: str = "audit",
    ) -> None:
        # Use a fresh ring buffer if the caller did not supply one. The
        # default maxlen of 1000 matches the spec; tuning is a future
        # concern (and would live in src/settings.py).
        self._buffer = ring_buffer if ring_buffer is not None else RingBuffer(
            maxlen=1000
        )
        # Named logger so operators can filter the audit channel without
        # touching application logs. Configuring this logger (handlers,
        # level, formatter) is the deployment's responsibility — we just
        # emit on it.
        self._logger = logging.getLogger(logger_name)

    # -- public ----------------------------------------------------------

    def record(self, **fields: Any) -> AuditEvent:
        """Construct an :class:`AuditEvent` from ``**fields`` and store it.

        Pydantic validates every field (closed-literal ``event_type``,
        ``outcome``) and rejects any unknown key because of
        ``extra="forbid"``. The resulting event is appended to the
        ring buffer AND emitted on the stdlib audit logger as a single
        line of JSON for downstream observability.

        Parameters
        ----------
        **fields : Any
            Keyword arguments matching :class:`AuditEvent` attributes.
            At minimum ``event_type`` must be supplied; everything else
            has sensible defaults.

        Returns
        -------
        AuditEvent
            The validated, recorded event. Returned so callers can
            chain (e.g. attach the ``event_id`` to an HTTP response).

        Notes
        -----
        We use ``model_dump_json(exclude_none=True)`` for the log line
        so the payload only carries the fields that were actually set —
        no ``"failure_reason": null`` noise on the happy path.
        """
        # Pydantic does the heavy lifting: closed-literal validation on
        # event_type / outcome, plus rejection of any unknown key
        # because of extra="forbid". Caller-side typos surface here.
        event = AuditEvent(**fields)

        # Append before logging so the buffer is the source of truth
        # even if the logger has a slow handler. The buffer is bounded,
        # so this never grows unboundedly.
        self._buffer.append(event)

        # Structured emission. exclude_none=True keeps the line compact:
        # we don't want "failure_reason": null on every success event.
        # The "audit %s" format means standard logging filters can grep
        # for the literal prefix and JSON-parse the tail.
        self._logger.info("audit %s", event.model_dump_json(exclude_none=True))

        return event

    def recent(self, limit: int | None = None) -> list[AuditEvent]:
        """Return the most recent events in the buffer.

        Parameters
        ----------
        limit : int | None
            If provided, returns at most the last ``limit`` events.
            Otherwise returns the entire snapshot. The events are
            returned oldest-first within the slice — the natural
            iteration order of the underlying deque.

        Notes
        -----
        Negative limits and ``limit > len(buffer)`` are handled
        gracefully: we always return a valid list (possibly the full
        snapshot). Defensive against the dashboard / API caller's
        edge cases.
        """
        snap = self._buffer.snapshot()
        if limit is None:
            return snap
        # Trim from the END (most recent). The buffer is oldest-first
        # so the last `limit` entries are the newest. Negative limits
        # are clamped to a no-op slice -> empty list, which is the
        # safest behaviour.
        if limit <= 0:
            return []
        return snap[-limit:]
