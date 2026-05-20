"""Sealed-schema audit event model.

The :class:`AuditEvent` model is the canonical representation of every
security-relevant action the redaction engine takes — one redaction,
one detokenize, one config reload, one regex timeout, one detect
preview. It is intentionally constrained:

* ``event_type`` is a closed ``Literal`` — typos at the call site fail
  pydantic validation rather than slipping into the audit log under a
  never-queried name.
* ``outcome`` is also closed-literal (``success`` / ``failure``).
* ``model_config = ConfigDict(extra="forbid", frozen=True)``.

The ``extra="forbid"`` line is load-bearing: it is impossible to
construct an :class:`AuditEvent` with a ``plaintext`` / ``value`` /
``redacted_value`` field, because the schema has no slot for them.
This is the schema-level enforcement of the no-plaintext invariant —
not a convention, not a comment, an actual ``ValidationError`` at
construction time.

The ``frozen=True`` flag means every event is immutable after
construction, so a snapshot returned by the ring buffer is safe to
share across threads without further copying.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class AuditEvent(BaseModel):
    """One immutable record of a security-relevant operation.

    Attributes
    ----------
    event_id : UUID
        Random UUID4 generated at construction time. Stable identifier
        so operators can correlate this event with downstream tooling.
    timestamp_utc : datetime
        UTC instant the event was recorded. Set at construction time
        (close enough for observability and avoids threading wall-clock
        state through the call chain).
    event_type : Literal[...]
        Closed set of legal event types. A typo at the call site fails
        pydantic validation rather than slipping through.
    outcome : Literal["success", "failure"]
        ``"success"`` for the happy path; ``"failure"`` when an
        exception was raised during the audited operation.
    pattern_name : str | None
        The pattern that fired (``"ssn"``, ``"credit_card"``, etc.) for
        redaction / detect / detokenize events. ``None`` for events not
        tied to a specific pattern (config reload).
    strategy : str | None
        The strategy applied to the matched value (``"mask"``,
        ``"partial"``, ``"hash"``, ``"tokenize"``). ``None`` when not
        applicable.
    compliance_tags : list[str]
        Which regulatory regimes care about the affected pattern.
        Drives the C8 compliance report's filter-by-tag query.
    actor : str
        Caller identity. ``"system"`` by default for internal flows.
    failure_reason : str | None
        Operator-readable description of a failure; only set when
        ``outcome == "failure"``. Crucially, this is ``str(exc)`` of
        the upstream exception, NOT a stacktrace or any field value —
        we never want to leak the plaintext that was being processed.

    Notes
    -----
    There is no ``plaintext``, ``value``, or ``redacted_value`` field.
    ``extra="forbid"`` means there never will be one unless this schema
    is explicitly widened by a code change — that deliberate friction
    is the entire point of the sealed-schema design.
    """

    event_id: UUID = Field(default_factory=uuid4)
    timestamp_utc: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    event_type: Literal["redaction", "detokenize", "config_reload", "regex_timeout", "detect"]
    outcome: Literal["success", "failure"]
    pattern_name: str | None = None
    strategy: str | None = None
    compliance_tags: list[str] = Field(default_factory=list)
    actor: str = "system"
    failure_reason: str | None = None

    # CRITICAL: extra="forbid" forbids plaintext/value/redacted_value
    # fields entirely — the schema cannot accidentally grow a plaintext
    # slot via a downstream caller passing extra kwargs. frozen=True
    # keeps every event immutable after construction so snapshots are
    # safe to share across threads.
    model_config = ConfigDict(extra="forbid", frozen=True)
