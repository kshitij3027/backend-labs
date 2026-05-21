"""Compliance report aggregation over the audit ring buffer (C8).

A :class:`ComplianceReport` is a point-in-time roll-up of every
successful redaction event tagged with a given regulatory regime
(``GDPR``, ``HIPAA``, or ``PCI_DSS``). It exposes:

* ``total_redactions`` — count of qualifying redaction events.
* ``breakdown`` — pattern name → count (e.g. ``{"ssn": 42, "mrn": 7}``).
* ``strategies_used`` — strategy name → count (e.g. ``{"mask": 49}``).
* ``report_window_start`` / ``report_window_end`` — observed event time
  range. When no events match, both bounds fall back to ``since`` (or
  ``generated_at`` if ``since`` is unset) so the window is always a
  valid pair.
* ``report_generation_time_ms`` — wall-clock cost of the aggregation
  measured via :func:`time.monotonic_ns` so it isn't affected by NTP
  step adjustments mid-call.

Performance budget
------------------
The spec mandates a 30-second ceiling for 100 000 events; the inner
loop is a single pass over the filtered list with two
:class:`collections.Counter` accumulators — O(n) with very small per-
event work, comfortably inside the budget on a laptop-class CPU.

Outcome filter
--------------
The ring buffer's :meth:`RingBuffer.filter` doesn't carry an
``outcome`` axis today; this module applies it in-process after the
filter call. Doing the filter in two steps (ring-buffer side first,
outcome side second) keeps the buffer API minimal and lets the report
encode "what counts as a redaction" as a policy decision local to the
compliance layer.
"""
from __future__ import annotations

import time
from collections import Counter
from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from src.audit.events import AuditEvent
from src.audit.ring_buffer import RingBuffer


# ---------------------------------------------------------------------------
# Public type alias
# ---------------------------------------------------------------------------

# Closed Literal — FastAPI will surface a 422 for any value outside this
# set when the alias is used as a Path parameter type. The exact string
# values must match what callers pass in compliance_tags on AuditEvent
# (uppercase, no aliases).
ComplianceRuleSet = Literal["GDPR", "HIPAA", "PCI_DSS"]


# ---------------------------------------------------------------------------
# Response model
# ---------------------------------------------------------------------------


class ComplianceReport(BaseModel):
    """One regulatory-regime redaction summary suitable for JSON wire output.

    Attributes
    ----------
    rule_set : ComplianceRuleSet
        Which regime this report covers. Constrained to the public
        :data:`ComplianceRuleSet` literal so wire validation rejects
        anything else with a 422.
    generated_at : datetime
        UTC instant the report was assembled — set once at the start
        of :func:`generate_report`. Useful for dashboards that want to
        know how fresh a cached report is.
    report_window_start : datetime
        Earliest event timestamp included in this report (or the
        ``since`` parameter / ``generated_at`` fallback when no events
        matched).
    report_window_end : datetime
        Latest event timestamp included in this report.
    total_redactions : int
        Count of successful redaction events tagged with ``rule_set``.
    breakdown : dict[str, int]
        Pattern name → redaction count. Pulled from the
        ``pattern_name`` field of each :class:`AuditEvent`; events
        with ``pattern_name is None`` are skipped (the report is
        per-pattern, so a name-less event would have nowhere to land).
    strategies_used : dict[str, int]
        Strategy name → application count. Useful for verifying that
        the configured strategy for a sensitive pattern is in fact
        the one being applied at runtime.
    report_generation_time_ms : int
        Wall-clock cost of building this report (``monotonic_ns``
        delta in milliseconds). Surfaced on the wire so a dashboard
        can plot generation latency over time.

    Notes
    -----
    ``model_config = ConfigDict(frozen=True)`` matches the audit event
    model — once generated, the report is an immutable snapshot. The
    aggregation function builds a fresh report each call rather than
    mutating an existing one.
    """

    rule_set: ComplianceRuleSet
    generated_at: datetime
    report_window_start: datetime
    report_window_end: datetime
    total_redactions: int
    # ``default_factory=dict`` so the field can serialize to ``{}`` when
    # there are zero matching events — the spec wants an empty dict, not
    # a missing key.
    breakdown: dict[str, int] = Field(default_factory=dict)
    strategies_used: dict[str, int] = Field(default_factory=dict)
    report_generation_time_ms: int

    # Immutable after construction — matches the audit event policy.
    model_config = ConfigDict(frozen=True)


# ---------------------------------------------------------------------------
# Aggregator
# ---------------------------------------------------------------------------


def generate_report(
    ring_buffer: RingBuffer,
    rule_set: ComplianceRuleSet,
    since: Optional[datetime] = None,
) -> ComplianceReport:
    """Aggregate the ring buffer's audit events into a :class:`ComplianceReport`.

    Parameters
    ----------
    ring_buffer : RingBuffer
        The audit channel to inspect. Must be the same buffer the
        :class:`AuditLogger` is appending to (typically the singleton
        wired on ``app.state.ring_buffer``).
    rule_set : ComplianceRuleSet
        Which regime to report on. Filters events whose
        ``compliance_tags`` list contains this exact uppercase tag.
    since : datetime | None, optional
        If provided, exclude events older than this timestamp from
        the aggregation. Lets dashboards request a rolling window
        (e.g. "last 5 minutes") without resetting the audit buffer.

    Returns
    -------
    ComplianceReport
        Immutable, JSON-serializable summary. Empty buffers return a
        well-formed report with ``total_redactions == 0`` and empty
        breakdown / strategies_used dicts.

    Notes
    -----
    The aggregation is a single O(n) pass after the ring-buffer filter
    — comfortably inside the 30 s budget for 100 000 events on
    laptop-class CPUs. The buffer's :meth:`filter` already holds a
    short lock while snapshotting; the heavy counting happens here on
    the snapshot copy, outside the critical section.
    """
    # Monotonic clock for the measurement — wall-clock would be subject
    # to NTP corrections mid-call. ``time.monotonic_ns`` is the
    # standard-library nanosecond counter; we floor-divide to ms at the
    # end so the integer subtraction stays exact.
    t0 = time.monotonic_ns()

    # Generation timestamp is wall-clock UTC — this is the value that
    # ships back to clients, so it has to make sense in calendar time.
    generated_at = datetime.now(timezone.utc)

    # Ring-buffer side filter handles the cheap axes (event_type,
    # compliance_tag, since). The outcome filter is layered on top
    # because the buffer API doesn't carry an outcome axis today.
    candidates = ring_buffer.filter(
        since=since,
        event_type="redaction",
        compliance_tag=rule_set,
    )
    events: list[AuditEvent] = [e for e in candidates if e.outcome == "success"]

    # Window bounds. When no events match we fall back to a deterministic
    # pair: (since or generated_at, generated_at). That keeps the model
    # construction valid (datetime fields are required) and gives the
    # dashboard a sensible "empty window" to render.
    if not events:
        start = since if since is not None else generated_at
        end = generated_at
    else:
        observed_min = min(e.timestamp_utc for e in events)
        observed_max = max(e.timestamp_utc for e in events)
        # If a caller passed a ``since`` that's strictly earlier than the
        # first observed event, honor it as the window start. That matches
        # the intent of "the rolling window the caller asked for, even if
        # the buffer happened to be empty at the start of that window."
        if since is not None and since < observed_min:
            start = since
        else:
            start = observed_min
        end = observed_max

    # Two single-pass counters. ``Counter`` deduplicates the build cost
    # (one C-level loop per Counter) and the ``if e.pattern_name`` guard
    # drops the small number of pattern-less events (e.g. a redaction
    # event recorded without a pattern_name, defensive).
    breakdown: dict[str, int] = dict(
        Counter(e.pattern_name for e in events if e.pattern_name)
    )
    strategies_used: dict[str, int] = dict(
        Counter(e.strategy for e in events if e.strategy)
    )

    # Integer ms — floor-divide rather than round so the reported number
    # is a strict lower bound on the elapsed time.
    elapsed_ms = (time.monotonic_ns() - t0) // 1_000_000

    return ComplianceReport(
        rule_set=rule_set,
        generated_at=generated_at,
        report_window_start=start,
        report_window_end=end,
        total_redactions=len(events),
        breakdown=breakdown,
        strategies_used=strategies_used,
        report_generation_time_ms=int(elapsed_ms),
    )
