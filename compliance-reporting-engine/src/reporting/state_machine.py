"""Report lifecycle state machine.

A ``Report`` row threads through a small linear state machine:

    PENDING -> AGGREGATING -> EXPORTING -> SIGNING -> COMPLETED

with any state allowed to short-circuit to ``FAILED`` so the coordinator
can capture aggregation, export, signing, or encryption errors without
having to invent a half-dozen one-off error states.

The :func:`assert_transition` guard runs before every write — it's the
single chokepoint that keeps the table from drifting into illegal
states (e.g. ``COMPLETED -> PENDING`` after a retry bug, or
``FAILED -> AGGREGATING`` after a stale handler swallows an exception).
``COMPLETED`` and ``FAILED`` are both terminal: their allow-sets are
empty so any further write attempt raises.

The accepted-input contract is intentionally lenient: callers may pass
either ``ReportState`` enum members **or** plain strings (since the
``Report.state`` column stores the value as a string). The guard
coerces strings via ``ReportState(value)`` before checking the map.
"""
from __future__ import annotations

from enum import StrEnum


class ReportState(StrEnum):
    """Canonical lifecycle states for a ``Report`` row.

    ``StrEnum`` (Python 3.11+) makes each member equal to its string
    value, so ``ReportState.PENDING == "PENDING"`` is true. That keeps
    DB writes simple — ``report.state = ReportState.AGGREGATING.value``
    or just ``= ReportState.AGGREGATING`` both round-trip cleanly.
    """

    PENDING = "PENDING"
    AGGREGATING = "AGGREGATING"
    EXPORTING = "EXPORTING"
    SIGNING = "SIGNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


# Directed-graph adjacency: state -> set of legal next states.
# Every non-terminal state allows -> FAILED so the coordinator can
# always record an error. COMPLETED and FAILED are terminal.
ALLOWED_TRANSITIONS: dict[ReportState, set[ReportState]] = {
    ReportState.PENDING: {ReportState.AGGREGATING, ReportState.FAILED},
    ReportState.AGGREGATING: {ReportState.EXPORTING, ReportState.FAILED},
    ReportState.EXPORTING: {ReportState.SIGNING, ReportState.FAILED},
    ReportState.SIGNING: {ReportState.COMPLETED, ReportState.FAILED},
    ReportState.COMPLETED: set(),
    ReportState.FAILED: set(),
}


def assert_transition(from_: ReportState | str, to: ReportState | str) -> None:
    """Raise ``ValueError`` if ``from_ -> to`` is not an allowed transition.

    Both arguments accept the enum or its string value — the ``Report``
    table stores the column as ``String(16)`` so callers most often
    pass strings directly off the ORM row.

    Args:
        from_: Current state (``ReportState`` or string value).
        to: Target state (``ReportState`` or string value).

    Raises:
        ValueError: If the transition isn't in ``ALLOWED_TRANSITIONS``,
            or if either input isn't a recognised state value.
    """
    from_state = ReportState(from_) if isinstance(from_, str) else from_
    to_state = ReportState(to) if isinstance(to, str) else to
    if to_state not in ALLOWED_TRANSITIONS.get(from_state, set()):
        raise ValueError(
            f"Illegal report state transition: {from_state.value} -> {to_state.value}"
        )
