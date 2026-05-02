"""Deterministic conflict resolution for replicated log entries.

When a secondary region receives an incoming :class:`LogEntry` whose
``log_id`` already exists in its store, it must choose **one** version to
keep. The choice has to be deterministic across all regions so they
converge on the same state without coordinating.

Rules (per ``project_requirements.md`` §2 and ``plan.md`` lines 119-130):

* If the existing entry happens-before the incoming one (``cmp == -1``)
  → keep the incoming one (it is causally newer).
* If the incoming entry happens-before the existing one (``cmp == 1``)
  → keep the existing one (the local copy is causally newer).
* If the two vector clocks are identical (``cmp == 0``) → keep the
  incoming one. Re-applying the same write is idempotent: same data,
  same vector clock, same logical_ts — the outcome is unchanged.
* If the two vector clocks are concurrent (``cmp is None``) → fall back
  to deterministic last-write-wins on the tuple
  ``(logical_ts, created_at, region, log_id)``. The entry with the
  **larger** tuple wins (lex compare on the full 4-tuple).

The tiebreaker fields (``logical_ts``, ``created_at``, ``region``,
``log_id``) are stamped at write time by the writing region and never
mutated thereafter, so every region in the cluster — applied to the
exact same pair of entries — picks the same winner.
"""

from __future__ import annotations

from .models import LogEntry
from .vector_clock import vector_clock_compare


def resolve(existing: LogEntry, incoming: LogEntry) -> LogEntry:
    """Pick the winning :class:`LogEntry` between two versions of the same log_id.

    Args:
        existing: The version currently in a region's ``log_store``.
        incoming: The version just received via replication.

    Returns:
        Whichever of the two should be retained, per the rules in this
        module's docstring. The function never mutates its inputs.
    """
    cmp = vector_clock_compare(existing.vector_clock, incoming.vector_clock)

    if cmp == -1:
        # existing happens-before incoming → incoming is causally newer.
        return incoming
    if cmp == 1:
        # incoming happens-before existing → keep the local copy.
        return existing
    if cmp == 0:
        # Identical clocks → idempotent re-application; either is fine.
        # We pick ``incoming`` so the resolver never silently drops a
        # newly-arrived message (it lands in the store with the same
        # data, so no observable behavior changes).
        return incoming

    # Concurrent (cmp is None) — break the tie deterministically.
    existing_key = (
        existing.logical_ts,
        existing.created_at,
        existing.region,
        existing.log_id,
    )
    incoming_key = (
        incoming.logical_ts,
        incoming.created_at,
        incoming.region,
        incoming.log_id,
    )
    return incoming if incoming_key > existing_key else existing
