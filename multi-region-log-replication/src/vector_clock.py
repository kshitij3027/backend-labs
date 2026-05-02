"""Vector clock primitives used by every region.

The three operations we need are ``compare``, ``merge``, and ``increment``.
They are pure functions over plain dicts so callers can copy/store/reason
about results without sharing mutable state.

Compare semantics (per ``plan.md`` lines 99-113 and ``project_requirements.md``
§2):

    -1   : ``a`` happens-before ``b`` (a < b)
     1   : ``b`` happens-before ``a`` (a > b)
     0   : identical
     None: concurrent / incomparable

``merge`` is a *pure* per-key max merge. The "merge then increment local
region" semantics from the spec are realised by calling ``merge`` first and
then ``increment`` separately at the secondary's receive site — keeping the
pieces composable and individually unit-testable.
"""

from __future__ import annotations

from typing import Dict, Optional

VectorClock = Dict[str, int]


def vector_clock_compare(a: VectorClock, b: VectorClock) -> Optional[int]:
    """Compare two vector clocks for causal ordering.

    Returns:
        -1 if ``a`` happens-before ``b``,
         1 if ``b`` happens-before ``a``,
         0 if they are identical,
         None if they are concurrent (incomparable).

    Missing keys are treated as ``0``.
    """
    keys = set(a) | set(b)
    a_le_b = all(a.get(k, 0) <= b.get(k, 0) for k in keys)
    b_le_a = all(b.get(k, 0) <= a.get(k, 0) for k in keys)
    if a_le_b and b_le_a:
        return 0
    if a_le_b:
        return -1
    if b_le_a:
        return 1
    return None


def merge(local: VectorClock, incoming: VectorClock) -> VectorClock:
    """Per-key max merge of two vector clocks.

    Returns a *new* dict; neither input is mutated. Missing keys count as 0.
    Note: this is a pure merge — it does NOT increment the local region. The
    secondary calls ``increment(merged, my_region)`` separately to advance
    its own logical time after a replication receipt.
    """
    keys = set(local) | set(incoming)
    return {k: max(local.get(k, 0), incoming.get(k, 0)) for k in keys}


def increment(vc: VectorClock, region: str) -> VectorClock:
    """Return a new vector clock with ``region``'s counter advanced by 1.

    A missing region key is treated as 0 and becomes 1. Input is not mutated.
    """
    out: VectorClock = dict(vc)
    out[region] = out.get(region, 0) + 1
    return out
