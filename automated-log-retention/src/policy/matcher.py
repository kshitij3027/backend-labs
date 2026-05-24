"""Pure-function policy matching helpers (C05).

Two responsibilities, both stateless and side-effect free:

  * ``pick_policy(file, policy_set)`` — choose the single ``Policy``
    that wins for a given file. Conflict resolution is deterministic:
    higher ``priority`` first, then higher ``selector.specificity()``,
    then lexicographically smallest ``name`` as a final tiebreaker.

  * ``next_due_phase(file, policy, now)`` — return the next ``Phase``
    that should fire for ``file`` under ``policy``, or ``None`` if the
    file is either too young for the first phase or already past its
    last applicable phase. "Already applied" is inferred from the
    file's current ``tier``: a ``promote``/``compress``/``archive``
    phase whose ``target_tier`` is the current tier (or earlier in the
    hot → warm → cold → archive → pending order) is treated as
    already-applied. A ``delete`` phase is only skipped when the file
    is mid-delete (tier == "pending").

The two functions are pure: no DB, no filesystem, no ``datetime.now()``
inside (the caller passes ``now``). The scanner in C09 wires them to
the catalog; this module knows nothing about either.

The ``file`` argument is duck-typed — any object with ``source``,
``level``, ``category``, ``oldest_record_ts``, ``tier`` attributes
works. Tests use ``SimpleNamespace`` for cheap fixtures; production
uses the ORM ``File`` model.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from src.policy.schema import Phase, Policy, PolicySet


# Ordering used by ``next_due_phase`` to decide whether a given phase
# has "already been applied" to the file. A phase whose ``target_tier``
# sits at the same index as — or earlier than — the file's current
# tier is treated as no-op for that file. ``pending`` is the terminal
# state used during the mark-then-sweep delete window.
_TIER_ORDER: dict[str, int] = {
    "hot": 0,
    "warm": 1,
    "cold": 2,
    "archive": 3,
    "pending": 4,
}


def _tier_index(tier: str) -> int:
    """Return the canonical order index for ``tier``.

    Raises:
        ValueError: if ``tier`` is not one of the known names. Callers
            (the matcher) should treat this as a data bug — a file's
            ``tier`` should always be one of the five strings above.
    """
    try:
        return _TIER_ORDER[tier]
    except KeyError as e:
        raise ValueError(f"unknown tier: {tier!r}") from e


def pick_policy(file: Any, policy_set: PolicySet) -> Policy | None:
    """Return the policy that wins for ``file``, or ``None`` if none match.

    The winner is selected by three deterministic criteria, in order:

      1. Highest ``priority``.
      2. Highest ``selector.specificity()`` (more constrained selector
         wins ties — a "category=payment AND source=billing" beats a
         "category=payment").
      3. Lexicographically smallest ``name`` (final deterministic
         tiebreaker — same name twice should never happen because the
         loader can enforce uniqueness, but be defensive here).

    Pure function — no I/O. Iterates ``policy_set.policies`` once.
    """
    matching = [p for p in policy_set.policies if p.selector.matches(file)]
    if not matching:
        return None

    # ``min`` with a sort key works because higher priority and higher
    # specificity should win — negate them so that ``min`` selects them
    # while the lexicographic ``name`` is left positive. This gives a
    # single-pass selection without an explicit ``sorted()``.
    return min(
        matching,
        key=lambda p: (-p.priority, -p.selector.specificity(), p.name),
    )


def next_due_phase(file: Any, policy: Policy, now: datetime) -> Phase | None:
    """Return the next phase due for ``file`` under ``policy``, or ``None``.

    The file's age is computed as ``(now - file.oldest_record_ts)`` in
    days (fractional). ``now`` is supplied by the caller so the function
    stays pure (no ``datetime.now()`` inside) — freezegun is unnecessary.

    A phase is returned if **both**:
      * ``phase.after_days <= file_age_days`` (it's time), AND
      * it has not already been applied to this file.

    "Already applied" rules:
      * ``promote``/``compress``/``archive`` with ``target_tier=T`` is
        already applied if the file is currently at tier T or any later
        tier in the hot → warm → cold → archive → pending order.
      * ``delete`` is already applied only when the file is at tier
        ``pending`` (mid-delete) — in that case the function returns
        ``None`` (no further action; sweeper will finish).

    Returns ``None`` if the file is too young for the first phase or
    if every phase has already been applied (lifecycle complete).
    """
    age_days = (now - file.oldest_record_ts).total_seconds() / 86400.0
    current_tier_idx = _tier_index(file.tier)

    for phase in policy.phases:
        if phase.after_days > age_days:
            # Phases are sorted ascending — anything beyond this is
            # also too far in the future. Done.
            return None

        if _is_phase_already_applied(phase, current_tier_idx):
            continue

        return phase

    # Every phase has matured AND been applied; lifecycle complete.
    return None


def _is_phase_already_applied(phase: Phase, current_tier_idx: int) -> bool:
    """Decide whether ``phase`` has already taken effect for a file
    currently sitting at ``current_tier_idx``.

    See ``next_due_phase`` docstring for the rules; this helper keeps
    the dispatch readable.
    """
    if phase.action == "delete":
        # The delete phase is special: it doesn't move a file to a
        # higher tier — it queues a sweep. We consider it applied only
        # when the file has already been moved into ``pending`` (the
        # mark-then-sweep limbo). Otherwise it remains the next-due
        # action even at archive tier.
        return current_tier_idx == _TIER_ORDER["pending"]

    # promote / compress / archive — all three end with the file
    # residing at ``target_tier``. If the file is already at that tier
    # or beyond, this phase contributed nothing new.
    target_tier = phase.target_tier
    if target_tier is None:
        # A non-delete phase without a target tier is not actionable —
        # treat it as already applied so we skip past it. The Pydantic
        # schema allows ``target_tier`` to be None on Phase, but the
        # promote/compress/archive actions all require it in practice.
        return True

    target_idx = _TIER_ORDER[target_tier]
    return current_tier_idx >= target_idx
