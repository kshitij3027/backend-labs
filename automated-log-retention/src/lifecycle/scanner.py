"""Lifecycle scanner job — plan retention work into the ``transitions`` queue.

The scanner is the first of three lifecycle jobs the APScheduler runs on
a tick:

  * ``scan_job`` (C09, this module) — pick due files, decide what should
    happen next via the C05 matcher, insert pending ``Transition`` rows.
  * ``apply_job`` (C10) — execute pending ``transitions`` (filesystem
    work + tier mutation).
  * ``sweep_job`` (C11) — hard-delete files that have aged out of the
    ``pending_deletes`` grace window.

This module is **pure orchestration**: no filesystem I/O, no compression,
no audit emission. It runs against the database only:

  1. Pulls a batch of due files from the catalog
     (:meth:`CatalogRepo.list_due_files`).
  2. For each file, asks the matcher which policy wins
     (:func:`pick_policy`).
  3. Computes the next-due phase (:func:`next_due_phase`).
  4. Inserts a single pending ``Transition`` row capturing the planned
     move; or marks the file for re-check (``next_eval_at`` bump) when
     no policy matches; or retires the file (``next_eval_at = None``)
     when every phase has been applied.

The applier owns the actual tier flip — the scanner simply queues the
work. We deliberately keep the **scanner stateless about prior
transitions**: even if a pending ``Transition`` row already exists for
a file, the scanner will plan another. The applier is the dedupe point
(it inspects the file's current tier before acting). This keeps the
scanner cheap and lets the per-job intervals be tuned independently —
documented in ``test_scan_currently_plans_even_with_existing_pending_transition``.

All ``datetime`` values are caller-supplied (``now`` argument) so the
function stays trivially testable without ``freezegun``.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from src.policy.matcher import next_due_phase, pick_policy
from src.policy.schema import PolicySet
from src.storage.catalog import CatalogRepo


@dataclass(frozen=True)
class ScanReport:
    """Structured outcome of one :func:`scan_once` call.

    Fields:
      * ``scanned`` — count of due-file rows the pass inspected. Equals
        the length of the catalog's ``list_due_files`` page, so a value
        equal to ``batch_limit`` is a hint the operator should shorten
        the scheduler interval or raise the limit.
      * ``transitions_planned`` — count of pending ``Transition`` rows
        the pass inserted (one per file that matched a policy and had a
        non-terminal next-due phase).
      * ``unmatched`` — files in the page whose category/source/level did
        not satisfy any policy selector. Each was rescheduled
        ``no_match_recheck`` into the future so a later policy reload
        can pick them up.
      * ``terminal`` — files whose lifecycle has completed (every phase
        already applied). Their ``next_eval_at`` was cleared to
        ``NULL`` so they no longer appear in ``list_due_files``.
      * ``rescheduled`` — files whose matched policy has a non-applied
        phase still ahead in time (e.g. a 90-day archive phase for a
        35-day-old file). The matcher returns ``None`` in this case too,
        but the file is *not* terminal — we recompute the next phase's
        exact due time and bump ``next_eval_at`` to that moment so the
        file returns to the scanner queue precisely when it's needed.

    The sum
    ``transitions_planned + unmatched + terminal + rescheduled`` equals
    ``scanned``.
    """

    scanned: int
    transitions_planned: int
    unmatched: int
    terminal: int
    rescheduled: int = 0


# After a transition is planned for a file, we want the file to be
# re-checked on the next scanner tick once the applier has had a chance
# to flip its tier. A 1-day recheck window is generous on its own; the
# scheduler's ``next_eval_at <= now`` filter still lets the file return
# to the queue exactly when due. The number is intentionally an
# implementation detail (not configurable) — the scanner is a planner,
# not a control loop.
_POST_PLAN_RECHECK = timedelta(days=1)


def _next_future_phase_due_at(file, policy, now):
    """Return the UTC datetime when policy's earliest unfired, future phase
    is due — or None if every phase has already been applied (i.e. the file
    has reached its policy's terminal state).

    Used by scan_once to distinguish 'lifecycle complete' (None → retire
    file) from 'no phase due yet but more to come' (schedule re-check at
    the future phase's due time).
    """
    from src.policy.matcher import _TIER_ORDER
    current_idx = _TIER_ORDER[file.tier]
    age_days = (now - file.oldest_record_ts).total_seconds() / 86400.0
    for phase in policy.phases:
        if phase.action == "delete":
            applied = current_idx == _TIER_ORDER["pending"]
        else:
            target_tier = phase.target_tier or file.tier
            applied = current_idx >= _TIER_ORDER[target_tier]
        if not applied and phase.after_days > age_days:
            return file.oldest_record_ts + timedelta(days=phase.after_days)
    return None


async def scan_once(
    catalog_repo: CatalogRepo,
    policy_set: PolicySet,
    now: datetime,
    *,
    batch_limit: int = 1000,
    no_match_recheck: timedelta = timedelta(days=1),
) -> ScanReport:
    """Plan retention work for one batch of due files.

    Args:
        catalog_repo: source of truth for the files table; also receives
            the planned transition + ``next_eval_at`` updates.
        policy_set: the parsed YAML policy set (frozen at lifespan
            startup; the scanner does not reload it mid-tick).
        now: the scheduler's tick time (UTC). Passed explicitly so the
            scanner stays pure and trivially testable.
        batch_limit: hard cap on the number of files inspected per pass.
            Files beyond the cap simply wait for the next tick.
        no_match_recheck: how far in the future to push the
            ``next_eval_at`` of a file whose category/source/level does
            not match any current policy. The default 1-day window means
            a policy reload picks the file up within a day without
            requiring an immediate full rescan.

    Returns:
        A :class:`ScanReport` summarising the pass. Suitable to persist
        as the ``summary_json`` on a ``job_runs`` row in C12.
    """
    due_files = await catalog_repo.list_due_files(now, limit=batch_limit)

    transitions_planned = 0
    unmatched = 0
    terminal = 0
    rescheduled = 0

    for file in due_files:
        policy = pick_policy(file, policy_set)

        if policy is None:
            # Nothing matches today — but a policy reload might rescue
            # the file tomorrow. Push next_eval_at out so the scanner
            # does not keep re-evaluating the same orphan every tick.
            unmatched += 1
            await catalog_repo.mark_evaluated(file.id, now + no_match_recheck)
            continue

        phase = next_due_phase(file, policy, now)

        if phase is None:
            # The matcher returns None for two distinct cases:
            #   (a) every phase has already been applied — file is at
            #       its policy's terminal state, retire it forever.
            #   (b) no phase is currently due, but at least one phase
            #       still lies ahead in time — keep the file in the
            #       queue, but precise-schedule it for the exact
            #       moment that future phase becomes due.
            # ``_next_future_phase_due_at`` returns the schedule time
            # for case (b) and None for case (a).
            future_due = _next_future_phase_due_at(file, policy, now)
            if future_due is None:
                terminal += 1
                await catalog_repo.mark_evaluated(file.id, None)
            else:
                rescheduled += 1
                await catalog_repo.mark_evaluated(file.id, future_due)
            continue

        # Plan the move. For non-delete actions the destination is the
        # phase's ``target_tier`` directly; for the delete action the
        # applier first moves the file into ``pending`` (mark-then-sweep
        # handoff to the sweeper), so the recorded ``to_tier`` is
        # ``"pending"`` rather than ``None``. The action field carries
        # the original verb so the applier can dispatch on it.
        from_tier = file.tier
        if phase.action == "delete":
            to_tier = "pending"
        else:
            # Schema invariant: promote/compress/archive phases always
            # have a target_tier (Pydantic does not enforce it but the
            # matcher's already-applied logic relies on it). If a policy
            # author somehow lands a non-delete phase without one, fall
            # back to the file's current tier so the row is at least
            # well-formed; the applier will surface the misconfig.
            to_tier = phase.target_tier or file.tier

        await catalog_repo.add_transition(
            file_id=file.id,
            from_tier=from_tier,
            to_tier=to_tier,
            action=phase.action,
        )
        transitions_planned += 1

        # The applier will mutate this file's tier on its next tick; we
        # want the scanner to re-check the file shortly after so any
        # follow-on phase (e.g. warm → cold) is queued without waiting
        # for the next natural eval window. The applier is responsible
        # for the actual tier flip; the scanner only plans.
        await catalog_repo.mark_evaluated(file.id, now + _POST_PLAN_RECHECK)

    return ScanReport(
        scanned=len(due_files),
        transitions_planned=transitions_planned,
        unmatched=unmatched,
        terminal=terminal,
        rescheduled=rescheduled,
    )
