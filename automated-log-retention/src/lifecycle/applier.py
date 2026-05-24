"""Lifecycle applier job — execute pending ``Transition`` rows.

The applier is the second of three lifecycle jobs the APScheduler runs
on a tick:

  * ``scan_job`` (C09) — pick due files, plan ``Transition`` rows.
  * ``apply_job`` (C10, this module) — drain pending ``Transition`` rows,
    moving each file on disk (with compression for cold/archive) and
    updating the catalog + transition status atomically.
  * ``sweep_job`` (C11) — hard-delete the previous tier copies that the
    applier left behind in ``pending/`` once their grace window expires.

Per-transition dispatch:

  * ``delete`` → :func:`mover.mark_for_delete` (mark-then-sweep handoff
    to the sweeper); the file goes straight to ``tiers/pending/``.
  * ``promote`` → :func:`mover.stage_and_promote` (plain copy + atomic
    rename) into the target tier; no compression.
  * ``compress`` / ``archive`` (or any move where ``to_tier`` is
    ``cold`` / ``archive``) → :func:`compressor.compress_file` writing a
    ``.zst`` sidecar at the destination tier path; level is
    ``DEFAULT_COLD_LEVEL`` for cold, ``DEFAULT_ARCHIVE_LEVEL`` for
    archive.

After a successful non-delete move, the **source segment is itself
queued for delete** via :func:`mover.mark_for_delete` — the new tier
copy is the live one, and the original at the previous tier becomes
landfill. The catalog's ``segment_path`` is updated to the new location
in the same pass, so any reader following the catalog will follow the
new pointer.

**Failure isolation.** Each transition is wrapped in its own try/except.
An exception during one transition (file missing, disk full, mid-flight
rename race) is recorded on that transition row (``status='failed'``,
``error=<str(exc)>``) and the loop continues with the next row. A single
bad transition does NOT abort the whole pass — bounded blast radius is
the whole point of the per-row status column.

**Tier-mismatch defence.** If the catalog says the file is at a tier
other than the transition's ``from_tier`` (e.g. another applier pass
already handled this transition, or a scanner replanned stale work),
the row is marked failed with a ``tier_mismatch:...`` error and skipped.
This is a soft failure: the data is fine, only the plan is stale.

All ``datetime`` values are caller-supplied (``now`` argument) so the
function stays pure and trivially testable without ``freezegun``.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from src.storage.catalog import CatalogRepo
from src.storage.compressor import (
    DEFAULT_ARCHIVE_LEVEL,
    DEFAULT_COLD_LEVEL,
    compress_file,
)
from src.storage.mover import mark_for_delete, stage_and_promote
from src.storage.tiers import compose_segment_path

# Module logger — per-transition failures are logged at exception level
# (full traceback) so a misconfigured policy or filesystem hiccup leaves
# a recoverable trail in the container log stream; the structured
# ``error`` column gives the dashboard a one-liner the operator sees
# without ssh-ing in.
logger = logging.getLogger(__name__)

# Tiers whose contents are stored compressed. A move whose ``to_tier``
# is one of these implies the applier must run the segment through zstd
# regardless of whether the planning ``action`` happened to be named
# ``compress`` or ``archive`` (a future policy author could plausibly
# call a warm→cold step ``promote`` — the on-disk layout still demands
# compression). Mapping the level from the destination tier (not from
# the action verb) keeps the source-of-truth in one place.
_COMPRESSED_TIERS: frozenset[str] = frozenset({"cold", "archive"})


@dataclass(frozen=True)
class ApplyReport:
    """Structured outcome of one :func:`apply_once` call.

    Fields:
      * ``applied`` — count of ``Transition`` rows whose move succeeded
        AND whose post-state (catalog updated, source queued for delete)
        landed cleanly. Each contributes exactly one row's
        ``status='applied'`` write.
      * ``failed`` — count of ``Transition`` rows whose execution raised
        OR whose precondition check (tier mismatch, missing file) failed.
        Each contributes exactly one row's ``status='failed'`` write,
        with a short error message captured on the row.

    The sum ``applied + failed`` equals the number of transitions the
    pass dispatched (i.e. the length of the catalog's
    ``list_pending_transitions`` page, capped by ``batch_limit``).
    """

    applied: int
    failed: int


def _compute_dst_name(src_name: str, to_tier: str, action: str) -> str:
    """Compute the destination segment filename for a planned move.

    Strategy:
      * If the destination tier stores compressed data (``cold`` /
        ``archive``), append ``.zst`` unless the name already ends with
        it (idempotent — a second move within the compressed tier
        family keeps the existing extension).
      * Otherwise (plain promote into ``hot`` / ``warm``), preserve the
        original basename verbatim. A file moving from cold (``.zst``)
        back to warm via a hypothetical decompress action would keep its
        ``.zst`` extension; that's intentional because the applier does
        not currently decompress on demote. If such a path is ever added,
        the caller is responsible for stripping the extension before
        invoking the applier.

    ``action`` is accepted for completeness / future extension but the
    current logic relies solely on ``to_tier`` to stay consistent with
    ``_COMPRESSED_TIERS`` — see that constant's docstring for the
    rationale.
    """
    if to_tier in _COMPRESSED_TIERS and not src_name.endswith(".zst"):
        return src_name + ".zst"
    return src_name


async def apply_once(
    catalog_repo: CatalogRepo,
    storage_root: Path | str,
    now: datetime,
    *,
    batch_limit: int = 100,
    delete_delay_hours: int = 24,
) -> ApplyReport:
    """Execute one batch of pending ``Transition`` rows.

    Args:
        catalog_repo: source of truth for the files + transitions tables;
            also receives the post-execution status + tier updates.
        storage_root: filesystem root containing the 5 tier directories.
            Must be the same root used by ingest / scanner / sweeper so
            ``mark_for_delete`` lands files in the correct ``pending/``
            queue.
        now: the scheduler's tick time (UTC). Passed explicitly so the
            applier stays pure and trivially testable.
        batch_limit: hard cap on the number of transitions executed per
            pass (default 100). Transitions beyond the cap wait for the
            next tick — the queue is drained oldest-first.
        delete_delay_hours: grace window (in hours) added to ``now`` to
            compute each queued source-segment's ``delete_after`` field.
            Default 24 h gives an operator a full day to catch a bad
            policy push before bytes are unlinked.

    Returns:
        An :class:`ApplyReport` summarising the pass. Suitable to persist
        as the ``summary_json`` on a ``job_runs`` row in C12.
    """
    pending = await catalog_repo.list_pending_transitions(limit=batch_limit)

    applied = 0
    failed = 0
    delete_after = now + timedelta(hours=delete_delay_hours)

    for transition in pending:
        try:
            # 1. Fetch the file row — if the catalog has lost track of
            #    the file (shouldn't happen under normal flow because
            #    files outlive their transitions, but defensive against
            #    test data and future race conditions), mark the
            #    transition failed and continue.
            file = await catalog_repo.get_file(transition.file_id)
            if file is None:
                await catalog_repo.update_transition_status(
                    transition.id,
                    "failed",
                    executed_at=now,
                    error="file_not_found",
                )
                failed += 1
                continue

            # 2. Tier-mismatch guard. If another applier pass (or a
            #    manual fix) already moved this file, the planned
            #    ``from_tier`` no longer matches reality. Bail out
            #    soft-failure with a descriptive error so the operator
            #    can see why the row was dropped.
            if file.tier != transition.from_tier:
                msg = (
                    f"tier_mismatch: file is at {file.tier}, "
                    f"transition expected {transition.from_tier}"
                )
                logger.warning(
                    "apply_once: transition %d skipped: %s",
                    transition.id,
                    msg,
                )
                await catalog_repo.update_transition_status(
                    transition.id,
                    "failed",
                    executed_at=now,
                    error=msg,
                )
                failed += 1
                continue

            src_path = Path(file.segment_path)

            # 3. Dispatch on the action verb. ``delete`` is the simplest
            #    path — it skips the stage-and-promote step entirely and
            #    just queues the file for sweep.
            if transition.action == "delete":
                pending_row = await mark_for_delete(
                    catalog_repo,
                    file_id=file.id,
                    src=src_path,
                    storage_root=storage_root,
                    delete_after=delete_after,
                )
                # The catalog must reflect the file's new location so
                # any reader (dashboard, future query route) following
                # ``segment_path`` reaches the right bytes. We don't
                # change ``size_bytes`` because the bytes themselves
                # haven't shrunk — only their tier label has.
                await catalog_repo.update_tier(
                    file.id,
                    "pending",
                    pending_row.path,
                    file.size_bytes,
                )
            else:
                # promote / compress / archive — all three end with a
                # new file at ``to_tier`` and the old file marked for
                # delete. The two paths inside this branch differ only
                # in how the new file is produced (plain copy vs. zstd
                # stream).
                dst_name = _compute_dst_name(
                    src_path.name, transition.to_tier, transition.action
                )
                dst_path = compose_segment_path(
                    storage_root, transition.to_tier, dst_name
                )
                # Ensure the destination tier dir exists. ``mover`` /
                # ``compressor`` would both bomb on a missing parent
                # otherwise; the storage layer normally calls
                # ``ensure_tier_dirs`` once at boot, but doing it again
                # here costs nothing and keeps the applier robust in
                # tests that don't go through full lifespan startup.
                dst_path.parent.mkdir(parents=True, exist_ok=True)

                if transition.to_tier in _COMPRESSED_TIERS:
                    # Pick the right level from the *destination* tier,
                    # not the action verb — see ``_COMPRESSED_TIERS``
                    # docstring for why.
                    level = (
                        DEFAULT_ARCHIVE_LEVEL
                        if transition.to_tier == "archive"
                        else DEFAULT_COLD_LEVEL
                    )
                    new_size = compress_file(src_path, dst_path, level)
                else:
                    # Plain promote — atomic copy + rename, no
                    # compression. Read the new size off the destination
                    # after the rename has landed.
                    stage_and_promote(src_path, dst_path)
                    new_size = dst_path.stat().st_size

                # Update the catalog to point at the new tier copy.
                # This happens BEFORE the source is queued for delete,
                # so any concurrent reader following the catalog will
                # see the new copy as soon as the row is updated.
                await catalog_repo.update_tier(
                    file.id,
                    transition.to_tier,
                    str(dst_path),
                    new_size,
                )

                # Source is still on disk (``stage_and_promote`` and
                # ``compress_file`` both leave it untouched). Queue it
                # for delete via the same mark-then-sweep handoff the
                # delete action uses. The sweeper drains the queue on
                # its own tick.
                await mark_for_delete(
                    catalog_repo,
                    file_id=file.id,
                    src=src_path,
                    storage_root=storage_root,
                    delete_after=delete_after,
                )

        except Exception as exc:
            # Per-transition isolation. Anything that escapes the inner
            # try/except dispatch above gets a full traceback in the log
            # (logger.exception) plus a short message persisted on the
            # transition row for the dashboard.
            logger.exception(
                "apply_once: transition %d failed", transition.id
            )
            try:
                await catalog_repo.update_transition_status(
                    transition.id,
                    "failed",
                    executed_at=now,
                    error=str(exc),
                )
            except Exception:
                # If even the status write fails (e.g. DB unavailable),
                # log and move on — the next pass will re-pick the row
                # because it's still ``pending``. The pass-level counter
                # still increments so the report reflects the failure.
                logger.exception(
                    "apply_once: failed to record failure for transition %d",
                    transition.id,
                )
            failed += 1
            continue

        # Happy path — record success.
        await catalog_repo.update_transition_status(
            transition.id, "applied", executed_at=now
        )
        applied += 1

    return ApplyReport(applied=applied, failed=failed)
