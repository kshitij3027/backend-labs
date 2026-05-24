"""Atomic filesystem moves + ``PendingDelete`` queue coordination.

The retention applier (C10) needs three filesystem primitives that the
storage layer must guarantee are crash-consistent:

  1. **Stage-then-promote**: copy the source segment to a ``.tmp``
     sidecar in the destination tier directory, then ``os.replace`` the
     tmp file onto the final name. The ``replace`` step is atomic on
     POSIX when src and dst sit on the same filesystem, so a crash
     between the copy and the rename leaves only the half-written ``.tmp``
     behind — never a half-written ``dst``.

  2. **Mark-for-delete**: once the new tier copy is in place, atomically
     rename the original segment into the ``pending/`` tier and enqueue a
     ``PendingDelete`` row carrying the grace window. The sweeper drains
     this queue on its own schedule, which gives us a recovery window
     against bad policy.

  3. **Sweep**: a tick that walks rows with elapsed grace windows,
     ``unlink``s the file (tolerating already-gone files), and removes
     the row. Errors on individual files do not abort the pass.

All three helpers assume the storage root lives on a single filesystem
(in our docker setup it is bind-mounted to ``./data/tiers/`` — so the
hot/warm/cold/archive/pending dirs all share an inode space). If that
ever changes (e.g. archive on object storage), the ``os.replace``
guarantee no longer holds and the implementation here must grow a
fallback. The docstring on each function repeats the assumption.

This module is plain blocking I/O — it is meant to be called from inside
an async coroutine that has already decided what to move where. Wrapping
each call in ``asyncio.to_thread`` is the caller's call (C10 currently
runs the apply step directly because the file sizes are small and the
scheduler tick is bounded to one apply at a time via APScheduler's
``max_instances=1``).
"""
from __future__ import annotations

import logging
import os
import shutil
import uuid
from datetime import datetime
from pathlib import Path

from src.persistence.models import PendingDelete
from src.storage.tiers import tier_dir

# Module-level logger — ``sweep_deletes`` swallows individual unlink
# failures to keep the pass alive, but each one is logged at WARNING so
# repeated breakage is visible in the container log stream.
logger = logging.getLogger(__name__)


def stage_and_promote(src: Path | str, dst: Path | str) -> None:
    """Copy ``src`` to a ``.tmp`` sidecar of ``dst``, then atomic-rename.

    Steps:

      1. Compute ``tmp = dst + ".tmp"``.
      2. Ensure ``dst.parent`` exists (idempotent ``mkdir -p``).
      3. Stream-copy ``src`` -> ``tmp`` via :func:`shutil.copy2` so file
         metadata (mtime / mode) is preserved on the destination tier.
      4. ``os.replace(tmp, dst)`` — POSIX-atomic same-filesystem rename
         that overwrites any pre-existing ``dst``. This makes the
         function idempotent against re-runs: applying the same
         transition twice produces the same end state.

    **Same-filesystem assumption.** ``os.replace`` is only atomic when
    src and dst are on the same filesystem. In this project every tier
    lives under ``/app/data/tiers/`` (same bind mount), so the guarantee
    holds. Calling code that introduces cross-filesystem tiers must add
    a fallback (write + fsync + rename + verify) before the atomicity
    claim survives.

    On any exception during the copy, the partial ``.tmp`` MAY still
    exist; we make a best-effort attempt to remove it so a retry starts
    from a clean slate, and then re-raise the original exception so the
    caller can mark the transition failed and surface the cause.
    """
    src_path = Path(src)
    dst_path = Path(dst)
    # Tack ``.tmp`` onto the existing suffix rather than replacing it so
    # ``segment-1.jsonl`` becomes ``segment-1.jsonl.tmp`` — preserving the
    # original extension in the staged name aids debugging if a tmp ever
    # gets left behind.
    tmp_path = dst_path.with_suffix(dst_path.suffix + ".tmp")

    # Idempotent parent creation; safe to call when the dir already exists.
    dst_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        # ``copy2`` (not ``copy``) preserves mtime / mode bits so the
        # promoted copy looks identical to the source under ``ls -l``.
        # We copy rather than hardlink/symlink because the caller will
        # subsequently ``mark_for_delete`` the source — a hardlink would
        # decouple at unlink time, but a fresh copy is cheaper to reason
        # about and decouples the on-disk identity immediately.
        shutil.copy2(src_path, tmp_path)
    except Exception:
        # Best-effort cleanup. ``missing_ok=True`` tolerates the case
        # where ``copy2`` failed before creating ``tmp_path`` at all.
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            # Don't mask the original exception with a cleanup failure.
            pass
        raise

    # ``os.replace`` overwrites an existing ``dst`` atomically. We pass
    # ``str(...)`` because some older POSIX implementations of the
    # underlying ``rename(2)`` syscall reject path-like objects when
    # routed through certain libc shims; the explicit cast is harmless.
    os.replace(str(tmp_path), str(dst_path))


async def mark_for_delete(
    catalog_repo,
    file_id: int,
    src: Path | str,
    storage_root: Path | str,
    delete_after: datetime,
) -> PendingDelete:
    """Atomically move ``src`` into the ``pending/`` tier and enqueue it.

    The pending tier acts as a holding pen with a grace window: nothing
    in it is referenced by the catalog's tier column (it is dropped from
    ``list_due_files``), but the file remains on disk until the sweeper
    confirms ``delete_after <= now``. That window lets a human catch a
    bad policy push before bits are gone.

    The function performs:

      1. Compute the ``pending/`` directory and ensure it exists.
      2. Compute the destination filename: ``pending/<src.name>``. Names
         already include a unix timestamp (``segment-<unix_ts>.jsonl``),
         so collisions are vanishingly rare; if one does occur (e.g. a
         prior bad sweep left the same name behind), append an 8-char
         random suffix to disambiguate.
      3. ``os.replace`` the file into pending — atomic, same FS.
      4. Insert a ``PendingDelete`` row via the catalog so the sweeper
         knows about it.

    Returns the inserted ``PendingDelete`` row (with id + ``created_at``
    populated) so the caller can log it / attach it to an audit entry.
    """
    src_path = Path(src)
    pending_dir = tier_dir(storage_root, "pending")
    pending_dir.mkdir(parents=True, exist_ok=True)

    pending_path = pending_dir / src_path.name
    if pending_path.exists():
        # Disambiguate via short uuid — keeps the original name as a
        # prefix for human readability when scanning ``ls pending/``.
        suffix = uuid.uuid4().hex[:8]
        pending_path = pending_dir / f"{src_path.name}_{suffix}"

    # Same-filesystem assumption applies here too (see ``stage_and_promote``
    # docstring). All tiers live under the same storage root in this project.
    os.replace(str(src_path), str(pending_path))

    return await catalog_repo.add_pending_delete(
        file_id=file_id,
        path=str(pending_path),
        delete_after=delete_after,
    )


async def sweep_deletes(catalog_repo, now: datetime) -> list[int]:
    """Drain the ``pending_deletes`` queue for rows whose grace has expired.

    For each due row:

      * ``unlink`` the file at ``row.path`` (``missing_ok=True`` so an
        already-gone file does not stop the pass).
      * Delete the row via the catalog.
      * Append the row's id to the returned list.

    Errors during ``unlink`` (e.g. permission denied) are logged at
    WARNING and we still remove the row — a stuck file that the sweeper
    cannot delete would otherwise block the queue forever. That is a
    deliberate trade-off: visibility (the log line + operator alerting
    on it) over silent retry. If undeletable files become common, the
    right fix is a separate quarantine table, not retries inside the
    sweep loop.

    Returns the list of processed ``row.id`` values — useful for the
    sweeper telemetry summary written to ``job_runs``. The list is in
    the order rows were processed (oldest ``delete_after`` first).
    """
    due = await catalog_repo.list_pending_deletes_due(now)
    processed: list[int] = []

    for row in due:
        try:
            Path(row.path).unlink(missing_ok=True)
        except OSError as exc:
            # File present but un-unlinkable (permissions, EBUSY, etc).
            # Log loudly, but still remove the row — keeping the queue
            # moving matters more than this one file's bytes.
            logger.warning(
                "sweep_deletes: unlink failed for pending_delete id=%s path=%s: %s",
                row.id,
                row.path,
                exc,
            )
        except Exception as exc:
            # Unexpected: log at ERROR but still drop the row so a single
            # bad path cannot brick the whole sweep.
            logger.exception(
                "sweep_deletes: unexpected error unlinking pending_delete id=%s path=%s: %s",
                row.id,
                row.path,
                exc,
            )

        try:
            await catalog_repo.delete_pending_delete_by_id(row.id)
            processed.append(row.id)
        except Exception as exc:
            # If the row delete itself fails, we cannot claim the id as
            # processed (next pass will see it again). Log and continue.
            logger.exception(
                "sweep_deletes: failed to delete pending_delete row id=%s: %s",
                row.id,
                exc,
            )

    return processed
