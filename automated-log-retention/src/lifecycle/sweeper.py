"""Lifecycle sweeper job — drain the ``pending_deletes`` queue.

The sweeper is the third (and simplest) of the three lifecycle jobs the
APScheduler runs on a tick:

  * ``scan_job`` (C09) — pick due files, plan ``transitions``.
  * ``apply_job`` (C10) — execute pending ``transitions``, leaving the
    source segment behind in the ``pending/`` tier with a grace-window
    row in ``pending_deletes``.
  * ``sweep_job`` (C11, this module) — for each ``pending_deletes`` row
    whose ``delete_after`` has elapsed, ``unlink`` the file and drop the
    row.

The actual filesystem + DB work is owned by :func:`storage.mover.sweep_deletes`
(see C08). The sweeper here is a deliberately thin wrapper that:

  * runs one sweep pass,
  * counts what was processed,
  * returns a structured :class:`SweepReport` the scheduler can write
    into the ``job_runs`` table for the dashboard's recency display.

**No audit-entry emission here.** Per the plan, audit entries for hard
deletes are wired in C13 once the audit chain exists — this commit only
stands up the sweep accounting and report. ``errors=0`` is hard-coded
for the same reason: ``sweep_deletes`` already swallows per-row errors
and logs them at WARNING / ERROR; it returns only the successfully
processed ids and does not surface a separate error count. A future
enhancement noted in the plan is to have ``sweep_deletes`` return a
``(processed_ids, error_count)`` pair so this report can carry a real
``errors`` value — that change belongs with the audit-emission work in
C13 rather than here.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime

from src.storage.catalog import CatalogRepo
from src.storage.mover import sweep_deletes

# Module logger — the scheduler tick logs the report summary at INFO; the
# per-row WARNING/ERROR lines come from ``sweep_deletes`` itself so they
# carry the file path that failed (more useful than a per-pass count).
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SweepReport:
    """Structured outcome of one :func:`sweep_once` call.

    ``swept`` is the number of ``pending_deletes`` rows the pass cleared
    (file unlinked + row removed). ``errors`` is the number of rows that
    failed mid-pass — currently always 0 because the underlying
    :func:`storage.mover.sweep_deletes` does not return an error count;
    when C13 lifts that, this field becomes accurate without changing
    the public surface of the dataclass.
    """

    swept: int
    errors: int = 0


async def sweep_once(catalog_repo: CatalogRepo, now: datetime) -> SweepReport:
    """Run one sweep pass and return a :class:`SweepReport`.

    Delegates the per-row unlink + row-delete work to
    :func:`storage.mover.sweep_deletes`, which already:

      * Walks ``pending_deletes`` rows with ``delete_after <= now``
        oldest first, capped by an internal ``limit`` to bound the pass.
      * Tolerates already-missing files (``missing_ok=True``).
      * Logs per-row failures at WARNING / ERROR without aborting the
        rest of the pass.

    The wrapper's only job is to count the result and surface it in a
    typed dataclass the scheduler can persist to ``job_runs`` (and the
    dashboard can render under "last sweep").
    """
    processed_ids = await sweep_deletes(catalog_repo, now)
    logger.info("sweep_once swept %d pending deletes", len(processed_ids))
    # ``errors=0`` is intentional for this commit — see module docstring.
    return SweepReport(swept=len(processed_ids))
