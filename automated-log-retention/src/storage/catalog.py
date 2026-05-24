"""Async catalog repository wrapping the SQLAlchemy session factory.

The ``files`` table is the source of truth for every segment under
retention management — the lifecycle scanner never walks the filesystem.
This module centralises every query the scanner / applier / dashboard
runs against that table so callers don't open ad-hoc sessions or write
inline SQL.

Design notes:

  * Every method opens its own session via ``async with self._sf()``.
    That makes each call self-contained — no shared transaction state
    between methods, which suits the short-lived job runs (scan/apply/
    sweep) the scheduler drives.
  * ``add_file`` flushes + refreshes so the caller gets the autoincrement
    ``id`` populated on the returned ORM instance.
  * Aggregation methods (``count_by_tier``, ``total_bytes_by_tier``)
    return a dict keyed by *every* tier name in ``TIERS``, defaulting to
    0 for tiers with no rows — the dashboard expects all 5 keys present.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.persistence.models import File, PendingDelete, Transition
from src.storage.tiers import TIERS


class CatalogRepo:
    """Async repository over the ``files`` table.

    All methods are coroutines and each opens its own session. The
    constructor takes the ``async_sessionmaker`` (not a single session)
    so a long-lived repo instance can serve many independent operations
    across the scheduler's lifetime.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory

    async def add_file(
        self,
        *,
        source: str,
        segment_path: str,
        tier: str,
        size_bytes: int,
        oldest_record_ts: datetime,
        newest_record_ts: datetime,
        compliance_tag: str | None = None,
        immutable: bool = False,
        next_eval_at: datetime | None = None,
    ) -> File:
        """Insert a new ``File`` row and return the refreshed ORM instance.

        ``refresh`` is called after the commit so the returned object has
        ``id`` and any server-defaulted columns (``created_at``)
        populated — the ingest path needs the id to enqueue follow-up
        work in the same coroutine.
        """
        async with self._sf() as session:
            f = File(
                source=source,
                segment_path=segment_path,
                tier=tier,
                size_bytes=size_bytes,
                oldest_record_ts=oldest_record_ts,
                newest_record_ts=newest_record_ts,
                compliance_tag=compliance_tag,
                immutable=immutable,
                next_eval_at=next_eval_at,
            )
            session.add(f)
            await session.commit()
            await session.refresh(f)
            return f

    async def get_file(self, file_id: int) -> File | None:
        """Fetch a single ``File`` row by id, or None if missing."""
        async with self._sf() as session:
            result = await session.execute(
                select(File).where(File.id == file_id)
            )
            return result.scalar_one_or_none()

    async def mark_evaluated(
        self, file_id: int, next_eval_at: datetime | None
    ) -> None:
        """Set ``next_eval_at`` on a file (may be ``None`` to retire it).

        The scanner calls this after planning (or skipping) a file so the
        next pass can skip files that aren't ready or whose lifecycle
        has terminated.
        """
        async with self._sf() as session:
            await session.execute(
                update(File)
                .where(File.id == file_id)
                .values(next_eval_at=next_eval_at)
            )
            await session.commit()

    async def update_tier(
        self,
        file_id: int,
        new_tier: str,
        new_path: str,
        new_size: int,
    ) -> None:
        """Atomically update ``tier``, ``segment_path``, and ``size_bytes``.

        Called by the applier after a successful ``os.rename`` so the
        catalog reflects the new on-disk location and post-compression
        size in a single round-trip.
        """
        async with self._sf() as session:
            await session.execute(
                update(File)
                .where(File.id == file_id)
                .values(
                    tier=new_tier,
                    segment_path=new_path,
                    size_bytes=new_size,
                )
            )
            await session.commit()

    async def list_files(
        self,
        tier: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[File]:
        """Return a page of files, newest first, optionally filtered by tier.

        Used by ``GET /v1/files`` (C12) to surface catalog rows to
        operators and tests. The ordering (``id DESC``) means recently
        ingested segments appear first, which matches how an operator
        triaging "what just landed?" reads a list. Filter by ``tier`` to
        narrow to one stage (``hot`` / ``warm`` / ``cold`` / ``archive``
        / ``pending``).
        """
        async with self._sf() as session:
            stmt = select(File)
            if tier is not None:
                stmt = stmt.where(File.tier == tier)
            stmt = stmt.order_by(File.id.desc()).limit(limit).offset(offset)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def count_files(self, tier: str | None = None) -> int:
        """Return the total file count, optionally filtered by tier.

        Used together with ``list_files`` to render paging UI — the
        page itself comes from ``list_files``; the ``total`` field of
        the API response is this method's output.
        """
        async with self._sf() as session:
            stmt = select(func.count(File.id))
            if tier is not None:
                stmt = stmt.where(File.tier == tier)
            result = await session.execute(stmt)
            return int(result.scalar_one())

    async def list_due_files(
        self, now: datetime, limit: int = 1000
    ) -> list[File]:
        """Return files whose ``next_eval_at`` has elapsed.

        Excludes the ``pending`` tier (already on its way out via the
        sweeper) and files with ``next_eval_at IS NULL`` (terminal
        phase). Ordered by ``next_eval_at`` ascending so the oldest
        overdue files get picked up first; capped by ``limit`` to bound
        a single scanner pass.
        """
        async with self._sf() as session:
            result = await session.execute(
                select(File)
                .where(File.next_eval_at.is_not(None))
                .where(File.next_eval_at <= now)
                .where(File.tier != "pending")
                .order_by(File.next_eval_at.asc())
                .limit(limit)
            )
            return list(result.scalars().all())

    async def count_by_tier(self) -> dict[str, int]:
        """Return ``{tier: count}`` with every tier in ``TIERS`` present.

        Tiers with zero files still appear in the result (default 0) so
        the dashboard can render a stable 5-row table without
        conditionals.
        """
        result: dict[str, int] = {tier: 0 for tier in TIERS}
        async with self._sf() as session:
            rows = await session.execute(
                select(File.tier, func.count(File.id)).group_by(File.tier)
            )
            for tier, count in rows.all():
                if tier in result:
                    result[tier] = int(count)
                else:
                    # Unknown tier in the DB shouldn't happen, but be defensive.
                    result[tier] = int(count)
        return result

    async def total_bytes_by_tier(self) -> dict[str, int]:
        """Return ``{tier: total_size_bytes}`` with every tier present.

        ``COALESCE(SUM, 0)`` handles the empty-group case in SQL; the
        dict pre-seed handles tiers with no rows at all.
        """
        result: dict[str, int] = {tier: 0 for tier in TIERS}
        async with self._sf() as session:
            rows = await session.execute(
                select(
                    File.tier,
                    func.coalesce(func.sum(File.size_bytes), 0),
                ).group_by(File.tier)
            )
            for tier, total in rows.all():
                if tier in result:
                    result[tier] = int(total)
                else:
                    result[tier] = int(total)
        return result

    # --- pending_deletes queue (mark-then-sweep) ----------------------------
    #
    # The applier (C10) calls ``add_pending_delete`` when it has finished
    # promoting a segment to its new tier and wants the original copy
    # cleaned up after a grace window. The sweeper (C11) drains the
    # queue via ``list_pending_deletes_due`` + ``delete_pending_delete_by_id``.
    # ``count_pending_deletes`` exists only for the dashboard / job
    # telemetry — neither the applier nor the sweeper read it.

    async def add_pending_delete(
        self,
        *,
        file_id: int,
        path: str,
        delete_after: datetime,
    ) -> PendingDelete:
        """Insert a new ``PendingDelete`` row and return the refreshed instance.

        The returned object has ``id`` and ``created_at`` populated by the
        commit + refresh, mirroring ``add_file``'s contract so callers can
        log the row id immediately for audit / debugging.
        """
        async with self._sf() as session:
            row = PendingDelete(
                file_id=file_id,
                path=path,
                delete_after=delete_after,
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return row

    async def list_pending_deletes_due(
        self, now: datetime, limit: int = 500
    ) -> list[PendingDelete]:
        """Return rows whose ``delete_after`` has elapsed, oldest first.

        Capped by ``limit`` (default 500) so one sweeper pass is bounded
        even if a long pause built up a large backlog. The next pass will
        pick up the rest on the next tick.
        """
        async with self._sf() as session:
            result = await session.execute(
                select(PendingDelete)
                .where(PendingDelete.delete_after <= now)
                .order_by(PendingDelete.delete_after.asc())
                .limit(limit)
            )
            return list(result.scalars().all())

    async def delete_pending_delete_by_id(self, pending_id: int) -> None:
        """Remove a single ``PendingDelete`` row by id.

        Called by the sweeper after the underlying file has been unlinked
        (or confirmed already absent). Idempotent at the SQL layer — a
        no-op against an already-removed id, so repeated sweeps cannot
        explode.
        """
        async with self._sf() as session:
            await session.execute(
                delete(PendingDelete).where(PendingDelete.id == pending_id)
            )
            await session.commit()

    async def count_pending_deletes(self) -> int:
        """Return the total number of queued deletes (all timestamps).

        The dashboard surfaces this as a backpressure indicator: a slowly
        growing queue with no sweep activity is the canonical "sweeper
        misconfigured" symptom.
        """
        async with self._sf() as session:
            result = await session.execute(
                select(func.count(PendingDelete.id))
            )
            return int(result.scalar_one())

    # --- transitions queue (scanner → applier handoff) ----------------------
    #
    # The scanner (C09) plans work by inserting ``pending`` ``Transition``
    # rows; the applier (C10) drains them oldest-first via
    # ``list_pending_transitions``. ``add_transition`` mirrors ``add_file``
    # / ``add_pending_delete``: commit + refresh so the returned ORM
    # instance carries the autoincrement id (useful for logging and audit
    # entries downstream).

    async def add_transition(
        self,
        *,
        file_id: int,
        from_tier: str,
        to_tier: str,
        action: str,
        status: str = "pending",
        planned_at: datetime | None = None,
    ) -> Transition:
        """Insert a new ``Transition`` row and return the refreshed instance.

        ``planned_at`` defaults to ``datetime.utcnow()`` if not supplied,
        keeping the call sites at the scanner concise — the planner does
        not need to materialize ``now`` twice when the model column
        default would produce the same value.
        """
        async with self._sf() as session:
            row = Transition(
                file_id=file_id,
                from_tier=from_tier,
                to_tier=to_tier,
                action=action,
                status=status,
                planned_at=planned_at if planned_at is not None else datetime.utcnow(),
            )
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return row

    async def list_pending_transitions(
        self, limit: int = 100
    ) -> list[Transition]:
        """Return ``status='pending'`` rows ordered oldest-planned first.

        Bounded by ``limit`` (default 100) so a single applier pass cannot
        block on a huge backlog — the next tick will pick up the rest.
        """
        async with self._sf() as session:
            result = await session.execute(
                select(Transition)
                .where(Transition.status == "pending")
                .order_by(Transition.planned_at.asc())
                .limit(limit)
            )
            return list(result.scalars().all())

    async def update_transition_status(
        self,
        transition_id: int,
        status: str,
        *,
        executed_at: datetime | None = None,
        error: str | None = None,
    ) -> None:
        """Mark a Transition as ``applied`` (success) or ``failed`` (with error).

        Called by the applier (C10) once each transition has been carried
        out (or definitively failed). The three columns updated together
        form the transition's post-execution audit footprint:

          * ``status`` — final state (``applied`` / ``failed``).
          * ``executed_at`` — when the applier finished; ``None`` if the
            caller does not want to overwrite an existing value (rare,
            but allowed so callers can mark status alone in tests).
          * ``error`` — short failure description for the dashboard /
            operator. Stored verbatim from ``str(exception)``; the
            applier wraps the offending Transition in try/except and
            records the message here instead of crashing the pass.

        Single ``UPDATE`` statement — atomic at the SQL layer; no read
        round-trip required.
        """
        async with self._sf() as session:
            await session.execute(
                update(Transition)
                .where(Transition.id == transition_id)
                .values(
                    status=status,
                    executed_at=executed_at,
                    error=error,
                )
            )
            await session.commit()
