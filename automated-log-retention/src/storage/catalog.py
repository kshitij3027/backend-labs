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

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.persistence.models import File
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
