"""Background migration engine — copy-on-write reformatting with no query disruption.

This is the component that actually *moves* a partition from one on-disk
:class:`~src.models.Format` to another (e.g. an ageing ROW partition that has
turned scan-heavy into COLUMNAR). It is the highest-risk module in the system
because it rewrites live data while queries are flowing, so its single design
goal is: **never disrupt an in-flight read and never lose a row, even if the
process is killed mid-migration.**

Copy-on-write (COW) migration
-----------------------------
Every migration follows the same three-phase shape (see :meth:`migrate_to`):

1. **Rewrite (no locks).** The target backend reads the source partition through
   a consistent snapshot and writes the reformatted data to ``*.new`` *staging*
   files (e.g. ``data.parquet.new``). The live files and the manifest are never
   touched, so this heavy, potentially slow step holds **no lock** and is
   invisible to concurrent readers and writers.
2. **Commit files (atomic per file).** Each staged ``(staging, final)`` pair is
   committed with :func:`os.replace`, which is an atomic rename on a single
   filesystem (the ``./data`` volume is one fs — see ``plan.md`` §"Risks" #1).
3. **Flip the pointer (atomic).** ``manifest.swap_format`` atomically re-points
   the partition at its new format/paths/codecs under the manifest's own
   per-tenant lock. Only this final, fast step is serialized against ingest's
   metadata updates.

No-disruption rationale (why old files are *not* deleted inline)
----------------------------------------------------------------
After the swap we deliberately leave the old-format files on disk as harmless
orphans rather than deleting them inline. Each backend only ever reads files
belonging to *its own* format (ROW reads ``row.jsonl.lz4``, COLUMNAR reads
``data.parquet``), so a stale file from the previous format is never read by
the new backend. A reader that resolved the partition to the **old** format an
instant before the swap therefore still finds the file it expects and completes
correctly; a reader that resolves *after* the swap sees the new format. Deleting
the old file inline would open a window where the first kind of reader fails.
Orphan cleanup is handled out-of-band by :meth:`sweep_orphans` (called on
startup / maintenance), never on the hot migration path.

Crash safety
------------
If the process dies during phase 1, only a partial ``*.new`` orphan exists; the
live files and manifest are untouched, so the partition stays fully readable in
its old format (``plan.md`` §"Risks" #2). If it dies between committing the
files and the manifest flip, the manifest still points at the old format, which
remains intact on disk — the freshly-committed files become orphans swept later.
Either way the partition is always consistent.

Anti-flapping
-------------
:meth:`migrate_partition_once` consults the :class:`~src.format_selector.FormatSelector`
(which has its own confidence floor) **and** enforces a per-partition cooldown
derived from ``last_migration`` so a partition cannot oscillate between formats
(``plan.md`` §"Risks" #7). :meth:`migrate_to` is the unconditional primitive used
by forced/admin/test migrations that bypass both gates.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Callable

from src.format_selector import FormatSelector
from src.manifest import ManifestStore
from src.metrics import Metrics
from src.models import Format
from src.paths import PARQUET_NAME, RECENT_NAME, ROW_NAME, partition_paths
from src.pattern_tracker import PatternTracker
from src.settings import Settings
from src.storage.base import StorageBackend
from src.tier_manager import TierManager

__all__ = ["MigrationEngine"]

logger = logging.getLogger(__name__)


class MigrationEngine:
    """Background engine that reformats partitions copy-on-write, without disruption.

    Construct one per process, wiring it to the shared manifest, the per-format
    storage backends, the format selector / tier manager / pattern tracker that
    decide *whether* and *to what* a partition should migrate, and the metrics
    sink. Drive it either reactively via :meth:`migrate_partition_once` /
    :meth:`migrate_to`, or continuously via :meth:`run` (the background loop).

    The engine holds no per-partition state of its own; every decision is a pure
    function of the manifest, the live access stats, and the injected clock, so a
    single instance safely serves every tenant.
    """

    def __init__(
        self,
        *,
        manifest: ManifestStore,
        backends: dict[Format, StorageBackend],
        selector: FormatSelector,
        tier_manager: TierManager,
        pattern_tracker: PatternTracker,
        compression,
        index_manager,
        metrics: Metrics,
        settings: Settings,
        clock: Callable[[], float] = time.time,
    ) -> None:
        """Wire the migration engine to its collaborators.

        Args:
            manifest: The source-of-truth :class:`~src.manifest.ManifestStore`;
                read for partition metadata and flipped via ``swap_format``.
            backends: Map of :class:`~src.models.Format` to the
                :class:`~src.storage.base.StorageBackend` that materialises it.
                Must contain an entry for every format that can be a source or a
                migration target (ROW, COLUMNAR, HYBRID).
            selector: The :class:`~src.format_selector.FormatSelector` that
                recommends a target format from access stats.
            tier_manager: The :class:`~src.tier_manager.TierManager` used to
                classify a partition's hot/warm/cold tier (an input to the
                selector).
            pattern_tracker: The live :class:`~src.pattern_tracker.PatternTracker`
                supplying real-time per-partition access statistics.
            compression: The compression chooser. Accepted now for C17/C18
                wiring (learned codecs / index-aware rewrites) and stored unused
                in this commit.
            index_manager: The index manager. Accepted now for C17/C18 wiring
                and stored unused in this commit.
            metrics: The :class:`~src.metrics.Metrics` sink for migration
                gauges / outcome records.
            settings: Runtime :class:`~src.settings.Settings` (bucket width, loop
                interval, per-tick cap, cooldown, data dir).
            clock: Zero-argument callable returning the current time in seconds.
                Defaults to :func:`time.time`; tests inject a controllable clock.
                Used for age math, cooldown checks, and loop scheduling.
        """
        self._manifest = manifest
        self._backends = backends
        self._selector = selector
        self._tier_manager = tier_manager
        self._pattern_tracker = pattern_tracker
        # Accepted for forward (C17/C18) wiring; intentionally unused this commit.
        self._compression = compression
        self._index_manager = index_manager
        self._metrics = metrics
        self._settings = settings
        self._clock = clock

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _new_paths_for(self, pid: str, fmt: Format) -> dict[str, str]:
        """Return the manifest ``paths`` map (logical name -> relative path) for ``fmt``.

        The relative paths are rooted at the partition directory (``<pid>/...``)
        exactly as the manifest stores them, and name only the files the format
        actually uses:

        * ``ROW`` -> ``{"row": "<pid>/row.jsonl.lz4"}``
        * ``COLUMNAR`` -> ``{"parquet": "<pid>/data.parquet"}``
        * ``HYBRID`` -> ``{"recent": "<pid>/recent.jsonl.lz4",
          "parquet": "<pid>/data.parquet"}`` (recent ROW side + sealed Parquet).

        Args:
            pid: The partition id (``p_<bucket>``).
            fmt: The format whose path map is wanted.

        Returns:
            The logical-name -> relative-path dict for ``swap_format``'s
            ``new_paths``.
        """
        if fmt == Format.ROW:
            return {"row": f"{pid}/{ROW_NAME}"}
        if fmt == Format.COLUMNAR:
            return {"parquet": f"{pid}/{PARQUET_NAME}"}
        # HYBRID: a recent ROW side over a sealed Parquet side.
        return {
            "recent": f"{pid}/{RECENT_NAME}",
            "parquet": f"{pid}/{PARQUET_NAME}",
        }

    def _age_seconds(self, pid: str, now: float) -> float:
        """Return how long ago the partition's time-bucket began, in seconds.

        The bucket index is parsed from the partition id (``pid[2:]`` strips the
        ``"p_"`` prefix); the bucket's start wall-clock time is
        ``bucket * partition_bucket_seconds``. The result is floored at ``0.0`` so
        a partition whose bucket is in the (clock-skewed) future never reports a
        negative age. A malformed id (non-integer suffix) yields ``0.0`` rather
        than raising, so one bad partition can never abort a migration tick.

        Args:
            pid: The partition id (expected shape ``p_<bucket>``).
            now: The current wall-clock time in seconds.

        Returns:
            ``max(0.0, now - bucket_start)`` seconds.
        """
        try:
            bucket = int(pid[2:])
        except (ValueError, IndexError):
            return 0.0
        start = bucket * self._settings.partition_bucket_seconds
        return max(0.0, now - start)

    # ------------------------------------------------------------------ #
    # Core copy-on-write migration
    # ------------------------------------------------------------------ #
    async def migrate_to(
        self,
        tenant: str,
        pid: str,
        target_format: Format,
        *,
        reason: str = "forced",
    ) -> bool:
        """Migrate one partition to ``target_format`` copy-on-write, no disruption.

        This is the unconditional migration primitive (no selector / cooldown
        gating — those live in :meth:`migrate_partition_once`). It is what forced
        / admin / test migrations call directly.

        Sequence (see the module docstring for the full rationale):

        1. Look up the partition. If it is absent, or already in
           ``target_format``, there is nothing to do -> ``False``.
        2. Run the target backend's ``rewrite_from`` against the source backend.
           This reads a consistent snapshot and writes **only** to ``*.new``
           staging files — no lock is held, the live files and manifest are
           untouched.
        3. Commit each staged ``(staging, final)`` pair with :func:`os.replace`
           (atomic same-filesystem rename), then flip the manifest pointer with
           ``manifest.swap_format`` (atomic, under the manifest's per-tenant
           lock).
        4. Leave the **old-format files in place** as harmless orphans (never
           read by the new backend) so an in-flight read that already resolved
           the old format still succeeds. Orphans are reclaimed later by
           :meth:`sweep_orphans`.

        Failure handling: any exception during the rewrite/commit is caught,
        logged, and recorded as a failed migration; the in-flight gauge is always
        balanced in a ``finally``. A failed rewrite leaves at most a stale
        ``*.new`` orphan — the live files **and** the manifest are untouched, so
        the partition remains fully intact in its original format. One partition's
        failure is never allowed to propagate.

        Args:
            tenant: Tenant identifier.
            pid: Partition id to migrate.
            target_format: The format to migrate the partition into.
            reason: Human-readable reason recorded on the migration (defaults to
                ``"forced"`` for direct callers; the decision path passes the
                selector's reason).

        Returns:
            ``True`` if the partition was migrated and committed; ``False`` if
            there was nothing to do or the migration failed.
        """
        meta = self._manifest.get_partition(tenant, pid)
        if meta is None or meta.format == target_format:
            return False

        source_format = meta.format
        source = self._backends[source_format]
        target = self._backends[target_format]

        # Source and destination share the same partition directory: the rewrite
        # stages alongside the live files and the engine renames in place.
        src_paths = partition_paths(self._settings.data_dir, tenant, pid)
        dst_paths = src_paths

        self._metrics.migration_started()
        try:
            # Phase 1 — heavy rewrite to *.new staging only (no lock held, live
            # files + manifest untouched).
            #
            # Pass codecs=None (NOT the old meta.codecs) so the target backend
            # is free to LEARN/infer fresh codecs for the partition's current
            # data: a migration is precisely when re-learning the best per-column
            # codec pays off. Passing the OLD codecs as an override would pin the
            # previous choices and defeat adaptive compression. The learned
            # codecs surface on ``rr.codecs`` and flow into ``swap_format`` below
            # (``new_codecs=rr.codecs``) so they are persisted into the manifest.
            rr = target.rewrite_from(
                source,
                src_paths,
                dst_paths,
                codecs=None,
            )

            # Phase 2 — commit each staged file with an atomic same-fs rename.
            for staging, final in rr.staged:
                os.replace(staging, final)

            # Phase 3 — atomically flip the manifest pointer to the new format.
            await self._manifest.swap_format(
                tenant,
                pid,
                new_format=target_format,
                new_paths=self._new_paths_for(pid, target_format),
                new_codecs=rr.codecs,
                new_size=target.size_bytes(dst_paths),
                uncompressed_estimate_bytes=meta.uncompressed_estimate_bytes
                or None,
                reason=reason,
                from_format=source_format,
            )

            # NOTE: the old-format files are intentionally NOT deleted here. They
            # are harmless orphans (never read by the new backend) that guarantee
            # an in-flight read which already resolved the OLD format still finds
            # its file. ``sweep_orphans`` reclaims them out-of-band.
            self._metrics.record_migration(
                tenant=tenant,
                partition=pid,
                from_fmt=source_format,
                to_fmt=target_format,
                ok=True,
                reason=reason,
            )
            return True
        except Exception as exc:  # noqa: BLE001 - isolate per-partition failures.
            # The rewrite failed before/at the commit: at most a stale ``*.new``
            # orphan exists; the live files and the manifest are untouched, so
            # the partition is still fully intact in its original format.
            logger.exception(
                "migration failed: tenant=%s partition=%s %s->%s",
                tenant,
                pid,
                source_format.value,
                target_format.value,
            )
            self._metrics.record_migration(
                tenant=tenant,
                partition=pid,
                from_fmt=source_format,
                to_fmt=target_format,
                ok=False,
                reason=f"failed: {exc}",
            )
            return False
        finally:
            # Always balance the in-flight gauge, on success or failure.
            self._metrics.migration_finished()

    # ------------------------------------------------------------------ #
    # Decision wrapper
    # ------------------------------------------------------------------ #
    async def migrate_partition_once(
        self,
        tenant: str,
        pid: str,
        *,
        now: float | None = None,
        ignore_cooldown: bool = False,
    ) -> bool:
        """Evaluate one partition and migrate it if (and only if) warranted.

        This is the *decision* layer on top of :meth:`migrate_to`. It snapshots
        the partition's live access stats, classifies its tier, asks the
        :class:`~src.format_selector.FormatSelector` for a recommendation, and
        migrates only when the selector says to **and** the per-partition cooldown
        has elapsed — the two anti-flapping gates (``plan.md`` §"Risks" #7).

        Steps:

        1. Resolve ``now`` (injected clock if omitted). Look up the partition;
           absent -> ``False``.
        2. Read live stats, compute age from the bucket, classify the tier.
        3. Get the selector recommendation for the current row count / tier /
           format.
        4. If ``selector.should_migrate`` is ``False`` (same format, or
           confidence below the floor) -> ``False``.
        5. Cooldown gate (skipped when ``ignore_cooldown``): if a previous
           migration's timestamp is within ``migration_cooldown_seconds`` of
           ``now`` -> ``False``.
        6. Otherwise perform the migration via :meth:`migrate_to` with the
           selector's reason.

        Args:
            tenant: Tenant identifier.
            pid: Partition id to evaluate.
            now: Current time; falls back to the injected clock when ``None``.
            ignore_cooldown: When ``True``, bypass the per-partition cooldown
                (used by forced re-evaluations / tests). The selector gate still
                applies.

        Returns:
            ``True`` if a migration was performed and committed; ``False`` if the
            partition was absent, no migration was warranted, the cooldown
            blocked it, or the migration failed.
        """
        now = now if now is not None else self._clock()

        meta = self._manifest.get_partition(tenant, pid)
        if meta is None:
            return False

        stats = self._pattern_tracker.get_stats(tenant, pid)
        age = self._age_seconds(pid, now)
        tier = self._tier_manager.tier_for(stats, age_seconds=age, now=now)

        rec = self._selector.recommend(
            stats,
            age_seconds=age,
            row_count=meta.row_count,
            tier=tier,
            current_format=meta.format,
        )

        if not self._selector.should_migrate(rec, meta.format):
            return False

        # Anti-flap cooldown: don't re-migrate a partition that was migrated very
        # recently (unless explicitly told to ignore the cooldown).
        if not ignore_cooldown and meta.last_migration:
            last_at = meta.last_migration.get("at", 0)
            if now - last_at < self._settings.migration_cooldown_seconds:
                return False

        return await self.migrate_to(tenant, pid, rec.format, reason=rec.reason)

    # ------------------------------------------------------------------ #
    # Background loop
    # ------------------------------------------------------------------ #
    async def run(self, stop_event: asyncio.Event) -> None:
        """Run the migration loop until ``stop_event`` is set.

        Each tick scans every tenant's partitions and evaluates them via
        :meth:`migrate_partition_once`, migrating at most
        ``migration_max_per_tick`` partitions per tick (a back-pressure /
        memory-bound knob — ``plan.md`` §"Risks" #4) so a backlog is drained
        gradually across ticks rather than all at once. The loop then sleeps
        ``migration_interval_seconds`` *interruptibly*: it waits on the stop
        event with that timeout, so it exits promptly when asked to stop and
        never busy-loops.

        Isolation is layered so the loop is effectively unkillable by data
        errors:

        * each partition evaluation runs in its own ``try/except`` (a bad
          partition is logged and skipped — its neighbours still get their turn);
        * the whole tick body is additionally wrapped so a transient error (e.g.
          an unreadable manifest mid-write) is logged and the loop simply waits
          for the next tick instead of dying.

        Args:
            stop_event: An :class:`asyncio.Event`; when set, the loop finishes
                the current wait and returns.
        """
        while not stop_event.is_set():
            try:
                migrated = 0
                for tenant in self._manifest.all_tenants():
                    if migrated >= self._settings.migration_max_per_tick:
                        break
                    for meta in self._manifest.list_partitions(tenant):
                        if migrated >= self._settings.migration_max_per_tick:
                            break
                        pid = meta.partition_id
                        try:
                            did = await self.migrate_partition_once(
                                tenant, pid, now=self._clock()
                            )
                        except Exception:  # noqa: BLE001 - per-partition isolation.
                            logger.exception(
                                "migration tick error: tenant=%s partition=%s",
                                tenant,
                                pid,
                            )
                            continue
                        if did:
                            migrated += 1
            except Exception:  # noqa: BLE001 - a tick error must not kill the loop.
                logger.exception("migration loop iteration failed")

            # Interruptible sleep until the next tick: wake immediately when the
            # stop event is set, otherwise resume after the interval.
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self._settings.migration_interval_seconds,
                )
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------ #
    # Maintenance
    # ------------------------------------------------------------------ #
    def sweep_orphans(self, tenant: str | None = None) -> int:
        """Delete files that don't belong to a partition's current format. Best-effort.

        Reclaims two kinds of leftover on the partition directories:

        * **Stale format files** — a data file for a format the partition is no
          longer in (e.g. a ``row.jsonl.lz4`` still sitting next to the
          ``data.parquet`` of a partition that has since migrated to COLUMNAR).
          These are the harmless orphans :meth:`migrate_to` deliberately leaves
          behind after a successful swap.
        * **Stale staging files** — any ``*.new`` files left by an interrupted
          rewrite (a crash during phase 1) or a partial commit.

        The set of files a partition is *allowed* to keep is derived from its
        current manifest format via :meth:`_new_paths_for` (just the base file
        names). Every other file directly in the partition directory is removed.
        This is intentionally conservative about *what* it scans (only direct
        children of known partition dirs) and tolerant of failures (per-file
        errors are swallowed) so it is safe to call on startup.

        Args:
            tenant: Restrict the sweep to a single tenant when given; otherwise
                sweep every tenant the manifest knows about.

        Returns:
            The number of files actually removed.
        """
        tenants = [tenant] if tenant is not None else self._manifest.all_tenants()
        removed = 0

        for tnt in tenants:
            for meta in self._manifest.list_partitions(tnt):
                pid = meta.partition_id
                paths = partition_paths(self._settings.data_dir, tnt, pid)
                pdir = paths.dir
                if not pdir.is_dir():
                    continue

                # File names this partition's CURRENT format legitimately owns.
                allowed = {
                    os.path.basename(rel)
                    for rel in self._new_paths_for(pid, meta.format).values()
                }

                try:
                    entries = list(pdir.iterdir())
                except OSError:
                    # Directory vanished or is unreadable — nothing to sweep.
                    continue

                for entry in entries:
                    try:
                        if not entry.is_file():
                            continue
                        name = entry.name
                        # Remove anything not owned by the current format, plus
                        # every leftover ``*.new`` staging file.
                        if name in allowed and not name.endswith(".new"):
                            continue
                        entry.unlink()
                        removed += 1
                    except OSError:
                        # Best-effort: skip files we can't stat/remove.
                        continue

        return removed
