"""Real-time, in-memory per-(tenant, partition) access-pattern tracking.

:class:`PatternTracker` is the engine's running memory of *how* each partition
is actually used: how often it is written vs. read, whether reads are point
lookups or scans, which columns are touched and how widely, and when it was
last touched. Those signals feed the format selector (row vs. columnar vs.
hybrid) and the tier manager (hot/warm/cold) in later commits.

Design notes:
    * **Stdlib only** (``collections``, ``dataclasses``, ``time``, ``typing``)
      so it stays import-light and trivially testable.
    * **Injectable clock.** Every timestamp is sourced from an injected
      ``clock`` callable or an explicit ``ts`` argument — never an implicit
      wall-clock read inside the logic — so tests are fully deterministic.
    * **Bounded.** Per-partition counters are bounded by the schema width (a
      :class:`~collections.Counter` over column names) and a fixed set of
      :class:`~src.models.QueryClass` keys, so memory stays modest.
    * **Lazy insert.** Per-partition stats rows are created on the first
      ``record_*`` call. A pure getter (:meth:`PatternTracker.get_stats`) never
      mutates the table — it returns a fresh empty :class:`PartitionAccessStats`
      when a partition has not been seen.
"""
from __future__ import annotations

import collections
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable

from src.models import QueryClass

__all__ = ["PatternTracker", "PartitionAccessStats"]


@dataclass
class PartitionAccessStats:
    """Mutable running access statistics for a single partition.

    The raw counters are accumulated by :class:`PatternTracker`; the ratios and
    averages are exposed as read-only :class:`property` values derived from
    those counters so they are always consistent with the underlying data.

    Attributes:
        reads: Total read operations recorded.
        writes: Total write operations recorded.
        point_lookups: Reads classified as point lookups (selective by key).
        scans: Reads classified as scans (range / full-partition reads).
        column_counter: How often each column has been touched by a read.
        by_class: Read counts keyed by :class:`~src.models.QueryClass` *value*
            string (e.g. ``"analytical"``).
        columns_per_read_total: Sum of columns touched across all reads, used to
            compute the mean columns-per-read.
        last_access: Timestamp of the most recent read *or* write.
        last_write: Timestamp of the most recent write.
    """

    reads: int = 0
    writes: int = 0
    point_lookups: int = 0
    scans: int = 0
    column_counter: collections.Counter = field(default_factory=collections.Counter)
    by_class: collections.Counter = field(default_factory=collections.Counter)
    columns_per_read_total: int = 0
    last_access: float = 0.0
    last_write: float = 0.0

    @property
    def total_ops(self) -> int:
        """Total operations (reads + writes) recorded for the partition."""
        return self.reads + self.writes

    @property
    def write_ratio(self) -> float:
        """Fraction of operations that were writes; ``0.0`` if no ops yet."""
        total = self.total_ops
        if total == 0:
            return 0.0
        return self.writes / total

    @property
    def read_ratio(self) -> float:
        """Fraction of operations that were reads; ``0.0`` if no ops yet.

        Defined as ``1 - write_ratio`` once any op exists, so read and write
        ratios always sum to ``1.0`` for a non-empty partition.
        """
        if self.total_ops == 0:
            return 0.0
        return 1.0 - self.write_ratio

    @property
    def point_lookup_ratio(self) -> float:
        """Fraction of shape-classified reads that were point lookups.

        Denominator is ``point_lookups + scans`` (reads whose shape was
        recorded); returns ``0.0`` when none have been seen.
        """
        denom = self.point_lookups + self.scans
        if denom == 0:
            return 0.0
        return self.point_lookups / denom

    @property
    def scan_ratio(self) -> float:
        """Fraction of shape-classified reads that were scans.

        Defined as ``1 - point_lookup_ratio`` once any shape-classified read
        exists; returns ``0.0`` when there are none.
        """
        if (self.point_lookups + self.scans) == 0:
            return 0.0
        return 1.0 - self.point_lookup_ratio

    @property
    def avg_columns_touched(self) -> float:
        """Mean number of columns touched per read; ``0.0`` if no reads."""
        if self.reads == 0:
            return 0.0
        return self.columns_per_read_total / self.reads

    @property
    def distinct_columns(self) -> int:
        """Count of distinct columns ever touched by a read."""
        return len(self.column_counter)

    @property
    def fraction_columns_touched(self) -> float:
        """Mean columns touched per read as a fraction of distinct columns.

        Returns ``0.0`` when no distinct columns have been observed, and is
        clamped to ``[0.0, 1.0]`` so a full-record read never reports a value
        above one even if a read touched a column not yet otherwise counted.
        """
        distinct = self.distinct_columns
        if distinct == 0:
            return 0.0
        frac = self.avg_columns_touched / distinct
        if frac < 0.0:
            return 0.0
        if frac > 1.0:
            return 1.0
        return frac

    def to_dict(self) -> dict:
        """Serialize to the manifest ``access{}`` shape.

        Matches the on-disk schema in ``plan.md`` (§"On-disk storage"):
        ``reads, writes, point_lookups, scans, avg_columns_touched,
        fraction_columns_touched, last_access, last_write, by_class``. The
        ``by_class`` counter is rendered as a plain ``dict`` for JSON
        serialization.
        """
        return {
            "reads": self.reads,
            "writes": self.writes,
            "point_lookups": self.point_lookups,
            "scans": self.scans,
            "avg_columns_touched": self.avg_columns_touched,
            "fraction_columns_touched": self.fraction_columns_touched,
            "last_access": self.last_access,
            "last_write": self.last_write,
            "by_class": dict(self.by_class),
        }


class PatternTracker:
    """In-memory access-pattern tracker keyed by ``(tenant, partition_id)``.

    The tracker is a process-local cache of usage signals. It performs no I/O
    and holds no locks; callers (ingest/query engines) drive it synchronously on
    their hot paths, and the migration/tier engines later read snapshots from
    it. Timestamps come from the injected ``clock`` (or an explicit ``ts``), so
    behaviour is deterministic under test.
    """

    def __init__(self, *, clock: Callable[[], float] = time.time) -> None:
        """Create an empty tracker.

        Args:
            clock: Zero-argument callable returning the current time as a float.
                Defaults to :func:`time.time`; tests inject a controllable
                clock. Used only when a ``record_*`` call omits an explicit
                ``ts``.
        """
        self._clock = clock
        self._stats: dict[tuple[str, str], PartitionAccessStats] = {}

    def _stats_for(self, tenant: str, partition_id: str) -> PartitionAccessStats:
        """Return the stats row for a partition, lazily creating it.

        Used only by the ``record_*`` mutators — never by the pure getter.
        """
        key = (tenant, partition_id)
        stats = self._stats.get(key)
        if stats is None:
            stats = PartitionAccessStats()
            self._stats[key] = stats
        return stats

    def record_write(
        self,
        tenant: str,
        partition_id: str,
        *,
        columns: Iterable[str] | None = None,
        ts: float | None = None,
    ) -> None:
        """Record a write to a partition.

        Increments the write counter and advances both ``last_write`` and
        ``last_access`` to ``ts`` (or the injected clock). ``columns`` is
        accepted for symmetry with :meth:`record_read` and to allow future
        write-side column accounting; it does not currently affect the
        read-oriented column counters.

        Args:
            tenant: Tenant identifier.
            partition_id: Partition identifier within the tenant.
            columns: Columns written, if known. Currently unused by the
                counters; reserved for forward compatibility.
            ts: Explicit timestamp; falls back to the injected clock when
                ``None``.
        """
        stats = self._stats_for(tenant, partition_id)
        now = self._clock() if ts is None else ts
        stats.writes += 1
        stats.last_write = now
        stats.last_access = now

    def record_read(
        self,
        tenant: str,
        partition_id: str,
        *,
        columns: Iterable[str] | None,
        query_class: QueryClass,
        is_point_lookup: bool,
        ts: float | None = None,
    ) -> None:
        """Record a read against a partition.

        Always counts as one read. ``columns is None`` is treated as a
        full-record read: it still counts as a read (and bumps the shape /
        class counters and timestamps) but contributes **zero** columns to the
        column counter and to ``columns_per_read_total`` — i.e. it widens
        neither the distinct-column set nor the mean columns-per-read.

        Args:
            tenant: Tenant identifier.
            partition_id: Partition identifier within the tenant.
            columns: Columns touched by the read, or ``None`` for a full-record
                read (counted as a 0-column read).
            query_class: The query's classified access shape; its ``.value``
                string keys the ``by_class`` counter.
            is_point_lookup: ``True`` if the read is a point lookup, else it is
                counted as a scan.
            ts: Explicit timestamp; falls back to the injected clock when
                ``None``.
        """
        stats = self._stats_for(tenant, partition_id)
        now = self._clock() if ts is None else ts

        stats.reads += 1
        if is_point_lookup:
            stats.point_lookups += 1
        else:
            stats.scans += 1

        # ``None`` columns -> full-record read: counts as a read but touches no
        # named columns, so it adds nothing to the counter or the mean.
        touched = list(columns) if columns is not None else []
        for col in touched:
            stats.column_counter[col] += 1
        stats.columns_per_read_total += len(touched)

        stats.by_class[query_class.value] += 1
        stats.last_access = now

    def get_stats(self, tenant: str, partition_id: str) -> PartitionAccessStats:
        """Return stats for a partition without mutating the table.

        This is a pure getter: if the partition has never been recorded it
        returns a fresh, empty :class:`PartitionAccessStats` rather than
        inserting one. (Lazy insertion happens only inside the ``record_*``
        methods.)
        """
        existing = self._stats.get((tenant, partition_id))
        if existing is not None:
            return existing
        return PartitionAccessStats()

    def partitions(self, tenant: str) -> list[str]:
        """Return the partition ids seen for ``tenant`` (insertion order)."""
        return [pid for (t, pid) in self._stats if t == tenant]

    def all_tenants(self) -> list[str]:
        """Return the distinct tenant ids seen, in first-seen order."""
        seen: dict[str, None] = {}
        for tenant, _pid in self._stats:
            seen.setdefault(tenant, None)
        return list(seen)

    def tenant_rollup(self, tenant: str) -> dict:
        """Aggregate access counts across all of a tenant's partitions.

        Sums the raw operation counters over the tenant's partitions and
        reports the partition count. The summary is intentionally
        format-agnostic (counts only) — derived ratios are left to callers that
        also have format/age context.

        Args:
            tenant: Tenant identifier.

        Returns:
            A dict with ``tenant``, ``partition_count``, and the summed
            ``reads``, ``writes``, ``point_lookups`` and ``scans``. Counts are
            zero and ``partition_count`` is ``0`` for an unseen tenant.
        """
        reads = writes = point_lookups = scans = 0
        partition_count = 0
        for (t, _pid), stats in self._stats.items():
            if t != tenant:
                continue
            partition_count += 1
            reads += stats.reads
            writes += stats.writes
            point_lookups += stats.point_lookups
            scans += stats.scans
        return {
            "tenant": tenant,
            "partition_count": partition_count,
            "reads": reads,
            "writes": writes,
            "point_lookups": point_lookups,
            "scans": scans,
        }
