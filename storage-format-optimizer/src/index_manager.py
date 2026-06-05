"""Intelligent, self-pruning per-partition indexing (Feature C).

The optimizer only pays for an index when it earns its keep. Two collaborating
pieces live here:

:class:`PartitionIndex`
    The index payload itself: the set of indexed columns plus their per-column
    min/max statistics. It serializes to the exact ``{"columns": ..,
    "stats": ..}`` shape the manifest stores (see ``manifest.py`` — a built
    index is ``{"columns": [...], "stats": {col: {"min": .., "max": ..}}}``).
    These min/max bounds are what let the columnar backend skip Parquet
    row-groups whose range cannot match a filter. The stats are computed
    in-memory here and persisted into the manifest when indexing is wired into
    the query/migration path (C18).

:class:`IndexManager`
    The *policy* engine. It watches how queries filter each column and decides,
    per ``(tenant, partition_id, column)``:

    * **build** an index only where filters are both *frequent* (seen at least
      ``min_filter_hits`` times) **and** *selective* (their recent mean
      selectivity fraction is low, i.e. they prune a lot); and
    * **drop** an index that has stopped pulling its weight — once its recent
      mean skip-fraction falls below ``drop_min_benefit`` it is no longer
      skipping meaningful work and is pruned.

Design notes:
    * **Stdlib only** (``collections``, ``dataclasses``, ``statistics``) — no
      I/O, no locks. Callers drive it synchronously.
    * **Bounded.** All recent-history series are fixed-length
      :class:`collections.deque` objects (selectivity windows and benefit
      windows), so per-key memory is capped regardless of query volume.
    * **Pure decisions.** ``should_build`` / ``should_drop`` are deterministic
      functions of the recorded history; no clock, no randomness.
"""
from __future__ import annotations

import collections
import statistics
from dataclasses import dataclass, field
from typing import Any

__all__ = ["IndexManager", "PartitionIndex"]

# Window used for the *selectivity* history that gates index building. Kept
# small and fixed so a column's "is it selective?" verdict reflects recent
# behaviour rather than its entire history.
_SELECTIVITY_WINDOW = 50


@dataclass
class PartitionIndex:
    """A built index for one partition: indexed columns + their min/max stats.

    Attributes:
        columns: The columns covered by this index, in build order.
        stats: Per-column statistics keyed by column name. Each value is a
            mapping with ``"min"`` and ``"max"`` bounds, e.g.
            ``{"latency_ms": {"min": 3, "max": 980}}``. Columns with no
            comparable values are omitted.
    """

    columns: list[str] = field(default_factory=list)
    stats: dict[str, dict] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to the manifest index shape.

        Returns ``{"columns": [...], "stats": {col: {"min": .., "max": ..}}}``
        — the structure ``ManifestStore`` persists under a partition's
        ``index`` field. Shallow copies are returned so the caller cannot mutate
        this index's internal state.
        """
        return {
            "columns": list(self.columns),
            "stats": {col: dict(bounds) for col, bounds in self.stats.items()},
        }

    @classmethod
    def from_rows(
        cls, rows: list[dict], columns: list[str]
    ) -> "PartitionIndex":
        """Build an index by computing per-column min/max over ``rows``.

        For each requested column the min and max are taken over the non-``None``
        values present in ``rows``. A column is **skipped entirely** (absent from
        both ``columns`` and ``stats`` in the result) when it has no comparable
        values — either because it never appears, every value is ``None``, or its
        values are not mutually comparable (a ``TypeError`` from :func:`min`).
        The result's ``columns`` therefore lists only the columns that actually
        carry usable bounds, preserving the order of the input ``columns``.

        Args:
            rows: The partition's rows as plain dicts (e.g. decoded log
                ``fields``).
            columns: The candidate columns to index.

        Returns:
            A :class:`PartitionIndex` over the columns that yielded bounds.
        """
        built_columns: list[str] = []
        stats: dict[str, dict] = {}
        for column in columns:
            values = [
                row[column]
                for row in rows
                if column in row and row[column] is not None
            ]
            if not values:
                continue
            try:
                col_min = min(values)
                col_max = max(values)
            except TypeError:
                # Values are not mutually comparable (mixed types) — skip the
                # column rather than crash the whole index build.
                continue
            built_columns.append(column)
            stats[column] = {"min": col_min, "max": col_max}
        return cls(columns=built_columns, stats=stats)


class IndexManager:
    """Decide which partition columns to index, and prune the ones not paying off.

    State is held in bounded per-``(tenant, partition_id, column)`` structures:

    * ``_hits`` — total filter uses, gating the *frequency* test;
    * ``_selectivities`` — a bounded window of recent selectivity fractions,
      gating the *selectivity* test;
    * ``_benefits`` — a bounded window of recent skip-fractions, gating the
      *drop* test.

    The manager performs no I/O and holds no locks.
    """

    def __init__(
        self,
        *,
        min_filter_hits: int,
        min_selectivity: float,
        drop_benefit_window: int,
        drop_min_benefit: float,
    ) -> None:
        """Create an index manager.

        Args:
            min_filter_hits: Minimum number of filter uses on a column before it
                is eligible to be indexed. Mirrors
                ``settings.index_min_filter_hits`` (default ``5``).
            min_selectivity: Maximum mean recent selectivity *fraction* for a
                column to count as selective enough to index. A lower fraction
                means the filter matches fewer rows (more selective), so the
                test is ``mean(recent) <= min_selectivity``. Mirrors
                ``settings.index_min_selectivity`` (default ``0.2``).
            drop_benefit_window: Length of the bounded recent-benefit window per
                column (the ``maxlen`` of the skip-fraction deque). Mirrors
                ``settings.index_drop_benefit_window`` (default ``200``).
            drop_min_benefit: Minimum mean recent skip-fraction an index must
                deliver to be kept; below this it is dropped. Mirrors
                ``settings.index_drop_min_benefit`` (default ``0.01``).
        """
        self._min_filter_hits = min_filter_hits
        self._min_selectivity = min_selectivity
        self._drop_benefit_window = drop_benefit_window
        self._drop_min_benefit = drop_min_benefit

        # All keyed by (tenant, partition_id, column).
        self._hits: dict[tuple[str, str, str], int] = collections.defaultdict(int)
        self._selectivities: dict[
            tuple[str, str, str], collections.deque[float]
        ] = {}
        self._benefits: dict[
            tuple[str, str, str], collections.deque[float]
        ] = {}

    def note_filter(
        self,
        tenant: str,
        partition_id: str,
        column: str,
        *,
        selectivity: float,
    ) -> None:
        """Record that a query filtered on ``column``.

        Increments the column's hit count (the frequency signal) and pushes the
        observed ``selectivity`` fraction onto a bounded recent-selectivity
        window (the selectivity signal).

        Args:
            tenant: Tenant identifier.
            partition_id: Partition identifier within the tenant.
            column: The filtered column.
            selectivity: Fraction of rows the filter matched, in ``[0, 1]``.
                Lower means more selective.
        """
        key = (tenant, partition_id, column)
        self._hits[key] += 1
        window = self._selectivities.get(key)
        if window is None:
            window = collections.deque(maxlen=_SELECTIVITY_WINDOW)
            self._selectivities[key] = window
        window.append(selectivity)

    def should_build(self, tenant: str, partition_id: str, column: str) -> bool:
        """Whether ``column`` is frequent *and* selective enough to index.

        ``True`` iff the column has been filtered at least ``min_filter_hits``
        times **and** the mean of its recent selectivity fractions is
        ``<= min_selectivity`` (a low matched-fraction = highly selective =
        worth an index). A column with hits but no recorded selectivities is not
        built.
        """
        key = (tenant, partition_id, column)
        if self._hits[key] < self._min_filter_hits:
            return False
        window = self._selectivities.get(key)
        if not window:
            return False
        return statistics.mean(window) <= self._min_selectivity

    def candidate_columns(self, tenant: str, partition_id: str) -> list[str]:
        """Columns of one partition that currently pass :meth:`should_build`.

        Returns the columns in first-seen order. Only columns that have been
        observed via :meth:`note_filter` for this ``(tenant, partition_id)`` are
        considered.
        """
        seen: list[str] = []
        for t, pid, column in self._hits:
            if t == tenant and pid == partition_id:
                seen.append(column)
        return [c for c in seen if self.should_build(tenant, partition_id, c)]

    def record_benefit(
        self,
        tenant: str,
        partition_id: str,
        column: str,
        *,
        rows_skipped: int,
        rows_total: int,
    ) -> None:
        """Record how much work an existing index on ``column`` skipped.

        Pushes the *skip-fraction* ``rows_skipped / max(rows_total, 1)`` onto a
        bounded recent-benefit window (``maxlen=drop_benefit_window``) for the
        column. This is the signal :meth:`should_drop` averages over.

        Args:
            tenant: Tenant identifier.
            partition_id: Partition identifier within the tenant.
            column: The indexed column the benefit is attributed to.
            rows_skipped: Rows (or row-group rows) the index let the read skip.
            rows_total: Total rows the read would have scanned without the index.
        """
        key = (tenant, partition_id, column)
        window = self._benefits.get(key)
        if window is None:
            window = collections.deque(maxlen=self._drop_benefit_window)
            self._benefits[key] = window
        window.append(rows_skipped / max(rows_total, 1))

    def should_drop(self, tenant: str, partition_id: str, column: str) -> bool:
        """Whether an index on ``column`` has stopped earning its keep.

        ``True`` iff there is recorded benefit history **and** the mean of the
        recent skip-fractions is strictly below ``drop_min_benefit`` — i.e. the
        index is no longer skipping a meaningful fraction of work. With no
        benefit history recorded yet, the index is *not* dropped (it has not had
        a chance to prove itself).
        """
        key = (tenant, partition_id, column)
        window = self._benefits.get(key)
        if not window:
            return False
        return statistics.mean(window) < self._drop_min_benefit

    def prune(
        self, tenant: str, partition_id: str, built_columns: list[str]
    ) -> list[str]:
        """Return the subset of ``built_columns`` that should be dropped.

        Walks the currently built columns and selects those for which
        :meth:`should_drop` is ``True``. The returned list is the set of columns
        the caller should remove from the partition's index; input order is
        preserved.

        Args:
            tenant: Tenant identifier.
            partition_id: Partition identifier within the tenant.
            built_columns: Columns that currently have an index built.

        Returns:
            The columns to drop (possibly empty).
        """
        return [
            column
            for column in built_columns
            if self.should_drop(tenant, partition_id, column)
        ]

    # ------------------------------------------------------------------ #
    # Pure index-skip evaluation (no state) — used by the query engine.   #
    # ------------------------------------------------------------------ #
    def partition_can_match(
        self, index_stats: dict, filters: list
    ) -> tuple[bool, list[str]]:
        """Decide whether a partition *could* contain rows matching ``filters``.

        Uses the partition's persisted per-column min/max bounds
        (``index_stats`` = the ``meta.index["stats"]`` shape,
        ``{col: {"min": .., "max": ..}}``) to prove, for each filtered column
        that is indexed, whether the filter's value range can overlap the
        column's ``[min, max]``. If **any** indexed filter proves no overlap, the
        partition cannot hold a matching row (filters are AND-ed) and it is safe
        to skip reading it entirely.

        **Soundness is the contract here.** The method only returns "cannot
        match" when an indexed filter *provably* excludes the partition. In every
        ambiguous case it errs toward "can match" so a partition is never skipped
        incorrectly:

        * a filter on a column with no stats is ignored (no bound to prove on);
        * ``ne`` is never used to skip (the excluded value may be one of many in
          range, so the partition can still match);
        * a ``TypeError`` from comparing incomparable types (e.g. a string filter
          against numeric bounds) is treated as "can match".

        Overlap test per operator (bounds are ``lo = min``, ``hi = max``):

        * ``eq v``  → overlaps iff ``lo <= v <= hi``.
        * ``gt v``  → overlaps iff ``hi > v``   (some value strictly above ``v``).
        * ``gte v`` → overlaps iff ``hi >= v``.
        * ``lt v``  → overlaps iff ``lo < v``    (some value strictly below ``v``).
        * ``lte v`` → overlaps iff ``lo <= v``.
        * ``in [..]`` → overlaps iff the values' span ``[min(vals), max(vals)]``
          intersects ``[lo, hi]`` (equivalently, ``min(vals) <= hi and
          max(vals) >= lo``). An empty ``in`` list matches nothing → no overlap.
        * ``ne``    → always treated as overlapping (never decisive for skipping).

        This is a pure, deterministic function: no clock, no randomness, no
        recorded state is touched.

        Args:
            index_stats: The partition's per-column min/max bounds, i.e.
                ``meta.index.get("stats", {})``.
            filters: The query's filters (objects with ``.column``, ``.op`` and
                ``.value`` — e.g. :class:`~src.models.Filter`).

        Returns:
            ``(can_match, decisive_columns)``. When the partition can match,
            ``(True, [])``. When an indexed filter proves no overlap,
            ``(False, [column])`` naming the single column that ruled it out.
        """
        for f in filters:
            bounds = index_stats.get(f.column)
            if not bounds or "min" not in bounds or "max" not in bounds:
                # No usable index for this column — cannot prove exclusion.
                continue
            if f.op == "ne":
                # ``ne`` never proves a partition empty: the excluded value is
                # only one of potentially many values in range.
                continue

            lo = bounds["min"]
            hi = bounds["max"]
            value = f.value
            try:
                if f.op == "eq":
                    overlaps = lo <= value <= hi
                elif f.op == "gt":
                    overlaps = hi > value
                elif f.op == "gte":
                    overlaps = hi >= value
                elif f.op == "lt":
                    overlaps = lo < value
                elif f.op == "lte":
                    overlaps = lo <= value
                elif f.op == "in":
                    vals = list(value) if value is not None else []
                    if not vals:
                        # ``in []`` matches no row at all.
                        overlaps = False
                    else:
                        # Range-overlap of the values' span with [lo, hi].
                        overlaps = (min(vals) <= hi) and (max(vals) >= lo)
                else:  # pragma: no cover - unknown op, be conservative.
                    overlaps = True
            except TypeError:
                # Incomparable types (e.g. str filter vs numeric bounds): we
                # cannot prove exclusion, so treat as a possible match.
                continue

            if not overlaps:
                # This indexed predicate cannot be satisfied anywhere in the
                # partition; since filters AND together, the partition is empty
                # for this query and can be skipped.
                return (False, [f.column])

        return (True, [])
