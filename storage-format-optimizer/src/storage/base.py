"""Storage backend contract + shared in-process query helpers.

This module defines the single abstraction every on-disk format implements
(:class:`StorageBackend`) plus the small set of result records its methods
return. It also hosts the **in-process** filter/projection helpers reused by the
ROW and HYBRID backends, which (unlike the COLUMNAR/Parquet backend) cannot push
predicates down into the storage layer and must evaluate them in Python after
decoding.

Record shape (shared contract for *all* backends)
-------------------------------------------------
A stored "row" is a **flat dict**: ``{"ts": <float>, <field>: <value>, ...}``.
The ingest engine flattens each log entry ``{ts, fields:{...}}`` into
``{"ts": ts, **fields}`` before it reaches a backend. Filters and ``columns``
therefore refer to these top-level keys (including ``"ts"``).

Result records
--------------
* :class:`WriteResult` — what an append/write produced (delta + on-disk size).
* :class:`ReadResult` — decoded rows plus scan accounting
  (``rows_scanned`` = rows decoded before filtering; ``rowgroups_skipped`` =
  Parquet row-groups pruned by min/max stats, always ``0`` for ROW).
* :class:`RewriteResult` — the outcome of a copy-on-write migration, including
  the ``staged`` ``(staging_path, final_path)`` pairs the migration engine will
  ``os.replace`` atomically.

The ABC is intentionally minimal: all three backends *and* the migration engine
depend only on this surface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..models import Filter, Format
from ..paths import PartitionPaths

__all__ = [
    "WriteResult",
    "ReadResult",
    "RewriteResult",
    "match_filter",
    "apply_filters",
    "project_columns",
    "StorageBackend",
]


# ---------------------------------------------------------------------------
# Result records
# ---------------------------------------------------------------------------
@dataclass
class WriteResult:
    """Outcome of a :meth:`StorageBackend.write` call.

    Attributes:
        row_count_delta: Number of rows added by this write (not the running
            total) — the manifest accumulates these.
        size_bytes: The partition file's size on disk *after* the write.
        codecs: Per-column codec map chosen for this write. Empty for ROW (which
            uses a single whole-block LZ4 frame, not per-column codecs).
    """

    row_count_delta: int
    size_bytes: int
    codecs: dict[str, str] = field(default_factory=dict)


@dataclass
class ReadResult:
    """Decoded rows plus scan accounting from :meth:`StorageBackend.read`.

    Attributes:
        rows: The result rows after filtering and column projection.
        rows_scanned: Rows decoded/scanned *before* filters were applied — a
            measure of read work, used to compute selectivity and speedups.
        rowgroups_skipped: Parquet row-groups pruned via min/max statistics.
            Always ``0`` for the ROW backend (no row-group structure).
    """

    rows: list[dict]
    rows_scanned: int
    rowgroups_skipped: int = 0


@dataclass
class RewriteResult:
    """Outcome of a copy-on-write rewrite (:meth:`StorageBackend.rewrite_from`).

    The rewrite writes to ``*.new`` *staging* paths only; it never touches the
    live files. The migration engine is responsible for performing the atomic
    ``os.replace(staging_path, final_path)`` for each entry in :attr:`staged`.

    Attributes:
        row_count: Number of rows written to the staging file(s).
        size_bytes: Total size on disk of the staging file(s).
        codecs: Per-column codec map used for the rewrite (empty for ROW).
        staged: ``(staging_path, final_path)`` pairs the migration engine must
            atomically replace to commit this rewrite.
    """

    row_count: int
    size_bytes: int
    codecs: dict[str, str] = field(default_factory=dict)
    staged: list[tuple[Path, Path]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# In-process query helpers (reused by ROW and HYBRID backends)
# ---------------------------------------------------------------------------
def match_filter(row: dict, f: Filter) -> bool:
    """Return whether ``row`` satisfies a single :class:`Filter` predicate.

    The column value is looked up by ``f.column`` (a top-level key, possibly
    ``"ts"``). A **missing** column (``row.get`` returns ``None``) is treated as
    "no value", which fails every predicate *except* ``ne`` — a row that lacks
    the column is, by definition, not equal to ``f.value`` and so passes ``ne``.

    Comparisons are evaluated defensively: any ``TypeError`` raised by comparing
    mismatched types (e.g. ``"abc" > 3``) is swallowed and reported as ``False``
    rather than propagated, so a single bad row can never abort a query.

    Args:
        row: A flat record dict.
        f: The predicate to evaluate (``op`` in eq/ne/gt/gte/lt/lte/in).

    Returns:
        ``True`` if the row matches, else ``False``.
    """
    value = row.get(f.column)

    # Missing column: fails everything except ``ne`` (absent != target).
    if value is None:
        return f.op == "ne"

    target = f.value
    op = f.op
    try:
        if op == "eq":
            return value == target
        if op == "ne":
            return value != target
        if op == "gt":
            return value > target
        if op == "gte":
            return value >= target
        if op == "lt":
            return value < target
        if op == "lte":
            return value <= target
        if op == "in":
            # ``target`` is expected to be a collection; membership test.
            return value in target
    except TypeError:
        # Incomparable / non-iterable types — treat as a non-match.
        return False

    # Unknown operator (should be unreachable given the Literal typing).
    return False


def apply_filters(rows: list[dict], filters: list[Filter] | None) -> list[dict]:
    """Return the subset of ``rows`` matching **all** ``filters`` (logical AND).

    Args:
        rows: Candidate records.
        filters: Predicates to AND together. ``None`` or an empty list means "no
            filtering" and every row is returned (a new list is still produced
            in that case for caller-mutation safety).

    Returns:
        A new list of the matching rows, in input order.
    """
    if not filters:
        return list(rows)
    return [row for row in rows if all(match_filter(row, f) for f in filters)]


def project_columns(rows: list[dict], columns: list[str] | None) -> list[dict]:
    """Project each row down to ``columns`` (full record when ``None``).

    Args:
        rows: Records to project.
        columns: Top-level keys to keep. ``None`` returns ``rows`` unchanged
            (full-record read). Otherwise a new dict per row is built containing
            exactly ``columns``; a requested key absent from a row maps to
            ``None``.

    Returns:
        The projected rows (the same list object when ``columns is None``).
    """
    if columns is None:
        return rows
    return [{c: row.get(c) for c in columns} for row in rows]


# ---------------------------------------------------------------------------
# Storage backend ABC
# ---------------------------------------------------------------------------
class StorageBackend(ABC):
    """Abstract on-disk format for a single partition.

    Concrete backends (ROW, COLUMNAR, HYBRID) implement append, read, sizing,
    and copy-on-write rewrite against the file paths in a
    :class:`~src.paths.PartitionPaths`. Each subclass advertises which
    :class:`~src.models.Format` it materialises via the class attribute
    :attr:`format`.
    """

    #: The on-disk format this backend materialises. Overridden by subclasses.
    format: Format

    @abstractmethod
    def write(self, entries: list[dict], paths: PartitionPaths) -> WriteResult:
        """Append ``entries`` (flat record dicts) to the partition.

        Args:
            entries: Flat records (``{"ts": ..., **fields}``) to append.
            paths: Resolved on-disk paths for the target partition.

        Returns:
            A :class:`WriteResult` describing the delta and resulting size.
        """
        raise NotImplementedError

    @abstractmethod
    def read(
        self,
        paths: PartitionPaths,
        *,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
        index: Any = None,
    ) -> ReadResult:
        """Read the partition, applying ``filters`` then projecting ``columns``.

        Args:
            paths: Resolved on-disk paths for the partition.
            columns: Top-level keys to project; ``None`` for the full record.
            filters: Predicates to AND together; ``None``/empty for no filter.
            index: Optional backend-specific index hint (e.g. column stats); may
                be ignored by backends that cannot use it.

        Returns:
            A :class:`ReadResult` with the rows and scan accounting.
        """
        raise NotImplementedError

    @abstractmethod
    def size_bytes(self, paths: PartitionPaths) -> int:
        """Return the partition's on-disk size in bytes (``0`` if absent)."""
        raise NotImplementedError

    @abstractmethod
    def rewrite_from(
        self,
        source: "StorageBackend",
        src_paths: PartitionPaths,
        dst_paths: PartitionPaths,
        *,
        codecs: dict[str, str] | None = None,
        index_cols: list[str] | None = None,
    ) -> RewriteResult:
        """Rewrite ``source``'s partition into this backend's format.

        Reads all rows from ``source`` (via its :meth:`read`) and writes them in
        *this* backend's format to the ``*.new`` **staging** paths under
        ``dst_paths``. The live files are left untouched; committing the rewrite
        (atomic ``os.replace`` of each :attr:`RewriteResult.staged` pair) is the
        migration engine's job.

        Args:
            source: The backend whose data is being migrated *from*.
            src_paths: Paths the ``source`` data currently lives at.
            dst_paths: Paths the rewritten data should target (staging side).
            codecs: Optional per-column codec overrides (used by COLUMNAR).
            index_cols: Optional columns to index/sort on (used by COLUMNAR).

        Returns:
            A :class:`RewriteResult`, including the staged path pairs.
        """
        raise NotImplementedError
