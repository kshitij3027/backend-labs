"""COLUMNAR storage backend — PyArrow Parquet with projection + pushdown + codecs.

The COLUMNAR format targets the access pattern of cold / scan-heavy / few-column
("analytical") partitions: read a handful of columns out of many, with selective
predicates. Unlike the ROW and HYBRID backends — which decode whole records and
filter/project *in process* via the helpers in :mod:`src.storage.base` — this
backend pushes that work **down into Parquet**:

* **Column projection** — :meth:`read` passes ``columns`` straight to
  :meth:`pyarrow.dataset.Dataset.to_table` so only the requested column chunks
  are decoded off disk (true projection, not a post-decode dict comprehension).
* **Predicate pushdown** — the :class:`~src.models.Filter` list is compiled to a
  single :mod:`pyarrow.dataset` expression (:func:`_filter_expr`) and handed to
  ``to_table(filter=...)``. PyArrow uses each row-group's min/max statistics to
  skip groups that cannot match, and applies the residual predicate per row.
* **Per-column codecs** — writes choose a Parquet codec *per column* from the
  column's inferred logical dtype (via :class:`~src.compression.CompressionChooser`)
  and pass them as the ``compression={col: codec}`` dict form, so e.g. a long
  ``message`` column can be ZSTD while a numeric column stays SNAPPY. Reading the
  file's Parquet metadata back will show the chosen codec on each column.

Record shape
------------
Flat dicts ``{"ts": <float>, <field>: value, ...}`` — identical to the ROW
contract. Keys may be **heterogeneous across rows**; a table is built from the
sorted **union** of all keys, with missing values filled as ``None`` (which
become Parquet nulls). See :func:`_table_from_rows`.

Write path note
---------------
COLUMNAR is *not* the hot write path. Parquet has no cheap append, so
:meth:`write` performs a **read-merge-rewrite**: it reads any existing rows,
prepends them to the new batch, and rewrites the whole partition file
(atomically, via a ``*.new`` staging file + :func:`os.replace`). This is
acceptable because the selector only routes rare/cold writes here; frequent
appends stay on ROW/HYBRID.

Empty-table behaviour
---------------------
Zero rows (or rows with no keys at all) produce a valid **0-row, 0-column**
Parquet file rather than an error; :meth:`read` of such a file returns ``[]``.
This keeps sizing/round-trips uniform with the other backends.
"""

from __future__ import annotations

import os
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from ..compression import CompressionChooser
from ..models import Filter, Format
from ..paths import PartitionPaths, ensure_dir
from .base import ReadResult, RewriteResult, StorageBackend, WriteResult

__all__ = ["ColumnarBackend"]


# ---------------------------------------------------------------------------
# Table construction
# ---------------------------------------------------------------------------
def _table_from_rows(rows: list[dict]) -> pa.Table:
    """Build a :class:`pyarrow.Table` from heterogeneous flat record dicts.

    The column set is the **sorted union** of every key seen across ``rows``;
    each column is materialised as ``[row.get(k) for row in rows]`` so a key
    missing from a given row becomes ``None`` (a Parquet null). Sorting the keys
    makes the resulting schema deterministic regardless of row dict ordering.

    Args:
        rows: Flat records ``{"ts": ..., **fields}``. May be empty, and
            individual rows may carry different key sets.

    Returns:
        A :class:`pyarrow.Table`. **Empty case:** if ``rows`` is empty *or* no
        row contributes any key, an empty table with **no columns and zero rows**
        is returned (``pa.table({})``); this is a valid table that PyArrow can
        write to and read back as a 0-row file.
    """
    if not rows:
        return pa.table({})

    # Sorted union of keys across all rows -> deterministic schema.
    keys: set[str] = set()
    for row in rows:
        keys.update(row.keys())
    sorted_keys = sorted(keys)

    if not sorted_keys:
        # Rows present but all empty dicts -> no columns to build.
        return pa.table({})

    col_dict: dict[str, list[Any]] = {
        k: [row.get(k) for row in rows] for k in sorted_keys
    }
    return pa.table(col_dict)


# ---------------------------------------------------------------------------
# Dtype inference (for per-column codec selection)
# ---------------------------------------------------------------------------
def _infer_dtype(name: str, field_type: pa.DataType, values_sample: list) -> str:
    """Infer a logical dtype string (understood by :func:`codec_for_dtype`).

    The returned string is intentionally one of the keys
    :func:`~src.compression.codec_for_dtype` recognises, so the codec mapping
    stays the single source of truth. Inference is simple and deterministic:

    * ``"timestamp"`` when ``name == "ts"`` or the Arrow type is temporal
      (date/time/timestamp/duration). Timestamps map to a fast codec.
    * Strings: scan the (already small) ``values_sample`` and decide by shape —
      ``"text_low_cardinality"`` when the distinct count is small (``<= 32`` or
      ``<= 10%`` of the sample), ``"message"`` when the average string length is
      large (``> 40`` chars), otherwise ``"string_high_cardinality"``.
    * Numeric / boolean Arrow types -> ``"number"`` (speed-first SNAPPY default).
    * Anything else -> ``"string"`` as a safe, ratio-oriented fallback.

    Args:
        name: The column name (``"ts"`` is special-cased to temporal).
        field_type: The column's Arrow :class:`pyarrow.DataType`.
        values_sample: A small list of the column's Python values (may contain
            ``None``); used only to characterise string columns.

    Returns:
        A logical dtype string suitable for :func:`codec_for_dtype` /
        :meth:`CompressionChooser.codec_for`.
    """
    # Timestamp: by name or by Arrow temporal type.
    if name == "ts":
        return "timestamp"
    if (
        pa.types.is_temporal(field_type)
        or pa.types.is_timestamp(field_type)
        or pa.types.is_date(field_type)
        or pa.types.is_time(field_type)
        or pa.types.is_duration(field_type)
    ):
        return "timestamp"

    # Numeric / boolean -> "number" (speed-first).
    if (
        pa.types.is_integer(field_type)
        or pa.types.is_floating(field_type)
        or pa.types.is_decimal(field_type)
        or pa.types.is_boolean(field_type)
    ):
        return "number"

    # String-like columns: characterise by cardinality and length.
    if pa.types.is_string(field_type) or pa.types.is_large_string(field_type):
        non_null = [v for v in values_sample if v is not None]
        if not non_null:
            # No observed values -> treat as low cardinality (very compressible).
            return "text_low_cardinality"

        distinct = len(set(non_null))
        n = len(non_null)
        if distinct <= 32 or distinct <= max(1, n // 10):
            return "text_low_cardinality"

        avg_len = sum(len(str(v)) for v in non_null) / n
        if avg_len > 40:
            return "message"
        return "string_high_cardinality"

    # Unknown / nested / null type -> safe ratio-oriented default.
    return "string"


# ---------------------------------------------------------------------------
# Predicate compilation (Filter list -> pyarrow.dataset expression)
# ---------------------------------------------------------------------------
def _filter_expr(filters: list[Filter] | None) -> ds.Expression | None:
    """Compile a :class:`~src.models.Filter` list into one ``pyarrow.dataset`` expr.

    Each filter maps to a :func:`pyarrow.dataset.field` comparison; all filters
    are AND-combined (logical conjunction) with ``&``. Operator mapping:

    ====  ===========================================
    op    expression
    ====  ===========================================
    eq    ``ds.field(col) == value``
    ne    ``ds.field(col) != value``
    gt    ``ds.field(col) > value``
    gte   ``ds.field(col) >= value``
    lt    ``ds.field(col) < value``
    lte   ``ds.field(col) <= value``
    in    ``ds.field(col).isin(list(value))``
    ====  ===========================================

    Args:
        filters: Predicates to AND together; ``None`` or empty means no filter.

    Returns:
        A combined :class:`pyarrow.dataset.Expression`, or ``None`` when there is
        nothing to push down (so callers can pass ``filter=None`` to
        :meth:`~pyarrow.dataset.Dataset.to_table`). An unrecognised operator is
        skipped defensively.
    """
    if not filters:
        return None

    expr: ds.Expression | None = None
    for f in filters:
        col = ds.field(f.column)
        op = f.op
        if op == "eq":
            clause = col == f.value
        elif op == "ne":
            clause = col != f.value
        elif op == "gt":
            clause = col > f.value
        elif op == "gte":
            clause = col >= f.value
        elif op == "lt":
            clause = col < f.value
        elif op == "lte":
            clause = col <= f.value
        elif op == "in":
            clause = col.isin(list(f.value))
        else:  # pragma: no cover - Literal typing makes this unreachable.
            continue

        expr = clause if expr is None else (expr & clause)

    return expr


class ColumnarBackend(StorageBackend):
    """PyArrow/Parquet column store with projection, pushdown, and per-col codecs.

    See the module docstring for the full design. Reads push column projection
    and predicate evaluation down into Parquet; writes choose a codec per column
    from its inferred dtype and rewrite the partition atomically.
    """

    format = Format.COLUMNAR

    def __init__(self, *, compression: CompressionChooser | None = None) -> None:
        """Initialise the backend.

        Args:
            compression: The :class:`~src.compression.CompressionChooser` used to
                pick a per-column codec (and consult any learned winners). A
                default ``CompressionChooser()`` is created when ``None``.
        """
        self._compression: CompressionChooser = compression or CompressionChooser()

    # ------------------------------------------------------------------
    # Codec selection
    # ------------------------------------------------------------------
    def _codecs_for_table(
        self, table: pa.Table, learn_key_prefix: str = ""
    ) -> tuple[dict[str, str], list[str]]:
        """Pick a per-column codec map and the dictionary-encoded column list.

        For every column the logical dtype is inferred (:func:`_infer_dtype`) from
        the column's Arrow type plus a small value sample, then a codec is chosen
        via :meth:`CompressionChooser.codec_for`. Low-cardinality string columns
        are additionally flagged for Parquet **dictionary encoding** (which pairs
        especially well with GZIP/ZSTD on a small value set).

        Args:
            table: The table about to be written.
            learn_key_prefix: Optional prefix combined with the column name to form
                the ``CompressionChooser`` key, so learned codecs can be scoped
                (e.g. per tenant). Empty string uses the bare column name.

        Returns:
            ``(compression_map, dictionary_cols)`` where ``compression_map`` maps
            each column name to its codec (suitable for ``pq.write_table(...,
            compression=compression_map)``) and ``dictionary_cols`` is the list of
            column names to dictionary-encode (``use_dictionary=...``).
        """
        compression_map: dict[str, str] = {}
        dictionary_cols: list[str] = []

        # Cap the per-column value sample we inspect for dtype inference.
        sample_n = 256

        for field in table.schema:
            name = field.name
            column = table.column(name)
            sample = column.slice(0, min(sample_n, table.num_rows)).to_pylist()

            dtype = _infer_dtype(name, field.type, sample)
            key = f"{learn_key_prefix}{name}" if learn_key_prefix else name
            compression_map[name] = self._compression.codec_for(key, dtype)

            if dtype == "text_low_cardinality":
                dictionary_cols.append(name)

        return compression_map, dictionary_cols

    @staticmethod
    def _write_table(
        table: pa.Table,
        dst: "os.PathLike[str] | str",
        compression_map: dict[str, str],
        dictionary_cols: list[str],
    ) -> None:
        """Write ``table`` to ``dst`` as Parquet with per-column codecs.

        Passes the ``compression`` map and ``use_dictionary`` list through to
        :func:`pyarrow.parquet.write_table`. An **empty table** (no columns) is
        written with no per-column options, yielding a valid 0-row file.

        Args:
            table: The table to persist.
            dst: Destination file path (typically a ``*.new`` staging path).
            compression_map: Per-column codec map.
            dictionary_cols: Columns to dictionary-encode.
        """
        if table.num_columns == 0:
            # No columns: write a bare (0-row, 0-column) parquet file.
            pq.write_table(table, str(dst))
            return

        pq.write_table(
            table,
            str(dst),
            compression=compression_map,
            use_dictionary=dictionary_cols,
        )

    # ------------------------------------------------------------------
    # StorageBackend API
    # ------------------------------------------------------------------
    def write(self, entries: list[dict], paths: PartitionPaths) -> WriteResult:
        """Append ``entries`` to the partition via a read-merge-rewrite.

        Parquet cannot be appended cheaply, so this reads any rows already in
        ``paths.parquet``, prepends them to ``entries`` (existing **then** new, so
        writes are additive and order-preserving), builds a table from the union,
        chooses per-column codecs, and rewrites the whole file. The write goes to
        the ``paths.parquet_new`` staging file first and is committed with
        :func:`os.replace`, which is atomic on the same filesystem.

        This is acceptable only because COLUMNAR receives **rare cold writes** —
        the selector keeps hot append traffic on ROW/HYBRID.

        Args:
            entries: New flat record dicts to add.
            paths: Resolved partition paths.

        Returns:
            A :class:`WriteResult` whose ``row_count_delta`` counts **only the new
            entries** (not the merged total), with the post-write file size and
            the per-column codec map used.
        """
        ensure_dir(paths.dir)

        new_count = len(entries)

        # Merge existing rows (if any) ahead of the new batch -> additive writes.
        if paths.parquet.exists():
            existing = pq.read_table(str(paths.parquet)).to_pylist()
            merged = existing + list(entries)
        else:
            merged = list(entries)

        table = _table_from_rows(merged)
        compression_map, dictionary_cols = self._codecs_for_table(table)

        # Write to staging, then atomically replace the live file.
        self._write_table(table, paths.parquet_new, compression_map, dictionary_cols)
        os.replace(paths.parquet_new, paths.parquet)

        return WriteResult(
            row_count_delta=new_count,
            size_bytes=self.size_bytes(paths),
            codecs=compression_map,
        )

    def read(
        self,
        paths: PartitionPaths,
        *,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
        index: Any = None,
    ) -> ReadResult:
        """Read the partition, pushing projection + predicates down into Parquet.

        Builds a :class:`pyarrow.dataset.Dataset` over ``paths.parquet`` and calls
        :meth:`~pyarrow.dataset.Dataset.to_table` with the projected ``columns``
        and the compiled filter expression (:func:`_filter_expr`). Column
        projection is *true* projection (only requested column chunks are
        decoded); the filter exploits row-group min/max statistics to skip groups.

        Projection safety: unknown column names are intersected with the file
        schema before being passed down (PyArrow raises on unknown projection
        columns). If every requested column is unknown, one empty dict is returned
        per matching row (mirroring the in-process ``project_columns`` contract).

        Args:
            paths: Resolved partition paths.
            columns: Top-level keys to project; ``None`` for the full record.
            filters: Predicates to AND together; ``None``/empty for no filter.
            index: Unused here (row-group skipping is automatic); accepted for ABC
                compatibility. Wired further in a later commit.

        Returns:
            A :class:`ReadResult`. ``rows_scanned`` is the post-pushdown row count
            (rows actually returned by Parquet after group skipping + predicate),
            and ``rowgroups_skipped`` is ``0`` here (surfaced in C18). A missing
            file yields ``ReadResult([], 0)``.
        """
        if not paths.parquet.exists():
            return ReadResult([], 0)

        dataset = ds.dataset(str(paths.parquet), format="parquet")
        expr = _filter_expr(filters)

        if columns is None:
            table = dataset.to_table(filter=expr)
            rows = table.to_pylist()
            return ReadResult(rows, rows_scanned=table.num_rows, rowgroups_skipped=0)

        # Projection: only request columns that actually exist in the schema.
        schema_names = set(dataset.schema.names)
        projected = [c for c in columns if c in schema_names]

        if not projected:
            # No requested column exists: count matching rows, return empty dicts.
            table = dataset.to_table(columns=[], filter=expr)
            return ReadResult(
                [{} for _ in range(table.num_rows)],
                rows_scanned=table.num_rows,
                rowgroups_skipped=0,
            )

        table = dataset.to_table(columns=projected, filter=expr)
        rows = table.to_pylist()
        return ReadResult(rows, rows_scanned=table.num_rows, rowgroups_skipped=0)

    def size_bytes(self, paths: PartitionPaths) -> int:
        """Return ``paths.parquet``'s size in bytes, or ``0`` if it does not exist."""
        try:
            return paths.parquet.stat().st_size
        except FileNotFoundError:
            return 0

    def rewrite_from(
        self,
        source: "StorageBackend",
        src_paths: PartitionPaths,
        dst_paths: PartitionPaths,
        *,
        codecs: dict[str, str] | None = None,
        index_cols: list[str] | None = None,
    ) -> RewriteResult:
        """Copy-on-write rewrite of ``source``'s data into COLUMNAR/Parquet.

        Reads **all** rows from ``source`` (full record, no filter), builds a table
        from their key-union, chooses per-column codecs, and writes the result to
        the ``dst_paths.parquet_new`` **staging** path — the live file is left
        untouched. The staging file is fsynced so it is durable before the
        migration engine flips the manifest pointer (the engine performs the
        atomic ``os.replace`` of the returned :attr:`RewriteResult.staged` pair).

        Explicit ``codecs`` (if provided) **override** the inferred map on a
        per-column basis (explicit wins; columns absent from the override keep
        their inferred codec). ``index_cols`` is accepted for ABC compatibility
        and is not used for sorting here.

        Args:
            source: Backend to migrate from.
            src_paths: Where the source data currently lives.
            dst_paths: Target partition paths (the ``.new`` staging side is used).
            codecs: Optional per-column codec overrides (merged over the inferred
                map).
            index_cols: Accepted for ABC compatibility; unused.

        Returns:
            A :class:`RewriteResult` whose ``staged`` holds the single
            ``(dst_paths.parquet_new, dst_paths.parquet)`` pair for the engine to
            ``os.replace``. ``row_count`` and ``size_bytes`` describe the staged
            file; ``codecs`` is the effective (inferred + overridden) map. The
            migration engine recomputes the uncompressed estimate itself via
            ``table.nbytes``.
        """
        rows = source.read(src_paths, columns=None, filters=None).rows
        ensure_dir(dst_paths.dir)

        table = _table_from_rows(rows)
        compression_map, dictionary_cols = self._codecs_for_table(table)

        # Explicit per-column overrides win; drop dictionary flags whose codec was
        # overridden away from the low-cardinality choice is unnecessary — keeping
        # the flag is harmless, so we only merge the codec map here.
        if codecs:
            for col, codec in codecs.items():
                if col in compression_map:
                    compression_map[col] = codec

        staging = dst_paths.parquet_new
        self._write_table(table, staging, compression_map, dictionary_cols)

        # Durably flush the staging file before the engine flips the pointer.
        fd = os.open(str(staging), os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

        return RewriteResult(
            row_count=table.num_rows,
            size_bytes=staging.stat().st_size,
            codecs=compression_map,
            staged=[(staging, dst_paths.parquet)],
        )
