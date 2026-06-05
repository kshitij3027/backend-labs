"""HYBRID storage backend — a hot ROW "recent" side over a sealed COLUMNAR side.

The HYBRID format targets partitions that are still receiving writes yet are
*also* queried analytically: it keeps the best of both worlds by composing the
two existing single-format backends rather than inventing a third on-disk layout.

Two physical sides, one logical partition
-----------------------------------------
* **Recent side (ROW).** A :class:`~src.storage.row_backend.RowBackend` bound to
  ``paths.recent`` (``RowBackend(file_attr="recent")``). New writes land here —
  appending one cheap length-prefixed LZ4 block per batch, exactly like a plain
  ROW partition. This is the fast write path.
* **Sealed side (COLUMNAR).** A :class:`~src.storage.columnar_backend.ColumnarBackend`
  over ``paths.parquet``. Older rows are periodically **sealed** (moved) out of
  the recent side into this Parquet file, where they gain column projection,
  predicate pushdown, and per-column codecs for analytical reads.

A :meth:`read` therefore unions both sides: it reads the sealed Parquet (with
projection/pushdown) and the recent ROW blocks (decoded + filtered in-process)
and concatenates the rows. Each side independently returns empty when its file is
absent, so a freshly-created HYBRID partition (recent rows only, no Parquet yet)
and a fully-sealed one (Parquet only, recent compacted to empty) both read
correctly.

Sealing
-------
:meth:`seal` moves rows whose ``ts`` is older than a cutoff from the recent side
into the sealed Parquet (an additive :meth:`ColumnarBackend.write` merge), then
rewrites the recent file to hold only the kept (younger) rows — atomically, via a
``paths.recent_new`` staging file and :func:`os.replace`. Every original recent
row ends up in **exactly one** side (no loss, no duplication); ordering within a
side may change, which is acceptable.

Migration into HYBRID
---------------------
:meth:`rewrite_from` seeds a partition's sealed side with *all* prior rows by
delegating to :meth:`ColumnarBackend.rewrite_from` (staging to ``parquet_new`` →
``parquet``). Post-migration the sealed side holds the historical rows and the
recent side starts empty; subsequent writes append to the recent side again.
"""

from __future__ import annotations

import os
from typing import Any

from ..compression import CompressionChooser
from ..models import Filter, Format
from ..paths import PartitionPaths, ensure_dir
from .base import ReadResult, RewriteResult, StorageBackend, WriteResult
from .columnar_backend import ColumnarBackend
from .row_backend import RowBackend, _encode_block

__all__ = ["HybridBackend"]


class HybridBackend(StorageBackend):
    """Composite backend: a ROW recent side unioned with a sealed COLUMNAR side.

    See the module docstring for the design. This class owns no on-disk format of
    its own — it composes a :class:`~src.storage.row_backend.RowBackend`
    (``file_attr="recent"`` → ``paths.recent``) for fast appends and a
    :class:`~src.storage.columnar_backend.ColumnarBackend` (``paths.parquet``) for
    sealed analytical data, and coordinates reads, sealing, and migration across
    the two.
    """

    format = Format.HYBRID

    def __init__(self, *, compression: CompressionChooser | None = None) -> None:
        """Initialise the two composed sides.

        Args:
            compression: The :class:`~src.compression.CompressionChooser` handed
                to the sealed COLUMNAR side for per-column codec selection. The
                recent ROW side takes no codec config (it uses a single LZ4
                frame per block). ``None`` lets the columnar side build its own
                default chooser.
        """
        self._recent: RowBackend = RowBackend(file_attr="recent")
        self._sealed: ColumnarBackend = ColumnarBackend(compression=compression)

    # ------------------------------------------------------------------
    # StorageBackend API
    # ------------------------------------------------------------------
    def write(self, entries: list[dict], paths: PartitionPaths) -> WriteResult:
        """Append ``entries`` to the recent (ROW) side.

        HYBRID takes new writes on the fast row side; rows migrate to the sealed
        Parquet side later via :meth:`seal`. This is a thin delegation to
        :meth:`RowBackend.write` against ``paths.recent``.

        Args:
            entries: Flat record dicts to append.
            paths: Resolved partition paths.

        Returns:
            The recent side's :class:`WriteResult` (``row_count_delta`` is the
            number of appended entries; ``size_bytes`` reflects only the recent
            file, consistent with that backend's contract).
        """
        return self._recent.write(entries, paths)

    def read(
        self,
        paths: PartitionPaths,
        *,
        columns: list[str] | None = None,
        filters: list[Filter] | None = None,
        index: Any = None,
    ) -> ReadResult:
        """Read **both** sides and union their rows.

        Reads the sealed Parquet (column projection + predicate pushdown via
        :meth:`ColumnarBackend.read`) and the recent ROW blocks (decoded then
        filtered/projected in-process via :meth:`RowBackend.read`) under the same
        ``columns``/``filters``, then concatenates ``sealed.rows + recent.rows``.
        Each side returns empty when its file is missing, so a HYBRID partition
        with only one side populated still reads correctly.

        Args:
            paths: Resolved partition paths.
            columns: Top-level keys to project; ``None`` for full records.
            filters: Predicates to AND together; ``None``/empty for no filter.
            index: Optional index hint; forwarded to the sealed side (the recent
                ROW side ignores it).

        Returns:
            A combined :class:`ReadResult`: ``rows`` is sealed-then-recent;
            ``rows_scanned`` sums both sides' scanned counts; ``rowgroups_skipped``
            is the sealed side's value (the recent ROW side has no row groups).
        """
        sealed = self._sealed.read(
            paths, columns=columns, filters=filters, index=index
        )
        recent = self._recent.read(paths, columns=columns, filters=filters)

        rows = sealed.rows + recent.rows
        rows_scanned = sealed.rows_scanned + recent.rows_scanned
        rowgroups_skipped = sealed.rowgroups_skipped
        return ReadResult(
            rows,
            rows_scanned=rows_scanned,
            rowgroups_skipped=rowgroups_skipped,
        )

    def size_bytes(self, paths: PartitionPaths) -> int:
        """Return the combined on-disk size of the recent + sealed sides.

        Sums :meth:`RowBackend.size_bytes` (``paths.recent``) and
        :meth:`ColumnarBackend.size_bytes` (``paths.parquet``); a missing file on
        either side contributes ``0``.
        """
        return self._recent.size_bytes(paths) + self._sealed.size_bytes(paths)

    def seal(
        self,
        paths: PartitionPaths,
        *,
        older_than_ts: float | None = None,
        age_seconds: float | None = None,
        now: float | None = None,
    ) -> int:
        """Move aged rows from the recent (ROW) side into the sealed (Parquet) side.

        A row is sealed when it has a timestamp and that timestamp is **strictly
        older** than ``cutoff``. The cutoff is resolved as:

        * ``older_than_ts`` when given (an absolute epoch boundary); else
        * ``now - age_seconds`` when both ``age_seconds`` and ``now`` are given; else
        * nothing to do → return ``0`` (no recent file is touched).

        Mechanics (no row loss or duplication — every original recent row ends up
        in exactly one of {sealed Parquet, kept recent}):

        1. Read all recent rows. Partition them into ``to_seal`` (``ts`` present
           and ``ts < cutoff``) and ``keep`` (everything else, including rows with
           no/None ``ts`` — these are never aged out).
        2. If ``to_seal`` is empty, return ``0`` immediately (recent file
           untouched).
        3. Append ``to_seal`` into the sealed Parquet via
           :meth:`ColumnarBackend.write` (an additive read-merge-rewrite).
        4. Rewrite the recent file to contain only ``keep``: encode ``keep`` as a
           single fresh ROW block to ``paths.recent_new``, fsync it, then
           :func:`os.replace` it over ``paths.recent`` (atomic on one filesystem).
           When ``keep`` is empty an **empty block** is written (rather than
           deleting the file) so the recent side stays a valid, readable 0-row
           file — uniform with :meth:`RowBackend.write`'s empty-batch policy.

        The sealed append (step 3) is committed before the recent file is
        truncated (step 4), so an interruption between them can at worst leave a
        row in *both* sides transiently; it can never drop a row.

        Args:
            paths: Resolved partition paths.
            older_than_ts: Absolute epoch cutoff; rows with ``ts`` below this seal.
            age_seconds: Relative age (seconds) used with ``now`` to derive a
                cutoff when ``older_than_ts`` is not given.
            now: Current epoch time, paired with ``age_seconds``.

        Returns:
            The number of rows sealed (moved recent → Parquet). ``0`` when there
            is no cutoff or nothing aged out.
        """
        # Resolve the cutoff; bail out cheaply when there is nothing to do.
        if older_than_ts is not None:
            cutoff = older_than_ts
        elif age_seconds is not None and now is not None:
            cutoff = now - age_seconds
        else:
            return 0

        rows = self._recent.read(paths).rows
        to_seal = [
            r for r in rows if (r.get("ts") is not None and r["ts"] < cutoff)
        ]
        if not to_seal:
            return 0
        keep = [
            r for r in rows if not (r.get("ts") is not None and r["ts"] < cutoff)
        ]

        # Step 3: additively merge the aged rows into the sealed Parquet.
        self._sealed.write(to_seal, paths)

        # Step 4: atomically rewrite the recent side to hold only ``keep``.
        ensure_dir(paths.dir)
        staging = paths.recent_new
        block = _encode_block(keep)  # empty ``keep`` -> valid empty block
        with open(staging, "wb") as fh:
            fh.write(block)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(staging, paths.recent)

        return len(to_seal)

    def rewrite_from(
        self,
        source: "StorageBackend",
        src_paths: PartitionPaths,
        dst_paths: PartitionPaths,
        *,
        codecs: dict[str, str] | None = None,
        index_cols: list[str] | None = None,
    ) -> RewriteResult:
        """Migrate ``source``'s data **into** HYBRID by seeding its sealed side.

        All source rows are written to the sealed COLUMNAR/Parquet staging exactly
        as a plain columnar migration would — this is a direct delegation to
        :meth:`ColumnarBackend.rewrite_from`, so the returned ``staged`` targets
        ``(dst_paths.parquet_new, dst_paths.parquet)``. After the engine commits
        that pair, the partition's sealed side holds every prior row and the
        recent side starts empty; later :meth:`write` calls append to the recent
        side again.

        Args:
            source: Backend to migrate from.
            src_paths: Where the source data currently lives.
            dst_paths: Target partition paths (the Parquet ``.new`` staging side
                is used).
            codecs: Optional per-column codec overrides (forwarded to the sealed
                side).
            index_cols: Optional columns to index/sort on (forwarded to the
                sealed side).

        Returns:
            The sealed side's :class:`RewriteResult` — its ``staged`` is the
            ``(parquet_new, parquet)`` pair for the migration engine to
            ``os.replace``.
        """
        return self._sealed.rewrite_from(
            source, src_paths, dst_paths, codecs=codecs, index_cols=index_cols
        )
