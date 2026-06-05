"""Unit tests for the COLUMNAR storage backend (``src.storage.columnar_backend``).

Exercises the Parquet-backed format end-to-end: read-merge-rewrite round-trips,
true column projection, predicate pushdown (eq / in / gte) compiled to a
``pyarrow.dataset`` expression, per-column codec selection (verified against the
on-disk Parquet metadata), the compression ratio achieved on repetitive data,
the copy-on-write ``rewrite_from`` staging contract, and the empty/missing path.

Codec metadata note: PyArrow's ``row_group(i).column(j).compression`` reports the
codec name in upper case (e.g. ``"GZIP"``), matching this project's
``VALID_CODECS``. We still ``.upper()`` it defensively in-test so a future PyArrow
that returns ``"gzip"`` would not break these assertions — the product code is
never adjusted to suit the test.
"""
from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.compression import VALID_CODECS, codec_for_dtype
from src.models import Filter
from src.paths import partition_paths
from src.storage.base import ReadResult
from src.storage.columnar_backend import ColumnarBackend
from src.storage.row_backend import RowBackend


def _rows() -> list[dict]:
    return [
        {"ts": 1.0, "user": "a", "level": "INFO"},
        {"ts": 2.0, "user": "b", "level": "ERROR"},
    ]


def _column_compression(parquet_path: Path) -> dict[str, str]:
    """Map each column name -> its row-group-0 Parquet codec (upper-cased)."""
    pf = pq.ParquetFile(str(parquet_path))
    rg = pf.metadata.row_group(0)
    out: dict[str, str] = {}
    for i in range(rg.num_columns):
        col = rg.column(i)
        out[col.path_in_schema] = str(col.compression).upper()
    return out


# ---------------------------------------------------------------------------
# 1. Round-trip
# ---------------------------------------------------------------------------
def test_round_trip_preserves_all_rows(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = ColumnarBackend()

    backend.write(_rows(), paths)
    result = backend.read(paths)

    assert result.rows_scanned == 2
    # Parquet may reorder columns within a dict; compare value-by-value.
    got = sorted(result.rows, key=lambda r: r["ts"])
    assert got == [
        {"ts": 1.0, "user": "a", "level": "INFO"},
        {"ts": 2.0, "user": "b", "level": "ERROR"},
    ]


# ---------------------------------------------------------------------------
# 2. Projection (true Parquet column projection)
# ---------------------------------------------------------------------------
def test_projection_returns_only_requested_column(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = ColumnarBackend()
    backend.write(_rows(), paths)

    result = backend.read(paths, columns=["user"])

    assert len(result.rows) == 2
    for row in result.rows:
        assert set(row) == {"user"}
    assert {r["user"] for r in result.rows} == {"a", "b"}


# ---------------------------------------------------------------------------
# 3. Predicate pushdown (eq / in / gte)
# ---------------------------------------------------------------------------
def test_pushdown_eq(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = ColumnarBackend()
    backend.write(_rows(), paths)

    result = backend.read(
        paths, filters=[Filter(column="level", op="eq", value="ERROR")]
    )
    assert len(result.rows) == 1
    assert result.rows[0]["user"] == "b"


def test_pushdown_in(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = ColumnarBackend()
    backend.write(_rows(), paths)

    result = backend.read(
        paths, filters=[Filter(column="level", op="in", value=["INFO", "ERROR"])]
    )
    assert len(result.rows) == 2
    assert {r["level"] for r in result.rows} == {"INFO", "ERROR"}


def test_pushdown_gte(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = ColumnarBackend()
    backend.write(_rows(), paths)

    result = backend.read(paths, filters=[Filter(column="ts", op="gte", value=2.0)])
    assert len(result.rows) == 1
    assert result.rows[0]["ts"] == 2.0
    assert result.rows[0]["user"] == "b"


# ---------------------------------------------------------------------------
# 4. Per-column codec applied (verified against on-disk Parquet metadata)
# ---------------------------------------------------------------------------
def test_per_column_codec_applied(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_codec")
    backend = ColumnarBackend()

    # Low-cardinality ``level`` (only two distinct values across many rows) and a
    # numeric ``ts`` column. The default chooser maps these via codec_for_dtype:
    #   ts    -> "timestamp"              -> SNAPPY
    #   level -> "text_low_cardinality"   -> GZIP
    rows = [
        {"ts": float(i), "level": "INFO" if i % 2 == 0 else "ERROR"}
        for i in range(100)
    ]
    backend.write(rows, paths)

    on_disk = _column_compression(paths.parquet)

    # Every column must use a codec from the supported set.
    for col, codec in on_disk.items():
        assert codec in VALID_CODECS, f"{col} -> {codec} not in {VALID_CODECS}"

    # The chooser's dtype mapping is the single source of truth; assert each
    # column matches what codec_for_dtype dictates for its inferred dtype.
    assert on_disk["ts"] == codec_for_dtype("timestamp")  # SNAPPY
    assert on_disk["level"] == codec_for_dtype("text_low_cardinality")  # GZIP
    # Sanity: at least one column uses a non-SNAPPY codec (level -> GZIP).
    assert on_disk["level"] != "SNAPPY"


# ---------------------------------------------------------------------------
# 5. Compression ratio < 1 (on-disk smaller than in-memory uncompressed)
# ---------------------------------------------------------------------------
def test_compression_shrinks_repetitive_data(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_ratio")
    backend = ColumnarBackend()

    rows = [
        {"ts": float(i), "level": "INFO", "msg": "same message"}
        for i in range(2000)
    ]
    backend.write(rows, paths)

    # Uncompressed in-memory footprint of the equivalent Arrow table.
    table = pa.table(
        {
            "ts": [r["ts"] for r in rows],
            "level": [r["level"] for r in rows],
            "msg": [r["msg"] for r in rows],
        }
    )
    uncompressed = table.nbytes
    on_disk = backend.size_bytes(paths)

    assert on_disk > 0
    assert on_disk < uncompressed, (
        f"on-disk {on_disk} not smaller than uncompressed {uncompressed}"
    )


# ---------------------------------------------------------------------------
# 6. rewrite_from (copy-on-write) from a ROW source
# ---------------------------------------------------------------------------
def test_rewrite_from_row_source_stages_then_commits(tmp_data_dir: Path) -> None:
    # Build a ROW source partition with 3 rows.
    src_paths = partition_paths(tmp_data_dir, "acme", "src")
    src_backend = RowBackend()
    src_backend.write(
        [
            {"ts": 1.0, "user": "a", "level": "INFO"},
            {"ts": 2.0, "user": "b", "level": "WARN"},
            {"ts": 3.0, "user": "c", "level": "ERROR"},
        ],
        src_paths,
    )
    expected = src_backend.read(src_paths).rows

    dst = partition_paths(tmp_data_dir, "acme", "dst")
    res = ColumnarBackend().rewrite_from(src_backend, src_paths, dst)

    # Copy-on-write: the staging file exists, the live file does NOT yet.
    assert dst.parquet_new.exists()
    assert not dst.parquet.exists()
    assert res.staged == [(dst.parquet_new, dst.parquet)]
    assert res.row_count == 3
    assert isinstance(res.codecs, dict) and len(res.codecs) > 0

    # Engine commits via os.replace; the live read then matches the source.
    os.replace(*res.staged[0])
    committed = ColumnarBackend().read(dst)
    assert committed.rows_scanned == 3
    got = sorted(committed.rows, key=lambda r: r["ts"])
    assert got == sorted(expected, key=lambda r: r["ts"])


# ---------------------------------------------------------------------------
# 7. Empty / missing partition
# ---------------------------------------------------------------------------
def test_read_missing_partition_returns_empty(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_missing")
    backend = ColumnarBackend()

    result = backend.read(paths)
    assert isinstance(result, ReadResult)
    assert result.rows == []
    assert result.rows_scanned == 0
    assert backend.size_bytes(paths) == 0
