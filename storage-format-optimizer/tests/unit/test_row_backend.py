"""Unit tests for the ROW storage backend (``src.storage.row_backend``).

Exercises the append-only length-prefixed LZ4 block format end-to-end: multi-block
round-trip reads, column projection, in-process filtering (eq/in/gte), scan
accounting (``rows_scanned``), sizing, the missing-partition path, and the
copy-on-write ``rewrite_from`` staging contract.
"""
from __future__ import annotations

import os
from pathlib import Path

from src.models import Filter
from src.paths import partition_paths
from src.storage.base import ReadResult
from src.storage.row_backend import RowBackend


def _rows() -> list[dict]:
    return [
        {"ts": 1.0, "user": "a", "level": "INFO"},
        {"ts": 2.0, "user": "b", "level": "WARN"},
        {"ts": 3.0, "user": "a", "level": "ERROR"},
    ]


def test_write_two_blocks_read_returns_all_rows_in_order(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = RowBackend()

    backend.write(_rows()[:2], paths)
    backend.write(_rows()[2:], paths)

    result = backend.read(paths)
    assert result.rows == _rows()
    assert result.rows_scanned == 3


def test_read_projection_keeps_only_requested_columns(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = RowBackend()
    backend.write(_rows(), paths)

    result = backend.read(paths, columns=["user"])
    assert [r["user"] for r in result.rows] == ["a", "b", "a"]
    # Projection drops every other key.
    for row in result.rows:
        assert set(row) == {"user"}


def test_read_filter_eq(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = RowBackend()
    backend.write(_rows(), paths)

    result = backend.read(
        paths, filters=[Filter(column="level", op="eq", value="ERROR")]
    )
    assert len(result.rows) == 1
    assert result.rows[0]["ts"] == 3.0
    # rows_scanned reflects pre-filter work: all 3 rows were decoded.
    assert result.rows_scanned == 3


def test_read_filter_in(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = RowBackend()
    backend.write(_rows(), paths)

    result = backend.read(
        paths, filters=[Filter(column="level", op="in", value=["INFO", "WARN"])]
    )
    assert len(result.rows) == 2
    assert {r["level"] for r in result.rows} == {"INFO", "WARN"}


def test_read_filter_gte(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = RowBackend()
    backend.write(_rows(), paths)

    result = backend.read(paths, filters=[Filter(column="ts", op="gte", value=2.0)])
    assert len(result.rows) == 2
    assert {r["ts"] for r in result.rows} == {2.0, 3.0}
    assert result.rows_scanned == 3


def test_size_bytes_grows_after_writes(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = RowBackend()

    # Missing partition reports zero size.
    assert backend.size_bytes(paths) == 0

    backend.write(_rows()[:1], paths)
    after_first = backend.size_bytes(paths)
    assert after_first > 0

    backend.write(_rows()[1:], paths)
    after_second = backend.size_bytes(paths)
    assert after_second > after_first


def test_read_missing_partition_returns_empty(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_missing")
    backend = RowBackend()

    result = backend.read(paths)
    assert isinstance(result, ReadResult)
    assert result.rows == []
    assert result.rows_scanned == 0
    assert backend.size_bytes(paths) == 0


def test_rewrite_from_stages_then_commits(tmp_data_dir: Path) -> None:
    # Source partition holds 3 rows in ROW format.
    src_paths = partition_paths(tmp_data_dir, "acme", "p_src")
    src_backend = RowBackend()
    src_backend.write(_rows(), src_paths)
    expected = src_backend.read(src_paths).rows

    # Rewrite into a separate destination partition's staging path.
    dst_paths = partition_paths(tmp_data_dir, "acme", "p_dst")
    result = RowBackend().rewrite_from(src_backend, src_paths, dst_paths)

    # The staging file exists; the live file does NOT yet (copy-on-write).
    assert dst_paths.row_new.exists()
    assert not dst_paths.row.exists()
    assert result.staged == [(dst_paths.row_new, dst_paths.row)]
    assert result.row_count == 3

    # Engine commits by os.replace-ing the staged pair; the live read then matches.
    os.replace(*result.staged[0])
    committed = RowBackend().read(dst_paths)
    assert committed.rows == expected
