"""Unit tests for the on-disk layout / partition-identity helpers in ``src.paths``.

Covers the pure time-bucket math (:func:`bucket_for`, :func:`partition_id_for`),
the resolved :class:`PartitionPaths` file names + copy-on-write staging variants,
the manifest path helpers, and the single side-effecting :func:`ensure_dir`.
"""
from __future__ import annotations

from pathlib import Path

from src.paths import (
    bucket_for,
    ensure_dir,
    manifest_path,
    manifest_tmp_path,
    partition_id_for,
    partition_paths,
)


def test_bucket_for_floors_by_bucket_seconds() -> None:
    assert bucket_for(0, 3600) == 0
    assert bucket_for(3599, 3600) == 0
    assert bucket_for(3600, 3600) == 1
    assert bucket_for(7200, 3600) == 2


def test_partition_id_for_formats_bucket() -> None:
    assert partition_id_for(7200, 3600) == "p_2"


def test_partition_paths_dir_and_file_names(tmp_path: Path) -> None:
    pp = partition_paths(tmp_path, "acme", "p_2")
    assert pp.dir == tmp_path / "acme" / "p_2"
    assert pp.row.name == "row.jsonl.lz4"
    assert pp.parquet.name == "data.parquet"
    assert pp.recent.name == "recent.jsonl.lz4"


def test_partition_paths_staging_variants(tmp_path: Path) -> None:
    pp = partition_paths(tmp_path, "acme", "p_2")
    # ``.new`` staging path is the live path with ".new" appended.
    assert pp.row_new == pp.row.with_name(pp.row.name + ".new")
    assert str(pp.row_new) == str(pp.row) + ".new"
    assert pp.row_new != pp.row
    # The other two staging variants follow the same rule.
    assert str(pp.parquet_new) == str(pp.parquet) + ".new"
    assert str(pp.recent_new) == str(pp.recent) + ".new"


def test_manifest_paths(tmp_path: Path) -> None:
    mp = manifest_path(tmp_path, "acme")
    mtp = manifest_tmp_path(tmp_path, "acme")
    assert mp == tmp_path / "acme" / "manifest.json"
    assert mtp == tmp_path / "acme" / "manifest.json.new"
    assert mp.name == "manifest.json"
    assert mtp.name == "manifest.json.new"


def test_ensure_dir_creates_and_is_idempotent(tmp_path: Path) -> None:
    target = tmp_path / "a" / "b"
    assert not target.exists()
    returned = ensure_dir(target)
    assert returned == target
    assert target.is_dir()
    # Idempotent: a second call on an existing dir must not raise and returns it.
    again = ensure_dir(target)
    assert again == target
    assert target.is_dir()
