"""Unit tests for the HYBRID storage backend (``src.storage.hybrid_backend``).

HYBRID composes a hot ROW "recent" side (``paths.recent``) with a sealed COLUMNAR
side (``paths.parquet``). These tests exercise the composition end-to-end:

* writes land on the recent side and read back before any Parquet exists;
* :meth:`read` unions the sealed Parquet and recent ROW rows;
* :meth:`seal` moves aged rows recent -> Parquet with no loss or duplication, is
  idempotent, and never seals rows lacking a ``ts``;
* projection and filtering work uniformly across both sides;
* :meth:`rewrite_from` seeds the sealed side (delegating to the columnar staging
  contract); and
* :meth:`size_bytes` sums both sides.
"""
from __future__ import annotations

import os
from pathlib import Path

from src.models import Filter
from src.paths import partition_paths
from src.storage.columnar_backend import ColumnarBackend
from src.storage.hybrid_backend import HybridBackend
from src.storage.row_backend import RowBackend


def test_write_then_read_recent_only(tmp_data_dir: Path) -> None:
    # A HYBRID partition with no Parquet yet reads purely from the recent side.
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    HybridBackend().write(
        [{"ts": 10.0, "u": "a"}, {"ts": 11.0, "u": "b"}], paths
    )

    rows = HybridBackend().read(paths).rows
    assert {r["u"] for r in rows} == {"a", "b"}
    assert len(rows) == 2
    # No sealed file should exist yet — recent-only partition.
    assert not paths.parquet.exists()


def test_read_unions_sealed_and_recent(tmp_data_dir: Path) -> None:
    # Seed the recent side with two rows, then seal the older one into Parquet so
    # the partition straddles both sides.
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    HybridBackend().write(
        [{"ts": 10.0, "u": "a"}, {"ts": 11.0, "u": "b"}], paths
    )

    sealed = HybridBackend().seal(paths, older_than_ts=10.5)
    assert sealed == 1  # ts=10.0 sealed; ts=11.0 stays recent.
    assert paths.parquet.exists()  # sealed Parquet now present.

    rows = HybridBackend().read(paths).rows
    users = [r["u"] for r in rows]
    # Both rows present exactly once: one from sealed Parquet, one from recent.
    assert sorted(users) == ["a", "b"]
    assert len(users) == 2  # no duplicates across the two sides.


def test_seal_no_loss_no_dup_and_idempotent(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = HybridBackend()
    backend.write([{"ts": float(i), "u": f"r{i}"} for i in range(1, 6)], paths)

    # Seal ts in {1,2,3} (strictly < 3.5); {4,5} stay recent.
    n = backend.seal(paths, older_than_ts=3.5)
    assert n == 3

    # Every original row is still readable exactly once (no loss, no dup).
    rows = backend.read(paths).rows
    assert sorted(r["u"] for r in rows) == ["r1", "r2", "r3", "r4", "r5"]
    assert len(rows) == 5

    # Idempotent: nothing left below the cutoff in recent -> second seal is a no-op.
    assert backend.seal(paths, older_than_ts=3.5) == 0

    # Rows with no/None ts are never aged out, even under a very high cutoff.
    # (The remaining ts-bearing recent rows r4/r5 are legitimately swept; the
    # no-ts row "x" must NOT be — it has no timestamp to compare against.)
    backend.write([{"u": "x"}], paths)
    backend.seal(paths, older_than_ts=1_000_000.0)
    # "x" is still readable and still lives on the recent side, not in Parquet.
    rows_after = backend.read(paths).rows
    assert "x" in {r.get("u") for r in rows_after}
    recent_users = {r.get("u") for r in RowBackend(file_attr="recent").read(paths).rows}
    assert "x" in recent_users
    sealed_users = {r.get("u") for r in ColumnarBackend().read(paths).rows}
    assert "x" not in sealed_users


def test_projection_and_filter_across_sides(tmp_data_dir: Path) -> None:
    # Put "a" on the sealed side and "b" on the recent side.
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = HybridBackend()
    backend.write([{"ts": 10.0, "u": "a"}, {"ts": 11.0, "u": "b"}], paths)
    assert backend.seal(paths, older_than_ts=10.5) == 1  # "a" -> Parquet.

    # Projection: each returned row dict has exactly the requested key.
    projected = backend.read(paths, columns=["u"]).rows
    assert len(projected) == 2
    for row in projected:
        assert set(row) == {"u"}
    assert {row["u"] for row in projected} == {"a", "b"}

    # Filter "u == a": matches the sealed-side row only.
    only_a = backend.read(
        paths, filters=[Filter(column="u", op="eq", value="a")]
    ).rows
    assert [r["u"] for r in only_a] == ["a"]

    # Filter "u == b": matches the recent-side row only.
    only_b = backend.read(
        paths, filters=[Filter(column="u", op="eq", value="b")]
    ).rows
    assert [r["u"] for r in only_b] == ["b"]


def test_rewrite_from_row_into_hybrid_seeds_sealed_side(tmp_data_dir: Path) -> None:
    # Source partition holds 3 rows in plain ROW format.
    src_paths = partition_paths(tmp_data_dir, "acme", "src")
    src_rows = [{"ts": float(i), "u": f"s{i}"} for i in range(1, 4)]
    RowBackend().write(src_rows, src_paths)

    dst = partition_paths(tmp_data_dir, "acme", "dst")
    res = HybridBackend().rewrite_from(RowBackend(), src_paths, dst)

    # Migration into HYBRID seeds the sealed (Parquet) side via the columnar
    # staging contract: stage to parquet_new, live parquet not yet present.
    assert res.staged == [(dst.parquet_new, dst.parquet)]
    assert dst.parquet_new.exists()
    assert not dst.parquet.exists()

    # Engine commits the staged pair; all rows now live on the sealed side.
    os.replace(*res.staged[0])
    rows = HybridBackend().read(dst).rows
    assert sorted(r["u"] for r in rows) == ["s1", "s2", "s3"]
    assert len(rows) == 3
    # Recent side starts empty post-migration (no recent file written).
    assert not dst.recent.exists()


def test_size_bytes_sums_recent_and_sealed(tmp_data_dir: Path) -> None:
    paths = partition_paths(tmp_data_dir, "acme", "p_1")
    backend = HybridBackend()
    backend.write([{"ts": float(i), "u": f"r{i}"} for i in range(1, 6)], paths)
    assert backend.seal(paths, older_than_ts=3.5) == 3  # split across both sides.

    recent_size = RowBackend(file_attr="recent").size_bytes(paths)
    parquet_size = paths.parquet.stat().st_size
    assert recent_size > 0
    assert parquet_size > 0
    assert backend.size_bytes(paths) == recent_size + parquet_size
