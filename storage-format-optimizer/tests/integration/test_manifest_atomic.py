"""Integration tests for the async mutators + atomic persistence of ``ManifestStore``.

These exercise the write path end-to-end through the filesystem: every assertion
reloads via a **fresh** :class:`ManifestStore` instance pointed at the same data
dir, so we verify what actually landed on disk (and survived ``os.replace``),
not just an in-memory cache. ``asyncio_mode=auto`` (see ``pytest.ini``) lets the
``async def test_*`` functions run without an explicit decorator.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from src.manifest import ManifestStore, PartitionMeta
from src.models import Format
from src.paths import manifest_path


async def test_upsert_partition_persists_to_disk(tmp_data_dir: Path) -> None:
    tenant = "acme"
    writer = ManifestStore(tmp_data_dir)
    await writer.upsert_partition(tenant, PartitionMeta("p_1", format=Format.ROW))

    # The live manifest file must exist.
    assert manifest_path(tmp_data_dir, tenant).exists()

    # A brand-new store (cold cache) must read the partition back from disk.
    reader = ManifestStore(tmp_data_dir)
    reloaded = reader.load(tenant)
    assert "p_1" in reloaded.partitions
    assert reloaded.partitions["p_1"].format is Format.ROW
    assert reloaded.version >= 1


async def test_swap_format_records_migration(tmp_data_dir: Path) -> None:
    tenant = "acme"
    writer = ManifestStore(tmp_data_dir)
    await writer.upsert_partition(
        tenant,
        PartitionMeta("p_1", format=Format.ROW, paths={"row": "p_1/row.jsonl.lz4"}),
    )

    await writer.swap_format(
        tenant,
        "p_1",
        new_format=Format.COLUMNAR,
        new_paths={"parquet": "p_1/data.parquet"},
        new_codecs={"ts": "SNAPPY"},
        new_size=123,
        from_format=Format.ROW,
        reason="test",
    )

    reader = ManifestStore(tmp_data_dir)
    meta = reader.get_partition(tenant, "p_1")
    assert meta is not None
    assert meta.format is Format.COLUMNAR
    assert meta.paths == {"parquet": "p_1/data.parquet"}
    assert meta.codecs == {"ts": "SNAPPY"}
    assert meta.size_bytes == 123
    assert meta.last_migration is not None
    assert meta.last_migration["to"] == "columnar"
    assert meta.last_migration["from"] == "row"
    assert meta.last_migration["reason"] == "test"


async def test_swap_format_missing_partition_raises_keyerror(
    tmp_data_dir: Path,
) -> None:
    store = ManifestStore(tmp_data_dir)
    with pytest.raises(KeyError):
        await store.swap_format(
            "acme",
            "p_missing",
            new_format=Format.COLUMNAR,
            new_paths={},
            new_codecs={},
            new_size=0,
        )


async def test_update_partition_stats_persists(tmp_data_dir: Path) -> None:
    tenant = "acme"
    writer = ManifestStore(tmp_data_dir)
    await writer.upsert_partition(tenant, PartitionMeta("p_1"))

    await writer.update_partition_stats(
        tenant,
        "p_1",
        row_count=10,
        size_bytes=999,
        last_write=123.0,
    )

    reader = ManifestStore(tmp_data_dir)
    meta = reader.get_partition(tenant, "p_1")
    assert meta is not None
    assert meta.row_count == 10
    assert meta.size_bytes == 999
    # last_write is stored inside the access dict, not a top-level field.
    assert meta.access["last_write"] == 123.0


async def test_concurrent_upserts_no_corruption(tmp_data_dir: Path) -> None:
    tenant = "acme"
    writer = ManifestStore(tmp_data_dir)

    await asyncio.gather(
        *[writer.upsert_partition(tenant, PartitionMeta(f"p_{i}")) for i in range(20)]
    )

    # All 20 partitions must be present after the concurrent burst...
    reader = ManifestStore(tmp_data_dir)
    reloaded = reader.load(tenant)
    assert len(reloaded.partitions) == 20
    assert {f"p_{i}" for i in range(20)} == set(reloaded.partitions)

    # ...and the on-disk file must be valid, non-corrupt JSON.
    raw = manifest_path(tmp_data_dir, tenant).read_text(encoding="utf-8")
    parsed = json.loads(raw)
    assert len(parsed["partitions"]) == 20
