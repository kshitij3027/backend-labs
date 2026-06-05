"""Unit tests for the sync surface of ``src.manifest``.

Covers the pure serialization round-trips of :class:`PartitionMeta` and
:class:`TenantManifest` (including enum coercion on ``from_dict``) and the
no-lock sync readers of :class:`ManifestStore` (:meth:`load`,
:meth:`get_partition`, :meth:`all_tenants`) against an empty/missing data dir.

The async mutators and atomic-persistence behaviour are exercised separately in
``tests/integration/test_manifest_atomic.py``.
"""
from __future__ import annotations

from pathlib import Path

from src.manifest import ManifestStore, PartitionMeta, TenantManifest
from src.models import Format, Tier


def test_partition_meta_round_trip_coerces_enums() -> None:
    meta = PartitionMeta(
        partition_id="p_1",
        format=Format.COLUMNAR,
        tier=Tier.COLD,
        codecs={"ts": "SNAPPY"},
        access={"reads": 5},
    )
    restored = PartitionMeta.from_dict(meta.to_dict())

    # Full structural equality (dataclass __eq__).
    assert restored == meta
    # Enum coercion: strings on disk become the real enums, not bare str.
    assert restored.format is Format.COLUMNAR
    assert restored.tier is Tier.COLD
    assert restored.codecs == {"ts": "SNAPPY"}
    assert restored.access == {"reads": 5}


def test_tenant_manifest_round_trip_two_partitions() -> None:
    manifest = TenantManifest(
        tenant="acme",
        version=3,
        updated_at=123.5,
        partitions={
            "p_1": PartitionMeta(partition_id="p_1", format=Format.ROW, tier=Tier.HOT),
            "p_2": PartitionMeta(
                partition_id="p_2", format=Format.COLUMNAR, tier=Tier.WARM
            ),
        },
    )
    restored = TenantManifest.from_dict(manifest.to_dict())

    assert restored == manifest
    assert restored.version == 3
    assert restored.updated_at == 123.5
    assert set(restored.partitions) == {"p_1", "p_2"}
    assert restored.partitions["p_2"].format is Format.COLUMNAR
    assert restored.partitions["p_2"].tier is Tier.WARM


def test_load_missing_tenant_returns_empty_manifest(tmp_data_dir: Path) -> None:
    store = ManifestStore(tmp_data_dir)
    manifest = store.load("ghost")

    # No file on disk -> a fresh, empty, version-0 manifest, never an exception.
    assert isinstance(manifest, TenantManifest)
    assert manifest.tenant == "ghost"
    assert manifest.version == 0
    assert manifest.partitions == {}


def test_all_tenants_empty_dir_returns_empty_list(tmp_data_dir: Path) -> None:
    store = ManifestStore(tmp_data_dir)
    assert store.all_tenants() == []


def test_all_tenants_missing_dir_returns_empty_list(tmp_path: Path) -> None:
    # Point the store at a directory that does not exist at all.
    missing = tmp_path / "does_not_exist"
    store = ManifestStore(missing)
    assert store.all_tenants() == []


def test_get_partition_unknown_returns_none(tmp_data_dir: Path) -> None:
    store = ManifestStore(tmp_data_dir)
    assert store.get_partition("ghost", "p_999") is None
