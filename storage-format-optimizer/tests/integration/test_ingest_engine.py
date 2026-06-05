"""Integration tests for :class:`~src.ingest_engine.IngestEngine` (C12).

These wire the engine with its *real* collaborators — a filesystem-backed
:class:`~src.manifest.ManifestStore`, the three storage backends, a live
:class:`~src.pattern_tracker.PatternTracker`, and a :class:`~src.metrics.Metrics`
aggregator — and push real batches through it. Assertions reload the manifest
via a **fresh** ``ManifestStore`` (cold cache) where the goal is to verify what
landed on disk, and read partition data back through ``RowBackend`` to confirm
the flattened rows were actually written.

``asyncio_mode=auto`` (see ``pytest.ini``) lets the ``async def test_*``
functions run without an explicit decorator. The engine clock is pinned to a
fixed value so partitioning and timestamps are fully deterministic.

Bucket math (``partition_bucket_seconds=3600``):
    * ``ts=3600.0``  -> ``3600 // 3600 == 1`` -> ``p_1``
    * ``ts=7300.0``  -> ``7300 // 3600 == 2`` -> ``p_2``
    * ``ts=10000.0`` -> ``10000 // 3600 == 2`` -> ``p_2``
"""
from __future__ import annotations

from pathlib import Path

from src.ingest_engine import IngestEngine
from src.manifest import ManifestStore
from src.metrics import Metrics
from src.models import Format
from src.paths import partition_id_for, partition_paths
from src.pattern_tracker import PatternTracker
from src.settings import Settings
from src.storage.columnar_backend import ColumnarBackend
from src.storage.hybrid_backend import HybridBackend
from src.storage.row_backend import RowBackend

BUCKET = 3600
NOW = 10000.0


def _backends() -> dict[Format, object]:
    """Return a fresh format -> backend map covering all three formats."""
    return {
        Format.ROW: RowBackend(),
        Format.COLUMNAR: ColumnarBackend(),
        Format.HYBRID: HybridBackend(),
    }


def _engine(
    tmp_data_dir: Path,
    *,
    metrics: Metrics | None = None,
    tracker: PatternTracker | None = None,
    clock=lambda: NOW,
) -> IngestEngine:
    """Build an :class:`IngestEngine` wired with real collaborators."""
    settings = Settings(data_dir=str(tmp_data_dir), partition_bucket_seconds=BUCKET)
    return IngestEngine(
        manifest=ManifestStore(tmp_data_dir),
        backends=_backends(),
        pattern_tracker=tracker if tracker is not None else PatternTracker(),
        metrics=metrics if metrics is not None else Metrics(),
        settings=settings,
        clock=clock,
    )


async def test_new_partition_is_row_and_counts_grow(tmp_data_dir: Path) -> None:
    engine = _engine(tmp_data_dir)
    pid = partition_id_for(3600.0, BUCKET)  # p_1

    result = await engine.ingest(
        "acme", [{"ts": 3600.0, "fields": {"user": "a", "level": "INFO"}}]
    )
    assert result["ingested"] == 1
    assert result["tenant"] == "acme"
    assert result["partitions_touched"] == [pid]

    # Fresh store -> read what actually landed on disk.
    reader = ManifestStore(tmp_data_dir)
    manifest = reader.load("acme")
    assert list(manifest.partitions) == [pid]
    meta = manifest.partitions[pid]
    assert meta.format is Format.ROW
    assert meta.row_count == 1
    assert meta.size_bytes > 0

    # A second ingest into the SAME bucket accumulates the row count to 2.
    await engine.ingest("acme", [{"ts": 3700.0, "fields": {"user": "b"}}])
    reader2 = ManifestStore(tmp_data_dir)
    meta2 = reader2.get_partition("acme", pid)
    assert meta2 is not None
    assert meta2.row_count == 2


async def test_read_back_via_backend(tmp_data_dir: Path) -> None:
    engine = _engine(tmp_data_dir)
    pid = partition_id_for(3600.0, BUCKET)

    await engine.ingest(
        "acme", [{"ts": 3600.0, "fields": {"user": "a", "level": "INFO"}}]
    )

    rows = RowBackend().read(partition_paths(tmp_data_dir, "acme", pid)).rows
    assert {"ts": 3600.0, "user": "a", "level": "INFO"} in rows


async def test_multi_partition_split(tmp_data_dir: Path) -> None:
    engine = _engine(tmp_data_dir)
    pid_a = partition_id_for(3600.0, BUCKET)  # p_1
    pid_b = partition_id_for(7300.0, BUCKET)  # p_2
    assert pid_a != pid_b

    result = await engine.ingest(
        "acme",
        [
            {"ts": 3600.0, "fields": {"user": "a"}},
            {"ts": 7300.0, "fields": {"user": "b"}},
        ],
    )

    assert result["ingested"] == 2
    assert set(result["partitions_touched"]) == {pid_a, pid_b}
    assert len(result["partitions_touched"]) == 2

    reader = ManifestStore(tmp_data_dir)
    assert set(reader.load("acme").partitions) == {pid_a, pid_b}


async def test_ts_defaulting_uses_engine_clock(tmp_data_dir: Path) -> None:
    engine = _engine(tmp_data_dir)
    expected_pid = partition_id_for(NOW, BUCKET)  # p_2 (10000 // 3600 == 2)

    # Entry with no ts -> falls back to the engine clock (NOW).
    result = await engine.ingest("acme", [{"fields": {"user": "a"}}])

    assert result["partitions_touched"] == [expected_pid]

    # The flattened row carries the defaulted ts.
    rows = RowBackend().read(
        partition_paths(tmp_data_dir, "acme", expected_pid)
    ).rows
    assert {"ts": NOW, "user": "a"} in rows


async def test_multi_tenant_isolation(tmp_data_dir: Path) -> None:
    engine = _engine(tmp_data_dir)
    pid_acme = partition_id_for(3600.0, BUCKET)
    pid_globex = partition_id_for(7300.0, BUCKET)

    await engine.ingest("acme", [{"ts": 3600.0, "fields": {"user": "a"}}])
    await engine.ingest("globex", [{"ts": 7300.0, "fields": {"user": "g"}}])

    reader = ManifestStore(tmp_data_dir)

    # Each tenant's manifest holds only its own partitions.
    assert list(reader.load("acme").partitions) == [pid_acme]
    assert list(reader.load("globex").partitions) == [pid_globex]

    tenants = set(reader.all_tenants())
    assert {"acme", "globex"} <= tenants


async def test_metrics_total_entries(tmp_data_dir: Path) -> None:
    metrics = Metrics()
    engine = _engine(tmp_data_dir, metrics=metrics)

    # Ingest N rows in total across two calls.
    await engine.ingest(
        "acme",
        [
            {"ts": 3600.0, "fields": {"user": "a"}},
            {"ts": 3601.0, "fields": {"user": "b"}},
        ],
    )
    await engine.ingest("acme", [{"ts": 3602.0, "fields": {"user": "c"}}])

    assert metrics.snapshot()["ingest"]["total_entries"] == 3


async def test_empty_batch_is_noop(tmp_data_dir: Path) -> None:
    metrics = Metrics()
    engine = _engine(tmp_data_dir, metrics=metrics)

    result = await engine.ingest("acme", [])
    assert result == {"ingested": 0, "partitions_touched": [], "tenant": "acme"}

    # No partition created, no metric recorded.
    reader = ManifestStore(tmp_data_dir)
    assert reader.load("acme").partitions == {}
    assert metrics.snapshot()["ingest"]["total_entries"] == 0
