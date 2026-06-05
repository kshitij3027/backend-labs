"""In-process end-to-end smoke for the adaptive storage-format flow (C23).

A dependency-free mirror of the cross-container ``scripts/verify_e2e.py``: it
builds the real engine object-graph in-process (like
``tests/integration/test_migration_engine.py``) and walks the full flow a running
server would — **ingest -> analytical queries -> migration -> re-query** — with no
network, no Docker, and no external service. This is the e2e check that runs under
``make test``.

The migration-triggering recipe is the same one the container verifier uses:

1. Ingest >=300 rows at an **OLD** timestamp so they collapse into one partition
   whose time-bucket is far in the past -> classified COLD, and whose row count
   clears ``select_min_rows``.
2. Run several **analytical** queries that each project a DIFFERENT single column
   (cycling 5+ distinct columns) so the access pattern is scan-heavy with a LOW
   fraction-of-columns-touched -> the selector recommends COLUMNAR.
3. Drive the decision via :meth:`MigrationEngine.migrate_partition_once` and
   assert the partition became COLUMNAR, then re-query through the real
   :class:`QueryEngine` and assert **every** row survived the migration.

A second test drives the same setup through the background :meth:`run` loop
(bounded poll) to prove the loop reaches the same outcome unattended.

``asyncio_mode=auto`` (see ``pytest.ini``) runs the ``async def test_*``
functions without an explicit decorator. The engine clock is pinned so the
tier / age math is deterministic.

Record shape (shared backend contract): a stored "row" is a flat dict
``{"ts": <float>, <field>: value, ...}``.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from src.format_selector import FormatSelector
from src.index_manager import IndexManager
from src.ingest_engine import IngestEngine
from src.manifest import ManifestStore
from src.metrics import Metrics
from src.models import Format, IngestEntry
from src.pattern_tracker import PatternTracker
from src.query_engine import QueryEngine
from src.settings import Settings
from src.storage.columnar_backend import ColumnarBackend
from src.storage.hybrid_backend import HybridBackend
from src.storage.row_backend import RowBackend
from src.tier_manager import TierManager
from src.migration_engine import MigrationEngine

TENANT = "e2e"
BUCKET = 3600
# Pinned engine clock. With BUCKET=3600, an OLD ts of ~100k lands in a low bucket
# whose start is ~100k -> age ≈ CLOCK - 100k ≈ 400k s >> cold_min_age (86400s),
# so the partition is COLD.
CLOCK = 500_000.0
# OLD ingest timestamp -> a single far-past partition.
OLD_TS = 100_000.0
# Rows in that one OLD partition; above select_min_rows so it is worth migrating.
OLD_ROWS = 320
# 5+ distinct single columns cycled through analytical projections -> scan-heavy,
# narrow access pattern (low fraction of columns touched per query) -> COLUMNAR.
PROJECTION_COLUMNS = ["c0", "c1", "c2", "c3", "c4", "c5"]


def _backends() -> dict[Format, object]:
    """Return a fresh format -> backend map covering all three formats."""
    return {
        Format.ROW: RowBackend(),
        Format.COLUMNAR: ColumnarBackend(),
        Format.HYBRID: HybridBackend(),
    }


def _selector() -> FormatSelector:
    """A selector with a tiny ``min_rows`` floor (OLD_ROWS clears it comfortably)."""
    return FormatSelector(
        write_ratio_row=0.3,
        point_lookup_row=0.5,
        scan_ratio_columnar=0.6,
        few_columns_fraction=0.4,
        min_confidence=0.6,
        min_rows=4,
    )


def _tier() -> TierManager:
    """Default thresholds: HOT < 1h, COLD >= 1d."""
    return TierManager(
        hot_max_age_seconds=3600,
        cold_min_age_seconds=86400,
        hot_min_reads_per_min=1.0,
    )


def _index_manager() -> IndexManager:
    return IndexManager(
        min_filter_hits=5,
        min_selectivity=0.2,
        drop_benefit_window=200,
        drop_min_benefit=0.01,
    )


def _settings(tmp_data_dir: Path, **overrides) -> Settings:
    """Settings rooted at ``tmp_data_dir`` with a tiny ``select_min_rows``."""
    base = dict(
        data_dir=str(tmp_data_dir),
        partition_bucket_seconds=BUCKET,
        migration_cooldown_seconds=60.0,
        select_min_rows=4,
    )
    base.update(overrides)
    return Settings(**base)


def _graph(tmp_data_dir: Path, *, settings: Settings | None = None):
    """Build the real ingest/query/migration engines over shared collaborators.

    Returns ``(ingest_engine, query_engine, migration_engine, manifest)`` all
    wired to the SAME manifest, backends, pattern tracker, and metrics — so a
    write through the ingest engine is visible to the query engine, and the access
    the query engine records is exactly what the migration engine reads.
    """
    st = settings if settings is not None else _settings(tmp_data_dir)
    backends = _backends()
    pt = PatternTracker()
    metrics = Metrics()
    mstore = ManifestStore(tmp_data_dir, clock=lambda: CLOCK)
    index_manager = _index_manager()

    ingest_engine = IngestEngine(
        manifest=mstore,
        backends=backends,
        pattern_tracker=pt,
        metrics=metrics,
        settings=st,
        clock=lambda: CLOCK,
    )
    query_engine = QueryEngine(
        manifest=mstore,
        backends=backends,
        pattern_tracker=pt,
        index_manager=index_manager,
        metrics=metrics,
        settings=st,
        clock=lambda: CLOCK,
    )
    migration_engine = MigrationEngine(
        manifest=mstore,
        backends=backends,
        selector=_selector(),
        tier_manager=_tier(),
        pattern_tracker=pt,
        compression=None,
        index_manager=index_manager,
        metrics=metrics,
        settings=st,
        clock=lambda: CLOCK,
    )
    return ingest_engine, query_engine, migration_engine, mstore


def _old_entries(n: int) -> list[IngestEntry]:
    """``n`` ingest entries at OLD_TS, each carrying every projection column."""
    entries: list[IngestEntry] = []
    for i in range(n):
        fields = {col: f"{col}_v{i % 7}" for col in PROJECTION_COLUMNS}
        fields["seq"] = i
        entries.append(IngestEntry(ts=OLD_TS, fields=fields))
    return entries


async def _drive_analytical_scans(query_engine: QueryEngine, *, rounds: int = 8) -> None:
    """Run analytical queries cycling 5+ distinct single columns.

    Distinct projected columns across many queries -> scan-heavy access with a low
    fraction of columns touched per query, which the selector reads as COLUMNAR.
    """
    for i in range(rounds):
        column = PROJECTION_COLUMNS[i % len(PROJECTION_COLUMNS)]
        result = await query_engine.query(TENANT, columns=[column])
        assert result.meta["query_class"] == "analytical"
        # Projection returns only the requested key.
        for row in result.rows or []:
            assert set(row.keys()) == {column}


def _row_key(row: dict) -> tuple:
    """A hashable identity for a row dict (for set comparison)."""
    return tuple(sorted(row.items()))


# --------------------------------------------------------------------------- #
# 1. Full flow via migrate_partition_once: ingest OLD -> scan -> COLUMNAR
# --------------------------------------------------------------------------- #
async def test_full_flow_cold_scan_heavy_migrates_to_columnar(tmp_data_dir: Path) -> None:
    ingest_engine, query_engine, migration_engine, mstore = _graph(tmp_data_dir)

    # 1) Ingest >=300 OLD-ts rows -> ONE cold partition above select_min_rows.
    summary = await ingest_engine.ingest(TENANT, _old_entries(OLD_ROWS))
    assert summary["ingested"] == OLD_ROWS
    assert len(summary["partitions_touched"]) == 1
    pid = summary["partitions_touched"][0]

    # Baseline full-record read: all OLD rows present, served from ROW.
    before = await query_engine.query(TENANT)
    assert before.rows is not None
    assert len(before.rows) == OLD_ROWS
    assert before.meta["query_class"] == "full_record"
    assert before.meta["formats_used"] == {"row": 1}
    baseline_keys = {_row_key(r) for r in before.rows}

    # 2) Drive the scan-heavy, narrow analytical access pattern.
    await _drive_analytical_scans(query_engine)

    # Sanity: the partition starts on ROW.
    meta = mstore.get_partition(TENANT, pid)
    assert meta is not None and meta.format == Format.ROW

    # 3) Decision layer migrates it to COLUMNAR.
    did = await migration_engine.migrate_partition_once(TENANT, pid, ignore_cooldown=True)
    assert did is True, "COLD + scan-heavy + narrow partition must migrate to COLUMNAR"

    meta = mstore.get_partition(TENANT, pid)
    assert meta is not None
    assert meta.format == Format.COLUMNAR
    assert meta.last_migration is not None
    assert meta.last_migration["from"] == "row"
    assert meta.last_migration["to"] == "columnar"

    # 4) Re-query through the real query engine: every row survived, now columnar.
    after = await query_engine.query(TENANT)
    assert after.rows is not None
    assert len(after.rows) == OLD_ROWS
    assert {_row_key(r) for r in after.rows} == baseline_keys
    assert after.meta["formats_used"] == {"columnar": 1}


# --------------------------------------------------------------------------- #
# 2. Same flow, but the background run() loop performs the migration unattended
# --------------------------------------------------------------------------- #
async def test_full_flow_background_loop_migrates_columnar(tmp_data_dir: Path) -> None:
    settings = _settings(tmp_data_dir, migration_interval_seconds=0.01)
    ingest_engine, query_engine, migration_engine, mstore = _graph(
        tmp_data_dir, settings=settings
    )

    summary = await ingest_engine.ingest(TENANT, _old_entries(OLD_ROWS))
    pid = summary["partitions_touched"][0]
    await _drive_analytical_scans(query_engine)

    stop = asyncio.Event()
    task = asyncio.create_task(migration_engine.run(stop))
    try:
        migrated = False
        for _ in range(60):  # bounded poll: up to ~3s total
            await asyncio.sleep(0.05)
            meta = mstore.get_partition(TENANT, pid)
            if meta is not None and meta.format == Format.COLUMNAR:
                migrated = True
                break
        assert migrated, "background loop should migrate the columnar candidate"
    finally:
        stop.set()
        await task

    # Data preserved across the unattended migration.
    after = await query_engine.query(TENANT)
    assert after.rows is not None
    assert len(after.rows) == OLD_ROWS
    assert after.meta["formats_used"] == {"columnar": 1}
