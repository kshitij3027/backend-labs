"""Integration tests for intelligent, self-pruning indexing (Feature C, C18).

C18 wires the min/max index built in :mod:`src.index_manager` into the live
read and migration paths:

* **Query engine** consults a partition's persisted ``index["stats"]`` before
  decoding it. When an indexed filter *proves* the partition's ``[min, max]``
  range cannot overlap the predicate, the partition holds no matching row and
  the read is skipped entirely — incrementing ``partitions_skipped`` /
  ``rowgroups_skipped`` and crediting the decisive column's skip-benefit so a
  useful index stays alive.
* **Migration engine** builds a :class:`~src.index_manager.PartitionIndex` over
  the columns query patterns show are worth indexing and persists it through
  ``swap_format`` when it migrates a partition.
* **Manifest** can drop index columns that have stopped earning their keep via
  the new ``drop_index_columns`` mutator, which the migration loop drives via
  the index manager's ``prune``.

The tests below cover four layers:

1. ``partition_can_match`` — the pure, sound overlap test, exhaustively per op.
2. The query engine genuinely skipping a non-overlapping partition (and *not*
   skipping when there are no filters), proving soundness: the skipped
   partition truly contains no matching row.
3. A migration building and persisting a correct min/max index.
4. ``drop_index_columns`` pruning an index column that no longer pays off.

They wire the engines to their *real* collaborators exactly as
``test_query_engine.py`` and ``test_migration_engine.py`` do (filesystem-backed
manifest, real backends, live pattern tracker / index manager / metrics).
``asyncio_mode=auto`` (see ``pytest.ini``) runs the ``async def test_*``
functions without an explicit decorator.

Record shape (the shared backend contract): a stored "row" is a flat dict
``{"ts": <float>, <field>: value, ...}``.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from src.format_selector import FormatSelector
from src.index_manager import IndexManager
from src.manifest import ManifestStore, PartitionMeta
from src.metrics import Metrics
from src.migration_engine import MigrationEngine
from src.models import Filter, Format, QueryClass
from src.paths import PARQUET_NAME, ROW_NAME, partition_paths
from src.pattern_tracker import PatternTracker
from src.query_engine import QueryEngine
from src.settings import Settings
from src.storage.columnar_backend import ColumnarBackend
from src.storage.hybrid_backend import HybridBackend
from src.storage.row_backend import RowBackend
from src.tier_manager import TierManager

BUCKET = 3600
NOW = 5000.0
CLOCK = 500000.0


def _backends() -> dict[Format, object]:
    """Return a fresh format -> backend map covering all three formats."""
    return {
        Format.ROW: RowBackend(),
        Format.COLUMNAR: ColumnarBackend(),
        Format.HYBRID: HybridBackend(),
    }


def _index_manager() -> IndexManager:
    """A manager with the default (settings-mirrored) indexing thresholds."""
    return IndexManager(
        min_filter_hits=5,
        min_selectivity=0.2,
        drop_benefit_window=200,
        drop_min_benefit=0.01,
    )


# --------------------------------------------------------------------------- #
# Query-engine wiring (mirrors test_query_engine.py)
# --------------------------------------------------------------------------- #
def _query_engine(
    tmp_data_dir: Path,
    *,
    backends: dict[Format, object],
    index_manager: IndexManager,
    metrics: Metrics | None = None,
    tracker: PatternTracker | None = None,
    clock=lambda: NOW,
) -> tuple[QueryEngine, ManifestStore]:
    """Build a :class:`QueryEngine` wired with real collaborators.

    The ``index_manager`` is shared back to the caller so a test can both feed
    the engine and assert on the manager's recorded benefit afterwards.
    """
    settings = Settings(
        data_dir=str(tmp_data_dir), partition_bucket_seconds=BUCKET
    )
    mstore = ManifestStore(tmp_data_dir)
    qe = QueryEngine(
        manifest=mstore,
        backends=backends,
        pattern_tracker=tracker if tracker is not None else PatternTracker(),
        index_manager=index_manager,
        metrics=metrics if metrics is not None else Metrics(),
        settings=settings,
        clock=clock,
    )
    return qe, mstore


async def _make_indexed_partition(
    mstore: ManifestStore,
    backends: dict[Format, object],
    tmp_data_dir: Path,
    tenant: str,
    pid: str,
    fmt: Format,
    rows: list[dict],
    *,
    index: dict | None = None,
) -> None:
    """Write ``rows`` in ``fmt`` and register the partition (optionally indexed).

    Data is written straight through the matching backend; a
    :class:`~src.manifest.PartitionMeta` carrying the logical-name -> path map
    (and, when given, a built ``index`` payload) is upserted so the engine reads
    it as source of truth.
    """
    paths = partition_paths(tmp_data_dir, tenant, pid)
    backends[fmt].write(rows, paths)

    if fmt == Format.ROW:
        pdict = {"row": f"{pid}/{ROW_NAME}"}
    elif fmt == Format.COLUMNAR:
        pdict = {"parquet": f"{pid}/{PARQUET_NAME}"}
    else:  # HYBRID
        pdict = {
            "recent": f"{pid}/recent.jsonl.lz4",
            "parquet": f"{pid}/{PARQUET_NAME}",
        }

    await mstore.upsert_partition(
        tenant,
        PartitionMeta(
            partition_id=pid,
            format=fmt,
            paths=pdict,
            row_count=len(rows),
            index=index or {},
        ),
    )


# --------------------------------------------------------------------------- #
# Migration-engine wiring (mirrors test_migration_engine.py)
# --------------------------------------------------------------------------- #
def _selector() -> FormatSelector:
    """A selector with a tiny ``min_rows`` so small test partitions still migrate."""
    return FormatSelector(
        write_ratio_row=0.3,
        point_lookup_row=0.5,
        scan_ratio_columnar=0.6,
        few_columns_fraction=0.4,
        min_confidence=0.6,
        min_rows=4,
    )


def _tier() -> TierManager:
    """A tier manager with the default thresholds (HOT < 1h, COLD >= 1d)."""
    return TierManager(
        hot_max_age_seconds=3600,
        cold_min_age_seconds=86400,
        hot_min_reads_per_min=1.0,
    )


def _migration_settings(tmp_data_dir: Path, **overrides) -> Settings:
    """Settings rooted at ``tmp_data_dir`` with a tiny ``select_min_rows``."""
    base = dict(
        data_dir=str(tmp_data_dir),
        partition_bucket_seconds=BUCKET,
        migration_cooldown_seconds=60.0,
        select_min_rows=4,
    )
    base.update(overrides)
    return Settings(**base)


def _migration_engine(
    tmp_data_dir: Path,
    *,
    index_manager: IndexManager,
    backends: dict[Format, object] | None = None,
    settings: Settings | None = None,
    clock=lambda: CLOCK,
) -> tuple[MigrationEngine, ManifestStore, dict[Format, object], PatternTracker]:
    """Build a :class:`MigrationEngine` sharing the given index manager.

    Returns the engine, the shared manifest store, the backend map, and the
    pattern tracker so a test can drive ``note_filter`` on the same manager the
    engine reads its candidate columns from.
    """
    bk = backends if backends is not None else _backends()
    pt = PatternTracker()
    st = settings if settings is not None else _migration_settings(tmp_data_dir)
    mstore = ManifestStore(tmp_data_dir, clock=clock)
    eng = MigrationEngine(
        manifest=mstore,
        backends=bk,
        selector=_selector(),
        tier_manager=_tier(),
        pattern_tracker=pt,
        compression=None,
        index_manager=index_manager,
        metrics=Metrics(),
        settings=st,
        clock=clock,
    )
    return eng, mstore, bk, pt


async def _make_row_partition(
    mstore: ManifestStore,
    backends: dict[Format, object],
    tmp_data_dir: Path,
    tenant: str,
    pid: str,
    rows: list[dict],
) -> None:
    """Create a ROW partition on disk and register it in the manifest."""
    paths = partition_paths(tmp_data_dir, tenant, pid)
    backends[Format.ROW].write(rows, paths)
    await mstore.upsert_partition(
        tenant,
        PartitionMeta(
            partition_id=pid,
            format=Format.ROW,
            paths={"row": f"{pid}/{ROW_NAME}"},
            row_count=len(rows),
        ),
    )


# =========================================================================== #
# 1. Unit-ish: partition_can_match — the pure, sound overlap test.
# =========================================================================== #
def test_partition_can_match_per_operator() -> None:
    im = _index_manager()
    stats = {"ts": {"min": 100, "max": 200}}

    def can_match(op: str, value) -> tuple[bool, list[str]]:
        return im.partition_can_match(
            stats, [Filter(column="ts", op=op, value=value)]
        )

    # --- eq: overlaps iff lo <= v <= hi ---
    assert can_match("eq", 150) == (True, [])
    assert can_match("eq", 500) == (False, ["ts"])

    # --- gt: overlaps iff hi > v ---
    assert can_match("gt", 250)[0] is False  # 200 > 250 is False -> skip
    assert can_match("gt", 150)[0] is True  # 200 > 150 -> some value above

    # --- lt: overlaps iff lo < v ---
    assert can_match("lt", 50)[0] is False  # 100 < 50 is False -> skip
    assert can_match("lt", 150)[0] is True  # 100 < 150 -> some value below

    # --- gte: overlaps iff hi >= v (boundary-exact) ---
    assert can_match("gte", 200)[0] is True  # 200 >= 200
    assert can_match("gte", 201)[0] is False  # 200 >= 201 is False -> skip

    # --- lte: overlaps iff lo <= v (boundary-exact) ---
    assert can_match("lte", 100)[0] is True  # 100 <= 100
    assert can_match("lte", 99)[0] is False  # 100 <= 99 is False -> skip

    # --- in: overlaps iff the values' span intersects [lo, hi] ---
    assert can_match("in", [300, 400])[0] is False  # span 300..400 disjoint
    assert can_match("in", [150, 400])[0] is True  # 150 is in range

    # --- ne is never decisive (the excluded value is one of many in range) ---
    assert can_match("ne", 150) == (True, [])

    # --- a filter on an un-indexed column can never prove a skip ---
    foo_result = im.partition_can_match(
        stats, [Filter(column="foo", op="eq", value=12345)]
    )
    assert foo_result == (True, [])


# =========================================================================== #
# 2. Integration: the query engine skips a non-overlapping partition.
# =========================================================================== #
async def test_query_engine_skips_non_overlapping_partition(
    tmp_data_dir: Path,
) -> None:
    """A ts>=1000 filter must skip the low partition and return only high rows.

    Soundness check: the skipped partition (ts 0..9) genuinely contains no row
    matching ``ts >= 1000``, so dropping its read loses nothing.
    """
    backends = _backends()
    im = _index_manager()
    qe, mstore = _query_engine(
        tmp_data_dir, backends=backends, index_manager=im
    )

    # p_lo: ts 0..9 (min 0, max 9); p_hi: ts 1000..1009 (min 1000, max 1009).
    lo_rows = [{"ts": float(i), "user": f"lo-{i}"} for i in range(10)]
    hi_rows = [{"ts": float(1000 + i), "user": f"hi-{i}"} for i in range(10)]

    await _make_indexed_partition(
        mstore, backends, tmp_data_dir, "acme", "p_lo", Format.ROW, lo_rows,
        index={"columns": ["ts"], "stats": {"ts": {"min": 0, "max": 9}}},
    )
    await _make_indexed_partition(
        mstore, backends, tmp_data_dir, "acme", "p_hi", Format.ROW, hi_rows,
        index={"columns": ["ts"], "stats": {"ts": {"min": 1000, "max": 1009}}},
    )

    result = await qe.query(
        "acme", filters=[Filter(column="ts", op="gte", value=1000)]
    )

    # Only the high partition's rows come back; NO low-partition row leaks in.
    assert result.rows is not None
    users = {r["user"] for r in result.rows}
    assert users == {f"hi-{i}" for i in range(10)}
    assert all(r["ts"] >= 1000 for r in result.rows)
    assert not any(r["user"].startswith("lo-") for r in result.rows)

    # The low partition was skipped via the index; only the high one was read.
    assert result.meta["partitions_skipped"] >= 1
    assert result.meta["partitions_read"] == 1
    assert result.meta["rowgroups_skipped"] >= 1

    # The skip credited p_lo's "ts" index with a beneficial (skip-fraction 1.0)
    # record, so the index must NOT be immediately droppable.
    assert im.should_drop("acme", "p_lo", "ts") is False


async def test_query_engine_no_filter_reads_all_partitions(
    tmp_data_dir: Path,
) -> None:
    """With no filters there is nothing to prove a skip on — read both partitions."""
    backends = _backends()
    im = _index_manager()
    qe, mstore = _query_engine(
        tmp_data_dir, backends=backends, index_manager=im
    )

    lo_rows = [{"ts": float(i), "user": f"lo-{i}"} for i in range(10)]
    hi_rows = [{"ts": float(1000 + i), "user": f"hi-{i}"} for i in range(10)]
    await _make_indexed_partition(
        mstore, backends, tmp_data_dir, "acme", "p_lo", Format.ROW, lo_rows,
        index={"columns": ["ts"], "stats": {"ts": {"min": 0, "max": 9}}},
    )
    await _make_indexed_partition(
        mstore, backends, tmp_data_dir, "acme", "p_hi", Format.ROW, hi_rows,
        index={"columns": ["ts"], "stats": {"ts": {"min": 1000, "max": 1009}}},
    )

    result = await qe.query("acme")  # no filters

    assert result.rows is not None
    assert result.meta["partitions_skipped"] == 0
    assert result.meta["partitions_read"] == 2
    users = {r["user"] for r in result.rows}
    assert users == (
        {f"lo-{i}" for i in range(10)} | {f"hi-{i}" for i in range(10)}
    )
    assert len(result.rows) == 20


# =========================================================================== #
# 3. Integration: a migration builds + persists a correct min/max index.
# =========================================================================== #
async def test_migration_builds_and_persists_index(tmp_data_dir: Path) -> None:
    backends = _backends()
    im = _index_manager()
    eng, mstore, backends, _pt = _migration_engine(
        tmp_data_dir, index_manager=im, backends=backends
    )

    pid = "p_0"
    # >= 8 ROW rows carrying a `ts` column with known bounds (min 0, max 70).
    rows = [{"ts": float(i * 10), "level": "INFO"} for i in range(8)]
    await _make_row_partition(
        mstore, backends, tmp_data_dir, "acme", pid, rows
    )

    # Drive >=5 selective filter observations so `ts` becomes a build candidate.
    for _ in range(5):
        im.note_filter("acme", pid, "ts", selectivity=0.05)
    assert im.should_build("acme", pid, "ts") is True
    assert "ts" in im.candidate_columns("acme", pid)

    ok = await eng.migrate_to("acme", pid, Format.COLUMNAR)
    assert ok is True

    # Reload from the live store: the persisted index covers `ts` with correct
    # min/max over the rows actually stored.
    meta = mstore.get_partition("acme", pid)
    assert meta is not None
    assert meta.format == Format.COLUMNAR
    assert "ts" in meta.index["columns"]
    assert meta.index["stats"]["ts"] == {"min": 0.0, "max": 70.0}


# =========================================================================== #
# 4. Integration: drop_index_columns prunes an index that stopped paying off.
# =========================================================================== #
async def test_prune_drops_unused_index_column(tmp_data_dir: Path) -> None:
    backends = _backends()
    im = _index_manager()
    eng, mstore, backends, _pt = _migration_engine(
        tmp_data_dir, index_manager=im, backends=backends
    )

    pid = "p_0"
    rows = [{"ts": float(i * 10), "level": "INFO"} for i in range(8)]
    await _make_row_partition(
        mstore, backends, tmp_data_dir, "acme", pid, rows
    )

    # Build + persist a `ts` index via a migration (as in test 3).
    for _ in range(5):
        im.note_filter("acme", pid, "ts", selectivity=0.05)
    assert await eng.migrate_to("acme", pid, Format.COLUMNAR) is True
    meta = mstore.get_partition("acme", pid)
    assert meta is not None
    assert "ts" in meta.index["columns"]

    # The index then stops skipping any work: near-zero recent benefit makes it
    # droppable (mean skip-fraction 0 < drop_min_benefit 0.01).
    for _ in range(10):
        im.record_benefit("acme", pid, "ts", rows_skipped=0, rows_total=100)
    assert im.should_drop("acme", pid, "ts") is True
    assert im.prune("acme", pid, ["ts"]) == ["ts"]

    # Direct, deterministic drop: the persisted index no longer lists `ts`.
    await mstore.drop_index_columns("acme", pid, ["ts"])
    meta_after = mstore.get_partition("acme", pid)
    assert meta_after is not None
    assert "ts" not in meta_after.index.get("columns", [])
    assert "ts" not in meta_after.index.get("stats", {})


async def test_prune_via_migration_run_loop_drops_index(
    tmp_data_dir: Path,
) -> None:
    """The background ``run`` loop prunes a no-benefit index column (end-to-end)."""
    backends = _backends()
    im = _index_manager()
    settings = _migration_settings(tmp_data_dir, migration_interval_seconds=0.01)
    eng, mstore, backends, _pt = _migration_engine(
        tmp_data_dir, index_manager=im, backends=backends, settings=settings
    )

    # A COLUMNAR partition that already carries a built `ts` index, but whose
    # access stats give the selector no reason to migrate it again — so the only
    # thing the loop does to it is prune the dead index.
    pid = "p_0"
    rows = [{"ts": float(i * 10), "level": "INFO"} for i in range(8)]
    await _make_indexed_partition(
        mstore, backends, tmp_data_dir, "acme", pid, Format.COLUMNAR, rows,
        index={"columns": ["ts"], "stats": {"ts": {"min": 0.0, "max": 70.0}}},
    )

    # Mark the index as no longer earning its keep.
    for _ in range(10):
        im.record_benefit("acme", pid, "ts", rows_skipped=0, rows_total=100)
    assert im.should_drop("acme", pid, "ts") is True

    stop = asyncio.Event()
    task = asyncio.create_task(eng.run(stop))
    try:
        pruned = False
        for _ in range(40):  # bounded poll, ~2s max
            await asyncio.sleep(0.05)
            meta = mstore.get_partition("acme", pid)
            if meta is not None and "ts" not in meta.index.get("columns", []):
                pruned = True
                break
        assert pruned, "run loop should prune the dead index column"
    finally:
        stop.set()
        await task

    meta = mstore.get_partition("acme", pid)
    assert meta is not None
    assert "ts" not in meta.index.get("columns", [])
    assert "ts" not in meta.index.get("stats", {})
