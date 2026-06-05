"""Integration tests for :class:`~src.query_engine.QueryEngine` (C15).

These wire the query engine with its *real* collaborators — a filesystem-backed
:class:`~src.manifest.ManifestStore`, the three storage backends
(:class:`~src.storage.row_backend.RowBackend`,
:class:`~src.storage.columnar_backend.ColumnarBackend`,
:class:`~src.storage.hybrid_backend.HybridBackend`), a live
:class:`~src.pattern_tracker.PatternTracker`, a real
:class:`~src.index_manager.IndexManager`, and a
:class:`~src.metrics.Metrics` aggregator — then push real queries through it.

Each test builds a tenant by writing partition data *directly* through the
backends and registering the partition in the manifest via
:meth:`~src.manifest.ManifestStore.upsert_partition` (the engine reads the
manifest as its source of truth). This mirrors what the ingest + migration
engines would produce, without coupling these tests to them.

``asyncio_mode=auto`` (see ``pytest.ini``) lets the ``async def test_*``
functions run without an explicit decorator. The engine clock is pinned to a
fixed value so the access timestamps handed to the pattern tracker are
deterministic.

Record shape (the shared backend contract): a stored "row" is a flat dict
``{"ts": <float>, <field>: value, ...}``.
"""
from __future__ import annotations

from pathlib import Path

from src.index_manager import IndexManager
from src.manifest import ManifestStore, PartitionMeta
from src.metrics import Metrics
from src.models import Aggregation, Filter, Format
from src.paths import partition_paths
from src.pattern_tracker import PatternTracker
from src.query_engine import QueryEngine
from src.settings import Settings
from src.storage.columnar_backend import ColumnarBackend
from src.storage.hybrid_backend import HybridBackend
from src.storage.row_backend import RowBackend

BUCKET = 3600
NOW = 5000.0


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
    backends: dict[Format, object],
    metrics: Metrics | None = None,
    tracker: PatternTracker | None = None,
    index_manager: IndexManager | None = None,
    clock=lambda: NOW,
) -> tuple[QueryEngine, ManifestStore]:
    """Build a :class:`QueryEngine` wired with real collaborators.

    Returns the engine plus the :class:`ManifestStore` it shares, so a test can
    register partitions on the same store the engine reads from.
    """
    settings = Settings(
        data_dir=str(tmp_data_dir), partition_bucket_seconds=BUCKET
    )
    mstore = ManifestStore(tmp_data_dir)
    qe = QueryEngine(
        manifest=mstore,
        backends=backends,
        pattern_tracker=tracker if tracker is not None else PatternTracker(),
        index_manager=index_manager
        if index_manager is not None
        else IndexManager(
            min_filter_hits=5,
            min_selectivity=0.2,
            drop_benefit_window=200,
            drop_min_benefit=0.01,
        ),
        metrics=metrics if metrics is not None else Metrics(),
        settings=settings,
        clock=clock,
    )
    return qe, mstore


async def _make_partition(
    mstore: ManifestStore,
    backends: dict[Format, object],
    tmp_data_dir: Path,
    tenant: str,
    pid: str,
    fmt: Format,
    rows: list[dict],
) -> None:
    """Write ``rows`` in ``fmt`` and register the partition in the manifest.

    The data is written straight through the matching backend, and a
    :class:`~src.manifest.PartitionMeta` (with the logical-name -> relative-path
    map the format uses) is upserted so the engine can route reads to it.
    """
    paths = partition_paths(tmp_data_dir, tenant, pid)
    backends[fmt].write(rows, paths)

    if fmt == Format.ROW:
        pdict = {"row": f"{pid}/row.jsonl.lz4"}
    elif fmt == Format.COLUMNAR:
        pdict = {"parquet": f"{pid}/data.parquet"}
    else:  # HYBRID
        pdict = {
            "recent": f"{pid}/recent.jsonl.lz4",
            "parquet": f"{pid}/data.parquet",
        }

    await mstore.upsert_partition(
        tenant,
        PartitionMeta(
            partition_id=pid,
            format=fmt,
            paths=pdict,
            row_count=len(rows),
        ),
    )


# --------------------------------------------------------------------------- #
# 1. ROW full-record read
# --------------------------------------------------------------------------- #
async def test_row_full_record_read(tmp_data_dir: Path) -> None:
    backends = _backends()
    qe, mstore = _engine(tmp_data_dir, backends=backends)

    rows = [
        {"ts": 1.0, "user": "a", "level": "INFO"},
        {"ts": 2.0, "user": "b", "level": "WARN"},
        {"ts": 3.0, "user": "c", "level": "ERROR"},
    ]
    await _make_partition(
        mstore, backends, tmp_data_dir, "acme", "p_1", Format.ROW, rows
    )

    result = await qe.query("acme")

    assert result.aggregates is None
    assert result.rows is not None
    assert len(result.rows) == 3
    # Full record: every original key is present.
    assert {r["user"] for r in result.rows} == {"a", "b", "c"}
    assert result.meta["query_class"] == "full_record"
    assert result.meta["formats_used"] == {"row": 1}
    assert result.meta["partitions_read"] == 1


# --------------------------------------------------------------------------- #
# 2. COLUMNAR projection + filter
# --------------------------------------------------------------------------- #
async def test_columnar_projection_and_filter(tmp_data_dir: Path) -> None:
    backends = _backends()
    qe, mstore = _engine(tmp_data_dir, backends=backends)

    rows = [
        {"ts": 1.0, "level": "INFO", "msg": "a"},
        {"ts": 2.0, "level": "ERROR", "msg": "b"},
        {"ts": 3.0, "level": "ERROR", "msg": "c"},
        {"ts": 4.0, "level": "WARN", "msg": "d"},
    ]
    await _make_partition(
        mstore, backends, tmp_data_dir, "acme", "p_1", Format.COLUMNAR, rows
    )

    result = await qe.query(
        "acme",
        columns=["level"],
        filters=[Filter(column="level", op="eq", value="ERROR")],
    )

    assert result.aggregates is None
    assert result.rows is not None
    # Only the projected key survives; only ERROR rows match.
    assert all(set(r.keys()) == {"level"} for r in result.rows)
    assert [r["level"] for r in result.rows] == ["ERROR", "ERROR"]

    # <=3 projected columns -> analytical.
    assert result.meta["query_class"] == "analytical"
    assert result.meta["formats_used"] == {"columnar": 1}
    # The columnar backend reports the rows it surfaced post-pushdown as scanned.
    assert result.meta["rows_scanned"] >= len(result.rows)
    assert result.meta["rows_scanned"] > 0


# --------------------------------------------------------------------------- #
# 3. HYBRID union read (recent ROW side + sealed Parquet side)
# --------------------------------------------------------------------------- #
async def test_hybrid_union_read(tmp_data_dir: Path) -> None:
    backends = _backends()
    qe, mstore = _engine(tmp_data_dir, backends=backends)

    pid = "p_1"
    paths = partition_paths(tmp_data_dir, "acme", pid)

    # Write rows to the recent (ROW) side via the hybrid backend, then seal the
    # two oldest into the Parquet side so BOTH sides are populated.
    recent_rows = [
        {"ts": 10.0, "user": "a"},
        {"ts": 20.0, "user": "b"},
        {"ts": 30.0, "user": "c"},
        {"ts": 40.0, "user": "d"},
    ]
    hybrid: HybridBackend = backends[Format.HYBRID]
    hybrid.write(recent_rows, paths)
    # ts < 25 seals -> rows a,b move to Parquet; c,d stay on the recent side.
    sealed = hybrid.seal(paths, older_than_ts=25.0)
    assert sealed == 2

    await mstore.upsert_partition(
        "acme",
        PartitionMeta(
            partition_id=pid,
            format=Format.HYBRID,
            paths={
                "recent": f"{pid}/recent.jsonl.lz4",
                "parquet": f"{pid}/data.parquet",
            },
            row_count=len(recent_rows),
        ),
    )

    result = await qe.query("acme")

    assert result.rows is not None
    # Union of sealed + recent == all four original rows (order-agnostic).
    assert {r["user"] for r in result.rows} == {"a", "b", "c", "d"}
    assert len(result.rows) == 4
    assert result.meta["formats_used"] == {"hybrid": 1}
    assert result.meta["partitions_read"] == 1


# --------------------------------------------------------------------------- #
# 4. Mixed-format tenant (ROW partition + COLUMNAR partition)
# --------------------------------------------------------------------------- #
async def test_mixed_format_tenant_reads_both(tmp_data_dir: Path) -> None:
    backends = _backends()
    qe, mstore = _engine(tmp_data_dir, backends=backends)

    await _make_partition(
        mstore,
        backends,
        tmp_data_dir,
        "acme",
        "p_1",
        Format.ROW,
        [{"ts": 1.0, "user": "row-a"}, {"ts": 2.0, "user": "row-b"}],
    )
    await _make_partition(
        mstore,
        backends,
        tmp_data_dir,
        "acme",
        "p_2",
        Format.COLUMNAR,
        [{"ts": 3.0, "user": "col-c"}, {"ts": 4.0, "user": "col-d"}],
    )

    result = await qe.query("acme")

    assert result.rows is not None
    assert result.meta["partitions_read"] == 2
    # Both formats contributed exactly one partition each.
    assert result.meta["formats_used"] == {"row": 1, "columnar": 1}
    # Rows from both partitions are present in the union.
    users = {r["user"] for r in result.rows}
    assert {"row-a", "row-b", "col-c", "col-d"} <= users
    assert len(result.rows) == 4


# --------------------------------------------------------------------------- #
# 5. Aggregations without group_by
# --------------------------------------------------------------------------- #
async def test_aggregations_no_group_by(tmp_data_dir: Path) -> None:
    backends = _backends()
    qe, mstore = _engine(tmp_data_dir, backends=backends)

    rows = [
        {"ts": 1.0, "latency": 10},
        {"ts": 2.0, "latency": 20},
        {"ts": 3.0, "latency": 30},
        {"ts": 4.0, "latency": 40},
    ]
    await _make_partition(
        mstore, backends, tmp_data_dir, "acme", "p_1", Format.ROW, rows
    )

    result = await qe.query(
        "acme",
        aggregations=[
            Aggregation(op="count"),
            Aggregation(op="sum", column="latency"),
            Aggregation(op="avg", column="latency"),
            Aggregation(op="min", column="latency"),
            Aggregation(op="max", column="latency"),
        ],
    )

    # Aggregating -> rows is None, aggregates carries the flat metric dict.
    assert result.rows is None
    assert result.aggregates is not None
    assert result.aggregates["count_all"] == 4
    assert result.aggregates["sum_latency"] == 100
    assert result.aggregates["avg_latency"] == 25
    assert result.aggregates["min_latency"] == 10
    assert result.aggregates["max_latency"] == 40
    # An aggregation is classified analytical regardless of projection.
    assert result.meta["query_class"] == "analytical"


# --------------------------------------------------------------------------- #
# 6. Aggregations with group_by
# --------------------------------------------------------------------------- #
async def test_aggregations_with_group_by(tmp_data_dir: Path) -> None:
    backends = _backends()
    qe, mstore = _engine(tmp_data_dir, backends=backends)

    rows = [
        {"ts": 1.0, "region": "us", "latency": 10},
        {"ts": 2.0, "region": "eu", "latency": 20},
        {"ts": 3.0, "region": "us", "latency": 30},
        {"ts": 4.0, "region": "us", "latency": 40},
        {"ts": 5.0, "region": "eu", "latency": 50},
    ]
    await _make_partition(
        mstore, backends, tmp_data_dir, "acme", "p_1", Format.ROW, rows
    )

    result = await qe.query(
        "acme",
        aggregations=[Aggregation(op="count")],
        group_by=["region"],
    )

    assert result.rows is None
    assert result.aggregates is not None
    assert result.aggregates["group_by"] == ["region"]

    groups = result.aggregates["groups"]
    # One entry per distinct region, each carrying the group col + the count.
    by_region = {g["region"]: g["count_all"] for g in groups}
    assert by_region == {"us": 3, "eu": 2}


# --------------------------------------------------------------------------- #
# 7. Empty tenant
# --------------------------------------------------------------------------- #
async def test_empty_tenant(tmp_data_dir: Path) -> None:
    backends = _backends()
    qe, _mstore = _engine(tmp_data_dir, backends=backends)

    # No partitions registered for "ghost" -> empty, no exception.
    result = await qe.query("ghost")

    assert result.rows == []
    assert result.aggregates is None
    assert result.meta["partitions_read"] == 0
    assert result.meta["formats_used"] == {}
    assert result.meta["rows_scanned"] == 0


# --------------------------------------------------------------------------- #
# 8. Recording side-effects (metrics + pattern tracker)
# --------------------------------------------------------------------------- #
async def test_query_records_side_effects(tmp_data_dir: Path) -> None:
    backends = _backends()
    metrics = Metrics()
    tracker = PatternTracker()
    qe, mstore = _engine(
        tmp_data_dir, backends=backends, metrics=metrics, tracker=tracker
    )

    pid = "p_1"
    await _make_partition(
        mstore,
        backends,
        tmp_data_dir,
        "acme",
        pid,
        Format.ROW,
        [{"ts": 1.0, "user": "a"}, {"ts": 2.0, "user": "b"}],
    )

    await qe.query("acme")

    # Metrics recorded at least one query for the format that was read.
    by_format = metrics.snapshot()["performance"]["by_format"]
    assert by_format["row"]["count"] >= 1

    # The pattern tracker registered a read against the partition.
    stats = tracker.get_stats("acme", pid)
    assert stats.reads >= 1
