"""Integration tests for :class:`~src.migration_engine.MigrationEngine` (C16).

This is the highest-risk module in the system: it rewrites *live* partition data
copy-on-write (COW) while queries flow, and its contract is **never lose a row
and never disrupt an in-flight read**. These tests wire the engine to its *real*
collaborators — a filesystem-backed :class:`~src.manifest.ManifestStore`, the
three storage backends, a live :class:`~src.pattern_tracker.PatternTracker`, a
real :class:`~src.format_selector.FormatSelector`, :class:`~src.tier_manager.TierManager`,
:class:`~src.index_manager.IndexManager`, and a :class:`~src.metrics.Metrics`
aggregator — then exercise the full COW path end-to-end.

What is asserted (mapping to the COW contract in the module docstring):

* **migrate_to** (the unconditional primitive) preserves every row, flips the
  manifest pointer atomically, and stamps ``last_migration`` (test 1).
* **No disruption** — a :class:`~src.query_engine.QueryEngine` reading the same
  manifest returns the *same data* before and after a swap (now routed to the
  new backend), and the OLD-format file survives on disk as an orphan, proving
  an in-flight handle that already resolved the old format would still find its
  file (test 2).
* **Failed rewrite leaves the old format intact** — when the target backend's
  ``rewrite_from`` raises, the manifest still points at ROW, every row is still
  readable through the ROW backend, and the failure is counted (test 3).
* **Decision gating** — ``migrate_partition_once`` keeps a HOT write-heavy
  partition on ROW (test 4) and migrates a COLD scan-heavy/narrow partition to
  COLUMNAR (test 5); the anti-flap cooldown blocks an immediate re-migrate
  (test 6).
* **Background loop** drains a clearly-columnar partition within a bounded poll
  (test 7).
* **sweep_orphans** reclaims the stale ROW file left behind after a swap while
  leaving the live Parquet (test 8).

``asyncio_mode=auto`` (see ``pytest.ini``) runs the ``async def test_*``
functions without an explicit decorator. The engine clock is pinned so tier /
cooldown math is deterministic.

Record shape (shared backend contract): a stored "row" is a flat dict
``{"ts": <float>, <field>: value, ...}``.
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from src.format_selector import FormatSelector
from src.index_manager import IndexManager
from src.manifest import ManifestStore, PartitionMeta
from src.metrics import Metrics
from src.models import Format, QueryClass
from src.paths import PARQUET_NAME, ROW_NAME, partition_paths
from src.pattern_tracker import PatternTracker
from src.query_engine import QueryEngine
from src.settings import Settings
from src.storage.columnar_backend import ColumnarBackend
from src.storage.hybrid_backend import HybridBackend
from src.storage.row_backend import RowBackend
from src.tier_manager import TierManager
from src.migration_engine import MigrationEngine

BUCKET = 3600
# Pinned engine clock. With BUCKET=3600 this puts a partition's tier under
# direct test control via its bucket index (see _hot_pid / "p_0").
CLOCK = 500000.0


def _backends() -> dict[Format, object]:
    """Return a fresh format -> backend map covering all three formats."""
    return {
        Format.ROW: RowBackend(),
        Format.COLUMNAR: ColumnarBackend(),
        Format.HYBRID: HybridBackend(),
    }


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


def _engine(
    tmp_data_dir: Path,
    *,
    backends: dict[Format, object] | None = None,
    metrics: Metrics | None = None,
    tracker: PatternTracker | None = None,
    settings: Settings | None = None,
    clock=lambda: CLOCK,
) -> tuple[MigrationEngine, ManifestStore, dict[Format, object], Metrics, PatternTracker]:
    """Build a :class:`MigrationEngine` wired with real collaborators.

    Returns the engine plus the shared manifest store, the backend map, the
    metrics sink, and the pattern tracker, so a test can register partitions and
    record access on exactly the instances the engine reads.
    """
    bk = backends if backends is not None else _backends()
    mt = metrics if metrics is not None else Metrics()
    pt = tracker if tracker is not None else PatternTracker()
    st = settings if settings is not None else _settings(tmp_data_dir)
    # Share the engine's pinned clock with the manifest so the migration
    # timestamp it stamps (``last_migration["at"]``) is deterministic and
    # consistent with the ``now`` the engine compares it against in the cooldown.
    mstore = ManifestStore(tmp_data_dir, clock=clock)
    eng = MigrationEngine(
        manifest=mstore,
        backends=bk,
        selector=_selector(),
        tier_manager=_tier(),
        pattern_tracker=pt,
        compression=None,
        index_manager=_index_manager(),
        metrics=mt,
        settings=st,
        clock=clock,
    )
    return eng, mstore, bk, mt, pt


async def _make_row_partition(
    mstore: ManifestStore,
    backends: dict[Format, object],
    tmp_data_dir: Path,
    tenant: str,
    pid: str,
    rows: list[dict],
) -> None:
    """Create a ROW partition on disk and register it in the manifest.

    Mirrors what the ingest engine would have produced for a brand-new ROW
    partition, without coupling the test to the ingest path.
    """
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


def _sample_rows(n: int) -> list[dict]:
    """``n`` simple, distinct, fully-comparable log rows."""
    return [{"ts": float(i), "level": "INFO", "msg": f"m{i}"} for i in range(n)]


def _row_key(row: dict) -> tuple:
    """A hashable identity for a row dict (for set comparison)."""
    return tuple(sorted(row.items()))


# --------------------------------------------------------------------------- #
# 1. migrate_to ROW->COLUMNAR preserves all rows + flips the pointer
# --------------------------------------------------------------------------- #
async def test_migrate_to_row_to_columnar_preserves_rows_and_flips_pointer(
    tmp_data_dir: Path,
) -> None:
    eng, mstore, backends, _metrics, _pt = _engine(tmp_data_dir)
    rows = [{"ts": float(i), "level": "INFO", "msg": "m"} for i in range(10)]
    await _make_row_partition(
        mstore, backends, tmp_data_dir, "acme", "p_0", rows
    )

    ok = await eng.migrate_to("acme", "p_0", Format.COLUMNAR)
    assert ok is True

    # Manifest pointer flipped to COLUMNAR (reloaded from the live store).
    meta = mstore.get_partition("acme", "p_0")
    assert meta is not None
    assert meta.format == Format.COLUMNAR
    assert meta.last_migration is not None
    assert meta.last_migration["from"] == "row"
    assert meta.last_migration["to"] == "columnar"

    # The parquet file exists and carries all 10 rows, same data.
    paths = partition_paths(tmp_data_dir, "acme", "p_0")
    assert paths.parquet.exists()
    read_rows = ColumnarBackend().read(paths).rows
    assert len(read_rows) == 10
    assert {_row_key(r) for r in read_rows} == {_row_key(r) for r in rows}


# --------------------------------------------------------------------------- #
# 2. No disruption: a read survives the swap, data preserved, old file orphaned
# --------------------------------------------------------------------------- #
async def test_no_disruption_read_survives_swap_and_old_file_remains(
    tmp_data_dir: Path,
) -> None:
    eng, mstore, backends, metrics, pt = _engine(tmp_data_dir)
    rows = _sample_rows(12)
    await _make_row_partition(
        mstore, backends, tmp_data_dir, "acme", "p_0", rows
    )

    # A query engine over the SAME manifest + backends (the real read path).
    qe = QueryEngine(
        manifest=mstore,
        backends=backends,
        pattern_tracker=pt,
        index_manager=_index_manager(),
        metrics=metrics,
        settings=_settings(tmp_data_dir),
        clock=lambda: CLOCK,
    )

    before = await qe.query("acme")
    assert before.rows is not None
    assert before.meta["formats_used"] == {"row": 1}

    # Flip the partition to COLUMNAR underneath the reader.
    ok = await eng.migrate_to("acme", "p_0", Format.COLUMNAR)
    assert ok is True

    after = await qe.query("acme")
    assert after.rows is not None

    # No loss, no duplication: identical data as a set, now via the columnar path.
    assert {_row_key(r) for r in after.rows} == {_row_key(r) for r in before.rows}
    assert len(after.rows) == len(before.rows) == 12
    assert after.meta["formats_used"] == {"columnar": 1}

    # The OLD row file is still on disk (a harmless orphan): an in-flight reader
    # that resolved the OLD format an instant before the swap would still find it.
    paths = partition_paths(tmp_data_dir, "acme", "p_0")
    assert paths.row.exists(), "old ROW file must survive the swap (orphan)"
    assert paths.parquet.exists(), "new COLUMNAR file must exist after the swap"


# --------------------------------------------------------------------------- #
# 3. Failed rewrite leaves the old format fully intact
# --------------------------------------------------------------------------- #
class _BoomColumnar(ColumnarBackend):
    """A COLUMNAR backend whose rewrite always fails mid-flight.

    Used to prove the COW failure contract: a rewrite that raises before/at the
    commit leaves the live files and the manifest untouched, so the partition
    stays fully intact in its original format.
    """

    def rewrite_from(self, *args, **kwargs):  # type: ignore[override]
        raise RuntimeError("boom")


async def test_failed_rewrite_leaves_old_format_intact(
    tmp_data_dir: Path,
) -> None:
    metrics = Metrics()
    backends = _backends()
    backends[Format.COLUMNAR] = _BoomColumnar()
    eng, mstore, backends, metrics, _pt = _engine(
        tmp_data_dir, backends=backends, metrics=metrics
    )

    rows = _sample_rows(10)
    await _make_row_partition(
        mstore, backends, tmp_data_dir, "acme", "p_0", rows
    )

    ok = await eng.migrate_to("acme", "p_0", Format.COLUMNAR)
    assert ok is False, "a failed rewrite must report False"

    # Manifest still points at ROW — the pointer was never flipped.
    meta = mstore.get_partition("acme", "p_0")
    assert meta is not None
    assert meta.format == Format.ROW
    assert meta.last_migration is None

    # Every row is still readable through the ROW backend (live file intact).
    paths = partition_paths(tmp_data_dir, "acme", "p_0")
    read_rows = RowBackend().read(paths).rows
    assert len(read_rows) == 10
    assert {_row_key(r) for r in read_rows} == {_row_key(r) for r in rows}

    # The failure was counted; no migration completed.
    snap = metrics.snapshot()
    assert snap["migrations"]["failed"] == 1
    assert snap["migrations"]["completed"] == 0
    # The in-flight gauge was balanced in the ``finally``.
    assert snap["migrations"]["in_flight"] == 0


# --------------------------------------------------------------------------- #
# 4. migrate_partition_once gating: HOT + write-heavy stays on ROW
# --------------------------------------------------------------------------- #
async def test_migrate_partition_once_hot_write_heavy_stays_row(
    tmp_data_dir: Path,
) -> None:
    eng, mstore, backends, _metrics, pt = _engine(tmp_data_dir)

    # Bucket chosen so the partition's age is < hot_max_age (3600s) at CLOCK,
    # i.e. it is YOUNG and therefore HOT once recently touched.
    bucket = int(CLOCK / BUCKET)  # 138 -> age = 500000 - 496800 = 3200s (< 3600)
    pid = f"p_{bucket}"
    rows = _sample_rows(10)
    await _make_row_partition(
        mstore, backends, tmp_data_dir, "acme", pid, rows
    )

    # Write-heavy access, touched just before CLOCK so idle is tiny -> HOT.
    for _ in range(8):
        pt.record_write("acme", pid, columns=["ts", "x"], ts=499900.0)

    did = await eng.migrate_partition_once("acme", pid)
    assert did is False, "HOT + write-heavy must stay ROW (no migration)"

    meta = mstore.get_partition("acme", pid)
    assert meta is not None
    assert meta.format == Format.ROW


# --------------------------------------------------------------------------- #
# 5. migrate_partition_once: COLD + scan-heavy + narrow -> COLUMNAR
# --------------------------------------------------------------------------- #
async def test_migrate_partition_once_cold_scan_heavy_migrates_to_columnar(
    tmp_data_dir: Path,
) -> None:
    eng, mstore, backends, _metrics, pt = _engine(tmp_data_dir)

    # Bucket 0 -> age == CLOCK (500000s) >> cold_min_age (86400s) -> COLD.
    pid = "p_0"
    rows = _sample_rows(8)
    await _make_row_partition(
        mstore, backends, tmp_data_dir, "acme", pid, rows
    )

    # Scan-heavy with a NARROW projection: 20 single-column scans over 5 distinct
    # columns -> avg_columns_touched 1, fraction 1/5 = 0.2 (<= 0.4),
    # scan_ratio 1.0 (>= 0.6), write_ratio 0. Reads are old (ts well before
    # CLOCK) so the read-rate stays below the hot threshold.
    for col in ["a", "b", "c", "d", "e"] * 4:
        pt.record_read(
            "acme",
            pid,
            columns=[col],
            query_class=QueryClass.ANALYTICAL,
            is_point_lookup=False,
            ts=400000.0,
        )

    did = await eng.migrate_partition_once(
        "acme", pid, ignore_cooldown=True
    )
    assert did is True, "COLD + scan-heavy + narrow must migrate to COLUMNAR"

    meta = mstore.get_partition("acme", pid)
    assert meta is not None
    assert meta.format == Format.COLUMNAR

    # Data preserved across the gated migration.
    paths = partition_paths(tmp_data_dir, "acme", pid)
    read_rows = ColumnarBackend().read(paths).rows
    assert {_row_key(r) for r in read_rows} == {_row_key(r) for r in rows}


# --------------------------------------------------------------------------- #
# 6. Cooldown blocks an immediate re-migrate
# --------------------------------------------------------------------------- #
async def test_cooldown_blocks_immediate_re_migrate(tmp_data_dir: Path) -> None:
    eng, mstore, backends, _metrics, _pt = _engine(tmp_data_dir)

    rows = _sample_rows(10)
    await _make_row_partition(
        mstore, backends, tmp_data_dir, "acme", "p_0", rows
    )

    # A successful forced migration stamps last_migration["at"] = CLOCK.
    ok = await eng.migrate_to("acme", "p_0", Format.COLUMNAR)
    assert ok is True
    meta = mstore.get_partition("acme", "p_0")
    assert meta is not None
    assert meta.last_migration is not None
    assert meta.last_migration["at"] == CLOCK

    # Immediately re-evaluating (now only +1s, well within the 60s cooldown)
    # must NOT migrate again. This holds whether the selector recommends a
    # further change (then the cooldown forces False) or recommends nothing
    # (then the selector gate returns False) — robust either way.
    did = await eng.migrate_partition_once("acme", "p_0", now=CLOCK + 1.0)
    assert did is False, "a partition migrated 1s ago must not re-migrate"

    # Pointer unchanged by the blocked call.
    meta2 = mstore.get_partition("acme", "p_0")
    assert meta2 is not None
    assert meta2.format == Format.COLUMNAR


# --------------------------------------------------------------------------- #
# 7. run() loop smoke (bounded): a clearly-columnar partition gets migrated
# --------------------------------------------------------------------------- #
async def test_run_loop_migrates_columnar_candidate_bounded(
    tmp_data_dir: Path,
) -> None:
    settings = _settings(tmp_data_dir, migration_interval_seconds=0.01)
    eng, mstore, backends, _metrics, pt = _engine(
        tmp_data_dir, settings=settings
    )

    # Same COLD + scan-heavy + narrow setup as test 5 -> a clear COLUMNAR pick.
    pid = "p_0"
    rows = _sample_rows(8)
    await _make_row_partition(
        mstore, backends, tmp_data_dir, "acme", pid, rows
    )
    for col in ["a", "b", "c", "d", "e"] * 4:
        pt.record_read(
            "acme",
            pid,
            columns=[col],
            query_class=QueryClass.ANALYTICAL,
            is_point_lookup=False,
            ts=400000.0,
        )

    stop = asyncio.Event()
    task = asyncio.create_task(eng.run(stop))
    try:
        migrated = False
        for _ in range(40):  # bounded poll: up to ~2s total
            await asyncio.sleep(0.05)
            meta = mstore.get_partition("acme", pid)
            if meta is not None and meta.format == Format.COLUMNAR:
                migrated = True
                break
        assert migrated, "background loop should migrate the columnar candidate"
    finally:
        stop.set()
        await task

    meta = mstore.get_partition("acme", pid)
    assert meta is not None
    assert meta.format == Format.COLUMNAR


# --------------------------------------------------------------------------- #
# 8. sweep_orphans reclaims the stale ROW file, keeps the live Parquet
# --------------------------------------------------------------------------- #
async def test_sweep_orphans_removes_stale_row_file_keeps_parquet(
    tmp_data_dir: Path,
) -> None:
    eng, mstore, backends, _metrics, _pt = _engine(tmp_data_dir)

    rows = _sample_rows(10)
    await _make_row_partition(
        mstore, backends, tmp_data_dir, "acme", "p_0", rows
    )

    ok = await eng.migrate_to("acme", "p_0", Format.COLUMNAR)
    assert ok is True

    paths = partition_paths(tmp_data_dir, "acme", "p_0")
    # Precondition: the orphan ROW file is still present right after the swap.
    assert paths.row.exists()
    assert paths.parquet.exists()

    removed = eng.sweep_orphans("acme")
    assert removed >= 1

    # The stale ROW orphan is gone; the live Parquet remains and still reads.
    assert not (paths.dir / ROW_NAME).exists()
    assert (paths.dir / PARQUET_NAME).exists()
    read_rows = ColumnarBackend().read(paths).rows
    assert {_row_key(r) for r in read_rows} == {_row_key(r) for r in rows}
