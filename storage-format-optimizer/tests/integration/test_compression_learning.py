"""Integration tests for adaptive-compression LEARNING wiring (C17, Feature B).

C16 proved the migration engine moves a partition copy-on-write without losing a
row. C17 wires the *learning* side of :class:`~src.compression.CompressionChooser`
into that path: during a ``rewrite_from(..., codecs=None)`` (which is exactly what
:meth:`~src.migration_engine.MigrationEngine.migrate_to` now issues), the
:class:`~src.storage.columnar_backend.ColumnarBackend` first trial-compresses each
column on a sample, *remembers the winning codec* per ``(column, dtype)``, and the
learned winners flow out on :attr:`RewriteResult.codecs` → ``swap_format`` →
persisted into the manifest's ``codecs`` map.

These tests wire the engine to its *real* collaborators (the same shape as
``test_migration_engine.py``) but inject the COLUMNAR / HYBRID backends with a
**real learning chooser** so the end-to-end learn→persist path is exercised, not
mocked:

Product note (C17 follow-up): adaptive codec LEARNING now happens **only during
migration** (``ColumnarBackend.rewrite_from`` with ``codecs=None``), never in
``ColumnarBackend.write()`` — ordinary writes use the fast, deterministic
dtype-default codecs. Every test below therefore exercises learning through the
*migration* path, not ``write()``.

* **Learning populates manifest codecs on migration** (test 1) — a ROW partition
  whose columns clearly favour different codecs is migrated to COLUMNAR; the
  manifest ends up with a non-empty, all-valid per-column codec map, the chooser
  has recorded learned winners, and every row round-trips back out.
* **Learned is never worse than the static default** (test 2) — two identical ROW
  partitions are migrated to COLUMNAR, one via a *learning* chooser and one via a
  *disabled* (dtype-default) chooser; the learned partition's Parquet file is
  ``<=`` the dtype-default partition's on disk. On a representative sample the
  learner must never pick a worse codec, and only the enabled chooser records
  learned state.
* **Disabled → dtype defaults, no learned entries** (test 3) — with the chooser
  disabled the migration still works and the manifest still carries valid
  dtype-default codecs, but the chooser stores nothing (learning is off).

``asyncio_mode=auto`` (see ``pytest.ini``) runs the ``async def test_*`` functions
without an explicit decorator.

Record shape (shared backend contract): a stored "row" is a flat dict
``{"ts": <float>, <field>: value, ...}``.
"""
from __future__ import annotations

from pathlib import Path

from src.compression import VALID_CODECS, CompressionChooser, codec_for_dtype
from src.format_selector import FormatSelector
from src.index_manager import IndexManager
from src.manifest import ManifestStore, PartitionMeta
from src.metrics import Metrics
from src.migration_engine import MigrationEngine
from src.models import Format
from src.paths import ROW_NAME, partition_paths
from src.pattern_tracker import PatternTracker
from src.settings import Settings
from src.storage.columnar_backend import ColumnarBackend
from src.storage.hybrid_backend import HybridBackend
from src.storage.row_backend import RowBackend
from src.tier_manager import TierManager

BUCKET = 3600
# Pinned engine clock (matches the C16 migration-engine test); deterministic
# tier / cooldown math is irrelevant here since every migration goes through the
# unconditional ``migrate_to`` primitive, but a fixed clock keeps the manifest's
# ``last_migration["at"]`` stable.
CLOCK = 500000.0


# --------------------------------------------------------------------------- #
# Wiring helpers (mirror test_migration_engine.py, but inject a real chooser)
# --------------------------------------------------------------------------- #
def _selector() -> FormatSelector:
    """A selector with a tiny ``min_rows`` (unused on the forced ``migrate_to`` path)."""
    return FormatSelector(
        write_ratio_row=0.3,
        point_lookup_row=0.5,
        scan_ratio_columnar=0.6,
        few_columns_fraction=0.4,
        min_confidence=0.6,
        min_rows=4,
    )


def _tier() -> TierManager:
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
    base = dict(
        data_dir=str(tmp_data_dir),
        partition_bucket_seconds=BUCKET,
        migration_cooldown_seconds=60.0,
        select_min_rows=4,
    )
    base.update(overrides)
    return Settings(**base)


def _backends(chooser: CompressionChooser) -> dict[Format, object]:
    """Format -> backend map with COLUMNAR / HYBRID sharing the given chooser."""
    return {
        Format.ROW: RowBackend(),
        Format.COLUMNAR: ColumnarBackend(compression=chooser),
        Format.HYBRID: HybridBackend(compression=chooser),
    }


def _engine(
    tmp_data_dir: Path,
    chooser: CompressionChooser,
    *,
    metrics: Metrics | None = None,
    clock=lambda: CLOCK,
) -> tuple[MigrationEngine, ManifestStore, dict[Format, object]]:
    """Build a :class:`MigrationEngine` wired to a learning chooser-backed backend map."""
    backends = _backends(chooser)
    mstore = ManifestStore(tmp_data_dir, clock=clock)
    eng = MigrationEngine(
        manifest=mstore,
        backends=backends,
        selector=_selector(),
        tier_manager=_tier(),
        pattern_tracker=PatternTracker(),
        compression=chooser,
        index_manager=_index_manager(),
        metrics=metrics if metrics is not None else Metrics(),
        settings=_settings(tmp_data_dir),
        clock=clock,
    )
    return eng, mstore, backends


async def _make_row_partition(
    mstore: ManifestStore,
    backends: dict[Format, object],
    tmp_data_dir: Path,
    tenant: str,
    pid: str,
    rows: list[dict],
) -> None:
    """Create a ROW partition on disk and register it in the manifest (as ingest would)."""
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


def _mixed_codec_rows(n: int) -> list[dict]:
    """``n`` rows whose columns clearly favour *different* codecs.

    * ``ts``    — a monotonically increasing float (temporal / numeric).
    * ``level`` — a 2-value low-cardinality enum ("INFO"/"ERROR").
    * ``msg``   — a long, highly repetitive text blob (very compressible).

    This shape is deliberately compressible so the learner has a clear winner per
    column and the learned file is at least as small as the dtype-default file.
    """
    return [
        {
            "ts": float(i),
            "level": ("INFO" if i % 2 else "ERROR"),
            "msg": "a fairly long and very repetitive log message " * 3,
        }
        for i in range(n)
    ]


def _row_key(row: dict) -> tuple:
    """A hashable identity for a row dict (for set comparison)."""
    return tuple(sorted(row.items()))


# --------------------------------------------------------------------------- #
# 1. Learning populates the manifest codec map on a ROW->COLUMNAR migration.
# --------------------------------------------------------------------------- #
async def test_learning_populates_manifest_codecs_on_migration(
    tmp_data_dir: Path,
) -> None:
    chooser = CompressionChooser(enabled=True, sample_rows=2000)
    eng, mstore, backends = _engine(tmp_data_dir, chooser)

    rows = _mixed_codec_rows(1500)
    await _make_row_partition(
        mstore, backends, tmp_data_dir, "acme", "p_0", rows
    )

    ok = await eng.migrate_to("acme", "p_0", Format.COLUMNAR)
    assert ok is True

    # The manifest pointer flipped to COLUMNAR and carries a learned codec map.
    meta = ManifestStore(tmp_data_dir).load("acme").partitions["p_0"]
    assert meta.format == Format.COLUMNAR

    # A non-empty per-column codec map, every entry a supported codec, covering
    # exactly the three data columns.
    assert isinstance(meta.codecs, dict)
    assert meta.codecs, "migration must persist a non-empty codec map"
    assert set(meta.codecs) == {"ts", "level", "msg"}
    for col, codec in meta.codecs.items():
        assert codec in VALID_CODECS, f"{col!r} got unsupported codec {codec!r}"

    # The chooser actually LEARNED something during the rewrite: at least one
    # column has a remembered winner. We don't hard-code the inferred dtype
    # string (the backend infers it internally); instead we confirm the chooser
    # holds learned state and that the manifest codecs are consistent with what
    # ``codec_for`` would now return for some learned (col, dtype) key.
    learned = chooser._learned  # internal map: {(col, dtype): codec}
    assert learned, "the enabled chooser must record at least one learned winner"
    learned_cols = {key[0] for key in learned}
    assert learned_cols <= {"ts", "level", "msg"}
    # Every learned winner is itself a valid codec, and matches what the manifest
    # persisted for that column (learning flowed end-to-end into the manifest).
    for (col, _dtype), codec in learned.items():
        assert codec in VALID_CODECS
        assert meta.codecs[col] == codec, (
            f"learned codec for {col!r} ({codec!r}) must match the persisted "
            f"manifest codec ({meta.codecs[col]!r})"
        )

    # Data round-trips: a fresh COLUMNAR backend reads back all 1500 rows intact.
    paths = partition_paths(tmp_data_dir, "acme", "p_0")
    read_rows = ColumnarBackend(compression=chooser).read(paths).rows
    assert len(read_rows) == 1500
    assert {_row_key(r) for r in read_rows} == {_row_key(r) for r in rows}


# --------------------------------------------------------------------------- #
# 2. The learned codec is never *larger* on disk than the static dtype default.
# --------------------------------------------------------------------------- #
async def test_learned_codec_not_larger_than_static_default(
    tmp_data_dir: Path,
) -> None:
    # Learning is migration-only: ``write()`` uses dtype defaults and never
    # learns, so we drive learning the way the engine does — through
    # ``migrate_to`` -> ``rewrite_from(codecs=None)``. Build TWO identical ROW
    # partitions, then migrate one with a LEARNING chooser and one with a
    # DISABLED chooser and compare the resulting COLUMNAR files on disk.
    rows_a = _mixed_codec_rows(1500)
    rows_b = _mixed_codec_rows(1500)
    assert rows_a == rows_b  # identical source data -> a fair size comparison.

    # (a) Migrate partition A with a real LEARNING chooser. Its own engine +
    # backend map share this chooser, so the rewrite trials candidates and
    # remembers winners.
    # Distinct tenants keep the two manifests fully isolated on disk so neither
    # migration can clobber the other's partition entry.
    learn_chooser = CompressionChooser(enabled=True, sample_rows=2000)
    learn_eng, learn_mstore, learn_backends = _engine(tmp_data_dir, learn_chooser)
    await _make_row_partition(
        learn_mstore, learn_backends, tmp_data_dir, "tenant_a", "p_a", rows_a
    )
    learn_ok = await learn_eng.migrate_to("tenant_a", "p_a", Format.COLUMNAR)
    assert learn_ok is True

    # (b) Migrate partition B with a DISABLED chooser (dtype defaults, no
    # learning). A separate engine + backend map so its migration uses its own
    # chooser.
    default_chooser = CompressionChooser(enabled=False)
    default_eng, default_mstore, default_backends = _engine(
        tmp_data_dir, default_chooser
    )
    await _make_row_partition(
        default_mstore, default_backends, tmp_data_dir, "tenant_b", "p_b", rows_b
    )
    default_ok = await default_eng.migrate_to("tenant_b", "p_b", Format.COLUMNAR)
    assert default_ok is True

    # Size on disk: the learned partition must not exceed the dtype-default one.
    learn_paths = partition_paths(tmp_data_dir, "tenant_a", "p_a")
    default_paths = partition_paths(tmp_data_dir, "tenant_b", "p_b")
    learned_size = learn_backends[Format.COLUMNAR].size_bytes(learn_paths)
    default_size = default_backends[Format.COLUMNAR].size_bytes(default_paths)

    assert learned_size > 0 and default_size > 0
    # Learning trials every candidate on a representative sample and keeps the
    # smallest-scoring one, so it can never end up *larger* than the fixed
    # dtype default. Equality is allowed (the default may already be optimal).
    assert learned_size <= default_size, (
        f"learned file ({learned_size} B) must not exceed the dtype-default "
        f"file ({default_size} B)"
    )

    # Sanity: the learning migration recorded winners; the disabled one did not.
    assert learn_chooser._learned, "learning chooser should have learned codecs"
    assert not default_chooser._learned, "disabled chooser must learn nothing"

    # Both files still contain every row (learning never trades correctness).
    learned_rows = ColumnarBackend(compression=learn_chooser).read(learn_paths).rows
    default_rows = ColumnarBackend(compression=default_chooser).read(default_paths).rows
    assert len(learned_rows) == 1500
    assert len(default_rows) == 1500
    assert {_row_key(r) for r in learned_rows} == {_row_key(r) for r in rows_a}
    assert {_row_key(r) for r in default_rows} == {_row_key(r) for r in rows_b}


# --------------------------------------------------------------------------- #
# 3. Disabled chooser -> dtype defaults persisted, nothing learned.
# --------------------------------------------------------------------------- #
async def test_disabled_chooser_uses_dtype_defaults_and_learns_nothing(
    tmp_data_dir: Path,
) -> None:
    chooser = CompressionChooser(enabled=False)
    eng, mstore, backends = _engine(tmp_data_dir, chooser)

    rows = _mixed_codec_rows(1500)
    await _make_row_partition(
        mstore, backends, tmp_data_dir, "acme", "p_0", rows
    )

    ok = await eng.migrate_to("acme", "p_0", Format.COLUMNAR)
    assert ok is True, "migration must still succeed with learning disabled"

    # The migration worked and persisted a valid codec map ...
    meta = ManifestStore(tmp_data_dir).load("acme").partitions["p_0"]
    assert meta.format == Format.COLUMNAR
    assert set(meta.codecs) == {"ts", "level", "msg"}
    for col, codec in meta.codecs.items():
        assert codec in VALID_CODECS, f"{col!r} got unsupported codec {codec!r}"

    # ... but the chooser stored NO learned winners (learning was off).
    assert chooser._learned == {}, "a disabled chooser must remember nothing"

    # With learning off, the persisted codecs are exactly the dtype defaults
    # (``codec_for`` falls straight through to ``codec_for_dtype``). We don't
    # know the inferred dtype string per column, but the disabled chooser's
    # ``codec_for(name, dtype)`` collapses to ``codec_for_dtype(dtype)``, and the
    # only codecs ``codec_for_dtype`` can return are members of VALID_CODECS —
    # already asserted above. As a concrete default check, the temporal ``ts``
    # column maps to SNAPPY under the dtype rules.
    assert meta.codecs["ts"] == codec_for_dtype("timestamp") == "SNAPPY"

    # Data still round-trips through the (disabled-chooser) columnar backend.
    paths = partition_paths(tmp_data_dir, "acme", "p_0")
    read_rows = ColumnarBackend(compression=chooser).read(paths).rows
    assert len(read_rows) == 1500
    assert {_row_key(r) for r in read_rows} == {_row_key(r) for r in rows}
