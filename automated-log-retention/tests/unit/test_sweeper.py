"""Unit tests for ``src/lifecycle/sweeper.py``.

The sweeper is a thin wrapper over :func:`storage.mover.sweep_deletes`
(already covered in ``tests/unit/test_mover.py``). These tests therefore
focus on the wrapper's own contract:

  * Return shape: always a :class:`SweepReport` dataclass with the
    correct ``swept`` count and ``errors=0`` (since C11 does not yet
    surface an error count — see the sweeper module docstring).
  * Zero-state behaviour: an empty queue returns ``SweepReport(0, 0)``.
  * Partial-due behaviour: of N planted rows with mixed ``delete_after``
    times, exactly the past-due rows are swept; future-due survive.
  * Missing-file tolerance: a row pointing at a non-existent path still
    counts as swept (the underlying mover tolerates it).

All tests use the project's ``session_factory`` fixture from
``tests/conftest.py`` plus the pytest ``tmp_path`` fixture for the
filesystem side. ``asyncio_mode = auto`` in ``pytest.ini`` means async
test functions need no explicit decorator.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from src.lifecycle.sweeper import SweepReport, sweep_once
from src.storage.catalog import CatalogRepo
from src.storage.tiers import ensure_tier_dirs, tier_dir


async def _seed_pending_delete(
    session_factory,
    *,
    file_id: int,
    path: str,
    delete_after: datetime,
):
    """Insert one ``PendingDelete`` row via the catalog repo.

    Helper to keep the per-test arrange blocks short. Returns the
    inserted row (with ``id`` populated) so callers can correlate the
    sweep result back to specific rows.
    """
    repo = CatalogRepo(session_factory)
    return await repo.add_pending_delete(
        file_id=file_id,
        path=path,
        delete_after=delete_after,
    )


async def _seed_parent_file(session_factory, segment_path: str):
    """Insert a parent ``File`` row so the ``PendingDelete`` FK is satisfied."""
    repo = CatalogRepo(session_factory)
    return await repo.add_file(
        source="app",
        segment_path=segment_path,
        tier="hot",
        size_bytes=10,
        oldest_record_ts=datetime(2026, 1, 1),
        newest_record_ts=datetime(2026, 1, 1),
    )


# --- zero-state -------------------------------------------------------------


async def test_sweep_once_zero_state_returns_zero(session_factory):
    """An empty ``pending_deletes`` table yields ``SweepReport(0, 0)``."""
    repo = CatalogRepo(session_factory)
    now = datetime(2026, 5, 23, 12, 0, 0)

    report = await sweep_once(repo, now)

    assert report == SweepReport(swept=0, errors=0)


# --- partial-due ------------------------------------------------------------


async def test_sweep_once_partial_due(tmp_path: Path, session_factory):
    """3 planted rows (2 past-due + 1 future) → 2 unlinked, 1 remains."""
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    pending_dir = tier_dir(storage_root, "pending")
    repo = CatalogRepo(session_factory)

    parent = await _seed_parent_file(
        session_factory,
        segment_path=str(storage_root / "hot" / "seed.jsonl"),
    )

    now = datetime(2026, 5, 23, 12, 0, 0)
    past_a = now - timedelta(hours=2)
    past_b = now - timedelta(hours=1)
    future = now + timedelta(hours=1)

    # Three real files on disk in the pending tier, one row per file.
    files: dict[str, Path] = {}
    for name, when in (
        ("past-a.jsonl", past_a),
        ("past-b.jsonl", past_b),
        ("future.jsonl", future),
    ):
        path = pending_dir / name
        path.write_bytes(b"x")
        await _seed_pending_delete(
            session_factory,
            file_id=parent.id,
            path=str(path),
            delete_after=when,
        )
        files[name] = path

    report = await sweep_once(repo, now)

    # Exactly the two past-due rows were swept.
    assert report.swept == 2
    assert report.errors == 0

    # Files: 2 unlinked, 1 remains untouched.
    assert not files["past-a.jsonl"].exists()
    assert not files["past-b.jsonl"].exists()
    assert files["future.jsonl"].exists()

    # DB: only the future row remains in the queue.
    assert await repo.count_pending_deletes() == 1


# --- missing-file tolerance -------------------------------------------------


async def test_sweep_once_tolerates_missing_files(
    tmp_path: Path, session_factory
):
    """A row whose path does not exist on disk still sweeps cleanly."""
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    pending_dir = tier_dir(storage_root, "pending")
    repo = CatalogRepo(session_factory)

    parent = await _seed_parent_file(
        session_factory,
        segment_path=str(storage_root / "hot" / "ghost-parent.jsonl"),
    )

    # Path that was never written to disk.
    ghost_path = pending_dir / "ghost-never-existed.jsonl"
    assert not ghost_path.exists()

    now = datetime(2026, 5, 23, 12, 0, 0)
    await _seed_pending_delete(
        session_factory,
        file_id=parent.id,
        path=str(ghost_path),
        delete_after=now - timedelta(hours=1),
    )

    report = await sweep_once(repo, now)

    # The missing file still counts as swept — the mover's missing_ok
    # contract is what we are relying on here.
    assert report.swept == 1
    assert report.errors == 0
    # And the row is gone.
    assert await repo.count_pending_deletes() == 0


# --- return shape -----------------------------------------------------------


async def test_sweep_once_returns_report_dataclass(session_factory):
    """The return value is a :class:`SweepReport`; ``errors`` is always 0
    in this commit (audit-aware error counting lands with C13)."""
    repo = CatalogRepo(session_factory)
    now = datetime(2026, 5, 23, 12, 0, 0)

    report = await sweep_once(repo, now)

    assert isinstance(report, SweepReport)
    assert report.errors == 0
