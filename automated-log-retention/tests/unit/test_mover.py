"""Unit tests for ``src/storage/mover.py``.

The mover covers three filesystem primitives that the applier and the
sweeper depend on for crash-consistency:

  * ``stage_and_promote(src, dst)`` — copy + atomic rename.
  * ``mark_for_delete(catalog, file_id, src, root, delete_after)`` —
    atomic move into ``pending/`` plus a queue row insert.
  * ``sweep_deletes(catalog, now)`` — drain expired queue rows + unlink.

All tests use ``tmp_path`` for the filesystem side and the project's
``session_factory`` fixture (from ``tests/conftest.py``) for the catalog
side. ``asyncio_mode = auto`` in ``pytest.ini`` means the async tests
need no explicit decorator.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.storage.catalog import CatalogRepo
from src.storage.mover import mark_for_delete, stage_and_promote, sweep_deletes
from src.storage.tiers import ensure_tier_dirs, tier_dir


# --- stage_and_promote ------------------------------------------------------


def test_stage_and_promote_creates_dst_and_removes_tmp(tmp_path: Path):
    """Successful promote leaves dst with the same bytes and no ``.tmp``."""
    src = tmp_path / "src.jsonl"
    dst = tmp_path / "warm" / "segment-1.jsonl"
    content = b"line1\nline2\nline3\n"
    src.write_bytes(content)

    stage_and_promote(src, dst)

    assert dst.exists(), "dst should exist after promote"
    assert dst.read_bytes() == content, "dst bytes should match src"
    # The ``.tmp`` sidecar must be gone — ``os.replace`` moves it onto dst.
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    assert not tmp.exists(), f"tmp sidecar should not linger: {tmp}"


def test_stage_and_promote_overwrites_existing_dst(tmp_path: Path):
    """A pre-existing ``dst`` is atomically replaced — promote is idempotent."""
    src = tmp_path / "src.jsonl"
    dst = tmp_path / "dst.jsonl"
    src.write_bytes(b"new content")
    dst.write_bytes(b"stale content from a previous run")

    stage_and_promote(src, dst)

    assert dst.read_bytes() == b"new content"


def test_stage_and_promote_handles_existing_dirs_idempotent(tmp_path: Path):
    """Missing ``dst.parent`` is created on the fly; existing dirs are fine."""
    src = tmp_path / "src.jsonl"
    src.write_bytes(b"data")
    # Three nesting levels below tmp_path — none of these exist yet.
    dst = tmp_path / "a" / "b" / "c" / "segment.jsonl"

    stage_and_promote(src, dst)

    assert dst.exists()
    assert dst.read_bytes() == b"data"

    # Calling again with the same parent dir should not raise.
    src2 = tmp_path / "src2.jsonl"
    src2.write_bytes(b"data2")
    dst2 = tmp_path / "a" / "b" / "c" / "segment2.jsonl"
    stage_and_promote(src2, dst2)
    assert dst2.read_bytes() == b"data2"


def test_stage_and_promote_cleans_tmp_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """If ``copy2`` raises mid-call, the ``.tmp`` is best-effort cleaned up
    and the original exception propagates."""
    src = tmp_path / "src.jsonl"
    dst = tmp_path / "warm" / "segment.jsonl"
    src.write_bytes(b"payload")

    boom = RuntimeError("disk full")

    def exploding_copy(*_args, **_kwargs):
        # Simulate a mid-copy failure. We don't write a partial tmp
        # ourselves — the contract is "best-effort cleanup" so we still
        # expect the function to issue the unlink (which is a no-op
        # against a missing file because of ``missing_ok=True``).
        raise boom

    # Monkeypatch the symbol in the ``src.storage.mover`` namespace, NOT
    # the stdlib — the function imports ``shutil`` at module load and
    # calls ``shutil.copy2`` as an attribute access.
    import src.storage.mover as mover_mod

    monkeypatch.setattr(mover_mod.shutil, "copy2", exploding_copy)

    with pytest.raises(RuntimeError, match="disk full"):
        stage_and_promote(src, dst)

    tmp = dst.with_suffix(dst.suffix + ".tmp")
    assert not tmp.exists(), "tmp sidecar must be cleaned up on failure"
    assert not dst.exists(), "dst must never be written on failure"


# --- mark_for_delete --------------------------------------------------------


async def test_mark_for_delete_moves_to_pending_dir_and_inserts_row(
    tmp_path: Path, session_factory
):
    """``mark_for_delete`` moves src into ``pending/`` and inserts a row."""
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    repo = CatalogRepo(session_factory)

    # Seed a parent ``File`` row so the PendingDelete FK is satisfied.
    parent = await repo.add_file(
        source="app",
        segment_path=str(storage_root / "hot" / "segment-x.jsonl"),
        tier="hot",
        size_bytes=42,
        oldest_record_ts=datetime(2026, 1, 1),
        newest_record_ts=datetime(2026, 1, 1),
    )

    # Plant a real file on disk at the hot tier.
    src_file = storage_root / "hot" / "segment-x.jsonl"
    src_file.write_bytes(b"hello world\n")

    delete_after = datetime(2026, 5, 1, 12, 0, 0)
    row = await mark_for_delete(
        repo,
        file_id=parent.id,
        src=src_file,
        storage_root=storage_root,
        delete_after=delete_after,
    )

    # The hot copy is gone; the pending copy exists with the same bytes.
    assert not src_file.exists()
    expected_pending = tier_dir(storage_root, "pending") / "segment-x.jsonl"
    assert expected_pending.exists()
    assert expected_pending.read_bytes() == b"hello world\n"

    # The returned row matches what was inserted.
    assert row.file_id == parent.id
    assert row.path == str(expected_pending)
    assert row.delete_after == delete_after
    assert row.id is not None


async def test_mark_for_delete_handles_name_collision(
    tmp_path: Path, session_factory
):
    """A pre-existing pending file with the same name does not raise —
    the second move lands at a suffix-disambiguated path."""
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    repo = CatalogRepo(session_factory)

    parent = await repo.add_file(
        source="app",
        segment_path=str(storage_root / "hot" / "segment-y.jsonl"),
        tier="hot",
        size_bytes=10,
        oldest_record_ts=datetime(2026, 1, 1),
        newest_record_ts=datetime(2026, 1, 1),
    )

    # Plant the colliding name in pending from a "prior" sweep cycle.
    pending_dir = tier_dir(storage_root, "pending")
    colliding = pending_dir / "segment-y.jsonl"
    colliding.write_bytes(b"old leftover")

    # Plant the new source.
    src_file = storage_root / "hot" / "segment-y.jsonl"
    src_file.write_bytes(b"fresh delete")

    row = await mark_for_delete(
        repo,
        file_id=parent.id,
        src=src_file,
        storage_root=storage_root,
        delete_after=datetime(2026, 5, 1, 12, 0, 0),
    )

    # The original pending file is untouched (still has the old bytes)…
    assert colliding.read_bytes() == b"old leftover"
    # …and the new file landed at a disambiguated path.
    assert row.path != str(colliding)
    assert row.path.startswith(str(colliding))  # prefix preserved
    assert Path(row.path).read_bytes() == b"fresh delete"
    # Source is gone from hot.
    assert not src_file.exists()


# --- sweep_deletes ----------------------------------------------------------


async def test_sweep_deletes_unlinks_and_removes_rows(
    tmp_path: Path, session_factory
):
    """Past-due rows are unlinked + dropped; future rows survive untouched."""
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    repo = CatalogRepo(session_factory)

    parent = await repo.add_file(
        source="app",
        segment_path=str(storage_root / "hot" / "seed.jsonl"),
        tier="hot",
        size_bytes=10,
        oldest_record_ts=datetime(2026, 1, 1),
        newest_record_ts=datetime(2026, 1, 1),
    )
    pending_dir = tier_dir(storage_root, "pending")

    now = datetime(2026, 5, 1, 12, 0, 0)
    past_a = now - timedelta(hours=2)
    past_b = now - timedelta(hours=1)
    future = now + timedelta(hours=1)

    # 2 past + 1 future, each with a real file on disk.
    files: dict[str, Path] = {}
    for name, when in (("past-a.jsonl", past_a),
                       ("past-b.jsonl", past_b),
                       ("future.jsonl", future)):
        path = pending_dir / name
        path.write_bytes(b"x")
        await repo.add_pending_delete(
            file_id=parent.id,
            path=str(path),
            delete_after=when,
        )
        files[name] = path

    swept = await sweep_deletes(repo, now)

    assert len(swept) == 2, f"expected 2 swept ids, got {swept}"
    # The two past files are gone; the future one remains.
    assert not files["past-a.jsonl"].exists()
    assert not files["past-b.jsonl"].exists()
    assert files["future.jsonl"].exists()
    # And the catalog count reflects the one remaining row.
    assert await repo.count_pending_deletes() == 1


async def test_sweep_deletes_tolerates_missing_files(
    tmp_path: Path, session_factory
):
    """A row whose file is already gone is still cleared from the queue."""
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    repo = CatalogRepo(session_factory)

    parent = await repo.add_file(
        source="app",
        segment_path=str(storage_root / "hot" / "ghost.jsonl"),
        tier="hot",
        size_bytes=10,
        oldest_record_ts=datetime(2026, 1, 1),
        newest_record_ts=datetime(2026, 1, 1),
    )
    pending_dir = tier_dir(storage_root, "pending")

    # Insert a PendingDelete row whose path never existed on disk.
    ghost_path = pending_dir / "ghost-never-existed.jsonl"
    assert not ghost_path.exists()
    now = datetime(2026, 5, 1, 12, 0, 0)
    row = await repo.add_pending_delete(
        file_id=parent.id,
        path=str(ghost_path),
        delete_after=now - timedelta(hours=1),
    )

    swept = await sweep_deletes(repo, now)

    assert swept == [row.id]
    assert await repo.count_pending_deletes() == 0
