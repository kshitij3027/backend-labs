"""Unit tests for ``src/lifecycle/applier.py`` (C10).

The applier turns pending ``Transition`` rows into real filesystem
moves (with compression where the destination tier demands it) and
keeps the catalog + transition status in lockstep. These tests drive it
against a real (in-memory) catalog plus ``tmp_path`` for the filesystem
side, asserting on the side-effects each call produces:

  * The expected file appears at the destination tier path (with the
    right extension and decompressible content for compressed tiers).
  * The source segment is queued for delete in ``tiers/pending/`` with
    a fresh ``PendingDelete`` row carrying the right ``delete_after``.
  * The ``files`` row's ``tier`` / ``segment_path`` / ``size_bytes``
    move to the new location.
  * The ``transitions`` row flips to ``status='applied'`` (or ``failed``)
    with ``executed_at`` populated.
  * The :class:`ApplyReport` counters match the SQL state.

The integration test in ``tests/integration/test_apply_pipeline.py``
combines this with the scanner to prove the scanner→applier handoff
works end-to-end.

Uses the per-test ``engine`` + ``session_factory`` fixtures from
``tests/conftest.py``. ``asyncio_mode = auto`` in ``pytest.ini`` means
no explicit ``@pytest.mark.asyncio`` is needed on each function.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import sqlalchemy as sa
import zstandard as zstd

from src.lifecycle.applier import ApplyReport, apply_once
from src.persistence.models import PendingDelete, Transition
from src.storage.catalog import CatalogRepo
from src.storage.tiers import ensure_tier_dirs, tier_dir


NOW = datetime(2026, 5, 23, 12, 0, 0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _get_transition(session_factory, transition_id: int) -> Transition:
    """Fetch a single Transition row by id."""
    async with session_factory() as session:
        result = await session.execute(
            sa.select(Transition).where(Transition.id == transition_id)
        )
        return result.scalar_one()


async def _list_pending_deletes(session_factory) -> list[PendingDelete]:
    """Return every PendingDelete row, oldest created first."""
    async with session_factory() as session:
        result = await session.execute(
            sa.select(PendingDelete).order_by(PendingDelete.id.asc())
        )
        return list(result.scalars().all())


async def _seed_file_with_segment(
    repo: CatalogRepo,
    storage_root: Path,
    *,
    tier: str,
    filename: str,
    content: bytes,
    source: str = "app",
):
    """Insert a File row + drop a real segment file at the tier dir."""
    tier_path = tier_dir(storage_root, tier)
    segment = tier_path / filename
    segment.write_bytes(content)
    return await repo.add_file(
        source=source,
        segment_path=str(segment),
        tier=tier,
        size_bytes=len(content),
        oldest_record_ts=NOW - timedelta(days=30),
        newest_record_ts=NOW - timedelta(days=30),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_apply_empty_returns_zero(tmp_path: Path, session_factory):
    """No pending transitions → ApplyReport(0, 0), no DB / FS mutation."""
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    repo = CatalogRepo(session_factory)

    report = await apply_once(repo, storage_root, NOW)

    assert report == ApplyReport(applied=0, failed=0)
    # No pending_delete rows materialised either.
    assert await _list_pending_deletes(session_factory) == []


async def test_apply_promote_hot_to_warm(tmp_path: Path, session_factory):
    """Plain promote (no compression): file appears in warm/, source in pending/.

    Acceptance:
      * dst file exists at warm tier with identical bytes.
      * source moved to pending tier (not unlinked yet — sweeper's job).
      * File row tier='warm', segment_path under warm/.
      * Transition status='applied', executed_at populated.
      * ApplyReport(1, 0).
    """
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    repo = CatalogRepo(session_factory)

    content = b"line1\nline2\nline3\n"
    file = await _seed_file_with_segment(
        repo,
        storage_root,
        tier="hot",
        filename="segment-1.jsonl",
        content=content,
    )
    t = await repo.add_transition(
        file_id=file.id,
        from_tier="hot",
        to_tier="warm",
        action="promote",
    )

    report = await apply_once(repo, storage_root, NOW)

    # Destination exists with the right bytes; no compression applied.
    warm_dst = tier_dir(storage_root, "warm") / "segment-1.jsonl"
    assert warm_dst.exists(), "promoted file should land in warm/"
    assert warm_dst.read_bytes() == content

    # Source moved into pending dir (mark-then-sweep handoff).
    src_in_hot = tier_dir(storage_root, "hot") / "segment-1.jsonl"
    assert not src_in_hot.exists(), "source must be moved out of hot/"
    pending_dst = tier_dir(storage_root, "pending") / "segment-1.jsonl"
    assert pending_dst.exists(), "source should be queued in pending/"
    assert pending_dst.read_bytes() == content

    # Catalog reflects the new tier + path.
    refreshed = await repo.get_file(file.id)
    assert refreshed is not None
    assert refreshed.tier == "warm"
    assert refreshed.segment_path == str(warm_dst)
    assert refreshed.size_bytes == len(content)

    # Transition flipped to applied.
    refreshed_t = await _get_transition(session_factory, t.id)
    assert refreshed_t.status == "applied"
    assert refreshed_t.executed_at == NOW
    assert refreshed_t.error is None

    # PendingDelete row inserted with the right grace window.
    pds = await _list_pending_deletes(session_factory)
    assert len(pds) == 1
    pd = pds[0]
    assert pd.file_id == file.id
    assert pd.path == str(pending_dst)
    assert pd.delete_after == NOW + timedelta(hours=24)

    assert report == ApplyReport(applied=1, failed=0)


async def test_apply_compress_warm_to_cold(tmp_path: Path, session_factory):
    """Compress action: dst gets ``.zst`` extension; bytes decompress to original."""
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    repo = CatalogRepo(session_factory)

    # Use a content payload >1 KiB so zstd actually produces a non-trivial
    # frame (level-3 on a few bytes is dominated by frame overhead).
    content = (b"compressible-log-line\n" * 200)
    file = await _seed_file_with_segment(
        repo,
        storage_root,
        tier="warm",
        filename="segment-2.jsonl",
        content=content,
    )
    t = await repo.add_transition(
        file_id=file.id,
        from_tier="warm",
        to_tier="cold",
        action="compress",
    )

    report = await apply_once(repo, storage_root, NOW)

    # Destination filename gains the .zst suffix.
    cold_dst = tier_dir(storage_root, "cold") / "segment-2.jsonl.zst"
    assert cold_dst.exists(), "compressed file should land at .zst path"
    # Compressed payload decompresses to the original bytes.
    decompressor = zstd.ZstdDecompressor()
    with cold_dst.open("rb") as fh:
        decompressed = decompressor.stream_reader(fh).read()
    assert decompressed == content

    # Catalog reflects the compressed size.
    refreshed = await repo.get_file(file.id)
    assert refreshed is not None
    assert refreshed.tier == "cold"
    assert refreshed.segment_path == str(cold_dst)
    assert refreshed.size_bytes == cold_dst.stat().st_size
    # Compression actually shrank the payload.
    assert refreshed.size_bytes < len(content)

    # Transition flipped to applied.
    refreshed_t = await _get_transition(session_factory, t.id)
    assert refreshed_t.status == "applied"
    assert refreshed_t.executed_at == NOW

    # Source moved into pending dir.
    pending_dst = tier_dir(storage_root, "pending") / "segment-2.jsonl"
    assert pending_dst.exists()

    assert report == ApplyReport(applied=1, failed=0)


async def test_apply_archive_warm_to_archive(tmp_path: Path, session_factory):
    """Archive action: lands at archive/ with ``.zst`` and applied status."""
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    repo = CatalogRepo(session_factory)

    content = (b"archival-line\n" * 300)
    file = await _seed_file_with_segment(
        repo,
        storage_root,
        tier="warm",
        filename="segment-3.jsonl",
        content=content,
    )
    t = await repo.add_transition(
        file_id=file.id,
        from_tier="warm",
        to_tier="archive",
        action="archive",
    )

    report = await apply_once(repo, storage_root, NOW)

    archive_dst = tier_dir(storage_root, "archive") / "segment-3.jsonl.zst"
    assert archive_dst.exists(), "archived file should land at .zst path"

    # Round-trip decompress sanity check.
    decompressor = zstd.ZstdDecompressor()
    with archive_dst.open("rb") as fh:
        assert decompressor.stream_reader(fh).read() == content

    refreshed = await repo.get_file(file.id)
    assert refreshed is not None
    assert refreshed.tier == "archive"
    assert refreshed.segment_path == str(archive_dst)

    refreshed_t = await _get_transition(session_factory, t.id)
    assert refreshed_t.status == "applied"

    assert report == ApplyReport(applied=1, failed=0)


async def test_apply_delete_moves_to_pending(tmp_path: Path, session_factory):
    """Delete action: source moved straight to pending/, File row tier='pending'."""
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    repo = CatalogRepo(session_factory)

    content = b"to-be-deleted\n"
    file = await _seed_file_with_segment(
        repo,
        storage_root,
        tier="archive",
        filename="segment-4.jsonl.zst",
        content=content,
    )
    t = await repo.add_transition(
        file_id=file.id,
        from_tier="archive",
        to_tier="pending",
        action="delete",
    )

    report = await apply_once(repo, storage_root, NOW)

    # Source moved out of archive into pending.
    src = tier_dir(storage_root, "archive") / "segment-4.jsonl.zst"
    assert not src.exists()
    pending_dst = tier_dir(storage_root, "pending") / "segment-4.jsonl.zst"
    assert pending_dst.exists()
    assert pending_dst.read_bytes() == content

    refreshed = await repo.get_file(file.id)
    assert refreshed is not None
    assert refreshed.tier == "pending"
    assert refreshed.segment_path == str(pending_dst)

    pds = await _list_pending_deletes(session_factory)
    assert len(pds) == 1
    assert pds[0].file_id == file.id
    assert pds[0].path == str(pending_dst)
    assert pds[0].delete_after == NOW + timedelta(hours=24)

    refreshed_t = await _get_transition(session_factory, t.id)
    assert refreshed_t.status == "applied"
    assert refreshed_t.executed_at == NOW

    assert report == ApplyReport(applied=1, failed=0)


async def test_apply_tier_mismatch_marks_failed(tmp_path: Path, session_factory):
    """File at warm but Transition planned from hot → row failed, no FS work."""
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    repo = CatalogRepo(session_factory)

    # File is at warm.
    file = await _seed_file_with_segment(
        repo,
        storage_root,
        tier="warm",
        filename="segment-5.jsonl",
        content=b"stale",
    )
    # Transition says from_tier=hot — stale plan.
    t = await repo.add_transition(
        file_id=file.id,
        from_tier="hot",
        to_tier="warm",
        action="promote",
    )

    report = await apply_once(repo, storage_root, NOW)

    refreshed_t = await _get_transition(session_factory, t.id)
    assert refreshed_t.status == "failed"
    assert refreshed_t.error is not None
    assert "tier_mismatch" in refreshed_t.error
    assert refreshed_t.executed_at == NOW

    # File untouched — still at warm, still where it was.
    refreshed_f = await repo.get_file(file.id)
    assert refreshed_f is not None
    assert refreshed_f.tier == "warm"

    # No FS work happened: pending dir is empty, no pending_delete rows.
    pending_dir = tier_dir(storage_root, "pending")
    assert list(pending_dir.iterdir()) == []
    assert await _list_pending_deletes(session_factory) == []

    assert report == ApplyReport(applied=0, failed=1)


async def test_apply_continues_on_individual_failure(
    tmp_path: Path, session_factory
):
    """Three transitions; middle one's segment is missing on disk → 2 applied, 1 failed.

    Plants three real File rows (FK-valid) plus three pending Transitions.
    The middle File's ``segment_path`` points at a filename that was
    never written to disk, so when the applier tries to copy it the
    underlying ``shutil.copy2`` raises ``FileNotFoundError``. That
    exception is caught by the applier's per-row try/except, the
    Transition is marked failed with the raised message captured in
    ``error``, and the loop continues with the third transition.

    This exercises the *actual* exception path (Option A in the spec),
    not the dedicated file_not_found guard at the top of the loop.
    """
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    repo = CatalogRepo(session_factory)

    # First & third are valid hot→warm promotes with real files on disk.
    a = await _seed_file_with_segment(
        repo,
        storage_root,
        tier="hot",
        filename="seg-a.jsonl",
        content=b"a",
        source="src-a",
    )
    c = await _seed_file_with_segment(
        repo,
        storage_root,
        tier="hot",
        filename="seg-c.jsonl",
        content=b"c",
        source="src-c",
    )
    # The middle File row is FK-valid (so add_transition succeeds) but
    # its segment_path points at a file that never exists on disk —
    # the applier will hit FileNotFoundError when stage_and_promote
    # calls shutil.copy2 on it.
    missing_path = tier_dir(storage_root, "hot") / "seg-b-never-written.jsonl"
    assert not missing_path.exists(), "fixture: missing file must really be absent"
    b = await repo.add_file(
        source="src-b",
        segment_path=str(missing_path),
        tier="hot",
        size_bytes=10,
        oldest_record_ts=NOW - timedelta(days=30),
        newest_record_ts=NOW - timedelta(days=30),
    )

    t_a = await repo.add_transition(
        file_id=a.id, from_tier="hot", to_tier="warm", action="promote"
    )
    t_b = await repo.add_transition(
        file_id=b.id, from_tier="hot", to_tier="warm", action="promote"
    )
    t_c = await repo.add_transition(
        file_id=c.id, from_tier="hot", to_tier="warm", action="promote"
    )

    report = await apply_once(repo, storage_root, NOW)

    # First & third applied; middle failed.
    assert (await _get_transition(session_factory, t_a.id)).status == "applied"
    assert (await _get_transition(session_factory, t_c.id)).status == "applied"
    refreshed_t_b = await _get_transition(session_factory, t_b.id)
    assert refreshed_t_b.status == "failed"
    # The error must be a non-empty string captured from the raised
    # exception (typically "[Errno 2] No such file or directory: ...").
    # We don't pin the exact text — the contract is "non-None and
    # informative" so the dashboard can show *something* to the operator.
    assert refreshed_t_b.error is not None
    assert refreshed_t_b.error != ""
    assert refreshed_t_b.executed_at == NOW

    # Two valid files landed at warm tier.
    assert (tier_dir(storage_root, "warm") / "seg-a.jsonl").exists()
    assert (tier_dir(storage_root, "warm") / "seg-c.jsonl").exists()
    # The bogus middle one did NOT land anywhere.
    assert not (tier_dir(storage_root, "warm") / missing_path.name).exists()

    assert report == ApplyReport(applied=2, failed=1)
