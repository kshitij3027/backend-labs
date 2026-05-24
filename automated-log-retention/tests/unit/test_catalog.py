"""Unit tests for ``src/storage/catalog.py``.

Uses the per-test ``engine`` + ``session_factory`` fixtures from
``tests/conftest.py``. Because ``pytest.ini`` sets
``asyncio_mode = auto``, async test functions need no explicit
``@pytest.mark.asyncio`` decorator.

The tests cover three behavioural surfaces:

  1. CRUD round-trips for ``add_file`` / ``get_file`` / ``mark_evaluated``
     / ``update_tier``.
  2. ``list_due_files`` filter / ordering / pagination semantics.
  3. The two aggregate methods returning fully-keyed dicts even on an
     empty database.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import sqlalchemy as sa

from src.persistence.models import File
from src.storage.catalog import CatalogRepo
from src.storage.tiers import TIERS


# --- add_file / get_file round-trips ----------------------------------------


async def test_add_file_round_trip(session_factory):
    """Insert one file via the repo, fetch it back, every field matches."""
    repo = CatalogRepo(session_factory)
    oldest = datetime(2026, 1, 1, 0, 0, 0)
    newest = datetime(2026, 1, 1, 12, 0, 0)
    next_eval = datetime(2026, 1, 2, 0, 0, 0)
    created = await repo.add_file(
        source="app-1",
        segment_path="/tiers/hot/segment-1.jsonl",
        tier="hot",
        size_bytes=2048,
        oldest_record_ts=oldest,
        newest_record_ts=newest,
        compliance_tag="sox",
        immutable=True,
        next_eval_at=next_eval,
    )
    assert created.id is not None
    fetched = await repo.get_file(created.id)
    assert fetched is not None
    assert fetched.id == created.id
    assert fetched.source == "app-1"
    assert fetched.segment_path == "/tiers/hot/segment-1.jsonl"
    assert fetched.tier == "hot"
    assert fetched.size_bytes == 2048
    assert fetched.oldest_record_ts == oldest
    assert fetched.newest_record_ts == newest
    assert fetched.compliance_tag == "sox"
    assert fetched.immutable is True
    assert fetched.next_eval_at == next_eval


async def test_get_file_missing_returns_none(session_factory):
    """Fetching an unknown id returns ``None`` instead of raising."""
    repo = CatalogRepo(session_factory)
    assert await repo.get_file(99999) is None


# --- list_due_files ---------------------------------------------------------


async def test_list_due_files_filters_by_time(session_factory):
    """Only files with ``next_eval_at <= now`` are returned."""
    repo = CatalogRepo(session_factory)
    now = datetime(2026, 5, 1, 12, 0, 0)
    past = now - timedelta(hours=1)
    future = now + timedelta(hours=1)
    # Past — should be returned.
    await repo.add_file(
        source="s1",
        segment_path="/tiers/hot/p.jsonl",
        tier="hot",
        size_bytes=10,
        oldest_record_ts=past,
        newest_record_ts=past,
        next_eval_at=past,
    )
    # Future — should NOT be returned.
    await repo.add_file(
        source="s2",
        segment_path="/tiers/hot/f.jsonl",
        tier="hot",
        size_bytes=10,
        oldest_record_ts=past,
        newest_record_ts=past,
        next_eval_at=future,
    )
    # None — should NOT be returned (terminal phase).
    await repo.add_file(
        source="s3",
        segment_path="/tiers/hot/n.jsonl",
        tier="hot",
        size_bytes=10,
        oldest_record_ts=past,
        newest_record_ts=past,
        next_eval_at=None,
    )

    due = await repo.list_due_files(now)
    assert len(due) == 1
    assert due[0].source == "s1"


async def test_list_due_files_excludes_pending_tier(session_factory):
    """Files in the ``pending`` tier are excluded even if due."""
    repo = CatalogRepo(session_factory)
    now = datetime(2026, 5, 1, 12, 0, 0)
    past = now - timedelta(hours=1)
    await repo.add_file(
        source="s-hot",
        segment_path="/tiers/hot/h.jsonl",
        tier="hot",
        size_bytes=10,
        oldest_record_ts=past,
        newest_record_ts=past,
        next_eval_at=past,
    )
    await repo.add_file(
        source="s-pending",
        segment_path="/tiers/pending/p.jsonl",
        tier="pending",
        size_bytes=10,
        oldest_record_ts=past,
        newest_record_ts=past,
        next_eval_at=past,
    )
    due = await repo.list_due_files(now)
    assert len(due) == 1
    assert due[0].tier == "hot"
    assert due[0].source == "s-hot"


async def test_list_due_files_orders_by_next_eval_at_asc(session_factory):
    """Returned files are sorted oldest-due first."""
    repo = CatalogRepo(session_factory)
    now = datetime(2026, 5, 1, 12, 0, 0)
    t_old = now - timedelta(hours=3)
    t_mid = now - timedelta(hours=2)
    t_new = now - timedelta(hours=1)

    # Insert deliberately out of order.
    await repo.add_file(
        source="mid",
        segment_path="/tiers/hot/mid.jsonl",
        tier="hot",
        size_bytes=10,
        oldest_record_ts=t_mid,
        newest_record_ts=t_mid,
        next_eval_at=t_mid,
    )
    await repo.add_file(
        source="new",
        segment_path="/tiers/hot/new.jsonl",
        tier="hot",
        size_bytes=10,
        oldest_record_ts=t_new,
        newest_record_ts=t_new,
        next_eval_at=t_new,
    )
    await repo.add_file(
        source="old",
        segment_path="/tiers/hot/old.jsonl",
        tier="hot",
        size_bytes=10,
        oldest_record_ts=t_old,
        newest_record_ts=t_old,
        next_eval_at=t_old,
    )

    due = await repo.list_due_files(now)
    assert [f.source for f in due] == ["old", "mid", "new"]


async def test_list_due_files_respects_limit(session_factory):
    """``limit`` caps the number of rows returned."""
    repo = CatalogRepo(session_factory)
    now = datetime(2026, 5, 1, 12, 0, 0)
    past = now - timedelta(hours=1)
    for i in range(5):
        await repo.add_file(
            source=f"s{i}",
            segment_path=f"/tiers/hot/s{i}.jsonl",
            tier="hot",
            size_bytes=10,
            oldest_record_ts=past,
            newest_record_ts=past,
            next_eval_at=past - timedelta(seconds=i),
        )
    due = await repo.list_due_files(now, limit=2)
    assert len(due) == 2


# --- mark_evaluated ---------------------------------------------------------


async def test_mark_evaluated_sets_next_eval_at(session_factory):
    """``mark_evaluated`` round-trips a fresh ``next_eval_at`` value."""
    repo = CatalogRepo(session_factory)
    f = await repo.add_file(
        source="s",
        segment_path="/tiers/hot/x.jsonl",
        tier="hot",
        size_bytes=10,
        oldest_record_ts=datetime(2026, 1, 1),
        newest_record_ts=datetime(2026, 1, 1),
        next_eval_at=datetime(2026, 1, 2),
    )
    new_ts = datetime(2026, 3, 5, 4, 5, 6)
    await repo.mark_evaluated(f.id, new_ts)
    fetched = await repo.get_file(f.id)
    assert fetched is not None
    assert fetched.next_eval_at == new_ts


async def test_mark_evaluated_can_clear_to_none(session_factory):
    """Passing ``None`` retires a file from future scans."""
    repo = CatalogRepo(session_factory)
    f = await repo.add_file(
        source="s",
        segment_path="/tiers/hot/x.jsonl",
        tier="hot",
        size_bytes=10,
        oldest_record_ts=datetime(2026, 1, 1),
        newest_record_ts=datetime(2026, 1, 1),
        next_eval_at=datetime(2026, 1, 2),
    )
    await repo.mark_evaluated(f.id, None)
    fetched = await repo.get_file(f.id)
    assert fetched is not None
    assert fetched.next_eval_at is None


# --- update_tier ------------------------------------------------------------


async def test_update_tier_changes_all_three_fields(session_factory):
    """``update_tier`` mutates ``tier``, ``segment_path``, and
    ``size_bytes`` atomically."""
    repo = CatalogRepo(session_factory)
    f = await repo.add_file(
        source="s",
        segment_path="/tiers/hot/x.jsonl",
        tier="hot",
        size_bytes=100,
        oldest_record_ts=datetime(2026, 1, 1),
        newest_record_ts=datetime(2026, 1, 1),
    )
    await repo.update_tier(
        f.id,
        new_tier="warm",
        new_path="/tiers/warm/x.jsonl",
        new_size=200,
    )
    fetched = await repo.get_file(f.id)
    assert fetched is not None
    assert fetched.tier == "warm"
    assert fetched.segment_path == "/tiers/warm/x.jsonl"
    assert fetched.size_bytes == 200


# --- count_by_tier ----------------------------------------------------------


async def test_count_by_tier_zero_state(session_factory):
    """Empty DB returns a dict with every tier key, all zero."""
    repo = CatalogRepo(session_factory)
    counts = await repo.count_by_tier()
    assert set(counts.keys()) == set(TIERS)
    for tier in TIERS:
        assert counts[tier] == 0


async def test_count_by_tier_populated(session_factory):
    """Counts reflect inserted rows; tiers with no files stay at 0."""
    repo = CatalogRepo(session_factory)
    base = datetime(2026, 1, 1)
    # 2 hot
    for i in range(2):
        await repo.add_file(
            source=f"h{i}",
            segment_path=f"/tiers/hot/h{i}.jsonl",
            tier="hot",
            size_bytes=10,
            oldest_record_ts=base,
            newest_record_ts=base,
        )
    # 1 warm
    await repo.add_file(
        source="w0",
        segment_path="/tiers/warm/w0.jsonl",
        tier="warm",
        size_bytes=20,
        oldest_record_ts=base,
        newest_record_ts=base,
    )
    # 3 cold
    for i in range(3):
        await repo.add_file(
            source=f"c{i}",
            segment_path=f"/tiers/cold/c{i}.jsonl",
            tier="cold",
            size_bytes=30,
            oldest_record_ts=base,
            newest_record_ts=base,
        )

    counts = await repo.count_by_tier()
    assert counts["hot"] == 2
    assert counts["warm"] == 1
    assert counts["cold"] == 3
    assert counts["archive"] == 0
    assert counts["pending"] == 0


# --- total_bytes_by_tier ----------------------------------------------------


async def test_total_bytes_by_tier_zero_state(session_factory):
    """Empty DB returns a dict with every tier key, all zero bytes."""
    repo = CatalogRepo(session_factory)
    totals = await repo.total_bytes_by_tier()
    assert set(totals.keys()) == set(TIERS)
    for tier in TIERS:
        assert totals[tier] == 0


async def test_total_bytes_by_tier_populated(session_factory):
    """Sums reflect inserted sizes per tier; empty tiers stay at 0."""
    repo = CatalogRepo(session_factory)
    base = datetime(2026, 1, 1)
    # hot: 100 + 250 = 350
    await repo.add_file(
        source="h1",
        segment_path="/tiers/hot/h1.jsonl",
        tier="hot",
        size_bytes=100,
        oldest_record_ts=base,
        newest_record_ts=base,
    )
    await repo.add_file(
        source="h2",
        segment_path="/tiers/hot/h2.jsonl",
        tier="hot",
        size_bytes=250,
        oldest_record_ts=base,
        newest_record_ts=base,
    )
    # cold: 1000
    await repo.add_file(
        source="c1",
        segment_path="/tiers/cold/c1.jsonl",
        tier="cold",
        size_bytes=1000,
        oldest_record_ts=base,
        newest_record_ts=base,
    )
    # archive: 5
    await repo.add_file(
        source="a1",
        segment_path="/tiers/archive/a1.jsonl",
        tier="archive",
        size_bytes=5,
        oldest_record_ts=base,
        newest_record_ts=base,
    )

    totals = await repo.total_bytes_by_tier()
    assert totals["hot"] == 350
    assert totals["warm"] == 0
    assert totals["cold"] == 1000
    assert totals["archive"] == 5
    assert totals["pending"] == 0
