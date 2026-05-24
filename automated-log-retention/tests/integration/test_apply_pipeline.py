"""Integration test for the scanner → applier handoff (C10).

This is the first commit that proves the **scanner→applier pipeline**
actually moves a file end-to-end:

  1. A 35-day-old segment is registered at the ``hot`` tier with a real
     file on disk.
  2. A tiny YAML-equivalent ``PolicySet`` matches it (selector on
     ``source=app``) and prescribes a promote→hot@0 / promote→warm@30 /
     compress→cold@90 phase progression.
  3. ``scan_once(now)`` plans the next-due phase as a pending
     ``Transition`` row (hot → warm, action=promote).
  4. ``apply_once(now)`` executes the transition: the file is copied
     into ``tiers/warm/``, the source is queued in ``tiers/pending/``
     with a 24-hour delete window, the ``files`` row's tier flips to
     ``warm``, and the ``transitions`` row flips to ``status='applied'``.

The assertions cover all four side-effects in one pass so a regression
in either the scanner OR the applier surfaces immediately as an
integration failure (the unit tests still flag exactly which side
broke, but this test is the proof the two halves wire up correctly).

Uses the per-test ``engine`` + ``session_factory`` fixtures from
``tests/conftest.py``. ``asyncio_mode = auto`` in ``pytest.ini`` means
no explicit ``@pytest.mark.asyncio`` is needed on each function.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import sqlalchemy as sa

from src.lifecycle.applier import ApplyReport, apply_once
from src.lifecycle.scanner import scan_once
from src.persistence.models import PendingDelete, Transition
from src.policy.schema import Phase, Policy, PolicySet, Selector
from src.storage.catalog import CatalogRepo
from src.storage.tiers import ensure_tier_dirs, tier_dir


NOW = datetime(2026, 5, 23, 12, 0, 0)


async def test_scan_then_apply_moves_hot_segment_to_warm(
    tmp_path: Path, session_factory
):
    """Full pipeline: scan plans 1 transition, apply executes it cleanly."""
    storage_root = tmp_path / "tiers"
    ensure_tier_dirs(storage_root)
    repo = CatalogRepo(session_factory)

    # --- Arrange ----------------------------------------------------------
    # A real segment file at the hot tier with deterministic bytes; 35d
    # old so the 30-day promote phase is due but the 90-day compress
    # phase is not.
    content = b"sample\n" * 50
    hot_segment = tier_dir(storage_root, "hot") / "segment-100.jsonl"
    hot_segment.write_bytes(content)
    file = await repo.add_file(
        source="app",
        segment_path=str(hot_segment),
        tier="hot",
        size_bytes=len(content),
        oldest_record_ts=NOW - timedelta(days=35),
        newest_record_ts=NOW - timedelta(days=35),
        # next_eval_at slightly in the past so list_due_files picks it up.
        next_eval_at=NOW - timedelta(minutes=1),
    )

    # PolicySet with three phases: only the warm-promote one fires today.
    policy_set = PolicySet(
        policies=[
            Policy(
                name="app-policy",
                selector=Selector(source="app"),
                phases=[
                    Phase(after_days=0, action="promote", target_tier="hot"),
                    Phase(after_days=30, action="promote", target_tier="warm"),
                    Phase(
                        after_days=90,
                        action="compress",
                        target_tier="cold",
                        compression_level=3,
                    ),
                ],
            )
        ]
    )

    # --- Act --------------------------------------------------------------
    scan_report = await scan_once(repo, policy_set, NOW)
    assert scan_report.scanned == 1
    assert scan_report.transitions_planned == 1

    apply_report = await apply_once(repo, storage_root, NOW)

    # --- Assert -----------------------------------------------------------
    assert apply_report == ApplyReport(applied=1, failed=0)

    # The single Transition row is now applied (with executed_at populated).
    async with session_factory() as session:
        rows = (
            await session.execute(
                sa.select(Transition).order_by(Transition.id.asc())
            )
        ).scalars().all()
    assert len(rows) == 1
    t = rows[0]
    assert t.status == "applied"
    assert t.executed_at == NOW
    assert t.from_tier == "hot"
    assert t.to_tier == "warm"
    assert t.action == "promote"
    assert t.error is None

    # The file moved from hot/ into warm/, with the source landing in pending/.
    warm_dst = tier_dir(storage_root, "warm") / "segment-100.jsonl"
    assert warm_dst.exists(), "promoted file should land in warm/"
    assert warm_dst.read_bytes() == content
    assert not hot_segment.exists(), "source must be moved out of hot/"
    pending_dst = tier_dir(storage_root, "pending") / "segment-100.jsonl"
    assert pending_dst.exists(), "source should be queued in pending/"
    assert pending_dst.read_bytes() == content

    # The catalog row points at the new tier copy.
    refreshed = await repo.get_file(file.id)
    assert refreshed is not None
    assert refreshed.tier == "warm"
    assert refreshed.segment_path == str(warm_dst)
    assert refreshed.size_bytes == len(content)

    # A PendingDelete row was inserted with a 24-hour grace window.
    async with session_factory() as session:
        pds = (
            await session.execute(sa.select(PendingDelete))
        ).scalars().all()
    assert len(pds) == 1
    assert pds[0].file_id == file.id
    assert pds[0].path == str(pending_dst)
    assert pds[0].delete_after == NOW + timedelta(hours=24)
