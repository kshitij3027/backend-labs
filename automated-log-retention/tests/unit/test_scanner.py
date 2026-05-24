"""Unit tests for ``src/lifecycle/scanner.py`` (C09).

The scanner is pure orchestration over the catalog and the C05 matcher.
These tests therefore drive it against a real (in-memory) catalog plus a
hand-built :class:`PolicySet`, asserting on the side-effects the scanner
produces:

  * One pending ``Transition`` row per planned move (with the right
    ``from_tier`` / ``to_tier`` / ``action``).
  * ``next_eval_at`` bumped one day out for files that got a transition
    planned.
  * ``next_eval_at`` bumped ``no_match_recheck`` for unmatched files.
  * ``next_eval_at`` cleared (``NULL``) for files at terminal phase.
  * Batch-limit honoured (the scanner only inspects what the catalog
    page returns).

Uses the per-test ``engine`` + ``session_factory`` fixtures from
``tests/conftest.py``. ``asyncio_mode = auto`` in ``pytest.ini`` means
no explicit ``@pytest.mark.asyncio`` is needed on each function.

Note on the ``category`` / ``level`` attributes: the ORM ``File`` model
deliberately does not have columns for those (the policy matcher reads
them duck-typed via ``getattr(file, 'category', None)``, returning
``None`` if absent). Tests that need a category-tagged file either
(a) build a policy whose selector matches on ``source`` (which *is* a
column), or (b) wrap ``list_due_files`` to attach the attribute to the
loaded ORM instance before the scanner sees it. The full-YAML demo test
uses the wrap-and-tag pattern.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import sqlalchemy as sa

from src.lifecycle.scanner import ScanReport, scan_once
from src.persistence.models import Transition
from src.policy.loader import load_policy_set
from src.policy.schema import Phase, Policy, PolicySet, Selector
from src.storage.catalog import CatalogRepo

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_YAML = REPO_ROOT / "config" / "retention_config.yaml"


NOW = datetime(2026, 5, 23, 12, 0, 0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _count_transitions(session_factory) -> int:
    """Return total Transition rows in the DB (any status)."""
    async with session_factory() as session:
        result = await session.execute(sa.select(sa.func.count(Transition.id)))
        return int(result.scalar_one())


async def _list_transitions(session_factory) -> list[Transition]:
    """Return every Transition row, oldest planned first."""
    async with session_factory() as session:
        result = await session.execute(
            sa.select(Transition).order_by(Transition.planned_at.asc())
        )
        return list(result.scalars().all())


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_scan_empty_catalog_returns_zero(session_factory):
    """Nothing in the catalog → scanner returns the zero report."""
    repo = CatalogRepo(session_factory)
    policy_set = PolicySet(policies=[])

    report = await scan_once(repo, policy_set, NOW)

    assert report == ScanReport(
        scanned=0,
        transitions_planned=0,
        unmatched=0,
        terminal=0,
        rescheduled=0,
    )
    assert await _count_transitions(session_factory) == 0


async def test_scan_one_due_file_matching_first_phase_plans_one_transition(
    session_factory,
):
    """A 35-day-old hot file with a 0/30 promote policy → one warm transition."""
    repo = CatalogRepo(session_factory)
    # Selector on ``source`` (which IS a column on File) so the matcher
    # picks the policy without needing a category attribute.
    policy_set = PolicySet(
        policies=[
            Policy(
                name="p",
                selector=Selector(source="app"),
                phases=[
                    Phase(after_days=0, action="promote", target_tier="hot"),
                    Phase(after_days=30, action="promote", target_tier="warm"),
                ],
            )
        ]
    )
    await repo.add_file(
        source="app",
        segment_path="/tiers/hot/x.jsonl",
        tier="hot",
        size_bytes=100,
        oldest_record_ts=NOW - timedelta(days=35),
        newest_record_ts=NOW - timedelta(days=35),
        next_eval_at=NOW - timedelta(minutes=1),
    )

    report = await scan_once(repo, policy_set, NOW)

    assert report.scanned == 1
    assert report.transitions_planned == 1
    assert report.unmatched == 0
    assert report.terminal == 0

    rows = await _list_transitions(session_factory)
    assert len(rows) == 1
    t = rows[0]
    assert t.from_tier == "hot"
    assert t.to_tier == "warm"
    assert t.action == "promote"
    assert t.status == "pending"


async def test_scan_file_not_matching_any_policy_bumps_unmatched_and_reschedules(
    session_factory,
):
    """No selector matches → unmatched++, next_eval_at = now + 1 day."""
    repo = CatalogRepo(session_factory)
    policy_set = PolicySet(
        policies=[
            Policy(
                name="only",
                selector=Selector(source="nobody-matches-this"),
                phases=[Phase(after_days=0, action="delete")],
            )
        ]
    )
    f = await repo.add_file(
        source="orphan",
        segment_path="/tiers/hot/orphan.jsonl",
        tier="hot",
        size_bytes=10,
        oldest_record_ts=NOW - timedelta(days=5),
        newest_record_ts=NOW - timedelta(days=5),
        next_eval_at=NOW - timedelta(minutes=1),
    )

    report = await scan_once(repo, policy_set, NOW)

    assert report.scanned == 1
    assert report.unmatched == 1
    assert report.transitions_planned == 0
    assert report.terminal == 0
    # No transition row inserted.
    assert await _count_transitions(session_factory) == 0
    # File's next_eval_at pushed exactly 1 day into the future (the
    # default ``no_match_recheck``).
    refreshed = await repo.get_file(f.id)
    assert refreshed is not None
    assert refreshed.next_eval_at == NOW + timedelta(days=1)


async def test_scan_file_at_terminal_phase_clears_next_eval_at(session_factory):
    """File at archive, all phases applied → terminal++, next_eval_at = NULL."""
    repo = CatalogRepo(session_factory)
    policy_set = PolicySet(
        policies=[
            Policy(
                name="lifecycle",
                selector=Selector(source="done"),
                phases=[
                    Phase(after_days=0, action="promote", target_tier="hot"),
                    Phase(after_days=30, action="promote", target_tier="warm"),
                    Phase(
                        after_days=90,
                        action="archive",
                        target_tier="archive",
                        compression_level=19,
                    ),
                ],
            )
        ]
    )
    f = await repo.add_file(
        source="done",
        segment_path="/tiers/archive/done.jsonl",
        tier="archive",
        size_bytes=10,
        oldest_record_ts=NOW - timedelta(days=1000),
        newest_record_ts=NOW - timedelta(days=1000),
        next_eval_at=NOW - timedelta(minutes=1),
    )

    report = await scan_once(repo, policy_set, NOW)

    assert report.scanned == 1
    assert report.terminal == 1
    assert report.transitions_planned == 0
    assert report.unmatched == 0
    assert report.rescheduled == 0
    assert await _count_transitions(session_factory) == 0
    # File retired from future scans.
    refreshed = await repo.get_file(f.id)
    assert refreshed is not None
    assert refreshed.next_eval_at is None


async def test_scan_file_with_future_phase_reschedules_to_due_date(session_factory):
    """File at hot, age 35d, policy phases at 0/90 (promote hot, promote warm).

    The 0-day promote→hot phase is already applied (file is hot); the
    90-day promote→warm phase is not yet due. The matcher returns None
    in this case but the scanner must NOT mark the file terminal — it
    is rescheduled to the exact moment the 90-day phase comes due
    (``oldest_record_ts + 90 days``).

    Catches the production bug noted in the implementation report: a
    PCI policy (phases 0/90/365) applied to a 35-day-old hot file would
    otherwise be retired and never advance to the 90-day archive phase.
    """
    repo = CatalogRepo(session_factory)
    policy_set = PolicySet(
        policies=[
            Policy(
                name="future",
                selector=Selector(source="future"),
                phases=[
                    Phase(after_days=0, action="promote", target_tier="hot"),
                    Phase(after_days=90, action="promote", target_tier="warm"),
                ],
            )
        ]
    )
    oldest = NOW - timedelta(days=35)
    f = await repo.add_file(
        source="future",
        segment_path="/tiers/hot/future.jsonl",
        tier="hot",
        size_bytes=10,
        oldest_record_ts=oldest,
        newest_record_ts=oldest,
        next_eval_at=NOW - timedelta(minutes=1),
    )

    report = await scan_once(repo, policy_set, NOW)

    assert report.scanned == 1
    assert report.transitions_planned == 0
    assert report.unmatched == 0
    assert report.terminal == 0
    assert report.rescheduled == 1

    # No transition planned for a file that is not yet due.
    assert await _count_transitions(session_factory) == 0

    # next_eval_at should be exactly the day the 90-day phase becomes
    # due — i.e. oldest_record_ts + 90 days.
    refreshed = await repo.get_file(f.id)
    assert refreshed is not None
    assert refreshed.next_eval_at == oldest + timedelta(days=90)


async def test_scan_delete_phase_plans_pending_transition(session_factory):
    """File at archive past its delete day → Transition(to_tier=pending, action=delete)."""
    repo = CatalogRepo(session_factory)
    policy_set = PolicySet(
        policies=[
            Policy(
                name="purge",
                selector=Selector(source="ephemeral"),
                phases=[
                    Phase(after_days=0, action="promote", target_tier="hot"),
                    Phase(
                        after_days=90,
                        action="archive",
                        target_tier="archive",
                        compression_level=19,
                    ),
                    Phase(after_days=365, action="delete"),
                ],
            )
        ]
    )
    await repo.add_file(
        source="ephemeral",
        segment_path="/tiers/archive/eph.jsonl",
        tier="archive",
        size_bytes=10,
        oldest_record_ts=NOW - timedelta(days=400),
        newest_record_ts=NOW - timedelta(days=400),
        next_eval_at=NOW - timedelta(minutes=1),
    )

    report = await scan_once(repo, policy_set, NOW)

    assert report.transitions_planned == 1
    rows = await _list_transitions(session_factory)
    assert len(rows) == 1
    t = rows[0]
    assert t.from_tier == "archive"
    # Per spec: delete action lands the file in pending (mark-then-sweep
    # handoff to the sweeper); the action verb stays ``delete``.
    assert t.to_tier == "pending"
    assert t.action == "delete"
    assert t.status == "pending"


async def test_scan_respects_batch_limit(session_factory):
    """5 due files, batch_limit=2 → only 2 inspected this pass."""
    repo = CatalogRepo(session_factory)
    policy_set = PolicySet(
        policies=[
            Policy(
                name="p",
                selector=Selector(),  # wildcard
                phases=[
                    Phase(after_days=0, action="promote", target_tier="hot"),
                    Phase(after_days=30, action="promote", target_tier="warm"),
                ],
            )
        ]
    )
    for i in range(5):
        await repo.add_file(
            source=f"s{i}",
            segment_path=f"/tiers/hot/s{i}.jsonl",
            tier="hot",
            size_bytes=10,
            oldest_record_ts=NOW - timedelta(days=35),
            newest_record_ts=NOW - timedelta(days=35),
            # Stagger so ordering is deterministic.
            next_eval_at=NOW - timedelta(seconds=i + 1),
        )

    report = await scan_once(repo, policy_set, NOW, batch_limit=2)

    assert report.scanned == 2
    # Both inspected files match a policy with a due phase, so each
    # produces a transition.
    assert report.transitions_planned == 2
    assert await _count_transitions(session_factory) == 2


async def test_scan_currently_plans_even_with_existing_pending_transition(
    session_factory,
):
    """Document current behaviour: scanner re-plans regardless of prior pending row.

    The spec leaves this open and we picked the simpler design (no
    cross-table dedupe in the scanner; the applier is the dedupe point
    because it inspects the file's current tier before acting). This
    test pins that contract so a future change (e.g., scanner-side
    dedupe) lands as a deliberate, visible diff.
    """
    repo = CatalogRepo(session_factory)
    policy_set = PolicySet(
        policies=[
            Policy(
                name="p",
                selector=Selector(source="dup"),
                phases=[
                    Phase(after_days=0, action="promote", target_tier="hot"),
                    Phase(after_days=30, action="promote", target_tier="warm"),
                ],
            )
        ]
    )
    f = await repo.add_file(
        source="dup",
        segment_path="/tiers/hot/dup.jsonl",
        tier="hot",
        size_bytes=10,
        oldest_record_ts=NOW - timedelta(days=35),
        newest_record_ts=NOW - timedelta(days=35),
        next_eval_at=NOW - timedelta(minutes=1),
    )
    # Plant a pre-existing pending transition for the same file.
    await repo.add_transition(
        file_id=f.id,
        from_tier="hot",
        to_tier="warm",
        action="promote",
    )

    report = await scan_once(repo, policy_set, NOW)

    assert report.transitions_planned == 1
    # Two rows total: the planted one + the freshly scanned one.
    assert await _count_transitions(session_factory) == 2


async def test_scan_full_yaml_demo(session_factory):
    """Load the demo YAML and walk 5 framework-tagged files through the scanner.

    The demo policies (see ``config/retention_config.yaml``) select on
    ``category`` — which is not an ORM column — so we wrap
    :meth:`CatalogRepo.list_due_files` to tag each returned ORM ``File``
    with its category in memory before the matcher sees it. The matcher
    reads ``category`` via :func:`getattr` so the in-memory attribute
    flows through transparently.
    """
    repo = CatalogRepo(session_factory)
    policy_set = load_policy_set(CONFIG_YAML)

    # Five files, one per framework category, each old enough to hit the
    # 30-day promote→warm phase from hot (the first non-no-op step for
    # most policies). The one exception:
    #   * payment_card (PCI) → policy has no 30-day phase (phases at
    #                         0/90/365). At 35 d the 0-day promote→hot
    #                         is already-applied (file is hot) and the
    #                         90-day phase is not yet due. The scanner's
    #                         ``_next_future_phase_due_at`` helper kicks
    #                         in: the file is *rescheduled* to the exact
    #                         moment the 90-day phase becomes due
    #                         (``oldest_record_ts + 90 days``), counted
    #                         under ``rescheduled`` (not ``terminal``).
    # Each row gets the right ``category`` tagged after load so the
    # selector matches.
    plan = [
        # (source, category, age_days, expected_to_tier_or_None)
        ("svc-ua", "user_activity", 35, "warm"),      # GDPR
        ("svc-pay", "payment", 35, "warm"),           # SOX
        ("svc-hr", "health", 35, "warm"),             # HIPAA
        ("svc-card", "payment_card", 35, None),       # PCI — no 30-day phase
        ("svc-ops", "ops_audit", 35, "warm"),         # SOC 2
    ]
    file_ids: dict[str, int] = {}
    for source, _category, age_days, _expected in plan:
        f = await repo.add_file(
            source=source,
            segment_path=f"/tiers/hot/{source}.jsonl",
            tier="hot",
            size_bytes=100,
            oldest_record_ts=NOW - timedelta(days=age_days),
            newest_record_ts=NOW - timedelta(days=age_days),
            next_eval_at=NOW - timedelta(minutes=1),
        )
        file_ids[source] = f.id

    # Wrap list_due_files to tag the loaded ORM instances with their
    # category (and a level, just in case the debug policy ever
    # matters here). The matcher reads these attributes duck-typed.
    original_list_due_files = repo.list_due_files
    source_to_category = {source: cat for source, cat, _, _ in plan}

    async def list_due_files_tagged(now, limit=1000):
        files = await original_list_due_files(now, limit=limit)
        for f in files:
            f.category = source_to_category.get(f.source)
            f.level = "INFO"
        return files

    repo.list_due_files = list_due_files_tagged  # type: ignore[method-assign]

    report = await scan_once(repo, policy_set, NOW)

    assert report.scanned == 5
    # 4 of 5 files get a transition (PCI has no 30-day phase).
    assert report.transitions_planned == 4
    # No files orphaned — every category in ``plan`` has a matching
    # policy in the demo YAML.
    assert report.unmatched == 0
    # No files truly terminal — PCI is rescheduled, not terminal.
    assert report.terminal == 0
    # PCI matched the card_data_pci policy; its 90-day phase is the
    # earliest unfired phase still ahead → rescheduled (not terminal).
    assert report.rescheduled == 1

    # The PCI file's next_eval_at should be precisely 90 days after its
    # oldest_record_ts (i.e. when the archive phase becomes due).
    pci_file = await repo.get_file(file_ids["svc-card"])
    assert pci_file is not None
    assert pci_file.next_eval_at == (NOW - timedelta(days=35)) + timedelta(days=90)

    # Verify each planned transition is to the expected tier per source.
    rows = await _list_transitions(session_factory)
    by_file_id = {r.file_id: r for r in rows}
    for source, _category, _age, expected_to_tier in plan:
        fid = file_ids[source]
        if expected_to_tier is None:
            # PCI at 35 d should NOT have planned anything.
            assert fid not in by_file_id, (
                f"unexpected transition for {source}: {by_file_id.get(fid)}"
            )
        else:
            assert fid in by_file_id, f"missing transition for {source}"
            t = by_file_id[fid]
            assert t.from_tier == "hot"
            assert t.to_tier == expected_to_tier
            assert t.action == "promote"
            assert t.status == "pending"
