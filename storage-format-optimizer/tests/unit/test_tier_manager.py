"""Unit tests for :class:`~src.tier_manager.TierManager` (C13).

The manager maps a partition's access stats onto a HOT / WARM / COLD
:class:`~src.models.Tier` using recency *and* frequency, not age alone. All
tests pin the clock to ``now == 1000.0`` and construct
:class:`~src.pattern_tracker.PartitionAccessStats` directly, setting the raw
counters the manager reads (``reads`` and ``last_access``).

Threshold config used throughout:
    * ``hot_max_age_seconds   == 3600``
    * ``cold_min_age_seconds  == 86400``
    * ``hot_min_reads_per_min == 1.0``
"""
from __future__ import annotations

from src.models import Tier
from src.pattern_tracker import PartitionAccessStats
from src.tier_manager import TierManager

NOW = 1000.0


def _manager() -> TierManager:
    """Return a TierManager with the canonical thresholds and a pinned clock."""
    return TierManager(
        hot_max_age_seconds=3600,
        cold_min_age_seconds=86400,
        hot_min_reads_per_min=1.0,
        clock=lambda: NOW,
    )


def test_hot_young_and_recently_touched() -> None:
    mgr = _manager()
    stats = PartitionAccessStats()
    stats.reads = 100
    stats.last_access = NOW  # idle == 0 -> recently touched

    # Young (age <= hot_max) and active -> HOT.
    assert mgr.tier_for(stats, age_seconds=60) is Tier.HOT


def test_cold_old_idle_and_unread() -> None:
    # For COLD the partition must also be idle >= hot_max (3600). last_access ==
    # 0 makes idle == now (the manager treats a never-accessed partition as
    # maximally idle), so pin now well above hot_max so the COLD idle floor is
    # actually cleared.
    mgr = TierManager(
        hot_max_age_seconds=3600,
        cold_min_age_seconds=86400,
        hot_min_reads_per_min=1.0,
        clock=lambda: 100000.0,
    )
    stats = PartitionAccessStats()
    stats.reads = 0
    stats.last_access = 0.0  # never accessed -> idle == now == 100000 >= hot_max

    # Old (age >= cold_min), not read, idle >= hot_max -> COLD.
    assert mgr.tier_for(stats, age_seconds=200000) is Tier.COLD


def test_warm_old_but_recently_touched() -> None:
    mgr = _manager()
    stats = PartitionAccessStats()
    stats.reads = 0
    stats.last_access = NOW  # idle == 0 -> recently touched

    # Old but recently touched: not HOT (too old) and not COLD (idle too low).
    assert mgr.tier_for(stats, age_seconds=200000) is Tier.WARM


def test_warm_young_but_quiet() -> None:
    # A young-but-quiet partition needs idle > hot_max to avoid the "recently
    # touched" HOT path. last_access == 0 makes idle == now, so pin now well
    # above hot_max (3600) to model a partition no one has touched in a while.
    mgr = TierManager(
        hot_max_age_seconds=3600,
        cold_min_age_seconds=86400,
        hot_min_reads_per_min=1.0,
        clock=lambda: 100000.0,
    )
    stats = PartitionAccessStats()
    stats.reads = 0
    stats.last_access = 0.0  # idle == 100000 > hot_max, and never read

    # Young (age 60 <= 3600) but idle > hot_max and reads_per_min < min
    # -> neither HOT (quiet) nor COLD (not old) -> WARM.
    assert mgr.tier_for(stats, age_seconds=60) is Tier.WARM


def test_tier_for_with_reason_nonempty() -> None:
    mgr = _manager()
    stats = PartitionAccessStats()
    stats.reads = 100
    stats.last_access = NOW

    tier, reason = mgr.tier_for_with_reason(stats, age_seconds=60)
    assert tier is Tier.HOT
    assert isinstance(reason, str)
    assert reason  # non-empty
