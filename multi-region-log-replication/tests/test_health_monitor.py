"""Unit tests for ``src.health_monitor.HealthMonitor``.

The monitor itself is dead simple — it composes :class:`Region` state
with a :class:`ReplicationStatsTracker` snapshot to produce a
:class:`HealthSnapshot`. We test the snapshot construction directly
(no need to spin up the asyncio task — that's exercised by the WS
tests) for fast, deterministic coverage.
"""

from __future__ import annotations

from typing import Dict

from src.health_monitor import HealthMonitor
from src.models import HealthSnapshot
from src.region import Region
from src.replication_controller import ReplicationController
from src.replication_stats import ReplicationStatsTracker


REGION_IDS = ["us-east", "europe", "asia"]
PRIMARY_PREFERENCE = ["us-east", "europe", "asia"]


def _make_monitor() -> tuple[
    HealthMonitor, Dict[str, Region], ReplicationController, ReplicationStatsTracker
]:
    """Build a fresh monitor wired to three healthy regions."""
    regions: Dict[str, Region] = {rid: Region(rid) for rid in REGION_IDS}
    stats = ReplicationStatsTracker(regions=REGION_IDS)
    controller = ReplicationController(
        regions=regions,
        primary_preference=PRIMARY_PREFERENCE,
        stats=stats,
    )
    monitor = HealthMonitor(
        regions=regions,
        controller=controller,
        stats=stats,
        check_interval_sec=1.0,
    )
    return monitor, regions, controller, stats


# ---------------------------------------------------------------------
# Snapshot shape
# ---------------------------------------------------------------------


def test_compute_snapshot_includes_three_regions() -> None:
    """A 3-region cluster produces a 3-entry RegionStatus list."""
    monitor, _, _, _ = _make_monitor()

    snap = monitor.compute_snapshot()
    assert isinstance(snap, HealthSnapshot)
    assert len(snap.regions) == 3
    # Region IDs should be exactly the three we configured.
    assert {r.region_id for r in snap.regions} == set(REGION_IDS)


def test_snapshot_marks_correct_primary() -> None:
    """Only the elected primary's RegionStatus has ``is_primary=True``."""
    monitor, _, _, _ = _make_monitor()

    snap = monitor.compute_snapshot()
    primaries = [r for r in snap.regions if r.is_primary]
    assert len(primaries) == 1
    assert primaries[0].region_id == "us-east"
    assert snap.current_primary == "us-east"


# ---------------------------------------------------------------------
# Overall status
# ---------------------------------------------------------------------


def test_snapshot_overall_healthy_when_all_healthy_and_primary_set() -> None:
    """Default 3-region cluster ⇒ ``overall_status == 'healthy'``."""
    monitor, _, _, _ = _make_monitor()

    snap = monitor.compute_snapshot()
    assert snap.overall_status == "healthy"


def test_snapshot_overall_degraded_when_any_region_unhealthy() -> None:
    """Marking a secondary offline flips ``overall_status`` to ``'degraded'``."""
    monitor, regions, _, _ = _make_monitor()
    regions["europe"].mark_offline()

    snap = monitor.compute_snapshot()
    assert snap.overall_status == "degraded"
    # The degraded region should be flagged in its RegionStatus too.
    europe_status = next(r for r in snap.regions if r.region_id == "europe")
    assert europe_status.is_healthy is False
