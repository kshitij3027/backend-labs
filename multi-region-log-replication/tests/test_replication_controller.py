"""Unit tests for ``src.replication_controller.ReplicationController``.

We exercise:

* :meth:`elect_primary` — preference order, skip-unhealthy, ``exclude``
  set, and the all-excluded → ``RuntimeError`` edge case.
* :meth:`write` — full fan-out lands the entry in every healthy
  secondary, an offline secondary records a failure (no sample, no
  raised exception), and successful replications add lag samples to
  the tracker.
* :meth:`secondaries` — the primary is excluded from the returned list.
"""

from __future__ import annotations

from typing import Dict, List

import pytest

from src.region import Region
from src.replication_controller import ReplicationController
from src.replication_stats import ReplicationStatsTracker


REGION_IDS: List[str] = ["us-east", "europe", "asia"]
PRIMARY_PREFERENCE: List[str] = ["us-east", "europe", "asia"]


def _make_three_regions() -> Dict[str, Region]:
    """Build the three default regions, all healthy.

    Helper kept local to this test file (rather than in ``conftest.py``)
    so the assertions about object identity remain crystal-clear at the
    callsite — each test gets its own fresh dict.
    """
    return {rid: Region(rid) for rid in REGION_IDS}


def _make_controller(
    regions: Dict[str, Region] | None = None,
    stats: ReplicationStatsTracker | None = None,
) -> tuple[ReplicationController, Dict[str, Region], ReplicationStatsTracker]:
    """Build a controller wired to fresh regions + tracker.

    Returns the trio so individual tests can poke at the underlying
    objects (e.g. ``regions["europe"].mark_offline()``).
    """
    regions = regions if regions is not None else _make_three_regions()
    stats = stats if stats is not None else ReplicationStatsTracker(REGION_IDS)
    controller = ReplicationController(
        regions=regions,
        primary_preference=PRIMARY_PREFERENCE,
        stats=stats,
    )
    return controller, regions, stats


# ---------------------------------------------------------------------
# elect_primary
# ---------------------------------------------------------------------


def test_elect_primary_returns_first_in_preference_when_all_healthy():
    """All three regions healthy ⇒ primary is the first preference entry."""
    controller, _, _ = _make_controller()
    # Eager election in __init__ already set the primary; we re-elect to
    # double-check the call returns the same id deterministically.
    chosen = controller.elect_primary()

    assert chosen == "us-east"
    assert controller.primary_id == "us-east"


def test_elect_primary_skips_unhealthy_regions():
    """Killing us-east shifts the next election to europe."""
    controller, regions, _ = _make_controller()
    regions["us-east"].mark_offline()

    chosen = controller.elect_primary()
    assert chosen == "europe"
    assert controller.primary_id == "europe"


def test_elect_primary_excludes_set():
    """``exclude={'us-east'}`` forces the algorithm past us-east to europe.

    This is the path the HealthMonitor uses to perform failover: it
    passes the old primary in ``exclude`` so the same region cannot be
    re-elected on the same call.
    """
    controller, _, _ = _make_controller()

    chosen = controller.elect_primary(exclude={"us-east"})
    assert chosen == "europe"
    assert controller.primary_id == "europe"


def test_elect_primary_raises_when_all_excluded():
    """No candidate left ⇒ RuntimeError, not a silent ``None`` primary."""
    controller, _, _ = _make_controller()
    with pytest.raises(RuntimeError):
        controller.elect_primary(exclude=set(REGION_IDS))


# ---------------------------------------------------------------------
# write — fan-out
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_write_replicates_to_all_healthy_secondaries():
    """A single ``write`` lands the same entry in every healthy region."""
    controller, regions, _ = _make_controller()

    entry = await controller.write({"message": "hello"})

    # Primary obviously has it (local_write put it there).
    assert entry.log_id in regions["us-east"].log_store
    # Both secondaries received the same entry (object equality on log_id
    # is enough — receive_replication may store ``incoming`` as-is when
    # there's no existing entry to resolve against).
    assert entry.log_id in regions["europe"].log_store
    assert entry.log_id in regions["asia"].log_store


@pytest.mark.asyncio
async def test_write_records_failure_for_offline_secondary():
    """Offline secondary records a failure but the write still completes.

    The primary write commits and the *other* healthy secondary still
    sees the entry. The offline secondary's snapshot reflects:

    * ``success_rate == 0.0`` (one failure, zero successes)
    * ``sample_count == 0`` (failures don't contribute lag samples)
    """
    controller, regions, stats = _make_controller()
    regions["europe"].mark_offline()

    entry = await controller.write({"message": "hello"})

    # The healthy regions all have the entry.
    assert entry.log_id in regions["us-east"].log_store
    assert entry.log_id in regions["asia"].log_store
    # The offline secondary did NOT receive it.
    assert entry.log_id not in regions["europe"].log_store

    # The tracker recorded a failure for europe; no lag samples.
    europe_snap = stats.snapshot()["europe"]
    assert europe_snap["success_rate"] == pytest.approx(0.0)
    assert europe_snap["sample_count"] == 0

    # The healthy secondary did get a successful sample.
    asia_snap = stats.snapshot()["asia"]
    assert asia_snap["success_rate"] == pytest.approx(1.0)
    assert asia_snap["sample_count"] == 1


@pytest.mark.asyncio
async def test_write_records_lag_samples_for_successful_secondaries():
    """One successful write ⇒ one lag sample + 100% success per secondary."""
    controller, _, stats = _make_controller()

    await controller.write({"message": "hello"})

    europe_snap = stats.snapshot()["europe"]
    asia_snap = stats.snapshot()["asia"]
    assert europe_snap["sample_count"] == 1
    assert europe_snap["success_rate"] == pytest.approx(1.0)
    assert asia_snap["sample_count"] == 1
    assert asia_snap["success_rate"] == pytest.approx(1.0)


# ---------------------------------------------------------------------
# secondaries
# ---------------------------------------------------------------------


def test_secondaries_excludes_primary():
    """The list returned by ``secondaries()`` does not contain the primary."""
    controller, regions, _ = _make_controller()
    # Sanity: us-east is the primary by preference.
    assert controller.primary_id == "us-east"

    secondaries = controller.secondaries()
    secondary_ids = {r.region_id for r in secondaries}

    assert "us-east" not in secondary_ids
    assert secondary_ids == {"europe", "asia"}
    # And the actual ``Region`` object identity matches the dict.
    assert regions["us-east"] not in secondaries
    assert regions["europe"] in secondaries
    assert regions["asia"] in secondaries
