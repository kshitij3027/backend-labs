"""Unit tests for ``src.region.Region``.

Coverage:
* ``local_write`` increments ``logical_ts`` and the region's vector-clock
  slot atomically; consecutive writes advance correctly; log_ids are
  unique; concurrent writes from ``asyncio.gather`` are serialized by
  the lock (no double-increments, no skipped slots).
* ``receive_replication`` stores new entries, merges-then-increments
  vector clocks per the spec, defers to the conflict resolver when an
  entry with the same ``log_id`` already exists, and returns the chosen
  entry to the caller.
* ``get_logs`` honors the limit and ordering contract.
* ``mark_offline`` / ``mark_online`` flip ``is_healthy``.
* ``stats()`` returns the documented shape.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from src.models import LogEntry
from src.region import Region


# ---------------------------------------------------------------------
# local_write
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_local_write_increments_logical_ts():
    """First write makes logical_ts=1 and vc[region]=1."""
    region = Region("us-east")
    entry = await region.local_write({"message": "hello"})

    assert region.logical_ts == 1
    assert region.vector_clock["us-east"] == 1
    assert entry.logical_ts == 1
    assert entry.vector_clock["us-east"] == 1
    assert entry.region == "us-east"
    assert entry.data == {"message": "hello"}
    assert entry.log_id in region.log_store
    # The stored entry is the same object we returned.
    assert region.log_store[entry.log_id] is entry


@pytest.mark.asyncio
async def test_local_write_two_writes_advance_to_2():
    """Sequential writes advance logical_ts: 1, then 2."""
    region = Region("us-east")
    e1 = await region.local_write({"i": 1})
    e2 = await region.local_write({"i": 2})

    assert e1.logical_ts == 1
    assert e2.logical_ts == 2
    assert region.logical_ts == 2
    assert region.vector_clock["us-east"] == 2
    assert len(region.log_store) == 2


@pytest.mark.asyncio
async def test_local_write_log_id_unique_across_writes():
    """Two writes get distinct log_ids (UUID factory does its job)."""
    region = Region("us-east")
    e1 = await region.local_write({"i": 1})
    e2 = await region.local_write({"i": 2})

    assert e1.log_id != e2.log_id


@pytest.mark.asyncio
async def test_concurrent_local_writes_serialized_by_lock():
    """asyncio.gather of 10 local_writes ⇒ logical_ts=10 and 10 unique log_ids.

    Without the lock the ``logical_ts += 1`` / ``vector_clock[id] = ts``
    pair could interleave: two coroutines could both read ts=N and both
    write ts=N+1, doubling up a slot or skipping one. The lock
    serializes the critical section so we end up with a clean run of
    1..10.
    """
    region = Region("us-east")
    entries = await asyncio.gather(
        *[region.local_write({"i": i}) for i in range(10)]
    )

    assert region.logical_ts == 10
    assert region.vector_clock["us-east"] == 10
    assert len(region.log_store) == 10
    assert len({e.log_id for e in entries}) == 10
    # Every value 1..10 appears exactly once across the returned entries.
    assert sorted(e.logical_ts for e in entries) == list(range(1, 11))


# ---------------------------------------------------------------------
# receive_replication
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_receive_replication_new_entry_stored():
    """An empty region accepts a brand-new entry into log_store."""
    region = Region("europe")
    incoming = LogEntry(
        log_id="L1",
        data={"message": "from us-east"},
        region="us-east",
        vector_clock={"us-east": 1, "europe": 0},
        logical_ts=1,
    )

    chosen = await region.receive_replication(incoming)

    assert chosen is incoming
    assert region.log_store["L1"] is incoming


@pytest.mark.asyncio
async def test_receive_replication_merges_vector_clock():
    """Spec: per-key max merge, then increment local region by 1.

    Region "europe" starts with vc={"europe": 5}. It receives an entry
    stamped {"europe": 3, "us-east": 7}. After receipt the vc must be
    {"europe": max(5,3) + 1 = 6, "us-east": 7}.
    """
    region = Region("europe")
    # Pre-seed europe to 5 so we can verify the per-key max
    # (max(5, 3) = 5) followed by the +1 increment.
    region.vector_clock["europe"] = 5
    region.logical_ts = 5

    incoming = LogEntry(
        log_id="L1",
        data={"x": 1},
        region="us-east",
        vector_clock={"europe": 3, "us-east": 7},
        logical_ts=7,
    )

    await region.receive_replication(incoming)

    assert region.vector_clock == {"europe": 6, "us-east": 7}
    assert region.logical_ts == 6


@pytest.mark.asyncio
async def test_receive_replication_resolves_conflict_keeps_newer():
    """Same log_id; incoming has a strictly newer vector clock → incoming wins.

    Region "us-east" already has an entry with vc={"us-east": 1}.
    A replicated entry arrives with vc={"us-east": 2} for the same log_id.
    The resolver picks the incoming entry (causally newer).
    """
    region = Region("us-east")
    existing = LogEntry(
        log_id="L1",
        data={"v": "old"},
        region="us-east",
        vector_clock={"us-east": 1},
        logical_ts=1,
    )
    region.log_store["L1"] = existing

    incoming = LogEntry(
        log_id="L1",
        data={"v": "new"},
        region="us-east",
        vector_clock={"us-east": 2},
        logical_ts=2,
    )

    chosen = await region.receive_replication(incoming)

    assert chosen is incoming
    assert region.log_store["L1"] is incoming
    assert region.log_store["L1"].data == {"v": "new"}


@pytest.mark.asyncio
async def test_receive_replication_returns_chosen_entry():
    """The returned value matches what was actually stored under that log_id."""
    region = Region("asia")
    incoming = LogEntry(
        log_id="L42",
        data={"a": "b"},
        region="us-east",
        vector_clock={"us-east": 1},
        logical_ts=1,
    )

    chosen = await region.receive_replication(incoming)

    assert chosen is region.log_store["L42"]


# ---------------------------------------------------------------------
# get_logs
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_logs_sorted_by_created_at_desc_limit_n():
    """get_logs returns entries newest-first and respects the limit."""
    region = Region("us-east")
    # Build 5 entries with explicit, monotonically-increasing created_at
    # so the ordering is unambiguous regardless of clock granularity.
    base = time.time()
    for i in range(5):
        e = LogEntry(
            log_id=f"L{i}",
            data={"i": i},
            region="us-east",
            created_at=base + i,
            vector_clock={"us-east": i + 1},
            logical_ts=i + 1,
        )
        region.log_store[e.log_id] = e

    top3 = region.get_logs(limit=3)
    assert len(top3) == 3
    # Most-recent (largest created_at) first.
    assert [e.log_id for e in top3] == ["L4", "L3", "L2"]

    # Limit larger than store size returns everything.
    everything = region.get_logs(limit=100)
    assert len(everything) == 5

    # Zero / negative limit returns empty.
    assert region.get_logs(limit=0) == []
    assert region.get_logs(limit=-1) == []


# ---------------------------------------------------------------------
# is_healthy / mark_offline / mark_online
# ---------------------------------------------------------------------


def test_mark_offline_flips_healthy_flag():
    """Region starts healthy; mark_offline flips it; mark_online restores."""
    region = Region("us-east")
    assert region.is_healthy is True

    region.mark_offline()
    assert region.is_healthy is False

    region.mark_online()
    assert region.is_healthy is True


# ---------------------------------------------------------------------
# stats
# ---------------------------------------------------------------------


def test_stats_shape():
    """stats() returns the documented keys with the right types."""
    region = Region("us-east")
    snap = region.stats()

    assert set(snap.keys()) == {
        "region_id",
        "log_count",
        "vector_clock",
        "logical_ts",
        "is_healthy",
    }
    assert snap["region_id"] == "us-east"
    assert isinstance(snap["log_count"], int) and snap["log_count"] == 0
    assert isinstance(snap["vector_clock"], dict)
    assert snap["vector_clock"] == {"us-east": 0}
    assert isinstance(snap["logical_ts"], int) and snap["logical_ts"] == 0
    assert snap["is_healthy"] is True


def test_stats_returns_a_copy_of_vector_clock():
    """Mutating the snapshot's vector_clock must not poke a hole into the region."""
    region = Region("us-east")
    snap = region.stats()
    snap["vector_clock"]["us-east"] = 9999
    # The region's own vector_clock is unchanged.
    assert region.vector_clock["us-east"] == 0
