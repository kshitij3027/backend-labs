"""Unit tests for :class:`src.region_ring.RegionRing`.

The ring's two contracts that matter to the cart workload:

1. **Determinism**: a given ``cart_id`` always maps to the same region —
   across calls, across fresh ring instances, across processes.
2. **Even distribution**: with 64 vnodes per region, 1000 sample
   cart_ids should land in each region within ±15% of the perfectly
   even baseline (per ``plan.md`` §6 / Feature B spec).

We deliberately don't test internals (``_ring``, ``_sorted_keys``) —
those are implementation details and the public contract is what
``http_server.py`` and ``scripts/demo.py`` actually rely on.
"""
from __future__ import annotations

from collections import Counter

import pytest

from src.region_ring import RegionRing


REGIONS = ["us-east", "europe", "asia"]


def test_get_home_region_returns_one_of_known_regions() -> None:
    """The home region is always one of the regions handed to the constructor."""
    ring = RegionRing(REGIONS)
    for i in range(50):
        home = ring.get_home_region(f"cart-{i}")
        assert home in REGIONS


def test_distribution_within_15pct() -> None:
    """1000 distinct cart_ids should split each region within ±15%.

    With 3 regions the perfectly-even baseline is 333.33 per region;
    ±15% gives a window of [283, 384] per region. 64 vnodes is the
    minimum we found in our spec to reliably hit this with SHA-1 keys.
    """
    ring = RegionRing(REGIONS, virtual_nodes=64)
    counts: Counter[str] = Counter()
    for i in range(1000):
        counts[ring.get_home_region(f"cart-{i:05d}")] += 1

    # All three regions must show up.
    assert set(counts.keys()) == set(REGIONS)

    # 333.33 ± 15% → [283, 384]. We use 1000 samples / 3 ≈ 333.33,
    # ±15% of 333.33 ≈ ±50, so [283, 384].
    low, high = 283, 384
    for region, count in counts.items():
        assert low <= count <= high, (
            f"{region} got {count} carts, outside ±15% window [{low}, {high}]"
        )


def test_deterministic_get_home_region() -> None:
    """The same cart_id always returns the same region across calls."""
    ring = RegionRing(REGIONS)
    for i in range(20):
        cart_id = f"cart-{i}"
        first = ring.get_home_region(cart_id)
        for _ in range(5):
            assert ring.get_home_region(cart_id) == first


def test_empty_regions_raises() -> None:
    """An empty ring is a programmer error — surface it loudly."""
    ring = RegionRing([])
    with pytest.raises(RuntimeError, match="region ring is empty"):
        ring.get_home_region("anything")


def test_two_rings_with_same_input_agree() -> None:
    """Two fresh rings built from identical inputs route identically.

    This is the determinism property cross-process: SHA-1 + sorted
    insertion + ``bisect_right`` is purely a function of the input
    list, so independent ring instances must agree on every key.
    """
    ring_a = RegionRing(REGIONS)
    ring_b = RegionRing(REGIONS)
    for i in range(100):
        cart_id = f"cart-{i:04d}"
        assert ring_a.get_home_region(cart_id) == ring_b.get_home_region(cart_id)


def test_regions_returns_sorted_list() -> None:
    """``regions()`` exposes the configured set as a sorted list."""
    ring = RegionRing(REGIONS)
    assert ring.regions() == sorted(REGIONS)
