"""Unit tests for the consistent hash ring."""

from __future__ import annotations

import random
import statistics
import string

from shared.hash_ring import ConsistentHashRing


def _random_term(rng: random.Random, length: int = 8) -> str:
    return "".join(rng.choices(string.ascii_lowercase, k=length))


def _build_ring(node_ids: list[str], virtual_nodes: int = 100) -> ConsistentHashRing:
    ring = ConsistentHashRing(virtual_nodes=virtual_nodes)
    for node_id in node_ids:
        ring.add_node(node_id)
    return ring


def test_empty_ring_returns_none() -> None:
    ring = ConsistentHashRing()
    assert ring.get_node("anything") is None
    assert ring.size() == 0
    assert ring.nodes() == []


def test_add_node_registers() -> None:
    ring = ConsistentHashRing(virtual_nodes=10)
    ring.add_node("node-1")
    assert ring.nodes() == ["node-1"]
    assert ring.size() == 1
    # Re-adding is a no-op.
    ring.add_node("node-1")
    assert ring.size() == 1
    ring.add_node("node-2")
    assert ring.nodes() == ["node-1", "node-2"]
    assert ring.size() == 2


def test_lookup_deterministic() -> None:
    ring = _build_ring(["node-1", "node-2", "node-3", "node-4"])
    rng = random.Random(0)
    terms = [_random_term(rng) for _ in range(100)]
    first = [ring.get_node(t) for t in terms]
    # Same ring must return the same assignment every time.
    for _ in range(10):
        again = [ring.get_node(t) for t in terms]
        assert again == first


def test_all_terms_routed() -> None:
    ring = _build_ring(["node-1", "node-2", "node-3", "node-4"])
    valid = set(ring.nodes())
    rng = random.Random(1)
    for _ in range(500):
        term = _random_term(rng)
        node = ring.get_node(term)
        assert node is not None
        assert node in valid


def test_distribution_quality() -> None:
    ring = _build_ring(["node-1", "node-2", "node-3", "node-4"], virtual_nodes=100)
    rng = random.Random(42)
    counts: dict[str, int] = {n: 0 for n in ring.nodes()}
    total = 2000
    for _ in range(total):
        term = _random_term(rng)
        node = ring.get_node(term)
        assert node is not None
        counts[node] += 1
    values = list(counts.values())
    mean = statistics.mean(values)
    stdev = statistics.pstdev(values)
    assert mean > 0
    assert stdev / mean < 0.20, f"stddev/mean={stdev/mean:.3f}, counts={counts}"


def test_remove_node_rebalances_minimally() -> None:
    ring = _build_ring(["node-1", "node-2", "node-3", "node-4"], virtual_nodes=100)
    rng = random.Random(7)
    terms = [_random_term(rng) for _ in range(1000)]
    before = {t: ring.get_node(t) for t in terms}

    ring.remove_node("node-4")
    assert "node-4" not in ring.nodes()
    after = {t: ring.get_node(t) for t in terms}

    # Every term still has a home.
    for t in terms:
        assert after[t] is not None
        assert after[t] != "node-4"

    # Terms that did not live on node-4 should overwhelmingly stay put.
    kept_terms = [t for t in terms if before[t] != "node-4"]
    stayed = sum(1 for t in kept_terms if after[t] == before[t])
    assert kept_terms, "expected some terms to not have been on node-4"
    stay_ratio = stayed / len(kept_terms)
    assert stay_ratio >= 0.99, f"stay_ratio={stay_ratio:.3f}"

    # Terms previously on node-4 are redistributed across the remaining nodes.
    moved_terms = [t for t in terms if before[t] == "node-4"]
    if moved_terms:
        new_homes = {after[t] for t in moved_terms}
        assert new_homes.issubset({"node-1", "node-2", "node-3"})


def test_get_nodes_for_terms_groups() -> None:
    ring = _build_ring(["node-1", "node-2", "node-3", "node-4"])
    rng = random.Random(99)
    terms = [_random_term(rng) for _ in range(200)]
    grouped = ring.get_nodes_for_terms(terms)

    # Keys must be a subset of physical nodes.
    assert set(grouped.keys()).issubset(set(ring.nodes()))

    # Every term must appear exactly once across all grouped values.
    flat: list[str] = []
    for v in grouped.values():
        flat.extend(v)
    assert sorted(flat) == sorted(terms)


def test_get_nodes_for_terms_empty_ring() -> None:
    ring = ConsistentHashRing()
    grouped = ring.get_nodes_for_terms(["a", "b", "c"])
    assert grouped == {}
