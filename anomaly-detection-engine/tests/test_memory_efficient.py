"""Tests for memory-efficient pattern storage (HyperLogLog + Count-Min Sketch)."""
from __future__ import annotations

import threading

import pytest

from src.advanced.memory_efficient import CountMinSketch, PatternStore


# ------------------------------------------------------------------
# CountMinSketch unit tests
# ------------------------------------------------------------------


class TestCountMinSketch:
    """Tests for the custom Count-Min Sketch implementation."""

    def test_cms_never_undercounts(self) -> None:
        """CMS estimates must always be >= the true count."""
        cms = CountMinSketch(width=1000, depth=5, seed=42)
        ip = "192.168.1.1"
        for _ in range(50):
            cms.add(ip)
        assert cms.query(ip) >= 50

    def test_cms_width_depth_affects_memory(self) -> None:
        """Larger width/depth should result in more memory usage."""
        small = CountMinSketch(width=100, depth=3)
        large = CountMinSketch(width=1000, depth=10)
        assert large.get_memory_bytes() > small.get_memory_bytes()


# ------------------------------------------------------------------
# PatternStore integration tests
# ------------------------------------------------------------------


class TestPatternStore:
    """Tests for the PatternStore combining HLL and CMS."""

    def test_hll_cardinality_accuracy(self) -> None:
        """HLL cardinality estimate should be within 10% of actual."""
        store = PatternStore()
        n = 10_000
        for i in range(n):
            store.add_pattern(
                ip=f"10.0.{i // 256}.{i % 256}",
                user_agent="Mozilla/5.0",
                path="/index.html",
            )
        estimate = store.get_unique_ip_count()
        assert 9000 <= estimate <= 11000, (
            f"HLL estimate {estimate} not within 10% of {n}"
        )

    def test_memory_constant_growth(self) -> None:
        """Memory should NOT grow proportionally with pattern count.

        After adding 10K patterns and then 100K more, the memory should
        not have doubled (ratio < 2x) because the underlying structures
        are fixed-size.
        """
        store = PatternStore()
        for i in range(10_000):
            store.add_pattern(
                ip=f"10.0.{i // 256}.{i % 256}",
                user_agent=f"UA-{i}",
                path=f"/page/{i}",
            )
        mem_after_10k = store.get_memory_usage()

        for i in range(10_000, 110_000):
            store.add_pattern(
                ip=f"10.0.{i // 256}.{i % 256}",
                user_agent=f"UA-{i}",
                path=f"/page/{i}",
            )
        mem_after_110k = store.get_memory_usage()

        ratio = mem_after_110k / mem_after_10k
        assert ratio < 2.0, (
            f"Memory grew {ratio:.1f}x after 11x more patterns (should be < 2x)"
        )

    def test_pattern_store_integration(self) -> None:
        """Adding patterns should update all HLL and CMS counters."""
        store = PatternStore()
        store.add_pattern("1.2.3.4", "Chrome/120", "/api/users")
        store.add_pattern("1.2.3.4", "Chrome/120", "/api/users")
        store.add_pattern("5.6.7.8", "Firefox/115", "/api/items")

        assert store.get_unique_ip_count() >= 1
        assert store.get_unique_ua_count() >= 1
        assert store.get_unique_path_count() >= 1
        assert store.get_ip_frequency("1.2.3.4") >= 2
        assert store.get_path_frequency("/api/users") >= 2

    def test_get_stats_keys(self) -> None:
        """The stats dict must contain the expected keys."""
        store = PatternStore()
        store.add_pattern("1.1.1.1", "bot/1.0", "/health")
        stats = store.get_stats()

        expected_keys = {
            "unique_ips",
            "unique_user_agents",
            "unique_paths",
            "total_patterns",
            "memory_usage_bytes",
        }
        assert set(stats.keys()) == expected_keys
        assert stats["total_patterns"] == 1
        assert stats["memory_usage_bytes"] > 0

    def test_thread_safety(self) -> None:
        """Concurrent add_pattern calls must not raise or corrupt state."""
        store = PatternStore()
        errors: list[Exception] = []

        def worker(thread_id: int) -> None:
            try:
                for i in range(500):
                    store.add_pattern(
                        ip=f"10.{thread_id}.0.{i % 256}",
                        user_agent=f"UA-thread-{thread_id}",
                        path=f"/path/{thread_id}/{i}",
                    )
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Thread errors: {errors}"
        assert store._total_patterns == 8 * 500
