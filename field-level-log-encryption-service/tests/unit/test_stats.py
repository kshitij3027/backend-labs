"""Unit tests for the C6 :class:`StatsCounters`.

Coverage targets:

* Pre-population — the well-known names are visible (at 0) on a fresh
  instance, so the dashboard never needs special-case KeyError handling.
* Atomic increment — :meth:`incr` returns the new value and creates
  unknown keys on first touch.
* Snapshot — returns a defensive copy, mutating it doesn't bleed back.
* Reset — zeroes every existing key (well-known AND custom) without
  removing them from the map.
* Thread safety — 10 producers × 100 increments resolves to 1000.
"""
from __future__ import annotations

import threading

from src.stats import StatsCounters


_WELL_KNOWN = (
    "logs_processed",
    "fields_detected",
    "fields_encrypted",
    "fields_decrypted",
    "errors",
    "keys_rotated",
)


class TestStatsCounters:
    """Behavioural contract of the atomic counter map."""

    def test_well_known_names_are_prepopulated_at_zero(self) -> None:
        # Fresh instance — every well-known counter exists at 0 so the
        # dashboard sees a stable shape on a cold process.
        sc = StatsCounters()
        snap = sc.snapshot()
        for name in _WELL_KNOWN:
            assert name in snap, name
            assert snap[name] == 0, name

    def test_incr_returns_new_value(self) -> None:
        # incr returns the value AFTER the increment, not before — so
        # callers can do "incr-then-threshold-check" in one round trip.
        sc = StatsCounters()
        assert sc.incr("logs_processed") == 1
        assert sc.incr("logs_processed") == 2
        assert sc.incr("logs_processed", 5) == 7

    def test_incr_creates_unknown_keys_on_first_touch(self) -> None:
        # The map is intentionally permissive — future commits can add
        # counters without touching this file. Custom keys get a 0
        # baseline implicitly.
        sc = StatsCounters()
        assert sc.incr("custom_key", 5) == 5
        assert sc.get("custom_key") == 5

    def test_get_returns_zero_for_missing_key(self) -> None:
        # Reading a never-seen key must be safe — return 0 rather
        # than raise so dashboard rendering can't crash on a typo.
        sc = StatsCounters()
        assert sc.get("never_seen_counter") == 0

    def test_snapshot_returns_independent_copy(self) -> None:
        # Mutating the returned dict must not bleed back into the
        # counter store.
        sc = StatsCounters()
        sc.incr("logs_processed", 3)
        snap = sc.snapshot()
        snap["logs_processed"] = 999
        snap["new_key"] = 42
        # Live state untouched by external mutation.
        assert sc.get("logs_processed") == 3
        assert sc.get("new_key") == 0

    def test_reset_zeroes_known_and_custom_keys(self) -> None:
        # reset must zero every existing key (well-known AND custom)
        # without removing them — so a consumer that cached the key
        # set sees a stable shape.
        sc = StatsCounters()
        sc.incr("logs_processed", 5)
        sc.incr("custom_key", 7)
        sc.reset()
        snap = sc.snapshot()
        # Every well-known name still present at 0.
        for name in _WELL_KNOWN:
            assert snap[name] == 0
        # Custom name still present (not removed!) but also at 0.
        assert "custom_key" in snap
        assert snap["custom_key"] == 0

    def test_negative_increment_decreases_value(self) -> None:
        # The contract allows negative n. Callers shouldn't do this in
        # practice on the well-known counters, but the lock guarantees
        # atomicity regardless of sign.
        sc = StatsCounters()
        sc.incr("logs_processed", 5)
        assert sc.incr("logs_processed", -2) == 3

    def test_thread_safety_under_concurrent_increment(self) -> None:
        # 10 threads × 100 increments → exactly 1000. A missing lock
        # would yield torn read-modify-writes and a final value below
        # 1000 (the classic race).
        sc = StatsCounters()

        def worker() -> None:
            for _ in range(100):
                sc.incr("logs_processed")

        threads = [threading.Thread(target=worker) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sc.get("logs_processed") == 1000
