"""Tests for :mod:`src.sample_data`.

The generator is the seed for both the demo script and the load-test
performance gate, so a quiet regression here (e.g. an invalid level
slipping through, or non-deterministic RNG usage) would cascade into
flaky perf numbers and broken demo transcripts. These tests lock the
contract down: exact count, valid levels/services, deterministic
output under a fixed seed, and "roughly now" default timestamps.
"""

from __future__ import annotations

import time

from src.models import LogEntry
from src.sample_data import generate_log_entries


_VALID_LEVELS = {"DEBUG", "INFO", "WARN", "WARNING", "ERROR", "FATAL"}


def test_generator_returns_exact_count() -> None:
    """The generator yields exactly ``count`` entries, no more, no less."""
    entries = generate_log_entries(42, seed=7)
    assert len(entries) == 42
    assert all(isinstance(e, LogEntry) for e in entries)


def test_generator_returns_valid_levels() -> None:
    """Every emitted entry has a level in the project-wide LogLevel set."""
    entries = generate_log_entries(200, seed=1)
    for e in entries:
        assert e.level in _VALID_LEVELS, (
            f"unexpected level={e.level!r} for message={e.message!r}"
        )


def test_generator_returns_valid_services() -> None:
    """Every emitted entry has a non-empty service string."""
    entries = generate_log_entries(200, seed=2)
    for e in entries:
        assert isinstance(e.service, str)
        assert e.service != "", f"empty service on {e!r}"


def test_generator_is_deterministic_under_same_seed() -> None:
    """Same seed + count + start_ts → identical message list."""
    a = generate_log_entries(50, seed=123, start_ts=1_700_000_000.0)
    b = generate_log_entries(50, seed=123, start_ts=1_700_000_000.0)
    assert [e.message for e in a] == [e.message for e in b]
    assert [e.service for e in a] == [e.service for e in b]
    assert [e.level for e in a] == [e.level for e in b]
    assert [e.timestamp for e in a] == [e.timestamp for e in b]


def test_different_seeds_produce_different_content() -> None:
    """Two different seeds diverge in at least one message (probabilistic)."""
    a = generate_log_entries(50, seed=0, start_ts=1_700_000_000.0)
    b = generate_log_entries(50, seed=999, start_ts=1_700_000_000.0)
    # At least one message must differ — with 50 samples across 22
    # templates the collision probability is vanishingly small.
    assert any(x.message != y.message for x, y in zip(a, b))


def test_default_start_ts_is_roughly_now() -> None:
    """Default ``start_ts`` puts entries close to the current wall clock.

    Matches the doctring contract: ``start_ts`` defaults to
    ``time.time() - count`` so the *last* entry's timestamp lands
    right around "now". We give ourselves some slack for clock drift
    between the generator call and the assertion.
    """
    count = 100
    before = time.time()
    entries = generate_log_entries(count)
    after = time.time()
    # Oldest entry should be at least ``count`` seconds in the past
    # relative to generation time, but no older than 2*count to cover
    # any scheduling slop.
    first_ts = entries[0].timestamp
    last_ts = entries[-1].timestamp
    assert first_ts >= before - count * 2
    assert last_ts <= after + 1
    # Monotonically increasing by 1s per entry (per the generator
    # contract: ts0 + i for i in range(count)).
    assert last_ts > first_ts
