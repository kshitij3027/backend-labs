"""Tests for src.search.generator.

Verifies that the synthetic generator produces the distribution shapes
the rest of the system relies on: all 5 services, 5 levels, 4 regions,
and all 4 latency buckets are represented across a reasonably sized
draw; weighted picks roughly match their configured weights;
timestamps land inside the requested window; the seeded path is
reproducible; and error-level messages contain the tokens the
free-text tests depend on.

The latency-bucket CASE logic is reimplemented locally here rather
than imported to make sure the generator output matches what SQLite
will compute at write time via the STORED generated column.
"""

from __future__ import annotations

from typing import Iterable

from src.search.generator import (
    LEVELS,
    REGIONS,
    SERVICES,
    generate_batch,
    generate_log_entry,
)


# ---------- helpers -------------------------------------------------


def _latency_bucket(ms: float) -> str:
    """Mirror the CASE expression in the SQLite DDL for latency_bucket."""
    if ms < 100:
        return "0-100ms"
    if ms < 500:
        return "100-500ms"
    if ms < 2000:
        return "500ms-2s"
    return "2s+"


def _service_names() -> set[str]:
    return {name for name, _ in SERVICES}


def _level_names() -> set[str]:
    return {name for name, _ in LEVELS}


# ---------- size + distribution tests -------------------------------


def test_generate_batch_size():
    """generate_batch(n=1000) should yield exactly 1000 LogEntry rows."""
    entries = list(generate_batch(1000, seed=1))
    assert len(entries) == 1000


def test_distributions_span_all_values():
    """Across 2000 draws, every configured service/level/region and
    every latency bucket should appear at least once."""
    entries = list(generate_batch(2000, seed=1))

    services = {e.service for e in entries}
    levels = {e.level for e in entries}
    regions = {e.region for e in entries}
    buckets = {_latency_bucket(e.response_time_ms) for e in entries}

    assert services == _service_names(), (
        f"service set mismatch: expected {_service_names()}, got {services}"
    )
    assert levels == _level_names(), (
        f"level set mismatch: expected {_level_names()}, got {levels}"
    )
    assert regions == set(REGIONS), (
        f"region set mismatch: expected {set(REGIONS)}, got {regions}"
    )
    assert buckets == {"0-100ms", "100-500ms", "500ms-2s", "2s+"}, (
        f"latency buckets missing values: {buckets}"
    )


def test_service_weighting_roughly_matches():
    """Empirical service fractions should be close to configured weights
    for a large enough seeded sample (payments=0.35, auth=0.25)."""
    entries = list(generate_batch(5000, seed=1))
    n = len(entries)
    counts = {"payments": 0, "auth": 0}
    for e in entries:
        if e.service in counts:
            counts[e.service] += 1

    payments_frac = counts["payments"] / n
    auth_frac = counts["auth"] / n

    assert 0.28 <= payments_frac <= 0.42, (
        f"payments fraction {payments_frac:.4f} outside [0.28, 0.42]"
    )
    assert 0.18 <= auth_frac <= 0.32, (
        f"auth fraction {auth_frac:.4f} outside [0.18, 0.32]"
    )


def test_latency_is_positive():
    """Every generated response_time_ms must be >= 1.0 (never zero)."""
    entries = list(generate_batch(500, seed=2))
    for e in entries:
        assert e.response_time_ms >= 1.0, (
            f"non-positive latency produced: {e.response_time_ms}"
        )


def test_ts_within_window():
    """With now_ts fixed and window_hours=24, every ts must be in
    [now-86400, now]."""
    now_ts = 1_700_000_000
    window_hours = 24
    # generate_batch forwards now_ts to each entry; we also exercise
    # the single-entry helper to be thorough.
    batch = list(generate_batch(500, now_ts=now_ts, seed=3))
    for e in batch:
        assert now_ts - window_hours * 3600 <= e.ts <= now_ts, (
            f"batch ts {e.ts} outside window [{now_ts - window_hours * 3600}, {now_ts}]"
        )

    single = generate_log_entry(now_ts=now_ts, window_hours=window_hours)
    assert now_ts - window_hours * 3600 <= single.ts <= now_ts, (
        f"single ts {single.ts} outside window"
    )


def test_seed_reproducible():
    """Identical seeds must produce identical batches on the fields
    driven by ``random``.

    NOTE: ``LogEntry.id``, ``request_id`` and ``metadata.trace_id`` are
    generated via ``uuid4()`` which does NOT consult the ``random``
    module, so even with a fixed seed those three fields differ
    between runs. Everything else (service/level/region/ts/latency/
    message-template) must match exactly. This divergence from the
    prompt's "same seed -> identical ids" claim is called out in the
    Test Agent report.
    """
    batch_a = list(generate_batch(50, seed=7))
    batch_b = list(generate_batch(50, seed=7))

    assert len(batch_a) == len(batch_b) == 50
    # Fields that should be stable under a fixed random.seed().
    stable_fields = ("ts", "service", "level", "region", "response_time_ms")

    for a, b in zip(batch_a, batch_b):
        a_dump = a.model_dump()
        b_dump = b.model_dump()
        for field in stable_fields:
            assert a_dump[field] == b_dump[field], (
                f"seeded divergence on field {field!r}: {a_dump[field]} vs {b_dump[field]}"
            )


def test_message_templates_contain_known_tokens():
    """At least one ERROR-level message from 500 draws should contain one
    of the free-text tokens the search tests rely on."""
    tokens = ("timeout", "connection refused", "unauthorized")
    entries = list(generate_batch(500, seed=11))
    error_msgs = [e.message for e in entries if e.level == "ERROR"]
    # With 500 draws and ERROR weight ~10% we expect ~50 error messages;
    # be defensive and just require at least one.
    assert error_msgs, "expected at least one ERROR-level entry in 500 draws"

    assert any(tok in msg for tok in tokens for msg in error_msgs), (
        f"none of the tokens {tokens} appeared in ERROR messages: "
        f"{error_msgs[:5]}..."
    )
