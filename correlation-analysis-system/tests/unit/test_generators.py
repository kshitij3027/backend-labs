"""Unit tests for src.generators.LogGenerator.

Everything here is fully deterministic: an explicit ``random.Random(42)`` is
injected, all time is simulated (now = 1000.0 + i), and Settings is built with
explicit kwargs so container environment variables cannot skew the tunables.
"""

import random

from src import models
from src.config import Settings
from src.generators import LogGenerator
from src.models import ScenarioKind, SourceType
from src.parsers import parse_line

EPS = 120  # events_per_second used by every test in this module
EPOCH = 1000.0  # simulated clock base


def make_gen(seed: int = 42) -> LogGenerator:
    """A LogGenerator with pinned settings and a seeded rng."""
    settings = Settings(
        _env_file=None,  # hermetic: ignore any ambient .env
        events_per_second=EPS,
        scenario_period_seconds=45,
        scenario_duration_seconds=20,
    )
    return LogGenerator(settings, rng=random.Random(seed))


def run_and_parse(gen: LogGenerator, start: float, ticks: int) -> list:
    """Run `ticks` 1-second ticks from `start` and parse every emitted line."""
    events = []
    for i in range(ticks):
        now = start + i
        for src, line in gen.generate(now):
            ev = parse_line(src, line, ingested_at=now)
            assert ev is not None, f"generated line failed to parse: {line!r}"
            events.append(ev)
    return events


def test_tick_covers_all_sources():
    gen = make_gen()
    seen: set[SourceType] = set()
    for i in range(10):
        seen.update(src for src, _ in gen.generate(EPOCH + i))
    assert seen == set(SourceType)


def test_volume_near_target():
    gen = make_gen()
    counts = [len(gen.generate(EPOCH + i)) for i in range(11)]
    # Ticks 3..10: the hop pipeline (journeys span ~4s) has filled by then.
    steady = counts[3:]
    avg = sum(steady) / len(steady)
    assert EPS * 0.5 <= avg <= EPS * 1.5, f"steady-state avg {avg:.1f} lines/tick"


def test_journey_coherence():
    gen = make_gen()
    events = run_and_parse(gen, EPOCH, 10)
    assert gen.journeys, "expected at least one journey to spawn"
    record = gen.journeys[0]  # spawned on tick 0 -> fully drained well before tick 10

    journey_events = [e for e in events if e.correlation_id == record.correlation_id]
    assert len(journey_events) >= 5
    # Every hop carries the journey's single user.
    assert {e.user_id for e in journey_events} == {record.user_id}
    # Emission order == hop order; embedded timestamps are non-decreasing and
    # the whole journey spans at most 5 seconds.
    stamps = [e.timestamp for e in journey_events]
    assert stamps == sorted(stamps)
    assert stamps[-1] - stamps[0] <= 5.0
    src_set = {e.source for e in journey_events}
    assert SourceType.WEB in src_set
    assert len(src_set) >= 3


def test_ground_truth_recorded():
    gen = make_gen()
    for i in range(10):
        gen.generate(EPOCH + i)
    assert len(gen.journeys) > 0
    for record in gen.journeys:
        assert record.correlation_id.startswith("corr_")
        assert record.user_id.startswith("user_")
        assert record.sources, "journey must record its hop sources"
        assert record.completed_at is not None
        assert record.completed_at >= record.started_at


def test_scenario_rotation():
    gen = make_gen()
    assert gen.active_scenario(EPOCH) is None  # no epoch until first generate()
    gen.generate(EPOCH)  # sets the scenario clock base
    # Slot 0 active window [epoch, epoch+20).
    assert gen.active_scenario(EPOCH + 5) is ScenarioKind.DB_POOL_SATURATION
    # Inside slot 0 but past the 20s active duration.
    assert gen.active_scenario(EPOCH + 25) is None
    # Slot 1 is a quiet slot.
    assert gen.active_scenario(EPOCH + 50) is None
    # Slot 2 = payment slowdown, slot 4 = inventory timeouts.
    assert gen.active_scenario(EPOCH + 95) is ScenarioKind.PAYMENT_SLOWDOWN
    assert gen.active_scenario(EPOCH + 185) is ScenarioKind.INVENTORY_TIMEOUTS
    # The 6-slot cycle wraps: epoch+275 is slot 6 % 6 = 0 again.
    assert gen.active_scenario(EPOCH + 275) is ScenarioKind.DB_POOL_SATURATION


def test_db_pool_saturation_signature():
    gen = make_gen()
    # Slot 0 (DB_POOL_SATURATION) is active from the epoch, so tick straight through it.
    events = run_and_parse(gen, EPOCH, 8)
    assert any(e.error_code == models.DB_POOL_EXHAUSTED for e in events)
    # Co-moving symptom: web 5xx responses in the same batch.
    assert any(
        e.source is SourceType.WEB and e.metrics.get("status", 0.0) >= 500
        for e in events
    )
    # Pool pegged at 20/20 on database lines.
    assert any(
        e.source is SourceType.DATABASE and e.metrics.get("pool_in_use") == 20.0
        for e in events
    )


def test_payment_slowdown_signature():
    gen = make_gen()
    gen.generate(EPOCH)  # pin the scenario clock
    # Slot 2 (PAYMENT_SLOWDOWN) active window is [epoch+90, epoch+110).
    events = run_and_parse(gen, EPOCH + 90, 8)
    payment = [e for e in events if e.source is SourceType.PAYMENT]
    assert any(e.metrics.get("latency_ms", 0.0) > 800 for e in payment)
    assert any(e.error_code == models.CART_ABANDONED for e in events)
    assert any(record.abandoned for record in gen.journeys)


def test_inventory_timeouts_signature():
    gen = make_gen()
    gen.generate(EPOCH)  # pin the scenario clock
    # Slot 4 (INVENTORY_TIMEOUTS) active window is [epoch+180, epoch+200).
    events = run_and_parse(gen, EPOCH + 180, 8)
    assert any(e.error_code == models.INVENTORY_TIMEOUT for e in events)
    assert any(e.error_code == models.CHECKOUT_FAILED for e in events)


def test_parse_rate():
    gen = make_gen()
    total = 0
    parsed = 0
    for i in range(20):
        now = EPOCH + i
        for src, line in gen.generate(now):
            total += 1
            if parse_line(src, line, ingested_at=now) is not None:
                parsed += 1
    assert total > 0
    assert parsed / total > 0.99, f"parse rate {parsed}/{total}"


def test_noise_has_no_ids():
    gen = make_gen()
    events = run_and_parse(gen, EPOCH, 5)
    assert any(e.correlation_id is None for e in events), "background noise must exist"
    # And noise never carries a user either.
    assert any(e.correlation_id is None and e.user_id is None for e in events)
