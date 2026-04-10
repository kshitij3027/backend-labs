"""Unit tests for :class:`src.generator.LogEventGenerator`."""

from __future__ import annotations

import asyncio
import math

import pytest

from src.generator import LogEventGenerator
from src.models import Event


KNOWN_METRICS = {"response_time", "throughput", "error_rate"}


def test_generate_one_deterministic_with_seed():
    gen_a = LogEventGenerator(rng_seed=1234)
    gen_b = LogEventGenerator(rng_seed=1234)
    for i in range(50):
        now = 1000.0 + i
        ea = gen_a.generate_one(now)
        eb = gen_b.generate_one(now)
        assert ea.metric == eb.metric
        assert ea.value == eb.value
        assert ea.metadata == eb.metadata
        assert ea.timestamp == eb.timestamp == now


def test_generate_one_event_fields():
    gen = LogEventGenerator(rng_seed=7)
    now = 2000.0
    for _ in range(50):
        event = gen.generate_one(now)
        assert isinstance(event, Event)
        assert event.event_id  # non-empty string
        assert event.timestamp == now
        assert event.metric in KNOWN_METRICS
        assert math.isfinite(event.value)
        if event.metric in ("response_time", "throughput"):
            assert event.value > 0.0
        else:  # error_rate
            assert 0.0 <= event.value <= 1.0


def test_response_time_service_in_metadata():
    gen = LogEventGenerator(rng_seed=99)
    known_services = set(LogEventGenerator.SERVICES.keys())
    seen = 0
    for _ in range(2000):
        event = gen.generate_one(0.0)
        if event.metric != "response_time":
            continue
        seen += 1
        assert "service" in event.metadata
        assert event.metadata["service"] in known_services
        if seen >= 100:
            break
    assert seen >= 100, "expected at least 100 response_time events in sample"


def test_throughput_region_in_metadata():
    gen = LogEventGenerator(rng_seed=42)
    known_regions = set(LogEventGenerator.REGIONS)
    seen = 0
    for _ in range(2000):
        event = gen.generate_one(0.0)
        if event.metric != "throughput":
            continue
        seen += 1
        assert "region" in event.metadata
        assert event.metadata["region"] in known_regions
        if seen >= 100:
            break
    assert seen >= 100, "expected at least 100 throughput events in sample"


def test_spike_injection_probability():
    # No spikes: mean should be close to the unweighted service mean (~128ms).
    no_spike = LogEventGenerator(spike_probability=0.0, rng_seed=11)
    all_spike = LogEventGenerator(spike_probability=1.0, rng_seed=11)

    def mean_rt(gen: LogEventGenerator) -> float:
        samples: list[float] = []
        while len(samples) < 200:
            e = gen.generate_one(0.0)
            if e.metric == "response_time":
                samples.append(e.value)
        return sum(samples) / len(samples)

    base_mean = mean_rt(no_spike)
    spike_mean = mean_rt(all_spike)
    # All-spike latencies should be ~5x the non-spiked mean; allow some
    # slack for the gaussian noise across only 200 samples.
    assert spike_mean > base_mean * 3.0


@pytest.mark.asyncio
async def test_async_run_drives_sink():
    received: list[Event] = []

    async def sink(event: Event) -> None:
        received.append(event)

    gen = LogEventGenerator(rate_per_second=500.0, rng_seed=2024)
    stop_event = asyncio.Event()

    task = asyncio.create_task(gen.run(sink, stop_event))
    await asyncio.sleep(0.2)
    stop_event.set()
    await task

    # At 500 evt/s over ~0.2s we'd expect ~100 events; allow for
    # scheduler overhead and assert a generous lower bound.
    assert len(received) > 10
    for event in received[:5]:
        assert event.metric in KNOWN_METRICS
