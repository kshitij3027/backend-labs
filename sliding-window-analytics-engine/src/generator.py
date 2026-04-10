"""Realistic streaming metric event generator.

:class:`LogEventGenerator` emits synthetic :class:`Event` objects
representing three metric types:

* ``response_time`` — per-service latency samples (ms), occasionally
  inflated by a 5x "spike" multiplier.
* ``throughput`` — requests-per-second samples tagged with a region.
* ``error_rate`` — unit-interval error ratios, mostly small with
  occasional bursts.

The generator is deterministic when seeded: callers pass ``rng_seed``
to get a reproducible stream of events for testing. All randomness
flows through a private :class:`random.Random` instance; the global
``random`` module is never touched.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from typing import Awaitable, Callable

from src.models import Event


# Type alias for the async sink callable that :meth:`run` pushes into.
Sink = Callable[[Event], Awaitable[None]]


class LogEventGenerator:
    """Produce realistic streaming metric events for simulated services.

    The async :meth:`run` method drives a loop that pushes events into a
    caller-supplied ``sink`` at roughly ``rate_per_second`` events per
    second, stopping when the provided ``stop_event`` is set.
    """

    # Service latency profiles: (mean_ms, stddev_ms).
    SERVICES: dict[str, tuple[float, float]] = {
        "api-gateway": (80.0, 15.0),
        "user-service": (120.0, 25.0),
        "payment-service": (250.0, 50.0),
        "notification-service": (60.0, 10.0),
    }
    REGIONS: list[str] = ["us-east-1", "us-west-2", "eu-west-1", "ap-south-1"]

    # Metric mix (must sum to 1.0): 60% latency, 30% throughput, 10% errors.
    _METRIC_MIX: tuple[tuple[str, float], ...] = (
        ("response_time", 0.60),
        ("throughput", 0.90),  # cumulative
        ("error_rate", 1.00),  # cumulative
    )

    def __init__(
        self,
        spike_probability: float = 0.1,
        rate_per_second: float = 600.0,
        rng_seed: int | None = None,
    ) -> None:
        self.spike_probability = spike_probability
        self.rate_per_second = rate_per_second
        self._rng = random.Random(rng_seed)

    def _pick_metric(self) -> str:
        """Sample a metric name from the configured mix."""
        r = self._rng.random()
        for name, cumulative in self._METRIC_MIX:
            if r < cumulative:
                return name
        return self._METRIC_MIX[-1][0]

    def _generate_response_time(self, now: float) -> Event:
        service = self._rng.choice(list(self.SERVICES.keys()))
        mean, stddev = self.SERVICES[service]
        value = self._rng.gauss(mean, stddev)
        # Clamp to a physically meaningful minimum. Latency can never be
        # 0 or negative in practice, and a floor of 1ms avoids downstream
        # divide-by-zero / log-of-zero surprises.
        if value < 1.0:
            value = 1.0
        # Inject a spike (5x multiplier) with the configured probability.
        if self._rng.random() < self.spike_probability:
            value *= 5.0
        return Event(
            event_id=str(uuid.uuid4()),
            timestamp=now,
            value=value,
            metric="response_time",
            metadata={"service": service},
        )

    def _generate_throughput(self, now: float) -> Event:
        value = self._rng.gauss(500.0, 100.0)
        if value < 1.0:
            value = 1.0
        region = self._rng.choice(self.REGIONS)
        return Event(
            event_id=str(uuid.uuid4()),
            timestamp=now,
            value=value,
            metric="throughput",
            metadata={"region": region},
        )

    def _generate_error_rate(self, now: float) -> Event:
        # Mostly quiet (<5% error), with occasional 10-30% blips.
        if self._rng.random() < 0.05:
            value = self._rng.uniform(0.1, 0.3)
        else:
            value = self._rng.uniform(0.0, 0.05)
        return Event(
            event_id=str(uuid.uuid4()),
            timestamp=now,
            value=value,
            metric="error_rate",
            metadata={},
        )

    def generate_one(self, now: float) -> Event:
        """Produce a single event with ``timestamp == now``.

        The metric type is sampled from the mix and then dispatched to
        the corresponding private builder. Each event is assigned a
        fresh uuid4 ``event_id`` so downstream systems can dedupe.
        """
        metric = self._pick_metric()
        if metric == "response_time":
            return self._generate_response_time(now)
        if metric == "throughput":
            return self._generate_throughput(now)
        return self._generate_error_rate(now)

    async def run(self, sink: Sink, stop_event: asyncio.Event) -> None:
        """Drive ``sink`` with events until ``stop_event`` is set.

        Sleeps ``1 / rate_per_second`` between events. This is a
        best-effort pacing loop — it does not attempt to catch up if
        the sink itself is slow, which is the desired semantics for a
        backpressure-aware pipeline.
        """
        delay = 1.0 / self.rate_per_second if self.rate_per_second > 0 else 0.0
        loop = asyncio.get_event_loop()
        while not stop_event.is_set():
            event = self.generate_one(loop.time())
            await sink(event)
            if delay > 0:
                await asyncio.sleep(delay)
