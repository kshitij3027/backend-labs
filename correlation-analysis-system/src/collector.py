"""Log collection stage: generate -> parse -> buffer -> aggregate -> Redis mirror.

:class:`LogCollector.tick` is the pipeline's 1-second heartbeat and its hot
path: it drains the generator, parses each raw line exactly once, folds parsed
events into the :class:`~src.aggregation.MetricAggregator`, keeps the newest
~5000 events in a bounded deque for the API, and mirrors the tick's batch to
Redis in one pipelined call. Nothing in the loop re-validates pydantic models,
serializes JSON, or allocates numpy arrays — the burst budget is 1000+ lines
per tick (proven by the unit-test burst gate).

All time flows through the explicit ``now`` argument so tests replay simulated
clocks; only the production pipeline task lets it default to ``time.time()``.
"""

from __future__ import annotations

import time
from collections import deque
from itertools import islice

from src.aggregation import MetricAggregator
from src.config import Settings
from src.generators import LogGenerator
from src.models import LogEvent
from src.parsers import parse_line
from src.store import RedisStore

#: EMA smoothing factor for the events/sec throughput gauge (higher = more reactive).
_EPS_EMA_ALPHA = 0.3


class LogCollector:
    """Turns raw generator output into buffered, aggregated, mirrored LogEvents."""

    def __init__(
        self,
        settings: Settings,
        generator: LogGenerator,
        aggregator: MetricAggregator,
        store: RedisStore | None = None,
    ) -> None:
        self.settings = settings
        self.generator = generator
        self.aggregator = aggregator
        #: Optional Redis mirror; None (or a dead Redis) degrades to memory-only.
        self.store = store
        #: Parsed events, oldest -> newest; maxlen bounds memory (default 5000).
        self.buffer: deque[LogEvent] = deque(maxlen=settings.event_buffer_size)
        #: Lifetime counters/gauges surfaced by /health.
        self.events_total: int = 0
        self.parse_errors: int = 0
        self.events_per_sec: float = 0.0

    def tick(self, now: float | None = None) -> list[LogEvent]:
        """Run one collection cycle at ``now``; returns the newly parsed events."""
        if now is None:
            now = time.time()

        new_events: list[LogEvent] = []
        add_event = self.aggregator.add_event  # bound once, called per event below
        for source, line in self.generator.generate(now):
            ev = parse_line(source, line, ingested_at=now)
            if ev is None:
                # parse_line never raises — garbage is counted, never fatal.
                self.parse_errors += 1
                continue
            new_events.append(ev)
            add_event(ev)
        self.buffer.extend(new_events)
        self.aggregator.roll(now)

        if self.store is not None and new_events:
            # The tick's single Redis round-trip (pipelined LPUSH + LTRIM).
            self.store.push_recent_logs(new_events)

        self.events_total += len(new_events)
        # EMA of instantaneous throughput: this tick's count over the tick interval.
        instantaneous = len(new_events) / max(self.settings.generation_interval_seconds, 1e-6)
        self.events_per_sec += _EPS_EMA_ALPHA * (instantaneous - self.events_per_sec)
        return new_events

    def recent(self, count: int) -> list[LogEvent]:
        """The newest ``count`` buffered events, newest first."""
        if count <= 0:
            return []
        # reversed(deque) iterates right-to-left in O(1) per step — no full copy.
        return list(islice(reversed(self.buffer), count))
