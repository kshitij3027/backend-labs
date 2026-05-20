"""Stats subpackage: throughput, latency, and per-pattern counters.

Three independent components live here:

* :class:`~src.stats.throughput.ThroughputCounter` — sliding-window
  ops-per-second counter using one bucket per second.
* :class:`~src.stats.latency.LatencyHistogram` — reservoir-sampled
  latency histogram with p50/p95/p99 percentile snapshots.
* :class:`~src.stats.counters.PatternCounters` — per-pattern hit
  counters (``ssn``, ``credit_card``, ...).

The :class:`Stats` namespace class bundles all three so the C5
processor can call ``self.stats.counters.incr(name)``,
``self.stats.throughput.record()``, and ``self.stats.latency.record(ms)``
through a single object. Every component is independently thread-safe;
the bundle adds no synchronization beyond what's already in each part.
"""
from __future__ import annotations

from .counters import PatternCounters
from .latency import LatencyHistogram
from .throughput import ThroughputCounter


class Stats:
    """Bundle of throughput / latency / counter sub-components.

    Constructed once at FastAPI startup (C7) and shared across the
    request handlers, the C5 processor, and any background tasks. The
    three children are passed in by the bootstrap so tests can inject
    mocks for any axis without rebuilding the bundle.

    Parameters
    ----------
    throughput : ThroughputCounter
        Sliding-window ops-per-second counter. ``record()`` is called
        once per processed entry by the redaction processor.
    latency : LatencyHistogram
        Reservoir-sampled latency histogram. ``record(latency_ms)`` is
        called once per processed entry.
    counters : PatternCounters
        Per-pattern counter map. ``incr(name)`` is called once per
        applied redaction (the processor passes the pattern name).
    """

    def __init__(
        self,
        throughput: ThroughputCounter,
        latency: LatencyHistogram,
        counters: PatternCounters,
    ) -> None:
        # Public attributes — the processor reaches in directly via
        # ``self.stats.counters.incr(...)`` etc., matching the API the
        # field-level encryption service uses.
        self.throughput = throughput
        self.latency = latency
        self.counters = counters
