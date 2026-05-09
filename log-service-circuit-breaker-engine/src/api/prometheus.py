"""Prometheus metrics exposition for the circuit breaker engine."""
from __future__ import annotations
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST

# Numeric mapping so prometheus can ingest a numeric Gauge for state.
STATE_VALUES = {"CLOSED": 0, "HALF_OPEN": 1, "OPEN": 2}


class PrometheusMetrics:
    """Holds all the Prometheus metric collectors for the breaker engine."""

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        self.state_gauge = Gauge(
            "circuit_breaker_state",
            "Numeric state of a breaker: 0=CLOSED, 1=HALF_OPEN, 2=OPEN.",
            ["name"],
            registry=self.registry,
        )
        self.success_rate = Gauge(
            "circuit_breaker_success_rate",
            "Per-breaker success rate (0..1).",
            ["name"],
            registry=self.registry,
        )
        self.calls_total = Counter(
            "circuit_breaker_calls_total",
            "Total calls through a breaker.",
            ["name", "outcome"],  # outcome: ok | failure | timeout | rejected
            registry=self.registry,
        )
        self.state_changes = Counter(
            "circuit_breaker_state_changes_total",
            "Number of state transitions per breaker.",
            ["name", "from_state", "to_state"],
            registry=self.registry,
        )
        self.processing = Counter(
            "circuit_breaker_processing_total",
            "Per-outcome counter for the LogProcessorService.",
            ["outcome"],  # outcome: total | successful | failed | fallback
            registry=self.registry,
        )

    def update_from_snapshot(self, circuits: dict, processing: dict) -> None:
        """Refresh gauges from the latest registry snapshot.

        Counters are NOT touched here — they accumulate via listeners only.
        """
        for name, data in circuits.items():
            state = data.get("state") or data.get("current_state") or "CLOSED"
            self.state_gauge.labels(name=name).set(STATE_VALUES.get(state, 0))
            self.success_rate.labels(name=name).set(float(data.get("success_rate") or 1.0))

    def render(self) -> bytes:
        return generate_latest(self.registry)


def state_change_metric_listener(metrics: PrometheusMetrics):
    """Returns a listener that increments state_changes for each transition."""

    def _listener(name: str, from_state, to_state, reason: str) -> None:
        f = from_state.value if hasattr(from_state, "value") else str(from_state)
        t = to_state.value if hasattr(to_state, "value") else str(to_state)
        metrics.state_changes.labels(name=name, from_state=f, to_state=t).inc()

    return _listener
