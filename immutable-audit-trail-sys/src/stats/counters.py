"""Atomic counters + Prometheus metrics for the audit subsystem.

Two layers:
1. ``Counters`` — a thread-safe dataclass with snapshot()-able state for
   the /api/stats JSON endpoint and human-readable observability.
2. Prometheus ``Counter`` / ``Histogram`` objects — exposed at /metrics
   by the prometheus_fastapi_instrumentator wired in main.py.

Both layers are incremented from the same call sites (ChainAppender,
ChainVerifier, decorator). Keeping them next to each other in one file
makes it obvious that adding a new instrument means hitting both layers.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass

from prometheus_client import Counter, Histogram


@dataclass
class _CountersState:
    records_appended: int = 0
    verifications_run: int = 0
    integrity_breaks_detected: int = 0
    decorator_invocations_total: int = 0
    decorator_failures_total: int = 0


class Counters:
    """Thread-safe counter set with snapshot()-able state."""

    def __init__(self) -> None:
        self._state = _CountersState()
        self._lock = threading.Lock()

    def incr_records_appended(self, n: int = 1) -> None:
        with self._lock:
            self._state.records_appended += n
        records_appended_total.inc(n)

    def incr_verifications_run(self, n: int = 1) -> None:
        with self._lock:
            self._state.verifications_run += n
        verifications_total.inc(n)

    def incr_integrity_breaks(self, n: int = 1) -> None:
        with self._lock:
            self._state.integrity_breaks_detected += n
        verify_breaks_total.inc(n)

    def incr_decorator_invocations(self, n: int = 1) -> None:
        with self._lock:
            self._state.decorator_invocations_total += n
        decorator_invocations_total.inc(n)

    def incr_decorator_failures(self, n: int = 1) -> None:
        with self._lock:
            self._state.decorator_failures_total += n
        decorator_failures_total.inc(n)

    def observe_decorator_overhead_ms(self, ms: float) -> None:
        decorator_overhead_ms.observe(ms)

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "records_appended": self._state.records_appended,
                "verifications_run": self._state.verifications_run,
                "integrity_breaks_detected": self._state.integrity_breaks_detected,
                "decorator_invocations_total": self._state.decorator_invocations_total,
                "decorator_failures_total": self._state.decorator_failures_total,
            }


# --- Prometheus instruments (module-level, registered once) ----------------

records_appended_total = Counter(
    "audit_records_appended_total",
    "Total audit records appended to the chain.",
)
verifications_total = Counter(
    "audit_verifications_total",
    "Total chain verifications run (full or range).",
)
verify_breaks_total = Counter(
    "audit_verify_breaks_total",
    "Total chain verifications that reported an integrity break.",
)
decorator_invocations_total = Counter(
    "audit_decorator_invocations_total",
    "Total times the @audit_access decorator wrapped a call.",
)
decorator_failures_total = Counter(
    "audit_decorator_failures_total",
    "Total times the wrapped function raised an exception.",
)
decorator_overhead_ms = Histogram(
    "audit_decorator_overhead_ms",
    "Per-call overhead of @audit_access in milliseconds.",
    buckets=(0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500),
)


# --- Process-wide singleton accessor --------------------------------------

_COUNTERS: Counters | None = None


def get_counters() -> Counters:
    global _COUNTERS
    if _COUNTERS is None:
        _COUNTERS = Counters()
    return _COUNTERS


def reset_counters_for_tests() -> None:
    """Test-only helper to drop the singleton (Prometheus registry stays)."""
    global _COUNTERS
    _COUNTERS = None
