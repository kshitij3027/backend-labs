"""Synthetic incident / event generators for tests, E2E, and load.

Two deterministic, seedable producers of :class:`LogEvent` batches:

* :func:`generate_incident` builds a realistic **cascading incident** with an
  injected ground-truth root cause — an early, high-severity, upstream event
  (``api-gateway`` ``CRITICAL`` at ``t0``) whose failure propagates to its direct
  downstream services and theirs, all within the temporal window and following the
  service dependency map's upstream -> downstream direction. It returns a
  :class:`Scenario` carrying the events plus the known root-cause id/service, so
  tests and the C10 E2E verifier can assert that a correct RCA ranks the injected
  root in the top 3 (it is the earliest event, the only ``CRITICAL``, and has the
  highest causal out-degree — the three terms the confidence formula rewards).
* :func:`generate_events` produces an arbitrary-size mixed batch for load testing.

Both use a **private** ``random.Random(seed)`` (never the global RNG) and an
injectable ``base_time`` (defaulting to a fixed constant) so output is fully
reproducible: the same arguments always yield byte-identical events and ids.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from src.models import LogEvent, LogLevel
from src.service_map import DEFAULT_SERVICE_DEPENDENCY_MAP

#: Fixed anchor so scenarios are reproducible without reading the wall clock.
DEFAULT_BASE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)

#: The upstream service seeded as the incident's ground-truth root cause. It has the
#: most direct downstream dependents in the map, so a correct causal graph gives it
#: the highest out-degree centrality.
ROOT_SERVICE = "api-gateway"

#: Max random jitter (seconds) added to a cascade event's base offset.
_JITTER_MAX = 2.0

#: Every known service (sorted for deterministic selection in generate_events).
_ALL_SERVICES: list[str] = sorted(DEFAULT_SERVICE_DEPENDENCY_MAP)

#: Realistic per-severity message fragments (chosen via the seeded RNG).
_MESSAGES: dict[LogLevel, list[str]] = {
    LogLevel.CRITICAL: [
        "upstream gateway unreachable",
        "request routing failure",
        "circuit breaker opened",
    ],
    LogLevel.ERROR: [
        "connection refused",
        "request timed out",
        "5xx from downstream dependency",
        "query execution failed",
    ],
    LogLevel.WARNING: [
        "elevated response latency",
        "retry budget nearly exhausted",
        "cache miss storm",
    ],
    LogLevel.INFO: [
        "health check ok",
        "configuration reloaded",
        "heartbeat",
    ],
}

#: The cascade following the root, as ``(service, level, base_offset_seconds)``.
#: Direction respects the dependency map: api-gateway -> {auth, user, payment} ->
#: {database, redis, file-storage, external-payment-api}. Several auth/user/payment
#: ERROR events give the root a dominant out-degree; a few INFO events are benign
#: noise (INFO never participates in causal edges).
_CASCADE: list[tuple[str, LogLevel, int]] = [
    ("auth", LogLevel.ERROR, 5),
    ("user", LogLevel.ERROR, 8),
    ("payment", LogLevel.ERROR, 11),
    ("database", LogLevel.ERROR, 15),
    ("redis", LogLevel.WARNING, 18),
    ("file-storage", LogLevel.WARNING, 22),
    ("auth", LogLevel.ERROR, 30),
    ("user", LogLevel.ERROR, 35),
    ("payment", LogLevel.ERROR, 40),
    ("database", LogLevel.ERROR, 45),
    ("external-payment-api", LogLevel.ERROR, 50),
    ("redis", LogLevel.INFO, 60),
    ("user", LogLevel.INFO, 70),
    ("api-gateway", LogLevel.INFO, 80),
]


@dataclass
class Scenario:
    """A generated incident plus its injected ground-truth root cause."""

    events: list[LogEvent]
    root_cause_event_id: str
    root_cause_service: str
    name: str


def _message(rng: random.Random, service: str, level: LogLevel) -> str:
    """A deterministic, realistic message for ``service`` at ``level``."""
    return f"[{service}] {rng.choice(_MESSAGES[level])}"


def generate_incident(seed: int = 0, base_time: datetime | None = None) -> Scenario:
    """Generate a deterministic cascading incident with a known root cause.

    The root is an ``api-gateway`` ``CRITICAL`` at ``t0``; its failure cascades to
    downstream services at increasing offsets (all within the 300s temporal window),
    with a few benign ``INFO`` events as noise. Every event gets a unique
    ``evt-{seed}-{i:03d}`` id; the root (index 0) is reported as the ground truth.

    Args:
        seed: Seeds a private RNG for jitter/message choice (reproducible per seed).
        base_time: Anchor for ``t0``; defaults to :data:`DEFAULT_BASE_TIME`.
    """
    rng = random.Random(seed)
    base_time = base_time or DEFAULT_BASE_TIME

    # Root first (offset 0), then the cascade — a single ordered spec list so ids are
    # assigned by chronological position and the root is deterministically index 0.
    specs: list[tuple[str, LogLevel, int]] = [
        (ROOT_SERVICE, LogLevel.CRITICAL, 0),
        *_CASCADE,
    ]

    events: list[LogEvent] = []
    for index, (service, level, base_offset) in enumerate(specs):
        # Draw jitter for every event (uniform RNG usage keeps output deterministic);
        # the root stays pinned at exactly t0 so it is always the earliest event.
        jitter = rng.uniform(0.0, _JITTER_MAX)
        offset = 0.0 if base_offset == 0 else base_offset + jitter
        timestamp = (base_time + timedelta(seconds=offset)).isoformat()
        events.append(
            LogEvent(
                timestamp=timestamp,
                service=service,
                level=level,
                message=_message(rng, service, level),
                event_id=f"evt-{seed}-{index:03d}",
            )
        )

    root = events[0]
    return Scenario(
        events=events,
        root_cause_event_id=root.event_id,
        root_cause_service=root.service,
        name=f"api-gateway-cascade-seed-{seed}",
    )


def generate_events(
    count: int, seed: int = 0, base_time: datetime | None = None
) -> list[LogEvent]:
    """Generate ``count`` mixed synthetic events for load testing.

    Fast and deterministic: services/levels are drawn from a private RNG, timestamps
    increase monotonically over a bounded span, and each event gets a unique
    ``evt-{seed}-{i:06d}`` id.

    Args:
        count: Number of events to produce (``count <= 0`` yields an empty list).
        seed: Seeds a private RNG (reproducible per seed).
        base_time: Start of the time span; defaults to :data:`DEFAULT_BASE_TIME`.
    """
    if count <= 0:
        return []

    rng = random.Random(seed)
    base_time = base_time or DEFAULT_BASE_TIME
    levels = [LogLevel.INFO, LogLevel.WARNING, LogLevel.ERROR, LogLevel.CRITICAL]
    weights = [0.55, 0.25, 0.15, 0.05]

    events: list[LogEvent] = []
    for i in range(count):
        service = rng.choice(_ALL_SERVICES)
        level = rng.choices(levels, weights=weights, k=1)[0]
        # Monotonic-ish offset with sub-second jitter keeps a dense, realistic stream.
        offset = i * 0.1 + rng.uniform(0.0, 0.5)
        timestamp = (base_time + timedelta(seconds=offset)).isoformat()
        events.append(
            LogEvent(
                timestamp=timestamp,
                service=service,
                level=level,
                message=_message(rng, service, level),
                event_id=f"evt-{seed}-{i:06d}",
            )
        )
    return events
