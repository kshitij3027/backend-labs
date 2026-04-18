"""Synthetic log generator.

Produces realistic-looking ``LogEntry`` rows with weighted service,
level, region, latency, and message distributions. The constants at
module top are the canonical lists — tests and later commits (C3+)
import from here rather than redefining their own.

Latency is biased toward the low end (90% normal-ish around 150ms,
10% heavy-tail up to 3s) so the ``latency_bucket`` generated column
in SQLite lights up with realistic counts across all four buckets.
"""

from __future__ import annotations

import random
import time
from typing import Iterator, List, Optional, Tuple
from uuid import uuid4

from src.models import LogEntry, LogLevel


# ---------------------------------------------------------------------------
# Canonical weighted choice lists. Sum roughly to 1.0 but random.choices
# normalizes for us so we don't have to be pedantic.
# ---------------------------------------------------------------------------

SERVICES: List[Tuple[str, float]] = [
    ("payments", 0.35),
    ("auth", 0.25),
    ("api-gateway", 0.20),
    ("cache", 0.10),
    ("orders", 0.10),
]

LEVELS: List[Tuple[str, float]] = [
    ("INFO", 0.70),
    ("WARN", 0.15),
    ("ERROR", 0.10),
    ("DEBUG", 0.04),
    ("FATAL", 0.01),
]

REGIONS: List[str] = ["us-east-1", "us-west-2", "eu-west-1", "ap-south-1"]


# Message templates keyed by level. Each template references a subset of
# ``{service}``, ``{latency}``, ``{request_id}``. Substrings like
# "timeout", "connection refused", "rate limit", "cache miss",
# "validation failed", "unauthorized", "ok", "processed" appear
# verbatim so free-text search tests can match them reliably.
MESSAGE_TEMPLATES: dict[str, List[str]] = {
    "DEBUG": [
        "{service} debug trace request_id={request_id}",
        "{service} cache miss lookup latency={latency}ms",
        "{service} processed request ok latency={latency}ms",
    ],
    "INFO": [
        "{service} request processed ok latency={latency}ms",
        "{service} served 200 ok for request_id={request_id}",
        "{service} cache hit latency={latency}ms",
        "{service} handled request ok",
    ],
    "WARN": [
        "{service} rate limit approaching request_id={request_id}",
        "{service} slow response latency={latency}ms threshold exceeded",
        "{service} cache miss fallback to origin",
        "{service} validation failed on optional field",
    ],
    "ERROR": [
        "{service} upstream timeout after {latency}ms request_id={request_id}",
        "{service} connection refused by downstream",
        "{service} rate limit exceeded for client",
        "{service} validation failed request_id={request_id}",
        "{service} unauthorized request rejected",
    ],
    "FATAL": [
        "{service} connection refused exhausted retries request_id={request_id}",
        "{service} fatal timeout latency={latency}ms aborting",
        "{service} unauthorized root token; shutting down",
    ],
}


def pick_weighted(choices: List[Tuple[str, float]]) -> str:
    """Return a single value from a ``[(value, weight), ...]`` list.

    Thin wrapper over ``random.choices`` so call-sites read cleanly.
    """
    values = [c[0] for c in choices]
    weights = [c[1] for c in choices]
    return random.choices(values, weights=weights, k=1)[0]


def random_latency_ms() -> float:
    """Return a single realistic latency in milliseconds.

    90% are drawn from a tight Gaussian around 150ms (clipped to
    [1, 500]) and 10% from a uniform heavy tail between 500 and
    3000ms. Final value is ``max(1.0, ...)`` so we never emit zero.
    """
    if random.random() < 0.9:
        v = random.gauss(150.0, 50.0)
        v = max(1.0, min(500.0, v))
    else:
        v = random.uniform(500.0, 3000.0)
    return max(1.0, v)


def generate_log_entry(
    now_ts: Optional[int] = None,
    window_hours: int = 24,
) -> LogEntry:
    """Produce one ``LogEntry`` with realistic field distributions."""
    now = now_ts if now_ts is not None else int(time.time())
    ts = random.randint(now - window_hours * 3600, now)

    service = pick_weighted(SERVICES)
    level: LogLevel = pick_weighted(LEVELS)  # type: ignore[assignment]
    region = random.choice(REGIONS)
    latency = round(random_latency_ms(), 2)
    source_ip = f"10.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}"
    request_id = f"req-{uuid4().hex[:12]}"

    template = random.choice(MESSAGE_TEMPLATES[level])
    message = template.format(
        service=service,
        latency=latency,
        request_id=request_id,
    )

    return LogEntry(
        ts=ts,
        service=service,
        level=level,
        region=region,
        response_time_ms=latency,
        source_ip=source_ip,
        request_id=request_id,
        message=message,
        metadata={"trace_id": uuid4().hex, "version": "v1"},
    )


def generate_batch(
    n: int,
    now_ts: Optional[int] = None,
    seed: Optional[int] = None,
) -> Iterator[LogEntry]:
    """Yield ``n`` ``LogEntry`` rows.

    When ``seed`` is supplied the module-level ``random`` is seeded so
    tests get reproducible output. Callers who care about isolation
    should pass a seed and accept the minor global-state quirk; we
    prefer this over threading a ``random.Random`` through every
    helper for test ergonomics.
    """
    if seed is not None:
        random.seed(seed)
    for _ in range(n):
        yield generate_log_entry(now_ts=now_ts)
