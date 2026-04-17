from __future__ import annotations

import hashlib
import random
from datetime import timedelta

from src.shared.models import TimeRange


# Deterministic salt so that a given partition_id always produces the same
# synthetic dataset run-to-run and process-to-process. This is intentionally
# independent of Python's built-in hash(), which is randomized across
# interpreter invocations unless PYTHONHASHSEED is pinned.
_SEED_SALT = 0xC0FFEE_BEEF


_SERVICES: tuple[str, ...] = ("api", "auth", "db", "billing", "cache")


# Weighted level distribution. Implemented via cumulative weights so the
# generator stays O(1) per record.
_LEVELS: tuple[tuple[str, float], ...] = (
    ("INFO", 0.40),
    ("DEBUG", 0.30),
    ("WARN", 0.20),
    ("ERROR", 0.10),
)


# Weighted message pool. Keeping the pool small makes the output easy to
# reason about in tests. The ``timeout fetching resource`` entry carries a
# non-trivial weight so CONTAINS('timeout') tests are never empty.
_MESSAGES: tuple[tuple[str, float], ...] = (
    ("request completed", 0.30),
    ("query executed", 0.20),
    ("cache miss", 0.15),
    ("timeout fetching resource", 0.15),
    ("user login", 0.10),
    ("db connection dropped", 0.10),
)


# Status-code distribution weighted toward success.
_STATUS_CODES: tuple[tuple[int, float], ...] = (
    (200, 0.60),
    (201, 0.15),
    (400, 0.10),
    (404, 0.08),
    (500, 0.07),
)


def _derive_seed(partition_id: str) -> int:
    """Return a deterministic 64-bit seed derived from ``partition_id``.

    We avoid Python's non-deterministic string ``hash()`` by running the id
    through SHA-256 and taking the first 8 bytes as an unsigned int, then
    XOR-ing with a fixed salt for additional dispersion.
    """

    digest = hashlib.sha256(partition_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big", signed=False) ^ _SEED_SALT


def _weighted_pick(rng: random.Random, choices: tuple[tuple, ...]):
    """Draw a value from a (value, weight) tuple list using ``rng``."""

    total = sum(weight for _, weight in choices)
    r = rng.random() * total
    upto = 0.0
    for value, weight in choices:
        upto += weight
        if r <= upto:
            return value
    # Floating-point slack: fall through to the last entry.
    return choices[-1][0]


def generate_logs(
    partition_id: str, time_range: TimeRange, count: int
) -> list[dict]:
    """Generate ``count`` deterministic synthetic log records.

    - Timestamps are uniformly distributed inside ``time_range`` as ISO 8601
      strings (via ``datetime.isoformat()``), matching how the planner emits
      time literals downstream.
    - Levels, services, messages, and status codes are drawn from weighted
      pools.
    - ``duration_ms`` uses ``expovariate(1/50)`` for a lognormal-ish tail,
      floored at 1.
    - The returned list is sorted by timestamp ascending so the partition's
      timestamp index can bisect into it directly.
    """

    if count < 0:
        raise ValueError("count must be non-negative")

    rng = random.Random(_derive_seed(partition_id))

    start = time_range.start
    end = time_range.end
    span_seconds = (end - start).total_seconds()
    if span_seconds <= 0:
        raise ValueError("time_range must have positive duration")

    records: list[dict] = []
    for _ in range(count):
        offset_seconds = rng.random() * span_seconds
        ts = start + timedelta(seconds=offset_seconds)
        # Strip tzinfo for consistent naive-UTC comparisons with partition
        # metadata. Timestamps round-trip as ISO 8601 strings.
        if ts.tzinfo is not None:
            ts = ts.replace(tzinfo=None)

        record = {
            "timestamp": ts.isoformat(),
            "level": _weighted_pick(rng, _LEVELS),
            "service": _SERVICES[rng.randrange(len(_SERVICES))],
            "message": _weighted_pick(rng, _MESSAGES),
            "status_code": _weighted_pick(rng, _STATUS_CODES),
            "duration_ms": max(1, int(rng.expovariate(1 / 50.0))),
        }
        records.append(record)

    records.sort(key=lambda r: r["timestamp"])
    return records
