"""Deterministic synthetic log generator for the clustering engine.

This module fabricates realistic streaming logs with *embedded, discoverable*
patterns so the downstream clustering algorithms (K-means / DBSCAN / HDBSCAN)
have real structure to rediscover. Output is **fully deterministic** for a given
seed: every random choice flows through a local :class:`random.Random` instance and
timestamps are anchored to a *fixed* reference datetime (never ``datetime.now()``).

The records validate against :class:`src.schemas.LogEntry` and carry variable
tokens (IP addresses, numeric ids, request paths, latencies) so the feature
pipeline's message-masking has something to normalize.

Pattern families (content/structural signal)
---------------------------------------------
* ``security``    — failed/invalid logins, brute force from a small "bad IP"
  pool, unauthorized access, expired/invalid tokens. Mostly ``auth``,
  levels WARN/ERROR/CRITICAL.
* ``performance`` — slow query, high latency, request timeout, connection pool
  exhausted, GC pause. Elevated ``response_time_ms`` (~800-5000ms) on
  ``database``/``api-gateway``, levels WARN/ERROR.
* ``error``       — unhandled exception, 500 internal error, null reference,
  connection refused, disk full. ``status_code`` 500/503, levels ERROR/CRITICAL.
* ``normal``      — routine INFO traffic (request served 200, healthcheck ok,
  cache hit, job completed) with low latencies. The bulk of traffic.

Temporal patterns (>= 5 distinct, time signal)
----------------------------------------------
1. **Nightly batch error spike** — elevated error/critical rate every day in the
   02:00-03:00 hour (a nightly batch job misbehaving).
2. **Tuesday-morning auth brute-force burst** — concentrated security/brute-force
   traffic on Tuesdays roughly 07:00-10:00.
3. **Business-hours performance degradation** — slow queries / high latency are
   far more likely on weekdays 09:00-17:00 (peak load).
4. **Top-of-the-hour cache-miss storm** — cache-miss / cold-cache performance
   blips cluster in the first few minutes of every hour.
5. **Friday-evening payment peak** — payment-service volume surges Friday
   17:00-21:00 (end-of-week purchasing).
6. **Quiet weekend overnights** — sparse, almost-entirely-normal traffic during
   weekend small hours (00:00-05:00 Sat/Sun).

These biases are applied to the *family mix*, *level*, and *latency* as a function
of hour-of-day and weekday, so the patterns are statistically visible (e.g. the
share of ERROR/CRITICAL logs in the 02:00 hour is materially above baseline).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from random import Random

from src.schemas import LogEntry

# --------------------------------------------------------------------------- #
# Public constants
# --------------------------------------------------------------------------- #

#: Services that emit logs. ``payment`` participates in the Friday-evening peak.
SERVICES: list[str] = [
    "auth",
    "api-gateway",
    "payment",
    "database",
    "web",
    "cache",
    "worker",
]

#: The four pattern families clustering should rediscover.
FAMILIES: tuple[str, ...] = ("security", "performance", "error", "normal")

#: Fixed reference "now" — the default end of the generated time window. Keeping
#: this constant (rather than ``datetime.now()``) is what makes the corpus
#: reproducible across runs and machines.
REFERENCE_NOW: datetime = datetime(2026, 6, 23, 12, 0, 0)

# Small pool of "bad" source IPs used by the brute-force / security family so a
# clustering pass can latch onto the repeated offenders.
_BAD_IPS: tuple[str, ...] = (
    "203.0.113.7",
    "203.0.113.42",
    "198.51.100.13",
    "198.51.100.99",
    "185.220.101.5",
)

_HTTP_PATHS: tuple[str, ...] = (
    "/api/v1/login",
    "/api/v1/users",
    "/api/v1/orders",
    "/api/v1/payments",
    "/api/v1/search",
    "/api/v1/cart",
    "/healthz",
    "/metrics",
)

_USER_AGENTS_ENDPOINTS = _HTTP_PATHS  # alias kept for readability below

# --------------------------------------------------------------------------- #
# Token helpers (all take an rng so they stay deterministic)
# --------------------------------------------------------------------------- #


def _rand_ip(rng: Random) -> str:
    """A pseudo-random private-range IPv4 address."""
    return f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"


def _rand_id(rng: Random, prefix: str) -> str:
    """A ``prefix``-tagged hex id token (e.g. ``req-9f3a2b``)."""
    return f"{prefix}-{rng.randint(0, 0xFFFFFF):06x}"


def _rand_path(rng: Random) -> str:
    return rng.choice(_HTTP_PATHS)


# --------------------------------------------------------------------------- #
# Temporal biasing
# --------------------------------------------------------------------------- #


def _family_weights(rng: Random, ts: datetime) -> dict[str, float]:
    """Return relative family weights for ``ts``, encoding the temporal patterns.

    Baseline is heavily ``normal``; specific hour/weekday windows tilt the mix so
    the embedded temporal patterns become statistically detectable.
    """
    hour = ts.hour
    weekday = ts.weekday()  # Mon=0 .. Sun=6
    minute = ts.minute
    is_weekend = weekday >= 5

    # Baseline mix: mostly normal traffic with a light sprinkle of issues.
    w = {"normal": 80.0, "performance": 8.0, "error": 7.0, "security": 5.0}

    # (1) Nightly batch error spike, 02:00-02:59 every day.
    if hour == 2:
        w["error"] += 45.0
        w["normal"] -= 25.0

    # (2) Tuesday-morning auth brute-force burst, 07:00-09:59.
    if weekday == 1 and 7 <= hour < 10:
        w["security"] += 55.0
        w["normal"] -= 30.0

    # (3) Business-hours performance degradation, weekdays 09:00-16:59.
    if (not is_weekend) and 9 <= hour < 17:
        w["performance"] += 30.0
        w["normal"] -= 18.0

    # (4) Top-of-the-hour cache-miss storm, first 5 minutes of every hour.
    if minute < 5:
        w["performance"] += 18.0
        w["normal"] -= 10.0

    # (5) Friday-evening payment peak handled in volume + service selection, but
    #     nudge normal (purchase) traffic up so the peak reads as healthy load.
    if weekday == 4 and 17 <= hour < 21:
        w["normal"] += 20.0

    # (6) Quiet weekend overnights, Sat/Sun 00:00-04:59 — almost entirely normal,
    #     and what little happens is calm.
    if is_weekend and hour < 5:
        w = {"normal": 96.0, "performance": 2.0, "error": 1.0, "security": 1.0}

    # Clamp to non-negative.
    return {k: max(v, 0.5) for k, v in w.items()}


def _hour_volume_weight(ts: datetime) -> float:
    """A relative likelihood that a sampled event lands in ``ts``'s hour.

    Drives *volume* temporal patterns: busy business hours, the Friday-evening
    payment peak, and quiet weekend overnights. Used to weight which timestamp a
    given log is assigned, so denser periods genuinely receive more logs.
    """
    hour = ts.hour
    weekday = ts.weekday()
    is_weekend = weekday >= 5

    weight = 1.0

    # Diurnal shape: quieter overnight, ramp through the working day. Kept gentle
    # so even quiet hours retain enough volume for their patterns to be visible.
    if 9 <= hour < 18:
        weight *= 1.8
    elif 6 <= hour < 9 or 18 <= hour < 22:
        weight *= 1.2
    elif hour < 6:
        weight *= 0.6

    # Weekends are generally lighter.
    if is_weekend:
        weight *= 0.7

    # (5) Friday-evening payment peak — extra volume Fri 17:00-20:59.
    if weekday == 4 and 17 <= hour < 21:
        weight *= 2.2

    # (1) Nightly batch window keeps solid volume at 02:00 (overriding the
    #     overnight dip) so its elevated error rate is statistically detectable.
    if hour == 2:
        weight *= 2.5

    return weight


# --------------------------------------------------------------------------- #
# Per-family record construction
# --------------------------------------------------------------------------- #


def _make_security(rng: Random, ts: datetime) -> LogEntry:
    bad_ip = rng.choice(_BAD_IPS)
    user = _rand_id(rng, "user")
    templates = [
        (f"Failed login attempt for {user} from {bad_ip}", "WARN", 401),
        (
            f"Multiple failed login attempts detected from {bad_ip} "
            f"({rng.randint(5, 40)} attempts)",
            "ERROR",
            429,
        ),
        (f"Invalid credentials for {user}", "WARN", 401),
        (f"Unauthorized access to {_rand_path(rng)} from {bad_ip}", "ERROR", 403),
        (f"Authentication token expired for session {_rand_id(rng, 'sess')}", "WARN", 401),
        (f"Invalid token signature from {bad_ip}", "ERROR", 401),
        (
            f"Brute force attack suspected from {bad_ip} on /api/v1/login",
            "CRITICAL",
            429,
        ),
    ]
    message, level, status = rng.choice(templates)
    return LogEntry(
        timestamp=ts,
        service="auth",
        level=level,
        message=message,
        source_ip=bad_ip,
        endpoint="/api/v1/login",
        response_time_ms=round(rng.uniform(20.0, 220.0), 1),
        status_code=status,
    )


def _make_performance(rng: Random, ts: datetime) -> LogEntry:
    service = rng.choice(["database", "api-gateway"])
    latency = round(rng.uniform(800.0, 5000.0), 1)
    path = _rand_path(rng)
    templates = [
        (f"Slow query detected on table orders took {latency:.0f}ms", "WARN", 200),
        (f"High latency serving {path}: {latency:.0f}ms", "WARN", 200),
        (f"Request timeout after {latency:.0f}ms on {path}", "ERROR", 504),
        (
            f"Connection pool exhausted ({rng.randint(50, 200)}/"
            f"{rng.randint(50, 200)} in use)",
            "ERROR",
            503,
        ),
        (f"GC pause of {rng.randint(200, 1200)}ms degraded throughput", "WARN", 200),
    ]
    message, level, status = rng.choice(templates)
    return LogEntry(
        timestamp=ts,
        service=service,
        level=level,
        message=message,
        source_ip=_rand_ip(rng),
        endpoint=path,
        response_time_ms=latency,
        status_code=status,
    )


def _make_error(rng: Random, ts: datetime) -> LogEntry:
    service = rng.choice(["api-gateway", "worker", "database", "web", "payment"])
    path = _rand_path(rng)
    templates = [
        (
            f"Unhandled exception in {_rand_id(rng, 'req')}: NullPointerException",
            "ERROR",
            500,
        ),
        (f"500 internal server error serving {path}", "ERROR", 500),
        (f"Null reference accessing user profile {_rand_id(rng, 'user')}", "ERROR", 500),
        (f"Connection refused to upstream {_rand_ip(rng)}:5432", "CRITICAL", 503),
        (f"Disk full on volume /data ({rng.randint(95, 100)}% used)", "CRITICAL", 507),
    ]
    message, level, status = rng.choice(templates)
    return LogEntry(
        timestamp=ts,
        service=service,
        level=level,
        message=message,
        source_ip=_rand_ip(rng),
        endpoint=path,
        response_time_ms=round(rng.uniform(50.0, 900.0), 1),
        status_code=status,
    )


def _make_normal(rng: Random, ts: datetime) -> LogEntry:
    weekday = ts.weekday()
    hour = ts.hour
    # During the Friday-evening peak, bias normal traffic toward payment.
    if weekday == 4 and 17 <= hour < 21 and rng.random() < 0.5:
        service = "payment"
    else:
        service = rng.choice(SERVICES)

    path = _rand_path(rng)
    latency = round(rng.uniform(2.0, 180.0), 1)
    templates = [
        (f"Request served {path} 200 in {latency:.0f}ms", "INFO", 200),
        ("Health check ok", "INFO", 200),
        (f"Cache hit for key {_rand_id(rng, 'key')}", "INFO", 200),
        (f"Job {_rand_id(rng, 'job')} completed successfully", "INFO", 200),
        (f"User {_rand_id(rng, 'user')} logged in successfully", "INFO", 200),
        (f"Payment {_rand_id(rng, 'pay')} processed for ${rng.randint(5, 500)}", "INFO", 200),
        (f"Cache miss for key {_rand_id(rng, 'key')}, fetching from origin", "DEBUG", 200),
    ]
    message, level, status = rng.choice(templates)
    return LogEntry(
        timestamp=ts,
        service=service,
        level=level,
        message=message,
        source_ip=_rand_ip(rng),
        endpoint=path,
        response_time_ms=latency,
        status_code=status,
    )


_FAMILY_BUILDERS = {
    "security": _make_security,
    "performance": _make_performance,
    "error": _make_error,
    "normal": _make_normal,
}


def _weighted_choice(rng: Random, weights: dict[str, float]) -> str:
    """Deterministically pick a key from ``weights`` proportional to its value."""
    keys = list(weights.keys())
    vals = [weights[k] for k in keys]
    return rng.choices(keys, weights=vals, k=1)[0]


def _sample_timestamp(rng: Random, start: datetime, span_hours: int) -> datetime:
    """Sample a timestamp in ``[start - span_hours, start]`` biased by hour volume.

    We draw a candidate, then accept/reject against the hour-volume weight so busy
    windows (business hours, Friday evening) receive proportionally more logs and
    quiet windows (weekend overnights) receive fewer — making the volume-based
    temporal patterns real rather than uniform noise.
    """
    span_seconds = max(span_hours, 1) * 3600
    # Best-of-k rejection sampling: draws a few candidates and keeps the one whose
    # hour-volume weight wins, biasing toward busy windows while keeping quiet
    # windows populated. k is small so the bias tracks the nominal weights rather
    # than exaggerating them; the fixed cap keeps sampling deterministic.
    best: datetime | None = None
    best_score = -1.0
    for _ in range(3):
        offset = rng.randint(0, span_seconds)
        cand = start - timedelta(seconds=offset)
        w = _hour_volume_weight(cand)
        score = rng.random() * w  # higher-weight hours win more often
        if score > best_score:
            best_score = score
            best = cand
    assert best is not None  # loop runs >= 1 time
    return best


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


def generate_logs(
    n: int,
    seed: int = 42,
    start: "datetime | None" = None,
    span_hours: int = 168,
) -> list[LogEntry]:
    """Generate ``n`` deterministic synthetic :class:`LogEntry` records.

    Args:
        n: Number of logs to produce.
        seed: Seed for the local RNG. Same seed -> identical output.
        start: End of the time window (logs span ``[start - span_hours, start]``).
            Defaults to the fixed :data:`REFERENCE_NOW` for reproducibility.
        span_hours: Width of the time window in hours (default 168 = 7 days).

    Returns:
        A list of ``n`` ``LogEntry`` objects, sorted ascending by timestamp, with a
        realistic family/level/service mix and embedded temporal patterns.
    """
    if n <= 0:
        return []

    rng = Random(seed)
    end = start if start is not None else REFERENCE_NOW

    logs: list[LogEntry] = []
    for _ in range(n):
        ts = _sample_timestamp(rng, end, span_hours)
        family = _weighted_choice(rng, _family_weights(rng, ts))
        logs.append(_FAMILY_BUILDERS[family](rng, ts))

    logs.sort(key=lambda e: e.timestamp)
    return logs


def generate_pattern_batch(family: str, n: int, seed: int = 42) -> list[LogEntry]:
    """Generate ``n`` logs of a single ``family`` for targeted tests/demo.

    Args:
        family: One of ``"security"``, ``"performance"``, ``"error"``, ``"normal"``.
        n: Number of logs to produce.
        seed: Seed for the local RNG (deterministic).

    Returns:
        A list of ``n`` ``LogEntry`` records all belonging to ``family``, sorted by
        timestamp.

    Raises:
        ValueError: If ``family`` is not a known family.
    """
    if family not in _FAMILY_BUILDERS:
        raise ValueError(
            f"unknown family {family!r}; expected one of {sorted(_FAMILY_BUILDERS)}"
        )
    if n <= 0:
        return []

    rng = Random(seed)
    builder = _FAMILY_BUILDERS[family]
    logs: list[LogEntry] = []
    for _ in range(n):
        ts = _sample_timestamp(rng, REFERENCE_NOW, 168)
        logs.append(builder(rng, ts))
    logs.sort(key=lambda e: e.timestamp)
    return logs


def write_corpus(path: str, n: int = 800, seed: int = 42) -> int:
    """Generate ``n`` logs and write them as JSON Lines to ``path``.

    Each line is one ``LogEntry.model_dump(mode="json")`` serialized as JSON. Parent
    directories are created if needed.

    Args:
        path: Destination file path.
        n: Number of logs to generate (default 800).
        seed: RNG seed (deterministic).

    Returns:
        The number of lines written.
    """
    import json

    logs = generate_logs(n, seed=seed)
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as fh:
        for entry in logs:
            fh.write(json.dumps(entry.model_dump(mode="json")))
            fh.write("\n")
    return len(logs)


if __name__ == "__main__":
    # Regenerate the committed sample corpus used by demo mode and tests.
    repo_root = Path(__file__).resolve().parents[1]
    sample_path = repo_root / "data" / "sample.jsonl"
    written = write_corpus(str(sample_path), n=800, seed=42)
    print(f"wrote {written} logs to {sample_path}")
