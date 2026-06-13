"""Deterministic synthetic structured-log generator.

This produces the kind of stream the delta encoder is built for: a wide,
mostly-static record where only a few fields move between consecutive lines.
Two behaviours make it a faithful exercise of the codec:

* **Low churn ⇒ high compressibility.** Each entry copies the previous one and
  mutates only ``round(churn * (schema_width - 1))`` of its non-timestamp
  fields, so most fields are omitted from the delta — which is what drives the
  60–80% storage-reduction target.
* **Schema drift on the ERROR path.** ERROR lines carry an extra ``error``
  field that non-ERROR lines lack, so entry width varies by ±1 as the level
  changes down the chain. This deterministically exercises the encoder's add
  (``~``) and remove (``-``) paths, not just value changes.

Everything is seeded through a *local* :class:`random.Random` so a given
``(count, seed, churn, schema_width)`` always yields byte-identical output and
the process-wide :mod:`random` state is never touched. All values are JSON-native
(``int`` / ``str`` / ``bool``); timestamps are integer epoch-ms that strictly
increase. There are no floats, ``NaN``/``Inf``, or ``datetime`` objects, and no
``None`` — a field is either present with a real value or genuinely absent.
"""
from __future__ import annotations

import random

from app.models import LogEntry
from app.settings import get_settings

# Ordered catalogue of candidate fields. ``ts`` is special (always present,
# always advancing) and is handled separately; the rest are the pool the schema
# is drawn from in this order. Keep this list >= 12 long so ``schema_width`` up
# to ~12 is meaningful before clamping kicks in.
_TS_FIELD = "ts"
_NON_TS_FIELDS: tuple[str, ...] = (
    "level",
    "service",
    "host",
    "trace_id",
    "status",
    "latency_ms",
    "msg",
    "bytes_sent",
    "region",
    "endpoint",
    "user_agent",
)

# Total catalogue size including ``ts`` — the upper clamp for ``schema_width``.
_CATALOGUE_SIZE = 1 + len(_NON_TS_FIELDS)

# Plausible value pools per field. Values are deliberately drawn from small
# vocabularies so that, at low churn, repeated draws frequently reproduce the
# previous value (extra-compressible) while still being realistic.
_LEVELS: tuple[str, ...] = ("INFO", "INFO", "INFO", "WARN", "ERROR")  # INFO-heavy
_SERVICES: tuple[str, ...] = (
    "auth-api",
    "billing",
    "gateway",
    "search",
    "notifications",
    "user-svc",
)
_HOSTS: tuple[str, ...] = (
    "node-1",
    "node-2",
    "node-7",
    "node-12",
    "edge-3",
    "edge-9",
)
_STATUSES: tuple[int, ...] = (200, 201, 204, 400, 404, 500, 503)
_MESSAGES: tuple[str, ...] = (
    "request completed",
    "request started",
    "cache miss",
    "cache hit",
    "upstream timeout",
    "validation failed",
    "retrying upstream",
)
_REGIONS: tuple[str, ...] = ("us-east-1", "us-west-2", "eu-west-1", "ap-south-1")
_ENDPOINTS: tuple[str, ...] = (
    "/v1/login",
    "/v1/charge",
    "/v1/search",
    "/v1/users",
    "/v1/health",
    "/v1/notify",
)
_USER_AGENTS: tuple[str, ...] = (
    "curl/8.4.0",
    "Mozilla/5.0",
    "okhttp/4.12",
    "python-httpx/0.28",
    "Go-http-client/2.0",
)
_ERROR_CODES: tuple[str, ...] = (
    "ETIMEDOUT",
    "ECONNRESET",
    "ECONNREFUSED",
    "EHOSTUNREACH",
    "EPIPE",
)


def _make_value(field: str, rng: random.Random, bytes_sent: int) -> object:
    """Return a fresh plausible value for ``field`` drawn from the local RNG.

    ``bytes_sent`` is treated as a roughly-monotonic counter: callers pass the
    previous value and we advance it by a positive delta, so it behaves like a
    cumulative byte meter rather than noise.
    """
    if field == "level":
        return rng.choice(_LEVELS)
    if field == "service":
        return rng.choice(_SERVICES)
    if field == "host":
        return rng.choice(_HOSTS)
    if field == "trace_id":
        # 12 hex chars — a short, realistic trace id as a plain string.
        return f"{rng.getrandbits(48):012x}"
    if field == "status":
        return rng.choice(_STATUSES)
    if field == "latency_ms":
        return rng.randint(1, 1500)
    if field == "msg":
        return rng.choice(_MESSAGES)
    if field == "bytes_sent":
        # Monotonic-ish cumulative counter (always a positive int delta).
        return bytes_sent + rng.randint(20, 4096)
    if field == "region":
        return rng.choice(_REGIONS)
    if field == "endpoint":
        return rng.choice(_ENDPOINTS)
    if field == "user_agent":
        return rng.choice(_USER_AGENTS)
    # Should be unreachable given the fixed catalogue; fail loudly if extended.
    raise ValueError(f"unknown field: {field!r}")


def _apply_error_field(entry: LogEntry, rng: random.Random) -> None:
    """Add or remove the ``error`` field so it tracks the entry's ``level``.

    ERROR lines gain a short error-code string; any other level drops the field
    entirely. This is the deterministic ±1 schema drift that drives the codec's
    add/remove paths. When ``level`` is not in the schema at all, ``error`` is
    always absent.
    """
    if entry.get("level") == "ERROR":
        entry["error"] = rng.choice(_ERROR_CODES)
    else:
        entry.pop("error", None)


def generate_logs(
    count: int,
    *,
    seed: int | None = None,
    churn: float = 0.2,
    schema_width: int = 8,
) -> list[LogEntry]:
    """Generate ``count`` synthetic structured log entries.

    Args:
        count: Number of entries to produce (must be >= 1).
        seed: Seed for the local RNG. With a seed the output is fully
            deterministic for a given ``(count, seed, churn, schema_width)``;
            ``None`` yields a non-deterministic run (but still never touches the
            global :mod:`random` state).
        churn: Fraction (0.0..1.0) of the non-timestamp fields that change from
            one entry to the next. Roughly ``round(churn * (schema_width - 1))``
            fields are mutated per step — low churn means highly compressible.
        schema_width: Number of base fields per entry, counting ``ts``. Clamped
            to the catalogue size. The schema is ``ts`` plus the first
            ``schema_width - 1`` non-ts fields. (The ``error`` field is *extra*
            on ERROR lines and not counted in this base width.)

    Returns:
        A list of ``count`` JSON-serializable entries in chronological order.
        Every value is a Python ``int`` / ``str`` / ``bool``; ``ts`` is integer
        epoch-ms and strictly increases down the list.
    """
    if count < 1:
        return []

    rng = random.Random(seed)

    # Resolve the schema: ts is always present; clamp width into [1, catalogue].
    width = max(1, min(int(schema_width), _CATALOGUE_SIZE))
    schema_non_ts: tuple[str, ...] = _NON_TS_FIELDS[: width - 1]

    # Number of non-ts fields to mutate per step. Clamp into [0, len(schema)] so
    # extreme churn values can't ask for more fields than exist.
    n_non_ts = len(schema_non_ts)
    per_step_changes = max(0, min(round(churn * n_non_ts), n_non_ts))

    logs: list[LogEntry] = []

    # --- Entry 0: full baseline, every schema field populated. ---
    # Start the clock at a fixed-ish epoch-ms point, jittered by the RNG so
    # different seeds begin at different (but deterministic) timestamps.
    ts = 1_700_000_000_000 + rng.randint(0, 1_000_000)
    bytes_sent = rng.randint(0, 8192)

    baseline: LogEntry = {_TS_FIELD: ts}
    for field in schema_non_ts:
        if field == "bytes_sent":
            bytes_sent = _make_value(field, rng, bytes_sent)  # type: ignore[assignment]
            baseline[field] = bytes_sent
        else:
            baseline[field] = _make_value(field, rng, bytes_sent)
    _apply_error_field(baseline, rng)
    logs.append(baseline)

    # --- Subsequent entries: copy previous, advance ts, mutate a few fields. ---
    prev = baseline
    for _ in range(1, count):
        entry: LogEntry = dict(prev)  # shallow copy is fine (values are scalars)

        # ts always advances by a small positive delta → strictly increasing.
        ts += rng.randint(1, 500)
        entry[_TS_FIELD] = ts

        # Choose which non-ts fields to mutate this step.
        if per_step_changes and schema_non_ts:
            for field in rng.sample(schema_non_ts, per_step_changes):
                if field == "bytes_sent":
                    bytes_sent = _make_value(field, rng, bytes_sent)  # type: ignore[assignment]
                    entry[field] = bytes_sent
                else:
                    entry[field] = _make_value(field, rng, bytes_sent)

        # Reconcile the optional error field with the (possibly changed) level.
        _apply_error_field(entry, rng)

        logs.append(entry)
        prev = entry

    return logs


def generate_from_settings(count: int, seed: int | None = None) -> list[LogEntry]:
    """Thin wrapper over :func:`generate_logs` using configured defaults.

    Reads ``generator_field_churn`` and ``generator_schema_width`` from
    :func:`app.settings.get_settings` for the churn / schema-width defaults.
    """
    settings = get_settings()
    return generate_logs(
        count,
        seed=seed,
        churn=settings.generator_field_churn,
        schema_width=settings.generator_schema_width,
    )
