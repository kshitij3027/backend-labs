"""Deterministic, template-driven synthetic log generator (Commit 2).

This module is the **training-data source** for the ML Log Classifier. It emits
~1000 realistic, *labeled* log entries spread across three services
(``web`` / ``database`` / ``cache``). Each entry is a flat dict that doubles as
one JSONL line and carries the **ground-truth** labels a downstream model learns
to predict:

    {
      "raw_log":   "<timestamped, noisy, free-text log line>",
      "service":   "web" | "database" | "cache",
      "severity":  "DEBUG" | "INFO" | "WARN" | "ERROR" | "CRITICAL",
      "category":  "SYSTEM" | "AUTH" | "NETWORK" | "DATABASE"
                   | "PERFORMANCE" | "SECURITY" | "APPLICATION",
      "timestamp": "2026-06-21T15:32:10.123456"   # ISO-8601 (also in raw_log)
    }

Design goals
------------
* **Label fidelity.** Every template hard-codes its ``(service, severity,
  category)`` triple. The same message pattern *always* yields the same labels,
  so the encoded mapping is perfectly learnable (supports the 90%+ accuracy
  success criterion). In particular the canonical example
  ``"Database connection failed with timeout error"`` is labeled **SYSTEM**
  (an infrastructure connection/timeout failure), *not* DATABASE.
* **Realistic noise.** ``raw_log`` embeds tokens that preprocessing later strips
  or normalizes: an ISO timestamp, an IP (usually IPv4, sometimes IPv6),
  frequently a UUID, plus HTTP method/path/status, latency ``NNNms``,
  ``key=...``/``conn_id=``/``req_id=`` pairs and ports. These are varied via the
  RNG.
* **Determinism.** All randomness flows through a single local
  ``random.Random(seed)``. Embedded UUIDs are built from RNG bytes and timestamps
  are derived by adding RNG-chosen offsets to a *fixed* base datetime — never
  ``datetime.now()`` or an unseeded ``uuid`` source. Same ``(count, seed)`` ⇒
  byte-identical output.

The generator deliberately depends on the **standard library only**
(``random`` / ``datetime`` / ``json`` / ``argparse``); preprocessing, feature
extraction and modeling live in later commits.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from random import Random
from typing import Any, Iterable

from src.config import get_config

# ---------------------------------------------------------------------------
# Allowed label values (the public taxonomy — see plan.md §2).
# ---------------------------------------------------------------------------

#: Source services the generator (and the rest of the pipeline) understand.
SERVICES: tuple[str, ...] = ("web", "database", "cache")

#: Severity levels, ordered from least to most severe.
SEVERITIES: tuple[str, ...] = ("DEBUG", "INFO", "WARN", "ERROR", "CRITICAL")

#: Semantic categories. SYSTEM = infrastructure/system-level failures (connection
#: failures, timeouts, OOM, crashes, disk-full); DATABASE = query-level DB
#: operations (slow query, deadlock, transaction, index/lock).
CATEGORIES: tuple[str, ...] = (
    "SYSTEM",
    "AUTH",
    "NETWORK",
    "DATABASE",
    "PERFORMANCE",
    "SECURITY",
    "APPLICATION",
)

_SERVICE_SET = frozenset(SERVICES)
_SEVERITY_SET = frozenset(SEVERITIES)
_CATEGORY_SET = frozenset(CATEGORIES)

# Fixed base instant for all derived timestamps. Using a constant (rather than
# ``datetime.now()``) is what keeps the generator reproducible.
_BASE_TIME = datetime(2026, 6, 21, 0, 0, 0, tzinfo=timezone.utc)

# Relative weights for the realistic severity mix. Most logs are INFO/DEBUG; a
# handful are CRITICAL. A per-(service, severity) top-up later guarantees no
# class ends up too sparse to train on.
_SEVERITY_WEIGHTS: dict[str, float] = {
    "DEBUG": 0.22,
    "INFO": 0.40,
    "WARN": 0.20,
    "ERROR": 0.14,
    "CRITICAL": 0.04,
}

# Minimum number of samples to guarantee for each (service, severity) pair and
# for each category, after the initial weighted draw. Keeps every class
# learnable.
_MIN_PER_SERVICE_SEVERITY = 15
_MIN_PER_CATEGORY = 15


# ---------------------------------------------------------------------------
# Template table.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LogTemplate:
    """A single labeled message pattern.

    ``message`` is a free-text string with ``{placeholder}`` slots filled by
    :func:`_render` (e.g. ``{ip}``, ``{uuid}``, ``{ms}``, ``{path}``,
    ``{status}``, ``{port}``, ``{key}``, ``{n}``). The ``service`` / ``severity``
    / ``category`` fields are the immutable ground-truth labels this pattern
    encodes.
    """

    service: str
    severity: str
    category: str
    message: str


# 36 templates: every service, every severity and every category appears at
# least once, and several SYSTEM templates describe connection/timeout failures
# across all three services (so the canonical DB-timeout example classifies as
# SYSTEM with high confidence).
_TEMPLATES: tuple[LogTemplate, ...] = (
    # ---- SYSTEM: infrastructure connection / timeout / resource failures ----
    LogTemplate(
        "database", "ERROR", "SYSTEM",
        "Database connection failed with timeout error after {ms}ms conn_id={uuid}",
    ),
    LogTemplate(
        "database", "CRITICAL", "SYSTEM",
        "Unable to connect to primary database host {ip}:{port} connection refused",
    ),
    LogTemplate(
        "web", "ERROR", "SYSTEM",
        "Upstream service connection timed out after {ms}ms req_id={uuid}",
    ),
    LogTemplate(
        "web", "CRITICAL", "SYSTEM",
        "Worker process crashed unexpectedly and is being restarted pid={n}",
    ),
    LogTemplate(
        "cache", "ERROR", "SYSTEM",
        "Cache server {ip}:{port} connection lost, failing over to replica",
    ),
    LogTemplate(
        "cache", "CRITICAL", "SYSTEM",
        "Out of memory: unable to allocate {n}MB for cache, evicting all keys",
    ),
    LogTemplate(
        "database", "ERROR", "SYSTEM",
        "Disk full on data volume {path}, write operations suspended",
    ),
    LogTemplate(
        "web", "WARN", "SYSTEM",
        "Health check timed out for backend {ip}:{port} after {ms}ms, retrying",
    ),

    # ---- DATABASE: query-level operations ----
    LogTemplate(
        "database", "WARN", "DATABASE",
        "Slow query detected took {ms}ms query_id={uuid} rows={n}",
    ),
    LogTemplate(
        "database", "ERROR", "DATABASE",
        "Deadlock detected while acquiring lock on table orders txn={uuid}",
    ),
    LogTemplate(
        "database", "INFO", "DATABASE",
        "Transaction committed successfully txn={uuid} duration={ms}ms",
    ),
    LogTemplate(
        "database", "INFO", "DATABASE",
        "Transaction rolled back txn={uuid} reason=constraint_violation",
    ),
    LogTemplate(
        "database", "DEBUG", "DATABASE",
        "Query executed plan=index_scan rows_examined={n} took={ms}ms",
    ),
    LogTemplate(
        "database", "DEBUG", "DATABASE",
        "Rebuilding index idx_users_email on shard {n} progress={status}",
    ),

    # ---- AUTH ----
    LogTemplate(
        "web", "INFO", "AUTH",
        "User login successful user_id={n} session={uuid} from {ip}",
    ),
    LogTemplate(
        "web", "WARN", "AUTH",
        "Login failed invalid password for user_id={n} from {ip} attempts={n}",
    ),
    LogTemplate(
        "web", "INFO", "AUTH",
        "Access token issued for client_id={uuid} expires_in={n}s",
    ),
    LogTemplate(
        "web", "WARN", "AUTH",
        "Access token expired for session={uuid}, prompting re-authentication",
    ),
    LogTemplate(
        "database", "ERROR", "AUTH",
        "Permission denied: user_id={n} lacks role for schema admin op={key}",
    ),

    # ---- NETWORK ----
    LogTemplate(
        "web", "ERROR", "NETWORK",
        "DNS resolution failed for upstream api.internal host_unreachable {ip}",
    ),
    LogTemplate(
        "web", "WARN", "NETWORK",
        "TLS handshake failed with peer {ip}:{port} cipher_mismatch",
    ),
    LogTemplate(
        "cache", "WARN", "NETWORK",
        "Packet loss {n}% detected on link to cache node {ip} latency={ms}ms",
    ),
    LogTemplate(
        "web", "ERROR", "NETWORK",
        "Connection refused by gateway {ip}:{port} route=upstream req_id={uuid}",
    ),
    LogTemplate(
        "cache", "INFO", "NETWORK",
        "Reconnected to cache cluster node {ip}:{port} after network blip",
    ),

    # ---- PERFORMANCE ----
    LogTemplate(
        "web", "WARN", "PERFORMANCE",
        "Slow response for {method} {path} took {ms}ms status={status}",
    ),
    LogTemplate(
        "cache", "INFO", "PERFORMANCE",
        "Cache hit rate {n}% over last window, evictions={n}",
    ),
    LogTemplate(
        "cache", "WARN", "PERFORMANCE",
        "High cache eviction rate {n} keys/s, memory pressure detected",
    ),
    LogTemplate(
        "web", "WARN", "PERFORMANCE",
        "GC pause of {ms}ms exceeded budget, heap_used={n}MB",
    ),
    LogTemplate(
        "database", "WARN", "PERFORMANCE",
        "Connection pool queue depth {n}, requests waiting avg={ms}ms",
    ),
    LogTemplate(
        "web", "INFO", "PERFORMANCE",
        "Throughput {n} req/s sustained, p95 latency {ms}ms",
    ),

    # ---- SECURITY ----
    LogTemplate(
        "web", "ERROR", "SECURITY",
        "Suspicious activity: possible SQL injection attempt blocked from {ip} payload={key}",
    ),
    LogTemplate(
        "web", "WARN", "SECURITY",
        "Rate limit triggered for client {ip} req_id={uuid}, throttling requests",
    ),
    LogTemplate(
        "web", "CRITICAL", "SECURITY",
        "Blocked IP {ip} after repeated authentication anomalies score={n}",
    ),

    # ---- APPLICATION: business logic / validation / lifecycle ----
    LogTemplate(
        "web", "INFO", "APPLICATION",
        "Request handled {method} {path} status={status} took {ms}ms req_id={uuid}",
    ),
    LogTemplate(
        "web", "WARN", "APPLICATION",
        "Validation error on field email for order={uuid}, rejecting request",
    ),
    LogTemplate(
        "web", "DEBUG", "APPLICATION",
        "Feature flag {key} evaluated to {status} for user_id={n}",
    ),
    LogTemplate(
        "cache", "INFO", "APPLICATION",
        "Configuration reloaded from {path}, {n} keys applied",
    ),
)


# ---------------------------------------------------------------------------
# Seeded "noise" helpers — every value derives from the local RNG.
# ---------------------------------------------------------------------------

_HTTP_METHODS = ("GET", "POST", "PUT", "DELETE", "PATCH")
_HTTP_PATHS = (
    "/api/v1/users",
    "/api/v1/orders",
    "/api/v1/login",
    "/api/v1/cache/get",
    "/api/v1/products",
    "/healthz",
    "/metrics",
    "/api/v1/sessions",
)
_HTTP_STATUSES = ("200", "201", "204", "400", "401", "403", "404", "429", "500", "503")
_KEY_NAMES = (
    "feature_x",
    "dark_mode",
    "op_read",
    "op_write",
    "beta_search",
    "payload_a",
    "rollout_b",
)


def _rng_uuid(rng: Random) -> str:
    """Build an RFC-4122-shaped UUIDv4 string purely from ``rng`` bytes.

    We construct it manually (rather than ``uuid.uuid4()``) so the value is fully
    determined by the seeded RNG and therefore reproducible.
    """
    b = bytearray(rng.getrandbits(8) for _ in range(16))
    b[6] = (b[6] & 0x0F) | 0x40  # version 4
    b[8] = (b[8] & 0x3F) | 0x80  # RFC 4122 variant
    h = b.hex()
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _rng_ipv4(rng: Random) -> str:
    """Return a private-range-ish IPv4 string derived from ``rng``."""
    return f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"


def _rng_ipv6(rng: Random) -> str:
    """Return an IPv6 string derived from ``rng`` (used for occasional variety)."""
    groups = [f"{rng.getrandbits(16):04x}" for _ in range(8)]
    return ":".join(groups)


def _rng_ip(rng: Random) -> str:
    """Return an IP address — IPv4 most of the time, IPv6 occasionally."""
    return _rng_ipv6(rng) if rng.random() < 0.15 else _rng_ipv4(rng)


def _rng_timestamp(rng: Random) -> datetime:
    """Derive a timestamp by offsetting :data:`_BASE_TIME` by RNG-chosen amounts."""
    offset = timedelta(
        seconds=rng.randint(0, 7 * 24 * 3600 - 1),  # within a one-week window
        milliseconds=rng.randint(0, 999),
        microseconds=rng.randint(0, 999),
    )
    return _BASE_TIME + offset


def _iso_z(dt: datetime) -> str:
    """Render ``dt`` as an ISO-8601 string with a millisecond ``Z`` suffix.

    Example: ``2026-06-21T15:32:10.123Z`` — the noisy form embedded in
    ``raw_log`` for preprocessing to strip.
    """
    millis = dt.microsecond // 1000
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{millis:03d}Z"


def _render(message: str, rng: Random) -> str:
    """Fill the ``{placeholder}`` slots in ``message`` using ``rng``.

    Only the placeholders actually present are computed. Unknown placeholders are
    left intact (defensive — should not happen with the curated templates).
    """
    # Cheap membership checks avoid generating values a template doesn't use.
    values: dict[str, Any] = {}
    if "{ip}" in message:
        values["ip"] = _rng_ip(rng)
    if "{uuid}" in message:
        values["uuid"] = _rng_uuid(rng)
    if "{ms}" in message:
        values["ms"] = rng.randint(1, 30000)
    if "{path}" in message:
        values["path"] = rng.choice(_HTTP_PATHS)
    if "{method}" in message:
        values["method"] = rng.choice(_HTTP_METHODS)
    if "{status}" in message:
        values["status"] = rng.choice(_HTTP_STATUSES)
    if "{port}" in message:
        values["port"] = rng.choice((80, 443, 5432, 6379, 8080, 9200, 27017))
    if "{key}" in message:
        values["key"] = rng.choice(_KEY_NAMES)
    if "{n}" in message:
        values["n"] = rng.randint(1, 9999)

    out = message
    for name, val in values.items():
        out = out.replace("{" + name + "}", str(val))
    return out


def _make_record(template: LogTemplate, rng: Random) -> dict[str, Any]:
    """Render one full labeled record from ``template`` using ``rng``."""
    ts = _rng_timestamp(rng)
    body = _render(template.message, rng)
    raw_log = f"{_iso_z(ts)} [{template.severity}] {template.service} {body}"
    return {
        "raw_log": raw_log,
        "service": template.service,
        "severity": template.severity,
        "category": template.category,
        # Microsecond ISO form (no Z) — used later for temporal features.
        "timestamp": ts.replace(tzinfo=None).isoformat(),
    }


# ---------------------------------------------------------------------------
# Template indexing & weighted selection.
# ---------------------------------------------------------------------------


def _index_templates() -> tuple[
    dict[str, list[LogTemplate]],
    dict[tuple[str, str], list[LogTemplate]],
    dict[str, list[LogTemplate]],
]:
    """Group the template table by severity, by (service, severity) and by category.

    Returns three lookup dicts used during sampling and top-up. Building them once
    keeps :func:`generate_logs` straightforward.
    """
    by_severity: dict[str, list[LogTemplate]] = {s: [] for s in SEVERITIES}
    by_service_severity: dict[tuple[str, str], list[LogTemplate]] = {}
    by_category: dict[str, list[LogTemplate]] = {c: [] for c in CATEGORIES}
    for tpl in _TEMPLATES:
        by_severity[tpl.severity].append(tpl)
        by_service_severity.setdefault((tpl.service, tpl.severity), []).append(tpl)
        by_category[tpl.category].append(tpl)
    return by_severity, by_service_severity, by_category


def _validate_record(rec: dict[str, Any]) -> None:
    """Assert a record matches the canonical schema and the allowed label sets."""
    assert rec["service"] in _SERVICE_SET, f"bad service: {rec['service']!r}"
    assert rec["severity"] in _SEVERITY_SET, f"bad severity: {rec['severity']!r}"
    assert rec["category"] in _CATEGORY_SET, f"bad category: {rec['category']!r}"
    assert isinstance(rec["raw_log"], str) and rec["raw_log"], "empty raw_log"
    assert isinstance(rec["timestamp"], str) and rec["timestamp"], "empty timestamp"


# ---------------------------------------------------------------------------
# Public API.
# ---------------------------------------------------------------------------


def generate_logs(count: int = 1000, seed: int = 42) -> list[dict[str, Any]]:
    """Generate ``count`` deterministic, labeled synthetic log records.

    The procedure guarantees minimum per-class coverage *first*, then fills the
    rest with a realistic weighted draw:

    1. **Minimum floors.** Emit enough records so every ``(service, severity)``
       pair that has a template reaches at least
       :data:`_MIN_PER_SERVICE_SEVERITY`, and every category reaches at least
       :data:`_MIN_PER_CATEGORY`. This prevents any class from being too sparse
       for a classifier to learn.
    2. **Weighted fill.** For the remaining slots up to ``count``, pick a severity
       by the realistic mix in :data:`_SEVERITY_WEIGHTS`, then a uniformly random
       template of that severity, and render it.

    Because the floors come first, the result has **exactly ``count`` records**
    whenever ``count`` is at least the total floor requirement (the default
    ``count=1000`` comfortably clears it). If ``count`` is smaller than the floors
    require, the floors win and the list may be slightly larger than ``count`` —
    the right trade-off for keeping every class trainable.

    Determinism: all randomness flows through a single ``random.Random(seed)``;
    embedded UUIDs/IPs/timestamps are derived from it. The same ``(count, seed)``
    therefore yields byte-identical output.

    Args:
        count: Target number of records (default 1000).
        seed: RNG seed controlling the whole output (default 42).

    Returns:
        A list of validated record dicts (see module docstring for the schema).

    Raises:
        ValueError: if ``count`` is negative.
    """
    if count < 0:
        raise ValueError(f"count must be non-negative, got {count}")

    rng = Random(seed)
    by_severity, by_service_severity, by_category = _index_templates()

    severities = list(SEVERITIES)
    weights = [_SEVERITY_WEIGHTS[s] for s in severities]

    records: list[dict[str, Any]] = []
    sev_service_counts: Counter[tuple[str, str]] = Counter()
    category_counts: Counter[str] = Counter()

    def _emit(tpl: LogTemplate) -> None:
        rec = _make_record(tpl, rng)
        records.append(rec)
        sev_service_counts[(rec["service"], rec["severity"])] += 1
        category_counts[rec["category"]] += 1

    # 1) Floors: guarantee a minimum per (service, severity) pair ...
    for key, pool in by_service_severity.items():
        while sev_service_counts[key] < _MIN_PER_SERVICE_SEVERITY:
            _emit(rng.choice(pool))

    # ... and per category. (Most categories are already satisfied by the step
    # above; this just covers any that span few service/severity pairs.)
    for category, pool in by_category.items():
        while category_counts[category] < _MIN_PER_CATEGORY:
            _emit(rng.choice(pool))

    # 2) Weighted fill for the remainder up to ``count``.
    for _ in range(count - len(records)):
        severity = rng.choices(severities, weights=weights, k=1)[0]
        tpl = rng.choice(by_severity[severity])
        _emit(tpl)

    for rec in records:
        _validate_record(rec)
    return records


def write_jsonl(logs: Iterable[dict[str, Any]], path: str) -> None:
    """Write ``logs`` to ``path`` as JSON Lines (one compact JSON object per line).

    The parent directory is created if it does not already exist.
    """
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        for rec in logs:
            fh.write(json.dumps(rec, ensure_ascii=False))
            fh.write("\n")


def read_jsonl(path: str) -> list[dict[str, Any]]:
    """Read a JSON Lines file written by :func:`write_jsonl` back into a list.

    Blank lines are skipped so a trailing newline does not produce an empty record.
    """
    out: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def summarize(logs: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Return counts of ``logs`` broken down by service, severity and category.

    The result has a stable shape suitable for CLI output and test assertions::

        {
          "total":      1000,
          "by_service":  {"web": ..., "database": ..., "cache": ...},
          "by_severity": {"DEBUG": ..., ..., "CRITICAL": ...},
          "by_category": {"SYSTEM": ..., ..., "APPLICATION": ...},
        }

    All known label values appear as keys (zero-filled), regardless of whether
    they occur in ``logs``.
    """
    by_service: Counter[str] = Counter()
    by_severity: Counter[str] = Counter()
    by_category: Counter[str] = Counter()
    total = 0
    for rec in logs:
        total += 1
        by_service[rec["service"]] += 1
        by_severity[rec["severity"]] += 1
        by_category[rec["category"]] += 1
    return {
        "total": total,
        "by_service": {k: by_service.get(k, 0) for k in SERVICES},
        "by_severity": {k: by_severity.get(k, 0) for k in SEVERITIES},
        "by_category": {k: by_category.get(k, 0) for k in CATEGORIES},
    }


# ---------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------


def _format_summary(summary: dict[str, Any]) -> str:
    """Render :func:`summarize` output as a compact, human-readable block."""
    lines = [f"Total records: {summary['total']}", ""]
    for title, key in (
        ("By service", "by_service"),
        ("By severity", "by_severity"),
        ("By category", "by_category"),
    ):
        lines.append(f"{title}:")
        for name, n in summary[key].items():
            lines.append(f"  {name:<12} {n}")
        lines.append("")
    return "\n".join(lines).rstrip()


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: generate logs, write JSONL, print a summary.

    Defaults for ``--count`` / ``--seed`` / ``--out`` are sourced from
    :func:`src.config.get_config` (``sample_size`` / ``random_seed`` /
    ``<data_dir>/sample.jsonl``).
    """
    cfg = get_config()
    default_out = os.path.join(cfg.data_dir, "sample.jsonl")

    parser = argparse.ArgumentParser(
        prog="python -m src.log_generator",
        description="Generate deterministic, labeled synthetic logs for training.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=cfg.sample_size,
        help=f"number of records to generate (default: {cfg.sample_size})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=cfg.random_seed,
        help=f"RNG seed for reproducible output (default: {cfg.random_seed})",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=default_out,
        help=f"output JSONL path (default: {default_out})",
    )
    args = parser.parse_args(argv)

    logs = generate_logs(count=args.count, seed=args.seed)
    write_jsonl(logs, args.out)

    summary = summarize(logs)
    print(_format_summary(summary))
    print(f"\nWrote {summary['total']} records to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
