"""Deterministic synthetic-log generator for tests, demo, and load tests.

The generator emits :class:`LogEntry` instances with realistic
messages across the five intent buckets (troubleshooting,
performance_analysis, user_activity, payment_analysis, general_search)
so downstream demos and load tests exercise the full ranking pipeline.
Use a fixed ``seed`` to make generated corpora reproducible.
"""
from __future__ import annotations

import random
import time

from src.models import LogEntry

_SERVICES = ["auth", "payment", "api", "gateway", "worker", "billing", "cart", "profile"]
_LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "WARN", "ERROR", "ERROR", "FATAL"]

_MESSAGE_TEMPLATES = [
    # troubleshooting
    "authentication error for user {uid} on service {svc}",
    "database connection refused for {svc}",
    "null pointer exception in {svc} handler",
    "unhandled exception stacktrace from {svc}",
    "request returned 500 internal server error on {svc}",
    "timeout waiting for {svc} response",
    # performance_analysis
    "slow response p99 latency breached on {svc}",
    "high cpu utilization on {svc}",
    "memory pressure observed on {svc}",
    "throughput dropped below baseline for {svc}",
    # user_activity
    "user {uid} login success",
    "user {uid} logout",
    "session {session} expired",
    "password reset requested for user {uid}",
    "account profile updated for user {uid}",
    # payment_analysis
    "payment charge succeeded for user {uid}",
    "payment charge declined for card ending {card}",
    "refund issued for transaction {tx}",
    "invoice {tx} created for user {uid}",
    # general
    "service {svc} started",
    "service {svc} config reloaded",
    "request served 200 on {svc}",
]


def generate_log_entries(count: int, seed: int = 0, start_ts: float | None = None) -> list[LogEntry]:
    """Return ``count`` deterministic :class:`LogEntry` rows.

    Args:
        count: number of entries to emit.
        seed: RNG seed so repeat calls produce identical output.
        start_ts: timestamp of the earliest entry; defaults to
            ``time.time() - count`` so entries look recent when
            ingested.
    """
    rng = random.Random(seed)
    ts0 = start_ts if start_ts is not None else time.time() - count
    out: list[LogEntry] = []
    for i in range(count):
        tmpl = rng.choice(_MESSAGE_TEMPLATES)
        svc = rng.choice(_SERVICES)
        level = rng.choice(_LEVELS)
        msg = tmpl.format(
            uid=rng.randint(1, 10_000),
            svc=svc,
            session=f"s{rng.randint(1000, 9999)}",
            card=f"{rng.randint(1000, 9999)}",
            tx=f"tx{rng.randint(100000, 999999)}",
        )
        out.append(
            LogEntry(
                message=msg,
                timestamp=ts0 + i,
                service=svc,
                level=level,
            )
        )
    return out
