"""Synthetic log generator with realistic templates.

Produces log dicts of the shape the Redis stream consumer expects —
``{"message", "timestamp", "service", "level"}`` — with placeholders
pre-rendered so the messages look like real logs (IPs, emails, URLs,
UUIDs, HTTP statuses, latencies). The tokenizer exercises its compound
patterns against these, so the fixtures double as coverage for the
rest of the pipeline.

``doc_id`` is deliberately not produced here — the inverted index
assigns the id on ingest. The ``generate_log_entry`` helper returns
raw dicts so the producer (either ``/api/generate-sample`` or the
load test) can XADD them directly without a model round-trip.

All randomness goes through an optional ``random.Random`` so tests
can seed the generator for deterministic output.
"""

from __future__ import annotations

import random
import time
import uuid


# ---------------------------------------------------------------------------
# Fixed pools used by the template placeholders. Small on purpose so
# searches have enough repetition across N generated entries to produce
# meaningful hits during load tests.
# ---------------------------------------------------------------------------

SERVICES: list[str] = [
    "auth-service",
    "payment-service",
    "api-gateway",
    "user-service",
    "order-service",
    "inventory-service",
    "notification-service",
    "cache-service",
    "search-service",
    "logging-service",
]

LEVELS: list[str] = ["INFO", "WARN", "ERROR", "DEBUG"]

_EMAILS: list[str] = [
    "alice@example.com",
    "bob@test.io",
    "carol@acme.com",
    "dave@foo.org",
    "erin@corp.net",
]

_HOSTS: list[str] = ["db-primary", "db-replica-1", "cache-01", "redis-02"]

_URLS: list[str] = [
    "/api/v1/users",
    "/api/v1/orders",
    "/api/v1/payments",
    "/health",
    "/search",
]


# ---------------------------------------------------------------------------
# Templates. Each entry is (level, message template) — message templates
# use ``str.format`` so placeholders line up with the renderer below.
# ---------------------------------------------------------------------------

LOG_TEMPLATES: list[tuple[str, str]] = [
    ("INFO",  "login successful for user={email} from {ip}"),
    ("WARN",  "slow query detected: took {ms}ms in service {svc}"),
    ("ERROR", "database connection timeout after {ms}ms host={host}"),
    ("INFO",  "cache miss for key=user:{uid} populating from db"),
    ("ERROR", "payment failed for transaction_id={uuid} amount={amount}"),
    ("INFO",  "http GET {url} status=200 took={ms}ms"),
    ("WARN",  "rate limit hit for {ip} endpoint={url}"),
    ("ERROR", "auth failure user={email} reason=invalid_password"),
    ("INFO",  "order_service.place_order completed order_id={uid}"),
    ("DEBUG", "trace id={uuid} span=db.query.users duration={ms}ms"),
    ("ERROR", "inventory.restock failed for sku={sku} reason=supplier_down"),
    ("INFO",  "notification sent via email to {email} template=welcome"),
    ("WARN",  "circuit breaker opened for upstream={host}"),
    ("ERROR", "HTTP 500 internal server error on {url} request_id={uuid}"),
    ("INFO",  "healthcheck ok service={svc} uptime={ms}ms"),
]


def _rand_ip(rng: random.Random) -> str:
    """Random public-looking IPv4 address (no CIDR check)."""
    return (
        f"{rng.randint(10, 250)}.{rng.randint(0, 255)}."
        f"{rng.randint(0, 255)}.{rng.randint(0, 255)}"
    )


def _render(template: str, rng: random.Random) -> str:
    """Fill every placeholder a template might use.

    We supply values for every placeholder name in the ``LOG_TEMPLATES``
    palette — templates that don't reference a given name simply ignore
    it. ``str.format_map`` would be marginally faster but ``format``
    with a superset of kwargs is the clearest.
    """
    return template.format(
        email=rng.choice(_EMAILS),
        ip=_rand_ip(rng),
        ms=rng.randint(5, 5000),
        svc=rng.choice(SERVICES),
        host=rng.choice(_HOSTS),
        uid=rng.randint(1000, 99999),
        uuid=str(uuid.UUID(bytes=rng.randbytes(16), version=4)),
        amount=f"{rng.uniform(1, 9999):.2f}",
        url=rng.choice(_URLS),
        sku=f"SKU-{rng.randint(1000, 9999)}",
    )


def generate_log_entry(rng: random.Random | None = None) -> dict:
    """Return a single synthetic log dict.

    Shape is ``{"message": str, "timestamp": float, "service": str,
    "level": str}`` — deliberately flat so it round-trips through
    Redis XADD / XREADGROUP without nested-structure encoding.

    ``rng`` is optional so production callers can rely on the default
    :class:`random.Random` while tests can pass a seeded instance for
    deterministic output.
    """
    r = rng if rng is not None else random.Random()
    level, template = r.choice(LOG_TEMPLATES)
    message = _render(template, r)
    return {
        "message": message,
        "timestamp": time.time(),
        "service": r.choice(SERVICES),
        "level": level,
    }


def generate_log_entries(
    count: int, rng: random.Random | None = None
) -> list[dict]:
    """Return *count* synthetic log dicts.

    Shares a single :class:`random.Random` across the whole batch so a
    seeded generator produces a reproducible list for property tests.
    """
    if count <= 0:
        return []
    r = rng if rng is not None else random.Random()
    return [generate_log_entry(r) for _ in range(count)]
