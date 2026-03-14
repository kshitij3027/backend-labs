"""Message producer for the Dead Letter Queue Log Processor."""

import asyncio
import json
import random
import string
import uuid
from datetime import datetime, timezone

from src.config import Settings
from src.models import LogLevel, LogMessage
from src.redis_client import RedisClient

# ---------------------------------------------------------------------------
# Message templates keyed by LogLevel
# ---------------------------------------------------------------------------

_SERVICES = [
    "api-gateway",
    "auth-service",
    "payment-processor",
    "user-service",
    "notification-engine",
    "data-pipeline",
    "search-indexer",
    "cache-manager",
]

_LEVEL_WEIGHTS: list[tuple[LogLevel, int]] = [
    (LogLevel.INFO, 50),
    (LogLevel.WARNING, 20),
    (LogLevel.ERROR, 15),
    (LogLevel.DEBUG, 10),
    (LogLevel.CRITICAL, 5),
]

_TEMPLATES: dict[LogLevel, list[str]] = {
    LogLevel.INFO: [
        "Request processed successfully",
        "User session started",
        "Cache hit for key {key}",
        "Health check passed",
    ],
    LogLevel.WARNING: [
        "High memory usage detected: {pct}%",
        "Slow query detected: {ms}ms",
        "Rate limit approaching for {service}",
    ],
    LogLevel.ERROR: [
        "Database connection timeout after {ms}ms",
        "Failed to process payment: {reason}",
        "Authentication failed for user {user}",
    ],
    LogLevel.CRITICAL: [
        "Out of memory: heap usage at {pct}%",
        "Disk space critically low: {pct}% used",
        "Cluster node unreachable: {node}",
    ],
    LogLevel.DEBUG: [
        "Cache miss for key {key}",
        "Query plan: {plan}",
        "Connection pool status: {active}/{max}",
    ],
}

# Small helper pools used to fill in placeholders
_KEYS = ["user:1234", "session:abc", "config:main", "token:xyz"]
_REASONS = ["insufficient_funds", "card_declined", "timeout", "invalid_amount"]
_USERS = ["alice", "bob", "charlie", "diana"]
_NODES = ["node-1", "node-2", "node-3"]
_PLANS = ["seq_scan", "index_scan", "hash_join", "nested_loop"]


def _fill_template(template: str) -> str:
    """Replace placeholders with random realistic values."""
    return (
        template.replace("{key}", random.choice(_KEYS))
        .replace("{pct}", str(random.randint(75, 99)))
        .replace("{ms}", str(random.randint(100, 5000)))
        .replace("{service}", random.choice(_SERVICES))
        .replace("{reason}", random.choice(_REASONS))
        .replace("{user}", random.choice(_USERS))
        .replace("{node}", random.choice(_NODES))
        .replace("{plan}", random.choice(_PLANS))
        .replace("{active}", str(random.randint(1, 20)))
        .replace("{max}", str(random.randint(20, 50)))
    )


class MessageProducer:
    """Produces valid and intentionally malformed log messages."""

    def __init__(self, redis_client: RedisClient, settings: Settings) -> None:
        self._redis = redis_client
        self._settings = settings

    # ------------------------------------------------------------------
    # Message generation
    # ------------------------------------------------------------------

    def _generate_valid_message(self) -> LogMessage:
        """Create a realistic, well-formed LogMessage."""
        levels, weights = zip(*_LEVEL_WEIGHTS)
        level: LogLevel = random.choices(levels, weights=weights, k=1)[0]
        source = random.choice(_SERVICES)
        template = random.choice(_TEMPLATES[level])
        message_text = _fill_template(template)

        return LogMessage(
            id=str(uuid.uuid4()),
            timestamp=datetime.now(timezone.utc).isoformat(),
            level=level,
            source=source,
            message=message_text,
            metadata={
                "source": source,
                "environment": "production",
                "version": "1.0.0",
            },
        )

    def _generate_malformed_message(self) -> str:
        """Return a raw string that will cause parsing failures."""
        kind = random.choice(
            ["invalid_json", "missing_fields", "bad_level", "oversized", "empty"]
        )

        if kind == "invalid_json":
            return "not json at all{{{"

        if kind == "missing_fields":
            # Valid JSON but missing required 'id' field
            return json.dumps(
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "level": "INFO",
                    "message": "this message has no id",
                }
            )

        if kind == "bad_level":
            return json.dumps(
                {
                    "id": str(uuid.uuid4()),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "level": "BANANA",
                    "source": "test-service",
                    "message": "invalid level value",
                }
            )

        if kind == "oversized":
            # ~100 KB payload
            return json.dumps(
                {
                    "id": str(uuid.uuid4()),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "level": "INFO",
                    "source": "test-service",
                    "message": "".join(
                        random.choices(string.ascii_letters, k=100_000)
                    ),
                }
            )

        # kind == "empty"
        return ""

    # ------------------------------------------------------------------
    # Producing
    # ------------------------------------------------------------------

    async def produce_batch(self, count: int) -> int:
        """Produce *count* messages, with *failure_rate* fraction malformed.

        Returns the number of messages actually enqueued.
        """
        produced = 0
        for _ in range(count):
            if random.random() < self._settings.failure_rate:
                raw = self._generate_malformed_message()
            else:
                raw = self._generate_valid_message().to_json()
            await self._redis.enqueue(self._settings.main_queue, raw)
            produced += 1
        return produced

    async def produce_continuous(self, stop_event: asyncio.Event) -> None:
        """Produce messages at *producer_rate* per second until stopped."""
        interval = 1.0 / self._settings.producer_rate
        while not stop_event.is_set():
            if random.random() < self._settings.failure_rate:
                raw = self._generate_malformed_message()
            else:
                raw = self._generate_valid_message().to_json()
            await self._redis.enqueue(self._settings.main_queue, raw)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
