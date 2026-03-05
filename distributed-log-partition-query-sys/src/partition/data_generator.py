import random
from datetime import datetime, timedelta, timezone
from src.models import LogEntry

SERVICES = ["auth-service", "api-gateway", "payment-service", "user-service", "notification-service", "order-service"]
LEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]
LEVEL_WEIGHTS = [0.3, 0.4, 0.2, 0.1]

MESSAGE_TEMPLATES = {
    "DEBUG": ["Processing request {id}", "Cache lookup for key {id}", "Database query executed in {ms}ms"],
    "INFO": ["Request completed successfully", "User {id} logged in", "Order {id} processed", "Health check passed"],
    "WARN": ["High latency detected: {ms}ms", "Connection pool running low", "Retry attempt {id} for request"],
    "ERROR": ["Connection refused to database", "Timeout after {ms}ms", "Authentication failed for user {id}", "Out of memory error"],
}


def generate_sample_logs(count: int, days_back: int, partition_id: str) -> list[LogEntry]:
    """Generate deterministic sample logs seeded by partition_id."""
    seed = hash(partition_id) % (2**32)
    rng = random.Random(seed)

    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(days=days_back)
    total_seconds = int((now - start).total_seconds())

    entries = []
    for i in range(count):
        level = rng.choices(LEVELS, weights=LEVEL_WEIGHTS, k=1)[0]
        service = rng.choice(SERVICES)
        template = rng.choice(MESSAGE_TEMPLATES[level])
        message = template.format(id=rng.randint(1000, 9999), ms=rng.randint(10, 5000))
        timestamp = start + timedelta(seconds=rng.randint(0, total_seconds))

        entries.append(LogEntry(
            timestamp=timestamp,
            level=level,
            service=service,
            message=message,
            partition_id=partition_id,
        ))

    entries.sort(key=lambda e: e.timestamp, reverse=True)
    return entries
