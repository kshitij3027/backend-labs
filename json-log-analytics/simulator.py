import random
from datetime import datetime, timezone, timedelta

SERVICES = ["auth-service", "api-gateway", "payment-service", "user-service", "notification-service"]
LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LEVEL_WEIGHTS = [0.05, 0.60, 0.15, 0.15, 0.05]

MESSAGES = {
    "DEBUG": ["Cache miss for key", "Retry attempt #2", "Connection pool stats"],
    "INFO": ["User logged in", "Request processed", "Health check passed", "Database query executed"],
    "WARNING": ["High memory usage detected", "Slow query detected", "Rate limit approaching"],
    "ERROR": ["Database connection failed", "Authentication failed", "Timeout exceeded", "Service unavailable"],
    "CRITICAL": ["System out of memory", "Disk space critically low", "Unrecoverable error"],
}

USER_IDS = [f"user-{i}" for i in range(1, 21)]


def generate_log(service=None, level=None, minutes_ago=0, error_rate=None):
    """Generate a single random log entry."""
    if service is None:
        service = random.choice(SERVICES)
    if level is None:
        if error_rate is not None:
            level = random.choice(["ERROR", "CRITICAL"]) if random.random() < error_rate else "INFO"
        else:
            level = random.choices(LEVELS, weights=LEVEL_WEIGHTS, k=1)[0]

    ts = datetime.now(timezone.utc)
    if minutes_ago:
        ts = ts - timedelta(minutes=random.uniform(0, minutes_ago))

    log = {
        "timestamp": ts.isoformat(),
        "level": level,
        "service": service,
        "message": random.choice(MESSAGES.get(level, MESSAGES["INFO"])),
    }

    # 40% chance of user_id
    if random.random() < 0.4:
        log["user_id"] = random.choice(USER_IDS)

    # 30% chance of metadata
    if random.random() < 0.3:
        log["metadata"] = {
            "processing_time_ms": round(random.uniform(1, 500), 2),
            "request_id": f"req-{random.randint(1000, 9999)}",
        }

    return log


def generate_batch(count=10, **kwargs):
    """Generate multiple log entries."""
    return [generate_log(**kwargs) for _ in range(count)]
