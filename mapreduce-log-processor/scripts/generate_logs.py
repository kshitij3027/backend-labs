"""
Generate sample JSONL log data for MapReduce processing.
Writes 1500 log lines to /data/sample-logs.jsonl with deterministic output.
"""

import json
import os
import random
from datetime import datetime, timedelta, timezone

LEVELS = ["INFO", "WARN", "ERROR", "FATAL"]
LEVEL_WEIGHTS = [60, 20, 15, 5]

URLS = [
    "/api/users",
    "/api/orders",
    "/api/products",
    "/health",
    "/login",
    "/api/users/login",
    "/api/users/register",
    "/api/orders/checkout",
    "/api/products/search",
    "/api/payments",
]

ERROR_CODES_FOR_ERRORS = ["400", "404", "500", "503"]
ERROR_CODES_FOR_WARNS = [None, "400", "429"]

MESSAGES_INFO = [
    "Request processed successfully",
    "User authenticated via token",
    "Cache miss fetching from database",
    "Connection established to upstream",
    "Background job completed successfully",
    "Session created for user",
    "Page rendered in time",
    "Health check passed",
]

MESSAGES_WARN = [
    "Rate limit approaching threshold",
    "Slow database query detected",
    "Cache eviction rate high",
    "Connection pool near capacity",
    "Retry attempt for upstream call",
]

MESSAGES_ERROR = [
    "Connection timeout to upstream service",
    "Invalid request payload received",
    "Authentication token expired",
    "Permission denied for resource",
    "Resource not found on server",
]

MESSAGES_FATAL = [
    "Internal server error encountered",
    "Bad gateway from upstream service",
    "Service temporarily unavailable",
    "Out of memory error triggered",
    "Database connection pool exhausted",
]

NUM_LINES = 1500
OUTPUT_DIR = "/data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "sample-logs.jsonl")


def generate_log_line(rng: random.Random, base_time: datetime, offset_seconds: int) -> dict:
    level = rng.choices(LEVELS, weights=LEVEL_WEIGHTS, k=1)[0]
    url = rng.choice(URLS)
    user_id = f"user_{rng.randint(1, 100):03d}"
    timestamp = base_time + timedelta(seconds=offset_seconds)

    error_code = None
    if level in ("ERROR", "FATAL"):
        error_code = rng.choice(ERROR_CODES_FOR_ERRORS)
    elif level == "WARN":
        error_code = rng.choice(ERROR_CODES_FOR_WARNS)

    if level == "INFO":
        message = rng.choice(MESSAGES_INFO)
    elif level == "WARN":
        message = rng.choice(MESSAGES_WARN)
    elif level == "ERROR":
        message = rng.choice(MESSAGES_ERROR)
    else:
        message = rng.choice(MESSAGES_FATAL)

    return {
        "timestamp": timestamp.isoformat(),
        "level": level,
        "message": message,
        "url": url,
        "error_code": error_code,
        "user_id": user_id,
    }


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    base_time = datetime(2025, 1, 15, 8, 0, 0, tzinfo=timezone.utc)
    rng = random.Random(42)

    with open(OUTPUT_FILE, "w") as f:
        for i in range(NUM_LINES):
            line = generate_log_line(rng, base_time, offset_seconds=i * 2)
            f.write(json.dumps(line) + "\n")

    print(f"Generated {NUM_LINES} log lines to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
