"""
Generate sample JSONL log data for MapReduce processing.
Writes 500 log lines to /data/sample-logs.jsonl.
"""

import json
import os
import random
from datetime import datetime, timedelta, timezone

LEVELS = ["INFO", "WARN", "ERROR", "FATAL"]
LEVEL_WEIGHTS = [70, 15, 12, 3]

URLS = [
    "/api/users",
    "/api/users/login",
    "/api/users/register",
    "/api/orders",
    "/api/orders/checkout",
    "/api/products",
    "/api/products/search",
    "/api/health",
    "/api/payments",
    "/api/notifications",
]

ERROR_CODES = [None, None, None, "400", "401", "403", "404", "500", "502", "503"]

MESSAGES = [
    "Request processed successfully",
    "User authenticated",
    "Cache miss, fetching from database",
    "Rate limit approaching threshold",
    "Connection timeout to upstream service",
    "Database query slow: exceeded 500ms",
    "Invalid request payload",
    "Authentication token expired",
    "Permission denied for resource",
    "Resource not found",
    "Internal server error",
    "Bad gateway from upstream",
    "Service temporarily unavailable",
    "Request queued for processing",
    "Background job completed",
]

NUM_LINES = 500
OUTPUT_DIR = "/data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "sample-logs.jsonl")


def generate_log_line(base_time: datetime, offset_seconds: int) -> dict:
    level = random.choices(LEVELS, weights=LEVEL_WEIGHTS, k=1)[0]
    url = random.choice(URLS)
    user_id = f"user_{random.randint(1, 100):03d}"
    timestamp = base_time + timedelta(seconds=offset_seconds)

    error_code = None
    if level in ("ERROR", "FATAL"):
        error_code = random.choice(["400", "401", "403", "404", "500", "502", "503"])
    elif level == "WARN":
        error_code = random.choice([None, "400", "429"])

    message_pool = MESSAGES[:8] if level == "INFO" else MESSAGES[4:]
    message = random.choice(message_pool)

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
    random.seed(42)

    with open(OUTPUT_FILE, "w") as f:
        for i in range(NUM_LINES):
            line = generate_log_line(base_time, offset_seconds=i * 2)
            f.write(json.dumps(line) + "\n")

    print(f"Generated {NUM_LINES} log lines to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
