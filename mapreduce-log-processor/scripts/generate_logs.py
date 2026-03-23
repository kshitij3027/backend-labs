"""
Generate sample JSONL log data for MapReduce processing.
Writes 10,000+ log lines to /data/sample-logs.jsonl with deterministic output (seed=42).
"""

import json
import os
import random
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

LEVELS = ["INFO", "WARN", "ERROR", "FATAL"]
LEVEL_WEIGHTS = [60, 20, 15, 5]

URLS = [
    "/api/users",
    "/api/orders",
    "/api/products",
    "/health",
    "/login",
    "/api/search",
    "/api/payments",
]
URL_WEIGHTS = [25, 20, 15, 10, 10, 12, 8]

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

NUM_LINES = 10_000
OUTPUT_DIR = "/data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "sample-logs.jsonl")


def generate_log_line(rng: random.Random, base_time: datetime, offset_seconds: int) -> dict:
    level = rng.choices(LEVELS, weights=LEVEL_WEIGHTS, k=1)[0]
    url = rng.choices(URLS, weights=URL_WEIGHTS, k=1)[0]
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


def generate_logs(output_path: str, num_lines: int = NUM_LINES) -> None:
    """Generate deterministic log lines and write to output_path."""
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    base_time = datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    rng = random.Random(42)

    # Spread 10K lines over 24 hours => ~8.64s between lines
    seconds_per_line = (24 * 3600) / num_lines

    level_counts: Counter = Counter()

    with open(output_path, "w") as f:
        for i in range(num_lines):
            offset = int(i * seconds_per_line)
            line = generate_log_line(rng, base_time, offset_seconds=offset)
            level_counts[line["level"]] += 1
            f.write(json.dumps(line) + "\n")

    # Print summary
    print(f"Generated {num_lines} log lines to {output_path}")
    print("Level distribution:")
    for level in LEVELS:
        count = level_counts[level]
        pct = count / num_lines * 100
        print(f"  {level:>5s}: {count:>5d} ({pct:.1f}%)")


def main():
    generate_logs(OUTPUT_FILE)


if __name__ == "__main__":
    main()
