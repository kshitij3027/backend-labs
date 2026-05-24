#!/usr/bin/env python3
"""Generate N sample log records as JSON lines.

Usage: python scripts/generate_test_logs.py [N]
Defaults to 10 records spread across 5 sources / 4 categories / 4 levels.

Records are timestamped at the current UTC time. For testing
retention policy boundaries, callers may want to backdate the ts —
see scripts/seed_demo.py for that pattern.
"""
import json
import random
import sys
from datetime import datetime, timezone

SOURCES = ["app", "db", "auth", "billing", "ingestion"]
LEVELS = ["DEBUG", "INFO", "WARN", "ERROR"]
CATEGORIES = ["user_activity", "payment", "health", "ops_audit", "payment_card"]


def gen(n: int) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    for i in range(n):
        record = {
            "ts": now.isoformat(),
            "level": random.choice(LEVELS),
            "source": random.choice(SOURCES),
            "category": random.choice(CATEGORIES),
            "message": f"sample log line {i}",
        }
        print(json.dumps(record))


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    gen(n)
