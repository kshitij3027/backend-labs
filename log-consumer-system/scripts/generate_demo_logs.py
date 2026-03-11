#!/usr/bin/env python3
"""Generate demo Apache combined format access logs and push to Redis stream."""

import os
import random
import sys
from datetime import datetime, timezone

import redis

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
STREAM_KEY = os.environ.get("STREAM_KEY", "logs:access")
NUM_LOGS = int(os.environ.get("NUM_LOGS", "150"))

IPS = [f"192.168.1.{i}" for i in range(1, 21)]
PATHS = ["/api/users", "/api/orders", "/api/products", "/health", "/login", "/dashboard",
         "/api/users/1", "/api/orders/create", "/api/products/search", "/static/main.js"]
METHODS = ["GET", "GET", "GET", "POST", "PUT", "DELETE"]  # weighted toward GET
STATUS_WEIGHTS = [(200, 70), (201, 5), (301, 3), (404, 10), (500, 8), (502, 2), (503, 2)]


def weighted_status():
    total = sum(w for _, w in STATUS_WEIGHTS)
    r = random.randint(1, total)
    cumulative = 0
    for status, weight in STATUS_WEIGHTS:
        cumulative += weight
        if r <= cumulative:
            return status
    return 200


def generate_log_line(malformed=False):
    if malformed:
        return random.choice([
            "this is not a log line",
            "INVALID FORMAT 123",
            "",
            "partial - - [bad",
        ])
    ip = random.choice(IPS)
    method = random.choice(METHODS)
    path = random.choice(PATHS)
    status = weighted_status()
    size = random.randint(100, 50000)
    response_time = round(random.uniform(5, 500), 1)
    ts = datetime.now(timezone.utc).strftime("%d/%b/%Y:%H:%M:%S +0000")
    return f'{ip} - - [{ts}] "{method} {path} HTTP/1.1" {status} {size} "-" "python-demo/1.0" {response_time}'


def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    print(f"Connected to Redis. Generating {NUM_LOGS} logs to stream '{STREAM_KEY}'...")

    for i in range(NUM_LOGS):
        malformed = random.random() < 0.05  # ~5% malformed
        line = generate_log_line(malformed=malformed)
        r.xadd(STREAM_KEY, {"log": line})

    stream_len = r.xlen(STREAM_KEY)
    print(f"Done. Stream '{STREAM_KEY}' now has {stream_len} entries.")


if __name__ == "__main__":
    main()
