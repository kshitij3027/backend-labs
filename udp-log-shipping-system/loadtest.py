"""Load test script — multi-threaded UDP log bombardment."""

import argparse
import json
import logging
import random
import socket
import sys
import threading
import time

from src.formatter import format_log_entry

logger = logging.getLogger(__name__)

LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
MESSAGES = [
    "Application started successfully",
    "Processing user request",
    "Database query completed",
    "Cache miss for key: user_session",
    "Failed to connect to external API",
    "Disk usage above 90%",
    "Authentication token expired",
    "Request timeout after 30s",
    "New user registered",
    "Scheduled job completed",
]


def worker(worker_id: int, server_host: str, server_port: int,
           count: int, interval: float, counter: list, lock: threading.Lock):
    """Send `count` logs at `interval` spacing."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    seq = worker_id * count

    for i in range(count):
        seq += 1
        level = random.choice(LEVELS)
        message = random.choice(MESSAGES)
        entry = format_log_entry(seq, level, message, app=f"loadtest-{worker_id}")
        data = json.dumps(entry).encode("utf-8")
        try:
            sock.sendto(data, (server_host, server_port))
        except OSError:
            pass
        if interval > 0:
            time.sleep(interval)

    sock.close()
    with lock:
        counter[0] += count


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        stream=sys.stderr,
    )

    parser = argparse.ArgumentParser(description="UDP Log Load Test")
    parser.add_argument("--server", default="localhost", help="Server host")
    parser.add_argument("--port", type=int, default=5514, help="Server port")
    parser.add_argument("--logs", type=int, default=10000, help="Total logs to send")
    parser.add_argument("--rate", type=int, default=1000, help="Target logs/sec")
    parser.add_argument("--clients", type=int, default=4, help="Number of worker threads")
    args = parser.parse_args()

    logs_per_client = args.logs // args.clients
    interval = args.clients / args.rate if args.rate > 0 else 0

    logger.info(
        "Load test: %d logs, %d clients, target %d logs/sec, interval %.4fs",
        args.logs, args.clients, args.rate, interval,
    )

    counter = [0]
    lock = threading.Lock()
    start = time.monotonic()

    threads = []
    for wid in range(args.clients):
        t = threading.Thread(
            target=worker,
            args=(wid, args.server, args.port, logs_per_client, interval, counter, lock),
        )
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    elapsed = time.monotonic() - start
    total = counter[0]
    actual_rate = total / elapsed if elapsed > 0 else 0

    logger.info("=" * 50)
    logger.info("Load Test Results:")
    logger.info("  Total sent:    %d", total)
    logger.info("  Elapsed:       %.2fs", elapsed)
    logger.info("  Target rate:   %d logs/sec", args.rate)
    logger.info("  Actual rate:   %.0f logs/sec", actual_rate)
    logger.info("  Workers:       %d", args.clients)
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
