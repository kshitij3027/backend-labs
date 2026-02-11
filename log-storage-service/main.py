"""Log storage service — generates demo logs with automatic rotation, compression, and purging."""

import logging
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from src.config import load_config
from src.rotator import compress_file, enforce_retention
from src.writer import LogWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [log-storage] %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

_running = True


def _signal_handler(sig, _frame):
    global _running
    logger.info("Shutdown signal received (signal %d), stopping...", sig)
    _running = False


LEVELS = ["INFO", "INFO", "INFO", "INFO", "DEBUG", "WARN", "ERROR"]
SERVICES = ["auth-api", "order-svc", "payment-gw", "user-svc", "catalog-api"]
MESSAGES = {
    "INFO": [
        "Request processed successfully",
        "Health check passed",
        "Cache hit for user session",
        "Database query completed in 12ms",
        "Outbound HTTP 200 from upstream",
    ],
    "DEBUG": [
        "Entering request handler",
        "Parsed request body",
        "Token validation started",
    ],
    "WARN": [
        "Slow query detected (>500ms)",
        "Connection pool nearing capacity",
        "Retry attempt 2 for upstream call",
    ],
    "ERROR": [
        "Failed to connect to database",
        "Timeout waiting for upstream response",
        "Invalid auth token received",
        "Unhandled exception in request handler",
    ],
}


def generate_entry() -> str:
    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S.") + f"{now.microsecond // 1000:03d}"
    level = random.choice(LEVELS)
    service = random.choice(SERVICES)
    req_id = uuid.uuid4().hex[:8]
    message = random.choice(MESSAGES[level])
    return f"{timestamp} [{level}] [{service}] [{req_id}] {message}"


def main():
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    config = load_config()
    logger.info("Starting log storage service")
    logger.info(
        "Config: log_dir=%s, max_size=%d bytes, interval=%ds, max_count=%d, max_age=%dd, compress=%s",
        config.log_dir, config.max_file_size_bytes, config.rotation_interval_seconds,
        config.max_file_count, config.max_age_days, config.compression_enabled,
    )

    writer = LogWriter(config)
    entries_written = 0

    try:
        while _running:
            entry = generate_entry()
            rotated_path = writer.write(entry)
            entries_written += 1

            if rotated_path:
                logger.info("Rotated: %s (%d entries written so far)", rotated_path, entries_written)

                if config.compression_enabled:
                    gz_path = compress_file(rotated_path)
                    logger.info("Compressed: %s", gz_path)

                deleted = enforce_retention(config)
                if deleted:
                    logger.info("Purged %d file(s): %s", len(deleted), ", ".join(deleted))

            time.sleep(0.05)
    except KeyboardInterrupt:
        pass

    writer.close()
    logger.info("Shut down cleanly. Total entries written: %d", entries_written)


if __name__ == "__main__":
    main()
