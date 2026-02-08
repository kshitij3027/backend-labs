#!/usr/bin/env python3
"""Configurable Log Generator — Entry Point."""

import sys
import os
import time
import signal
import random
import logging
from datetime import datetime

# Ensure src package is importable when run as `python src/main.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_config
from src.models import LogEntry, generate_short_id, generate_user_id, generate_request_id, generate_duration
from src.formatters import get_formatter
from src.output import LogWriter
from src.messages import get_random_message
from src.burst import BurstController

# Internal logging to stderr (separate from generated log output)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GENERATOR] %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

_running = True


def _signal_handler(sig, frame):
    global _running
    logger.info("Shutdown signal received, stopping...")
    _running = False


def _select_level(distribution: dict) -> str:
    """Weighted random selection of log level."""
    levels = list(distribution.keys())
    weights = list(distribution.values())
    return random.choices(levels, weights=weights, k=1)[0]


def _make_random_entry(config) -> LogEntry:
    level = _select_level(config.log_distribution)
    return LogEntry(
        timestamp=datetime.now(),
        level=level,
        id=generate_short_id(),
        service=random.choice(config.services),
        user_id=generate_user_id(),
        request_id=generate_request_id(),
        duration_ms=generate_duration(level),
        message=get_random_message(level),
    )


def main():
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    config = load_config()
    logger.info("Starting log generator with config: %s", config)

    formatter = get_formatter(config.log_format)
    writer = LogWriter(config.output_file, config.console_output, config.log_format)
    burst = BurstController(
        config.burst_frequency, config.burst_multiplier,
        config.burst_duration, config.enable_bursts,
    )

    try:
        while _running:
            second_start = time.time()
            multiplier = burst.get_current_multiplier()
            current_rate = config.log_rate * multiplier
            sleep_per_log = 1.0 / current_rate if current_rate > 0 else 1.0

            logs_generated = 0
            while _running and (time.time() - second_start) < 1.0:
                entry = _make_random_entry(config)
                writer.write(formatter(entry))
                logs_generated += 1

                if logs_generated >= current_rate:
                    remaining = 1.0 - (time.time() - second_start)
                    if remaining > 0:
                        time.sleep(remaining)
                    break

                time.sleep(sleep_per_log)
    except Exception as e:
        logger.error("Unexpected error: %s", e, exc_info=True)
    finally:
        writer.close()
        logger.info("Log generator stopped.")


if __name__ == "__main__":
    main()
