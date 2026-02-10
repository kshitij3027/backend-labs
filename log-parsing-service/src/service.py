#!/usr/bin/env python3
"""Log Parsing Service — watcher mode entry point."""

import sys
import os
import time
import signal
import logging

from watchdog.observers import Observer

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_config
from src.stats import StatsCollector
from src.watcher import FileWatcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PARSER] %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

_running = True


def _signal_handler(sig, frame):
    global _running
    logger.info("Shutdown signal received, stopping...")
    _running = False


def main():
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    config = load_config()
    logger.info("Config: input_dir=%s, output_dir=%s", config.input_dir, config.output_dir)

    os.makedirs(config.input_dir, exist_ok=True)
    os.makedirs(config.output_dir, exist_ok=True)

    stats = StatsCollector(config.output_dir)
    watcher = FileWatcher(config.output_dir, stats)

    # Process any existing .log files before starting the observer
    watcher.process_existing_files(config.input_dir)

    observer = Observer()
    observer.schedule(watcher, config.input_dir, recursive=False)
    observer.start()

    logger.info("Log Parsing Service running. Watching: %s", config.input_dir)

    try:
        while _running:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    logger.info("Shutting down...")
    observer.stop()
    observer.join(timeout=5)
    logger.info("Log Parsing Service stopped.")


if __name__ == "__main__":
    main()
