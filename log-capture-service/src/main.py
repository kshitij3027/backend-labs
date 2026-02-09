#!/usr/bin/env python3
"""Log Capture Service — Entry Point."""

import sys
import os
import time
import signal
import queue
import argparse
import logging

from watchdog.observers import Observer

# Ensure src package is importable when run as `python src/main.py`
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_yaml_config, load_config
from src.registry import OffsetRegistry
from src.harvester import LogHarvester
from src.buffer import BatchWriter
from src.parsers import get_parse_error_count

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [COLLECTOR] %(levelname)s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

_running = True


def _signal_handler(sig, frame):
    global _running
    logger.info("Shutdown signal received, stopping...")
    _running = False


def build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Log Capture Service")
    parser.add_argument(
        "--log-files", nargs="+", required=True,
        help="Paths to log files to watch",
    )
    parser.add_argument(
        "--output-dir", default="collected_logs/",
        help="Directory for collected JSON output (default: collected_logs/)",
    )
    parser.add_argument(
        "--config", default=None,
        help="Path to YAML config file for filter/tag rules",
    )
    return parser


def main():
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    parser = build_cli_parser()
    args = parser.parse_args()

    yaml_data = load_yaml_config(args.config)
    config = load_config(args, yaml_data)
    logger.info("Config: batch_size=%d, flush_interval=%.1f, watching %d file(s)",
                config.batch_size, config.flush_interval, len(config.log_files))

    # Processor will be wired in after filters.py is added (commit 9)
    processor = None
    if config.filter_rules or config.tag_rules:
        try:
            from src.filters import EntryProcessor
            processor = EntryProcessor(config.filter_rules, config.tag_rules)
            logger.info("EntryProcessor active: %d filter rules, %d tag rules",
                        len(config.filter_rules), len(config.tag_rules))
        except ImportError:
            logger.info("No filter module available, running without filters")

    q = queue.Queue()
    registry = OffsetRegistry(config.registry_file)
    harvester = LogHarvester(config.log_files, q, registry)
    writer = BatchWriter(q, config, processor)

    # Read any existing content before starting observer
    harvester.startup_read()

    writer.start()

    observer = Observer()
    for dir_path in harvester.get_watched_dirs():
        os.makedirs(dir_path, exist_ok=True)
        observer.schedule(harvester, dir_path, recursive=False)
        logger.info("Watching directory: %s", dir_path)
    observer.start()

    logger.info("Log Capture Service running. Press Ctrl+C to stop.")

    try:
        while _running:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    logger.info("Shutting down...")
    observer.stop()
    observer.join(timeout=5)
    writer.stop()
    writer.join(timeout=5)
    harvester.close_all()
    registry.save()

    logger.info("Stats: %d entries written in %d batches, %d parse errors",
                writer.total_entries, writer.batch_count, get_parse_error_count())
    logger.info("Log Capture Service stopped.")


if __name__ == "__main__":
    main()
