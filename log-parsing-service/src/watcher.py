"""File watcher — monitors input directory and processes .log files."""

import json
import logging
import os
import tempfile
import time

from watchdog.events import FileSystemEventHandler

from src.models import entry_to_dict
from src.parsers import parse_line
from src.stats import StatsCollector

logger = logging.getLogger(__name__)

DEBOUNCE_SECONDS = 0.5


class FileWatcher(FileSystemEventHandler):
    """Watches for .log file creation/modification and parses entire files."""

    def __init__(self, output_dir: str, stats: StatsCollector):
        super().__init__()
        self._output_dir = output_dir
        self._stats = stats
        self._last_processed: dict[str, float] = {}

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith(".log"):
            self._handle(event.src_path)

    def on_modified(self, event):
        if not event.is_directory and event.src_path.endswith(".log"):
            self._handle(event.src_path)

    def _handle(self, filepath: str):
        """Debounce and process a .log file."""
        now = time.time()
        last = self._last_processed.get(filepath, 0)
        if now - last < DEBOUNCE_SECONDS:
            return
        self._last_processed[filepath] = now
        self.process_file(filepath)

    def process_file(self, filepath: str):
        """Read entire file, parse every line, write output atomically, update stats."""
        logger.info("Processing: %s", filepath)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except OSError as e:
            logger.error("Failed to read %s: %s", filepath, e)
            return

        entries = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            entries.append(parse_line(stripped))

        self._stats.record_file(filepath, entries)

        output_dicts = [entry_to_dict(e) for e in entries]
        basename = os.path.splitext(os.path.basename(filepath))[0]
        output_name = f"parsed_{basename}.json"

        os.makedirs(self._output_dir, exist_ok=True)
        target = os.path.join(self._output_dir, output_name)
        tmp_fd, tmp_path = tempfile.mkstemp(dir=self._output_dir, suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(output_dicts, f, indent=2)
                f.write("\n")
            os.replace(tmp_path, target)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

        self._stats.save()

        success = sum(1 for e in entries if e.parsed)
        failure = len(entries) - success
        logger.info("  -> %s: %d parsed, %d failed", output_name, success, failure)

    def process_existing_files(self, input_dir: str):
        """Scan input directory for existing .log files at startup."""
        if not os.path.isdir(input_dir):
            return
        for name in sorted(os.listdir(input_dir)):
            if name.endswith(".log"):
                self.process_file(os.path.join(input_dir, name))
