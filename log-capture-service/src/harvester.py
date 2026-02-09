"""LogHarvester: watchdog event handler that reads new log lines from watched files."""

import os
import logging
import queue

from watchdog.events import FileSystemEventHandler

from src.registry import OffsetRegistry

logger = logging.getLogger(__name__)


class LogHarvester(FileSystemEventHandler):
    def __init__(self, watched_files: list[str], q: queue.Queue, registry: OffsetRegistry):
        super().__init__()
        self._watched = {os.path.abspath(f) for f in watched_files}
        self._queue = q
        self._registry = registry
        self._file_handles: dict[str, object] = {}
        self._partial_lines: dict[str, str] = {}

    def _open_file(self, path: str):
        """Open file and seek to registered offset. Handles rotation and truncation."""
        abs_path = os.path.abspath(path)

        # Close existing handle if any
        if abs_path in self._file_handles:
            self._file_handles[abs_path].close()

        try:
            stat = os.stat(abs_path)
        except FileNotFoundError:
            logger.debug("File not found: %s", abs_path)
            return

        current_inode = stat.st_ino
        file_size = stat.st_size
        saved_offset = self._registry.get_offset(abs_path)
        saved_inode = self._registry.get_inode(abs_path)

        # Detect rotation (inode changed) or truncation (file smaller than offset)
        if saved_inode is not None and saved_inode != current_inode:
            logger.info("File rotated (inode changed): %s", abs_path)
            saved_offset = 0
        elif file_size < saved_offset:
            logger.info("File truncated: %s", abs_path)
            saved_offset = 0

        fh = open(abs_path, "r", encoding="utf-8", errors="replace")
        fh.seek(saved_offset)
        self._file_handles[abs_path] = fh
        self._registry.update(abs_path, saved_offset, current_inode)
        logger.debug("Opened %s at offset %d", abs_path, saved_offset)

    def _read_new_lines(self, path: str):
        """Read from current position to EOF, enqueue complete lines."""
        abs_path = os.path.abspath(path)

        if abs_path not in self._file_handles:
            self._open_file(abs_path)
        if abs_path not in self._file_handles:
            return

        fh = self._file_handles[abs_path]
        data = fh.read()
        if not data:
            return

        # Prepend any partial line from last read
        if abs_path in self._partial_lines:
            data = self._partial_lines.pop(abs_path) + data

        lines = data.split("\n")

        # If data doesn't end with \n, last element is a partial line
        if not data.endswith("\n"):
            self._partial_lines[abs_path] = lines[-1]
            lines = lines[:-1]

        for line in lines:
            stripped = line.strip()
            if stripped:
                self._queue.put((stripped, abs_path))

        # Update registry with current file position
        offset = fh.tell()
        try:
            inode = os.stat(abs_path).st_ino
        except FileNotFoundError:
            inode = 0
        self._registry.update(abs_path, offset, inode)

    def on_modified(self, event):
        if event.is_directory:
            return
        abs_path = os.path.abspath(event.src_path)
        if abs_path in self._watched:
            self._read_new_lines(abs_path)

    def on_created(self, event):
        if event.is_directory:
            return
        abs_path = os.path.abspath(event.src_path)
        if abs_path in self._watched:
            logger.info("Watched file created: %s", abs_path)
            self._open_file(abs_path)
            self._read_new_lines(abs_path)

    def startup_read(self):
        """Read any existing content from watched files at startup."""
        for path in self._watched:
            if os.path.exists(path):
                logger.info("Startup read: %s", path)
                self._open_file(path)
                self._read_new_lines(path)

    def close_all(self):
        """Close all open file handles."""
        for fh in self._file_handles.values():
            try:
                fh.close()
            except Exception:
                pass
        self._file_handles.clear()

    def get_watched_dirs(self) -> set[str]:
        """Return unique parent directories of watched files (for Observer scheduling)."""
        return {os.path.dirname(p) for p in self._watched}
