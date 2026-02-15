"""File reader with batch and continuous tailing modes."""

import logging
import os
import threading
import time

logger = logging.getLogger(__name__)


def read_batch(path: str) -> list[str]:
    """Read all non-empty stripped lines from a file."""
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


class FileTailer:
    """Watches a log file for new lines and calls a callback for each one.

    Handles:
    - File not yet existing (waits for creation)
    - Log rotation (inode change detection)
    - File truncation (seek back to start)
    """

    def __init__(
        self,
        path: str,
        shutdown_event: threading.Event,
        callback=None,
        poll_interval: float = 0.5,
    ):
        self._path = path
        self._shutdown = shutdown_event
        self._callback = callback
        self._poll_interval = poll_interval
        self._file = None
        self._inode = None

    def run(self):
        """Main tailing loop — blocks until shutdown_event is set."""
        self._wait_for_file()
        if self._shutdown.is_set():
            return

        self._open_file(seek_end=True)

        while not self._shutdown.is_set():
            if self._check_rotation():
                continue

            if self._check_truncation():
                continue

            line = self._file.readline()
            if line:
                stripped = line.strip()
                if stripped and self._callback:
                    self._callback(stripped)
            else:
                self._shutdown.wait(self._poll_interval)

        self._close_file()

    def _wait_for_file(self):
        """Block until the file exists or shutdown is requested."""
        while not self._shutdown.is_set():
            if os.path.exists(self._path):
                return
            logger.debug("Waiting for file %s to appear...", self._path)
            self._shutdown.wait(self._poll_interval)

    def _open_file(self, seek_end: bool = False):
        """Open the file and optionally seek to the end."""
        self._file = open(self._path, "r", encoding="utf-8")
        self._inode = os.fstat(self._file.fileno()).st_ino
        if seek_end:
            self._file.seek(0, os.SEEK_END)
        logger.debug("Opened %s (inode=%d)", self._path, self._inode)

    def _close_file(self):
        """Close the current file handle."""
        if self._file:
            self._file.close()
            self._file = None

    def _check_rotation(self) -> bool:
        """Detect log rotation by comparing inodes. Returns True if rotated."""
        try:
            current_inode = os.stat(self._path).st_ino
        except FileNotFoundError:
            return False

        if current_inode != self._inode:
            logger.info("File rotation detected for %s", self._path)
            # Read any remaining lines from old file
            for line in self._file:
                stripped = line.strip()
                if stripped and self._callback:
                    self._callback(stripped)
            self._close_file()
            self._open_file(seek_end=False)
            return True
        return False

    def _check_truncation(self) -> bool:
        """Detect file truncation (e.g., > file). Returns True if truncated."""
        try:
            file_size = os.path.getsize(self._path)
        except FileNotFoundError:
            return False

        current_pos = self._file.tell()
        if current_pos > file_size:
            logger.info("File truncation detected for %s", self._path)
            self._file.seek(0)
            return True
        return False
