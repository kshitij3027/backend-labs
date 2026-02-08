"""Dual output writer: file + optional console, thread-safe."""

import os
import sys
import threading


class LogWriter:
    def __init__(self, output_file: str, console_enabled: bool, log_format: str):
        self._output_file = output_file
        self._console_enabled = console_enabled
        self._log_format = log_format
        self._lock = threading.Lock()
        self._file_handle = None
        self._ensure_directory()
        self._open_file()

    def _ensure_directory(self):
        dir_path = os.path.dirname(self._output_file)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)

    def _open_file(self):
        is_new = (
            not os.path.exists(self._output_file)
            or os.path.getsize(self._output_file) == 0
        )
        self._file_handle = open(self._output_file, "a", encoding="utf-8")
        if is_new and self._log_format == "csv":
            from src.formatters import CSV_HEADER
            self._file_handle.write(CSV_HEADER + "\n")
            self._file_handle.flush()

    def write(self, line: str):
        with self._lock:
            self._file_handle.write(line + "\n")
            self._file_handle.flush()
            if self._console_enabled:
                sys.stdout.write(line + "\n")
                sys.stdout.flush()

    def close(self):
        if self._file_handle:
            self._file_handle.close()
