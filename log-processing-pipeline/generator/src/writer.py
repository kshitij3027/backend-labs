"""Appends log lines to a file with immediate flushing."""

import os


class LogFileWriter:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._f = open(path, "a")

    def write(self, line: str) -> None:
        self._f.write(line + "\n")
        self._f.flush()

    def close(self) -> None:
        self._f.close()
