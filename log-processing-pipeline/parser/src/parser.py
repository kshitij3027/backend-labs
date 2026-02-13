"""Core parser logic — reads batch files and writes parsed JSON."""

import json
import os
import tempfile

from parser.src.auto_detect import parse_line
from parser.src.state_tracker import StateTracker


class Parser:
    def __init__(self, input_dir: str, output_dir: str, tracker: StateTracker):
        self._input_dir = input_dir
        self._output_dir = output_dir
        self._tracker = tracker
        os.makedirs(self._output_dir, exist_ok=True)

    def poll_once(self) -> int:
        """Parse unprocessed batch files. Return number of entries parsed."""
        if not os.path.isdir(self._input_dir):
            return 0

        files = sorted(
            f for f in os.listdir(self._input_dir)
            if f.endswith(".log") and not self._tracker.is_processed(f)
        )

        total = 0
        for filename in files:
            src = os.path.join(self._input_dir, filename)
            entries = self._parse_file(src)
            if entries:
                out_name = f"parsed_{os.path.splitext(filename)[0]}.json"
                self._write_json(entries, out_name)
                total += len(entries)
            self._tracker.mark_processed(filename)

        if files:
            self._tracker.save()
        return total

    def _parse_file(self, path: str) -> list[dict]:
        entries = []
        with open(path, "r") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                entry = parse_line(line)
                if entry:
                    entries.append(entry)
        return entries

    def _write_json(self, entries: list[dict], name: str) -> None:
        dest = os.path.join(self._output_dir, name)
        fd, tmp = tempfile.mkstemp(dir=self._output_dir)
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(entries, f, indent=2)
            os.replace(tmp, dest)
        except Exception:
            os.unlink(tmp)
            raise
