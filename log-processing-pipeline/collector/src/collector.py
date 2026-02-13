"""Core collector logic — reads new lines from source and writes batch files."""

import os
import tempfile
from datetime import datetime, timezone

from collector.src.offset_tracker import OffsetTracker


class Collector:
    def __init__(self, source_file: str, output_dir: str, batch_size: int,
                 tracker: OffsetTracker):
        self._source = source_file
        self._output_dir = output_dir
        self._batch_size = batch_size
        self._tracker = tracker
        self._batch_counter = 0
        os.makedirs(self._output_dir, exist_ok=True)

    def poll_once(self) -> int:
        """Read new lines from source, write batch files. Return lines collected."""
        if not os.path.exists(self._source):
            return 0

        stat = os.stat(self._source)
        self._tracker.check_truncation(stat.st_size, stat.st_ino)

        if stat.st_size <= self._tracker.offset:
            return 0

        total = 0
        with open(self._source, "r") as f:
            f.seek(self._tracker.offset)
            batch: list[str] = []
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue
                batch.append(line)
                if len(batch) >= self._batch_size:
                    self._write_batch(batch)
                    total += len(batch)
                    batch = []
            if batch:
                self._write_batch(batch)
                total += len(batch)
            self._tracker.offset = f.tell()

        self._tracker.inode = stat.st_ino
        self._tracker.save()
        return total

    def _write_batch(self, lines: list[str]) -> None:
        now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        self._batch_counter += 1
        name = f"batch_{now}_{self._batch_counter:06d}.log"
        dest = os.path.join(self._output_dir, name)

        fd, tmp = tempfile.mkstemp(dir=self._output_dir)
        try:
            with os.fdopen(fd, "w") as f:
                for line in lines:
                    f.write(line + "\n")
            os.replace(tmp, dest)
        except Exception:
            os.unlink(tmp)
            raise
