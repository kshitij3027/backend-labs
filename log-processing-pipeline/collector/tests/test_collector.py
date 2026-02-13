"""Tests for the collector batch writing."""

import os
import tempfile
import unittest

from collector.src.offset_tracker import OffsetTracker
from collector.src.collector import Collector


class TestCollector(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._source = os.path.join(self._tmpdir, "app.log")
        self._output = os.path.join(self._tmpdir, "collected")
        self._state = os.path.join(self._output, "state.json")
        os.makedirs(self._output, exist_ok=True)

    def _write_source(self, lines: list[str]) -> None:
        with open(self._source, "w") as f:
            for line in lines:
                f.write(line + "\n")

    def test_collects_all_lines(self):
        lines = [f"line {i}" for i in range(10)]
        self._write_source(lines)
        tracker = OffsetTracker(self._state)
        c = Collector(self._source, self._output, 100, tracker)
        n = c.poll_once()
        self.assertEqual(n, 10)

    def test_batch_splitting(self):
        lines = [f"line {i}" for i in range(25)]
        self._write_source(lines)
        tracker = OffsetTracker(self._state)
        c = Collector(self._source, self._output, 10, tracker)
        n = c.poll_once()
        self.assertEqual(n, 25)
        # Should have 3 batches (10, 10, 5)
        batch_files = [f for f in os.listdir(self._output) if f.startswith("batch_")]
        self.assertEqual(len(batch_files), 3)

    def test_incremental_reads(self):
        self._write_source(["line 1", "line 2"])
        tracker = OffsetTracker(self._state)
        c = Collector(self._source, self._output, 100, tracker)
        n1 = c.poll_once()
        self.assertEqual(n1, 2)

        # Append more lines
        with open(self._source, "a") as f:
            f.write("line 3\nline 4\n")

        n2 = c.poll_once()
        self.assertEqual(n2, 2)

    def test_no_new_data_returns_zero(self):
        self._write_source(["line 1"])
        tracker = OffsetTracker(self._state)
        c = Collector(self._source, self._output, 100, tracker)
        c.poll_once()
        n = c.poll_once()
        self.assertEqual(n, 0)


if __name__ == "__main__":
    unittest.main()
