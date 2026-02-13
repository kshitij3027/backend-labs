"""Tests for the offset tracker."""

import json
import os
import tempfile
import unittest

from collector.src.offset_tracker import OffsetTracker


class TestOffsetTracker(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._state_file = os.path.join(self._tmpdir, "state.json")

    def test_initial_state(self):
        t = OffsetTracker(self._state_file)
        self.assertEqual(t.offset, 0)
        self.assertEqual(t.inode, 0)

    def test_save_and_load(self):
        t = OffsetTracker(self._state_file)
        t.offset = 1234
        t.inode = 5678
        t.save()

        t2 = OffsetTracker(self._state_file)
        self.assertEqual(t2.offset, 1234)
        self.assertEqual(t2.inode, 5678)

    def test_truncation_resets_offset(self):
        t = OffsetTracker(self._state_file)
        t.offset = 500
        t.inode = 100
        # File shrank to 200 bytes
        t.check_truncation(200, 100)
        self.assertEqual(t.offset, 0)

    def test_inode_change_resets_offset(self):
        t = OffsetTracker(self._state_file)
        t.offset = 500
        t.inode = 100
        # Different inode = new file
        t.check_truncation(1000, 200)
        self.assertEqual(t.offset, 0)
        self.assertEqual(t.inode, 200)

    def test_no_reset_when_file_grows(self):
        t = OffsetTracker(self._state_file)
        t.offset = 500
        t.inode = 100
        t.check_truncation(1000, 100)
        self.assertEqual(t.offset, 500)


if __name__ == "__main__":
    unittest.main()
