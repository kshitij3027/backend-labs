"""Tests for the storage rotator."""

import os
import tempfile
import time
import unittest

from storage.src.rotator import Rotator


class TestRotator(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._active = os.path.join(self._tmpdir, "active")
        self._archive = os.path.join(self._tmpdir, "archive")

    def test_no_rotation_when_small(self):
        r = Rotator(self._active, self._archive,
                    size_threshold_bytes=1024, age_threshold_seconds=3600)
        # Create small file
        path = os.path.join(self._active, "store_current.ndjson")
        with open(path, "w") as f:
            f.write("small\n")
        self.assertFalse(r.needs_rotation(path))

    def test_rotation_when_size_exceeds(self):
        r = Rotator(self._active, self._archive,
                    size_threshold_bytes=10, age_threshold_seconds=3600)
        path = os.path.join(self._active, "store_current.ndjson")
        with open(path, "w") as f:
            f.write("x" * 100 + "\n")
        self.assertTrue(r.needs_rotation(path))

    def test_rotate_moves_file(self):
        r = Rotator(self._active, self._archive,
                    size_threshold_bytes=10, age_threshold_seconds=3600)
        path = os.path.join(self._active, "store_current.ndjson")
        with open(path, "w") as f:
            f.write("data\n")
        archive_path = r.rotate(path)
        self.assertIsNotNone(archive_path)
        self.assertFalse(os.path.exists(path))
        self.assertTrue(os.path.exists(archive_path))

    def test_rotate_nonexistent_returns_none(self):
        r = Rotator(self._active, self._archive,
                    size_threshold_bytes=10, age_threshold_seconds=3600)
        result = r.rotate(os.path.join(self._active, "nope.ndjson"))
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
