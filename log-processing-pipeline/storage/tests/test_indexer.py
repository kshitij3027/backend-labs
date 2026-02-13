"""Tests for the storage indexer."""

import json
import os
import tempfile
import unittest

from storage.src.indexer import Indexer


class TestIndexer(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()

    def test_add_and_save(self):
        idx = Indexer(self._tmpdir)
        idx.add_entry("store.ndjson", 0, {
            "level": "ERROR", "timestamp": "2026-02-13T10:00:00+00:00"
        })
        idx.add_entry("store.ndjson", 1, {
            "level": "INFO", "timestamp": "2026-02-13T10:00:01+00:00"
        })
        idx.add_entry("store.ndjson", 2, {
            "level": "ERROR", "timestamp": "2026-02-13T10:00:02+00:00"
        })
        idx.save()

        # Check level/ERROR manifest
        path = os.path.join(self._tmpdir, "level", "ERROR", "manifest.json")
        self.assertTrue(os.path.exists(path))
        with open(path) as f:
            manifest = json.load(f)
        self.assertEqual(len(manifest), 1)
        self.assertEqual(manifest[0]["file"], "store.ndjson")
        self.assertEqual(manifest[0]["line_numbers"], [0, 2])

        # Check date manifest
        path = os.path.join(self._tmpdir, "date", "2026-02-13", "manifest.json")
        self.assertTrue(os.path.exists(path))
        with open(path) as f:
            manifest = json.load(f)
        self.assertEqual(manifest[0]["line_numbers"], [0, 1, 2])

    def test_merge_with_existing(self):
        idx = Indexer(self._tmpdir)
        idx.add_entry("store.ndjson", 0, {"level": "INFO", "timestamp": "2026-01-01T00:00:00"})
        idx.save()

        idx2 = Indexer(self._tmpdir)
        idx2.add_entry("store.ndjson", 5, {"level": "INFO", "timestamp": "2026-01-01T00:00:05"})
        idx2.save()

        path = os.path.join(self._tmpdir, "level", "INFO", "manifest.json")
        with open(path) as f:
            manifest = json.load(f)
        self.assertEqual(manifest[0]["line_numbers"], [0, 5])


if __name__ == "__main__":
    unittest.main()
