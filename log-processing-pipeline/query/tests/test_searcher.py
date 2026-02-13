"""Tests for the query searcher."""

import json
import os
import tempfile
import unittest

from query.src.searcher import search_by_pattern, search_by_index


class TestSearcher(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.mkdtemp()
        self._storage = self._tmpdir
        self._active = os.path.join(self._storage, "active")
        self._archive = os.path.join(self._storage, "archive")
        self._index = os.path.join(self._storage, "index")
        os.makedirs(self._active)
        os.makedirs(self._archive)

        # Write sample NDJSON
        entries = [
            {"method": "GET", "path": "/", "status_code": 200, "level": "INFO",
             "timestamp": "2026-02-13T10:00:00"},
            {"method": "POST", "path": "/api", "status_code": 404, "level": "WARNING",
             "timestamp": "2026-02-13T10:00:01"},
            {"method": "GET", "path": "/error", "status_code": 500, "level": "ERROR",
             "timestamp": "2026-02-13T10:00:02"},
        ]
        with open(os.path.join(self._active, "store_current.ndjson"), "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")

        # Write index
        idx_dir = os.path.join(self._index, "level", "ERROR")
        os.makedirs(idx_dir)
        with open(os.path.join(idx_dir, "manifest.json"), "w") as f:
            json.dump([{"file": "store_current.ndjson", "line_numbers": [2]}], f)

    def test_pattern_search(self):
        results = list(search_by_pattern(self._storage, "GET", 10))
        self.assertEqual(len(results), 2)

    def test_pattern_search_with_limit(self):
        results = list(search_by_pattern(self._storage, "GET", 1))
        self.assertEqual(len(results), 1)

    def test_pattern_search_no_match(self):
        results = list(search_by_pattern(self._storage, "NONEXISTENT", 10))
        self.assertEqual(len(results), 0)

    def test_index_search(self):
        results = list(search_by_index(self._storage, "level", "ERROR", 10))
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["level"], "ERROR")

    def test_index_search_missing(self):
        results = list(search_by_index(self._storage, "level", "DEBUG", 10))
        self.assertEqual(len(results), 0)


if __name__ == "__main__":
    unittest.main()
