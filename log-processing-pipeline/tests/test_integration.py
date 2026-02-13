"""End-to-end integration test — exercises all components via direct Python calls."""

import json
import os
import tempfile
import unittest

from generator.src.apache_formatter import generate_apache_line
from generator.src.writer import LogFileWriter
from collector.src.offset_tracker import OffsetTracker
from collector.src.collector import Collector
from parser.src.state_tracker import StateTracker as ParserStateTracker
from parser.src.parser import Parser
from storage.src.state_tracker import StateTracker as StorageStateTracker
from storage.src.indexer import Indexer
from storage.src.rotator import Rotator
from storage.src.storage import StorageEngine
from query.src.searcher import search_by_pattern, search_by_index


class TestIntegration(unittest.TestCase):
    def test_full_pipeline(self):
        base = tempfile.mkdtemp()
        log_file = os.path.join(base, "logs", "app.log")
        collected_dir = os.path.join(base, "collected")
        parsed_dir = os.path.join(base, "parsed")
        storage_dir = os.path.join(base, "storage")

        # 1. Generate 20 lines
        writer = LogFileWriter(log_file)
        for _ in range(20):
            writer.write(generate_apache_line())
        writer.close()

        # 2. Collect
        tracker = OffsetTracker(os.path.join(collected_dir, ".state.json"))
        collector = Collector(log_file, collected_dir, 100, tracker)
        n_collected = collector.poll_once()
        self.assertEqual(n_collected, 20)

        batch_files = [f for f in os.listdir(collected_dir) if f.endswith(".log")]
        self.assertGreater(len(batch_files), 0)

        # 3. Parse
        p_tracker = ParserStateTracker(os.path.join(parsed_dir, ".state.json"))
        parser = Parser(collected_dir, parsed_dir, p_tracker)
        n_parsed = parser.poll_once()
        self.assertEqual(n_parsed, 20)

        json_files = [f for f in os.listdir(parsed_dir) if f.endswith(".json") and not f.startswith(".")]
        self.assertGreater(len(json_files), 0)

        # 4. Store
        s_tracker = StorageStateTracker(os.path.join(storage_dir, ".state.json"))
        indexer = Indexer(os.path.join(storage_dir, "index"))
        rotator = Rotator(
            os.path.join(storage_dir, "active"),
            os.path.join(storage_dir, "archive"),
            size_threshold_bytes=5 * 1024 * 1024,
            age_threshold_seconds=86400,
        )
        engine = StorageEngine(parsed_dir, storage_dir, s_tracker, indexer, rotator)
        n_stored = engine.poll_once()
        self.assertEqual(n_stored, 20)

        active = os.path.join(storage_dir, "active", "store_current.ndjson")
        self.assertTrue(os.path.exists(active))
        with open(active) as f:
            lines = f.readlines()
        self.assertEqual(len(lines), 20)

        # 5. Query by pattern
        results = list(search_by_pattern(storage_dir, "HTTP", 50))
        self.assertEqual(len(results), 20)

        # 6. Query by index — at least some INFO entries should exist
        info_results = list(search_by_index(storage_dir, "level", "INFO", 50))
        self.assertGreater(len(info_results), 0)

        # Verify all results have the expected fields
        for r in results:
            self.assertIn("timestamp", r)
            self.assertIn("remote_host", r)
            self.assertIn("method", r)
            self.assertIn("path", r)
            self.assertIn("status_code", r)
            self.assertIn("level", r)


if __name__ == "__main__":
    unittest.main()
