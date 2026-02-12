"""Tests for src/stats.py"""

import json
import unittest
from datetime import datetime

from src.parser import LogEntry
from src.stats import LogStats, compute_stats, format_stats_json, format_stats_text


def _entry(
    ts="2025-05-15 14:30:00",
    level="INFO",
    message="test message",
) -> LogEntry:
    return LogEntry(
        timestamp=datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"),
        level=level,
        message=message,
        raw=f"[{ts}] [{level}] {message}",
        source_file="test.log",
    )


class TestComputeStats(unittest.TestCase):
    def test_empty_stream(self):
        stats = compute_stats(iter([]))
        self.assertEqual(stats.total_entries, 0)
        self.assertEqual(stats.level_counts, {})
        self.assertEqual(stats.error_messages, [])

    def test_counts_entries(self):
        entries = [_entry(), _entry(), _entry()]
        stats = compute_stats(iter(entries))
        self.assertEqual(stats.total_entries, 3)

    def test_level_counts(self):
        entries = [
            _entry(level="INFO"),
            _entry(level="ERROR"),
            _entry(level="INFO"),
            _entry(level="DEBUG"),
        ]
        stats = compute_stats(iter(entries))
        self.assertEqual(stats.level_counts["INFO"], 2)
        self.assertEqual(stats.level_counts["ERROR"], 1)
        self.assertEqual(stats.level_counts["DEBUG"], 1)

    def test_entries_per_hour(self):
        entries = [
            _entry(ts="2025-05-15 14:00:00"),
            _entry(ts="2025-05-15 14:30:00"),
            _entry(ts="2025-05-15 15:00:00"),
        ]
        stats = compute_stats(iter(entries))
        self.assertEqual(stats.entries_per_hour["2025-05-15 14:00"], 2)
        self.assertEqual(stats.entries_per_hour["2025-05-15 15:00"], 1)

    def test_error_messages_collected(self):
        entries = [
            _entry(level="ERROR", message="Disk full"),
            _entry(level="INFO", message="OK"),
            _entry(level="ERROR", message="Timeout"),
        ]
        stats = compute_stats(iter(entries))
        self.assertEqual(len(stats.error_messages), 2)
        self.assertIn("Disk full", stats.error_messages)
        self.assertIn("Timeout", stats.error_messages)

    def test_no_errors_empty_list(self):
        entries = [_entry(level="INFO")]
        stats = compute_stats(iter(entries))
        self.assertEqual(stats.error_messages, [])


class TestFormatStatsText(unittest.TestCase):
    def test_contains_total(self):
        stats = LogStats(total_entries=42, level_counts={"INFO": 42})
        text = format_stats_text(stats)
        self.assertIn("Total entries: 42", text)

    def test_contains_level_counts(self):
        stats = LogStats(total_entries=2, level_counts={"ERROR": 1, "INFO": 1})
        text = format_stats_text(stats)
        self.assertIn("ERROR", text)
        self.assertIn("INFO", text)

    def test_no_errors_message(self):
        stats = LogStats(total_entries=1, level_counts={"INFO": 1})
        text = format_stats_text(stats)
        self.assertIn("No error messages", text)

    def test_shows_error_messages(self):
        stats = LogStats(
            total_entries=1,
            level_counts={"ERROR": 1},
            error_messages=["Disk full"],
        )
        text = format_stats_text(stats)
        self.assertIn("Disk full", text)


class TestFormatStatsJson(unittest.TestCase):
    def test_valid_json(self):
        stats = LogStats(
            total_entries=5,
            level_counts={"INFO": 3, "ERROR": 2},
            entries_per_hour={"2025-05-15 14:00": 5},
            error_messages=["err1"],
        )
        parsed = json.loads(format_stats_json(stats))
        self.assertEqual(parsed["total_entries"], 5)
        self.assertEqual(parsed["level_counts"]["INFO"], 3)
        self.assertEqual(parsed["error_messages"], ["err1"])


if __name__ == "__main__":
    unittest.main()
