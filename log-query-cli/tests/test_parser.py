"""Tests for src/parser.py"""

import unittest
from datetime import datetime

from src.parser import LogEntry, parse_line, LOG_PATTERN


class TestLogPattern(unittest.TestCase):
    """Verify the compiled regex matches expected log formats."""

    def test_matches_standard_format(self):
        line = "[2025-05-15 14:30:00] [INFO] Server started on port 8080"
        self.assertIsNotNone(LOG_PATTERN.match(line))

    def test_no_match_on_empty(self):
        self.assertIsNone(LOG_PATTERN.match(""))

    def test_no_match_on_plain_text(self):
        self.assertIsNone(LOG_PATTERN.match("just some random text"))

    def test_captures_groups(self):
        line = "[2025-05-15 14:30:00] [ERROR] Something broke"
        m = LOG_PATTERN.match(line)
        self.assertEqual(m.group(1), "2025-05-15 14:30:00")
        self.assertEqual(m.group(2), "ERROR")
        self.assertEqual(m.group(3), "Something broke")


class TestParseLine(unittest.TestCase):
    """Verify parse_line converts raw lines to LogEntry or None."""

    def test_valid_info_line(self):
        line = "[2025-05-15 14:30:00] [INFO] Server started"
        entry = parse_line(line, source_file="app.log")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.timestamp, datetime(2025, 5, 15, 14, 30, 0))
        self.assertEqual(entry.level, "INFO")
        self.assertEqual(entry.message, "Server started")
        self.assertEqual(entry.source_file, "app.log")
        self.assertEqual(entry.raw, line)

    def test_valid_error_line(self):
        line = "[2025-05-15 14:30:05] [ERROR] Database connection failed"
        entry = parse_line(line)
        self.assertEqual(entry.level, "ERROR")
        self.assertEqual(entry.message, "Database connection failed")

    def test_level_normalized_to_upper(self):
        line = "[2025-05-15 14:30:00] [info] lowercase level"
        entry = parse_line(line)
        self.assertIsNotNone(entry)
        self.assertEqual(entry.level, "INFO")

    def test_returns_none_for_empty_line(self):
        self.assertIsNone(parse_line(""))

    def test_returns_none_for_garbage(self):
        self.assertIsNone(parse_line("not a log line at all"))

    def test_returns_none_for_bad_timestamp(self):
        line = "[9999-99-99 99:99:99] [INFO] bad date"
        self.assertIsNone(parse_line(line))

    def test_strips_trailing_newline(self):
        line = "[2025-05-15 14:30:00] [INFO] with newline\n"
        entry = parse_line(line)
        self.assertIsNotNone(entry)
        self.assertFalse(entry.raw.endswith("\n"))

    def test_message_with_special_characters(self):
        line = "[2025-05-15 14:30:00] [WARN] user='admin' action=\"DELETE\" path=/api/v1"
        entry = parse_line(line)
        self.assertIsNotNone(entry)
        self.assertIn("admin", entry.message)

    def test_default_source_file_empty(self):
        line = "[2025-05-15 14:30:00] [DEBUG] test"
        entry = parse_line(line)
        self.assertEqual(entry.source_file, "")


class TestLogEntryFrozen(unittest.TestCase):
    """Verify LogEntry is immutable."""

    def test_cannot_mutate_level(self):
        line = "[2025-05-15 14:30:00] [INFO] test"
        entry = parse_line(line)
        with self.assertRaises(AttributeError):
            entry.level = "ERROR"

    def test_cannot_mutate_message(self):
        line = "[2025-05-15 14:30:00] [INFO] test"
        entry = parse_line(line)
        with self.assertRaises(AttributeError):
            entry.message = "changed"


if __name__ == "__main__":
    unittest.main()
