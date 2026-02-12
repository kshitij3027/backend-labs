"""Tests for src/formatter.py"""

import json
import unittest
from datetime import datetime

from src.parser import LogEntry
from src.formatter import (
    COLORS,
    RESET,
    format_color,
    format_json,
    format_text,
    get_formatter,
)


def _entry(
    ts="2025-05-15 14:30:00",
    level="INFO",
    message="Server started",
) -> LogEntry:
    return LogEntry(
        timestamp=datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"),
        level=level,
        message=message,
        raw=f"[{ts}] [{level}] {message}",
        source_file="app.log",
    )


class TestFormatText(unittest.TestCase):
    def test_returns_raw_line(self):
        entry = _entry()
        self.assertEqual(format_text(entry), entry.raw)


class TestFormatJson(unittest.TestCase):
    def test_valid_json(self):
        entry = _entry()
        result = format_json(entry)
        parsed = json.loads(result)
        self.assertEqual(parsed["level"], "INFO")
        self.assertEqual(parsed["message"], "Server started")
        self.assertEqual(parsed["source_file"], "app.log")

    def test_timestamp_is_iso(self):
        entry = _entry()
        parsed = json.loads(format_json(entry))
        self.assertEqual(parsed["timestamp"], "2025-05-15T14:30:00")


class TestFormatColor(unittest.TestCase):
    def test_info_uses_green(self):
        entry = _entry(level="INFO")
        result = format_color(entry)
        self.assertIn(COLORS["INFO"], result)
        self.assertIn(RESET, result)

    def test_error_uses_red(self):
        entry = _entry(level="ERROR")
        result = format_color(entry)
        self.assertIn(COLORS["ERROR"], result)

    def test_contains_message(self):
        entry = _entry(message="my message")
        result = format_color(entry)
        self.assertIn("my message", result)


class TestGetFormatter(unittest.TestCase):
    def test_default_is_text(self):
        fmt = get_formatter()
        self.assertEqual(fmt, format_text)

    def test_json_format(self):
        fmt = get_formatter(output_format="json")
        self.assertEqual(fmt, format_json)

    def test_color_flag(self):
        fmt = get_formatter(color=True)
        self.assertEqual(fmt, format_color)

    def test_json_overrides_color(self):
        fmt = get_formatter(output_format="json", color=True)
        self.assertEqual(fmt, format_json)


if __name__ == "__main__":
    unittest.main()
