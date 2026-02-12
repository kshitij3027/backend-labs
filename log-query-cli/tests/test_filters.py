"""Tests for src/filters.py"""

import unittest
from argparse import Namespace
from datetime import datetime

from src.parser import LogEntry
from src.filters import (
    build_filter_chain,
    filter_by_date,
    filter_by_level,
    filter_by_search,
    filter_by_time_range,
)


def _entry(
    ts="2025-05-15 14:30:00",
    level="INFO",
    message="test message",
) -> LogEntry:
    """Helper to create a LogEntry for testing."""
    return LogEntry(
        timestamp=datetime.strptime(ts, "%Y-%m-%d %H:%M:%S"),
        level=level,
        message=message,
        raw=f"[{ts}] [{level}] {message}",
        source_file="test.log",
    )


class TestFilterByLevel(unittest.TestCase):
    def test_exact_match(self):
        self.assertTrue(filter_by_level(_entry(level="ERROR"), "ERROR"))

    def test_case_insensitive(self):
        self.assertTrue(filter_by_level(_entry(level="ERROR"), "error"))

    def test_no_match(self):
        self.assertFalse(filter_by_level(_entry(level="INFO"), "ERROR"))


class TestFilterBySearch(unittest.TestCase):
    def test_keyword_found(self):
        self.assertTrue(filter_by_search(_entry(message="Database connection failed"), "database"))

    def test_keyword_not_found(self):
        self.assertFalse(filter_by_search(_entry(message="Server started"), "database"))

    def test_case_insensitive(self):
        self.assertTrue(filter_by_search(_entry(message="DATABASE error"), "database"))

    def test_partial_match(self):
        self.assertTrue(filter_by_search(_entry(message="NullPointerException"), "pointer"))


class TestFilterByDate(unittest.TestCase):
    def test_matching_date(self):
        self.assertTrue(filter_by_date(_entry(ts="2025-05-15 14:30:00"), "2025-05-15"))

    def test_non_matching_date(self):
        self.assertFalse(filter_by_date(_entry(ts="2025-05-15 14:30:00"), "2025-05-16"))


class TestFilterByTimeRange(unittest.TestCase):
    def test_within_range(self):
        entry = _entry(ts="2025-05-15 14:30:00")
        self.assertTrue(filter_by_time_range(entry, "14:00-15:00"))

    def test_at_start_boundary(self):
        entry = _entry(ts="2025-05-15 14:00:00")
        self.assertTrue(filter_by_time_range(entry, "14:00-15:00"))

    def test_at_end_boundary(self):
        entry = _entry(ts="2025-05-15 15:00:00")
        self.assertTrue(filter_by_time_range(entry, "14:00-15:00"))

    def test_outside_range(self):
        entry = _entry(ts="2025-05-15 16:00:00")
        self.assertFalse(filter_by_time_range(entry, "14:00-15:00"))

    def test_cross_midnight_late_evening(self):
        entry = _entry(ts="2025-05-15 23:30:00")
        self.assertTrue(filter_by_time_range(entry, "23:00-01:00"))

    def test_cross_midnight_early_morning(self):
        entry = _entry(ts="2025-05-16 00:30:00")
        self.assertTrue(filter_by_time_range(entry, "23:00-01:00"))

    def test_cross_midnight_outside(self):
        entry = _entry(ts="2025-05-15 12:00:00")
        self.assertFalse(filter_by_time_range(entry, "23:00-01:00"))


class TestBuildFilterChain(unittest.TestCase):
    def test_no_filters_passes_all(self):
        args = Namespace(level=None, search=None, date=None, time_range=None)
        chain = build_filter_chain(args)
        self.assertTrue(chain(_entry()))

    def test_single_level_filter(self):
        args = Namespace(level="ERROR", search=None, date=None, time_range=None)
        chain = build_filter_chain(args)
        self.assertTrue(chain(_entry(level="ERROR")))
        self.assertFalse(chain(_entry(level="INFO")))

    def test_combined_level_and_search(self):
        args = Namespace(level="ERROR", search="database", date=None, time_range=None)
        chain = build_filter_chain(args)
        self.assertTrue(chain(_entry(level="ERROR", message="Database connection failed")))
        self.assertFalse(chain(_entry(level="ERROR", message="Server started")))
        self.assertFalse(chain(_entry(level="INFO", message="Database query")))

    def test_all_filters_combined(self):
        args = Namespace(
            level="ERROR",
            search="timeout",
            date="2025-05-15",
            time_range="14:00-15:00",
        )
        chain = build_filter_chain(args)
        self.assertTrue(
            chain(_entry(ts="2025-05-15 14:30:00", level="ERROR", message="Connection timeout"))
        )
        self.assertFalse(
            chain(_entry(ts="2025-05-15 14:30:00", level="INFO", message="Connection timeout"))
        )
        self.assertFalse(
            chain(_entry(ts="2025-05-16 14:30:00", level="ERROR", message="Connection timeout"))
        )

    def test_missing_attributes_ignored(self):
        args = Namespace()
        chain = build_filter_chain(args)
        self.assertTrue(chain(_entry()))


if __name__ == "__main__":
    unittest.main()
