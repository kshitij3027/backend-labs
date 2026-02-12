"""Integration tests — E2E via subprocess against sample.log."""

import json
import os
import subprocess
import sys
import unittest

SAMPLE_LOG = os.path.join(os.path.dirname(__file__), "..", "logs", "sample.log")
MAIN_PY = os.path.join(os.path.dirname(__file__), "..", "main.py")


def _run(*args: str) -> subprocess.CompletedProcess:
    """Run main.py with given args, return CompletedProcess."""
    return subprocess.run(
        [sys.executable, MAIN_PY, SAMPLE_LOG, *args],
        capture_output=True,
        text=True,
    )


class TestNoFlags(unittest.TestCase):
    def test_all_entries_displayed(self):
        result = _run()
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split("\n")
        self.assertEqual(len(lines), 20)


class TestLevelFilter(unittest.TestCase):
    def test_error_only(self):
        result = _run("--level", "ERROR")
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split("\n")
        self.assertEqual(len(lines), 5)
        self.assertTrue(all("[ERROR]" in l for l in lines))

    def test_debug_only(self):
        result = _run("--level", "DEBUG")
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split("\n")
        self.assertEqual(len(lines), 3)

    def test_warn_only(self):
        result = _run("--level", "WARN")
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split("\n")
        self.assertEqual(len(lines), 3)


class TestSearchFilter(unittest.TestCase):
    def test_case_insensitive_search(self):
        result = _run("--search", "database")
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split("\n")
        self.assertEqual(len(lines), 4)

    def test_no_results(self):
        result = _run("--search", "zzz_nonexistent_zzz")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "")


class TestDateFilter(unittest.TestCase):
    def test_matching_date(self):
        result = _run("--date", "2025-05-15")
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split("\n")
        self.assertEqual(len(lines), 20)

    def test_non_matching_date(self):
        result = _run("--date", "2025-01-01")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "")


class TestTimeRange(unittest.TestCase):
    def test_narrow_range(self):
        result = _run("--time-range", "14:25-14:26")
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split("\n")
        # entries at 14:25:00, 14:25:01, 14:25:02, 14:25:03 (14:26:10 > 14:26:00)
        self.assertEqual(len(lines), 4)


class TestCombinedFilters(unittest.TestCase):
    def test_level_and_search(self):
        result = _run("--level", "ERROR", "--search", "database")
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split("\n")
        self.assertEqual(len(lines), 1)
        self.assertIn("Database connection timeout", lines[0])


class TestLinesLimit(unittest.TestCase):
    def test_limit_3(self):
        result = _run("--lines", "3")
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split("\n")
        self.assertEqual(len(lines), 3)

    def test_limit_1(self):
        result = _run("--lines", "1")
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split("\n")
        self.assertEqual(len(lines), 1)


class TestStatsMode(unittest.TestCase):
    def test_stats_text(self):
        result = _run("--stats")
        self.assertEqual(result.returncode, 0)
        self.assertIn("Total entries: 20", result.stdout)
        self.assertIn("INFO", result.stdout)
        self.assertIn("ERROR", result.stdout)

    def test_stats_json(self):
        result = _run("--stats", "--output", "json")
        self.assertEqual(result.returncode, 0)
        parsed = json.loads(result.stdout)
        self.assertEqual(parsed["total_entries"], 20)
        self.assertEqual(parsed["level_counts"]["ERROR"], 5)


class TestJsonOutput(unittest.TestCase):
    def test_valid_ndjson(self):
        result = _run("--output", "json", "--lines", "3")
        self.assertEqual(result.returncode, 0)
        lines = result.stdout.strip().split("\n")
        self.assertEqual(len(lines), 3)
        for line in lines:
            parsed = json.loads(line)
            self.assertIn("timestamp", parsed)
            self.assertIn("level", parsed)
            self.assertIn("message", parsed)


class TestColorOutput(unittest.TestCase):
    def test_contains_ansi_codes(self):
        result = _run("--color", "--lines", "1")
        self.assertEqual(result.returncode, 0)
        self.assertIn("\033[", result.stdout)


class TestValidation(unittest.TestCase):
    def test_tail_and_stats_error(self):
        result = _run("--tail", "--stats")
        self.assertEqual(result.returncode, 1)
        self.assertIn("--tail and --stats cannot be used together", result.stderr)

    def test_nonexistent_file(self):
        proc = subprocess.run(
            [sys.executable, MAIN_PY, "/nonexistent/file.log"],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(proc.returncode, 0)


if __name__ == "__main__":
    unittest.main()
