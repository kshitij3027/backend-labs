"""Tests for the inspector module."""

import gzip
import os
import shutil
import tempfile
import unittest

from src.inspector import list_log_files, read_file, search_files


class TestListLogFiles(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _touch(self, name, content=""):
        path = os.path.join(self.tmpdir, name)
        with open(path, "w") as f:
            f.write(content)

    def test_discovers_all_log_types(self):
        self._touch("application.log")
        self._touch("application.log.20250115_120000_000000")
        self._touch("application.log.20250115_130000_000000.gz")
        self._touch("unrelated.txt")
        self._touch("notes.md")

        result = list_log_files(self.tmpdir)

        self.assertEqual(result, [
            "application.log",
            "application.log.20250115_120000_000000",
            "application.log.20250115_130000_000000.gz",
        ])

    def test_empty_directory(self):
        self.assertEqual(list_log_files(self.tmpdir), [])


class TestReadFile(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_read_plain_text(self):
        path = os.path.join(self.tmpdir, "test.log")
        with open(path, "w") as f:
            f.write("hello\nworld\n")

        content = read_file(self.tmpdir, "test.log")
        self.assertEqual(content, "hello\nworld\n")

    def test_read_gzip(self):
        path = os.path.join(self.tmpdir, "test.log.20250115_120000_000000.gz")
        with gzip.open(path, "wt") as f:
            f.write("compressed line\n")

        content = read_file(self.tmpdir, "test.log.20250115_120000_000000.gz")
        self.assertEqual(content, "compressed line\n")

    def test_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            read_file(self.tmpdir, "nonexistent.log")


class TestSearchFiles(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_search_across_plain_and_compressed(self):
        # Plain file
        with open(os.path.join(self.tmpdir, "app.log"), "w") as f:
            f.write("INFO all good\nERROR something broke\nINFO fine\n")

        # Compressed file
        with gzip.open(
            os.path.join(self.tmpdir, "app.log.20250115_120000_000000.gz"), "wt"
        ) as f:
            f.write("INFO old stuff\nERROR old failure\n")

        results = search_files(self.tmpdir, "ERROR")

        self.assertEqual(len(results), 2)
        filenames = [r[0] for r in results]
        self.assertIn("app.log", filenames)
        self.assertIn("app.log.20250115_120000_000000.gz", filenames)

    def test_search_no_results(self):
        with open(os.path.join(self.tmpdir, "app.log"), "w") as f:
            f.write("INFO all good\n")

        results = search_files(self.tmpdir, "FATAL")
        self.assertEqual(results, [])

    def test_search_returns_line_numbers(self):
        with open(os.path.join(self.tmpdir, "app.log"), "w") as f:
            f.write("line one\nline two TARGET\nline three\nline four TARGET\n")

        results = search_files(self.tmpdir, "TARGET")

        self.assertEqual(len(results), 2)
        self.assertEqual(results[0][1], 2)  # line number
        self.assertEqual(results[1][1], 4)


if __name__ == "__main__":
    unittest.main()
