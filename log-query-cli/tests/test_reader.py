"""Tests for src/reader.py"""

import os
import tempfile
import threading
import time
import unittest

from src.reader import expand_paths, read_lines, read_multiple, tail_file


class TestReadLines(unittest.TestCase):
    """Verify single-file line reading."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.filepath = os.path.join(self.tmpdir, "test.log")

    def test_reads_all_lines(self):
        with open(self.filepath, "w") as f:
            f.write("line one\nline two\nline three\n")

        lines = list(read_lines(self.filepath))
        self.assertEqual(len(lines), 3)
        self.assertEqual(lines[0], ("line one\n", self.filepath))
        self.assertEqual(lines[2], ("line three\n", self.filepath))

    def test_empty_file(self):
        with open(self.filepath, "w") as f:
            f.write("")

        lines = list(read_lines(self.filepath))
        self.assertEqual(lines, [])

    def test_single_line_no_trailing_newline(self):
        with open(self.filepath, "w") as f:
            f.write("only line")

        lines = list(read_lines(self.filepath))
        self.assertEqual(len(lines), 1)
        self.assertEqual(lines[0][0], "only line")


class TestReadMultiple(unittest.TestCase):
    """Verify multi-file sequential reading."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_reads_files_in_order(self):
        f1 = os.path.join(self.tmpdir, "a.log")
        f2 = os.path.join(self.tmpdir, "b.log")
        with open(f1, "w") as f:
            f.write("from a\n")
        with open(f2, "w") as f:
            f.write("from b\n")

        lines = list(read_multiple([f1, f2]))
        self.assertEqual(len(lines), 2)
        self.assertEqual(lines[0], ("from a\n", f1))
        self.assertEqual(lines[1], ("from b\n", f2))

    def test_empty_list(self):
        lines = list(read_multiple([]))
        self.assertEqual(lines, [])


class TestExpandPaths(unittest.TestCase):
    """Verify glob expansion, dedup, and validation."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def test_plain_file(self):
        f = os.path.join(self.tmpdir, "app.log")
        with open(f, "w") as fh:
            fh.write("data\n")

        result = expand_paths([f])
        self.assertEqual(result, [f])

    def test_glob_expansion(self):
        for name in ("a.log", "b.log", "c.txt"):
            with open(os.path.join(self.tmpdir, name), "w") as f:
                f.write("x\n")

        pattern = os.path.join(self.tmpdir, "*.log")
        result = expand_paths([pattern])
        self.assertEqual(len(result), 2)
        self.assertTrue(all(r.endswith(".log") for r in result))

    def test_deduplication(self):
        f = os.path.join(self.tmpdir, "app.log")
        with open(f, "w") as fh:
            fh.write("data\n")

        result = expand_paths([f, f])
        self.assertEqual(len(result), 1)

    def test_nonexistent_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            expand_paths(["/nonexistent/file.log"])

    def test_empty_glob_raises(self):
        pattern = os.path.join(self.tmpdir, "*.zzz")
        with self.assertRaises(FileNotFoundError):
            expand_paths([pattern])

    def test_empty_input_raises(self):
        with self.assertRaises(FileNotFoundError):
            expand_paths([])


class TestTailFile(unittest.TestCase):
    """Verify tail follows new lines appended to a file."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.filepath = os.path.join(self.tmpdir, "tail.log")
        with open(self.filepath, "w") as f:
            f.write("existing line\n")

    def test_yields_new_lines(self):
        collected = []

        def reader():
            for line, path in tail_file(self.filepath, poll_interval=0.05):
                collected.append(line)
                if len(collected) >= 2:
                    break

        t = threading.Thread(target=reader)
        t.start()

        time.sleep(0.15)
        with open(self.filepath, "a") as f:
            f.write("new line 1\n")
            f.write("new line 2\n")

        t.join(timeout=3)
        self.assertFalse(t.is_alive(), "Tail thread didn't finish in time")
        self.assertEqual(len(collected), 2)
        self.assertIn("new line 1\n", collected)
        self.assertIn("new line 2\n", collected)


if __name__ == "__main__":
    unittest.main()
