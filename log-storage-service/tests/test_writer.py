"""Tests for the log writer module."""

import os
import re
import shutil
import tempfile
import threading
import unittest
from datetime import datetime, timezone, timedelta

from src.config import Config
from src.writer import LogWriter


class TestWriterBasic(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _config(self, **overrides):
        defaults = dict(
            log_dir=self.tmpdir,
            log_filename="test.log",
            max_file_size_bytes=10 * 1024 * 1024,
            rotation_interval_seconds=3600,
            max_file_count=10,
            max_age_days=7,
            compression_enabled=True,
        )
        defaults.update(overrides)
        return Config(**defaults)

    def test_write_creates_file(self):
        cfg = self._config()
        writer = LogWriter(cfg)
        writer.write("hello world")
        writer.close()
        path = os.path.join(self.tmpdir, "test.log")
        self.assertTrue(os.path.exists(path))
        with open(path) as f:
            self.assertEqual(f.read(), "hello world\n")

    def test_write_appends_newline(self):
        cfg = self._config()
        writer = LogWriter(cfg)
        writer.write("line1")
        writer.write("line2\n")
        writer.close()
        with open(os.path.join(self.tmpdir, "test.log")) as f:
            lines = f.readlines()
        self.assertEqual(lines, ["line1\n", "line2\n"])

    def test_no_rotation_returns_none(self):
        cfg = self._config()
        writer = LogWriter(cfg)
        result = writer.write("small line")
        writer.close()
        self.assertIsNone(result)


class TestSizeRotation(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_rotation_on_size(self):
        cfg = Config(
            log_dir=self.tmpdir,
            log_filename="test.log",
            max_file_size_bytes=50,
            rotation_interval_seconds=999999,
            max_file_count=10,
            max_age_days=7,
            compression_enabled=True,
        )
        writer = LogWriter(cfg)
        rotated_path = None
        # Write enough data to exceed 50 bytes
        for i in range(20):
            result = writer.write(f"line {i:04d} padding data here")
            if result is not None:
                rotated_path = result
                break
        writer.close()

        self.assertIsNotNone(rotated_path, "Expected a rotation to occur")
        self.assertTrue(os.path.exists(rotated_path))
        # Active log should still exist (fresh file)
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, "test.log")))

    def test_rotated_file_naming(self):
        cfg = Config(
            log_dir=self.tmpdir,
            log_filename="test.log",
            max_file_size_bytes=50,
            rotation_interval_seconds=999999,
            max_file_count=10,
            max_age_days=7,
            compression_enabled=True,
        )
        writer = LogWriter(cfg)
        rotated_path = None
        for i in range(20):
            result = writer.write(f"line {i:04d} padding data here")
            if result is not None:
                rotated_path = result
                break
        writer.close()

        filename = os.path.basename(rotated_path)
        # Should match test.log.YYYYMMDD_HHMMSS_ffffff
        pattern = r"^test\.log\.\d{8}_\d{6}_\d{6}$"
        self.assertRegex(filename, pattern)


class TestTimeRotation(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_rotation_on_time(self):
        now = datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        clock = [now]

        def time_func():
            return clock[0]

        cfg = Config(
            log_dir=self.tmpdir,
            log_filename="test.log",
            max_file_size_bytes=10 * 1024 * 1024,
            rotation_interval_seconds=60,
            max_file_count=10,
            max_age_days=7,
            compression_enabled=True,
        )
        writer = LogWriter(cfg, time_func=time_func)

        # Write before interval — no rotation
        result = writer.write("first line")
        self.assertIsNone(result)

        # Advance clock past the 60-second interval
        clock[0] = now + timedelta(seconds=61)
        result = writer.write("second line after interval")
        self.assertIsNotNone(result)
        self.assertTrue(os.path.exists(result))
        writer.close()


class TestConcurrentWrites(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_concurrent_writes(self):
        cfg = Config(
            log_dir=self.tmpdir,
            log_filename="test.log",
            max_file_size_bytes=10 * 1024 * 1024,
            rotation_interval_seconds=999999,
            max_file_count=10,
            max_age_days=7,
            compression_enabled=True,
        )
        writer = LogWriter(cfg)
        num_threads = 5
        writes_per_thread = 100
        errors = []

        def worker(thread_id):
            try:
                for i in range(writes_per_thread):
                    writer.write(f"thread-{thread_id}-line-{i}")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        writer.close()

        self.assertEqual(errors, [])

        # Collect all lines from all files
        all_lines = []
        for fname in os.listdir(self.tmpdir):
            fpath = os.path.join(self.tmpdir, fname)
            with open(fpath) as f:
                all_lines.extend(f.readlines())

        self.assertEqual(len(all_lines), num_threads * writes_per_thread)
        # Verify no corrupted lines (each should end with newline and have expected format)
        for line in all_lines:
            self.assertTrue(line.endswith("\n"))
            self.assertRegex(line.strip(), r"^thread-\d+-line-\d+$")


if __name__ == "__main__":
    unittest.main()
