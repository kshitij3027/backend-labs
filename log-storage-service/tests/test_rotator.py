"""Tests for the rotator module."""

import gzip
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timezone, timedelta

from src.config import Config
from src.rotator import (
    compress_file,
    get_rotated_files,
    parse_rotation_timestamp,
    enforce_retention,
)

LOG_FILENAME = "application.log"


class TestCompressFile(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def test_compress_creates_gz_and_removes_original(self):
        path = os.path.join(self.tmpdir, "test.log.20250115_120000_000000")
        content = b"line one\nline two\nline three\n"
        with open(path, "wb") as f:
            f.write(content)

        gz_path = compress_file(path)

        self.assertEqual(gz_path, path + ".gz")
        self.assertTrue(os.path.exists(gz_path))
        self.assertFalse(os.path.exists(path))

    def test_compressed_content_roundtrips(self):
        path = os.path.join(self.tmpdir, "test.log.20250115_120000_000000")
        content = b"hello world\n" * 100
        with open(path, "wb") as f:
            f.write(content)

        gz_path = compress_file(path)

        with gzip.open(gz_path, "rb") as f:
            decompressed = f.read()
        self.assertEqual(decompressed, content)


class TestGetRotatedFiles(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _touch(self, name):
        open(os.path.join(self.tmpdir, name), "w").close()

    def test_returns_only_rotated_files_sorted(self):
        self._touch(LOG_FILENAME)
        self._touch(f"{LOG_FILENAME}.20250115_120000_000000")
        self._touch(f"{LOG_FILENAME}.20250115_130000_000000.gz")
        self._touch(f"{LOG_FILENAME}.20250115_110000_000000")
        self._touch("unrelated.txt")

        result = get_rotated_files(self.tmpdir, LOG_FILENAME)

        self.assertEqual(result, [
            f"{LOG_FILENAME}.20250115_110000_000000",
            f"{LOG_FILENAME}.20250115_120000_000000",
            f"{LOG_FILENAME}.20250115_130000_000000.gz",
        ])

    def test_empty_directory(self):
        result = get_rotated_files(self.tmpdir, LOG_FILENAME)
        self.assertEqual(result, [])


class TestParseRotationTimestamp(unittest.TestCase):
    def test_valid_name(self):
        ts = parse_rotation_timestamp(
            f"{LOG_FILENAME}.20250115_120000_000000", LOG_FILENAME
        )
        self.assertEqual(ts, datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc))

    def test_valid_name_with_gz(self):
        ts = parse_rotation_timestamp(
            f"{LOG_FILENAME}.20250115_120000_123456.gz", LOG_FILENAME
        )
        self.assertEqual(
            ts, datetime(2025, 1, 15, 12, 0, 0, 123456, tzinfo=timezone.utc)
        )

    def test_invalid_name(self):
        self.assertIsNone(parse_rotation_timestamp("random.txt", LOG_FILENAME))

    def test_bad_timestamp(self):
        self.assertIsNone(
            parse_rotation_timestamp(f"{LOG_FILENAME}.not_a_timestamp", LOG_FILENAME)
        )


class TestEnforceRetention(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _touch(self, name):
        path = os.path.join(self.tmpdir, name)
        open(path, "w").close()
        return path

    def _config(self, **overrides):
        defaults = dict(
            log_dir=self.tmpdir,
            log_filename=LOG_FILENAME,
            max_file_size_bytes=10 * 1024 * 1024,
            rotation_interval_seconds=3600,
            max_file_count=3,
            max_age_days=7,
            compression_enabled=True,
        )
        defaults.update(overrides)
        return Config(**defaults)

    def test_count_based_retention(self):
        # Create 5 rotated files, max_file_count=3 → 2 oldest should be deleted
        names = [
            f"{LOG_FILENAME}.20250110_100000_000000",
            f"{LOG_FILENAME}.20250111_100000_000000",
            f"{LOG_FILENAME}.20250112_100000_000000",
            f"{LOG_FILENAME}.20250113_100000_000000",
            f"{LOG_FILENAME}.20250114_100000_000000",
        ]
        for name in names:
            self._touch(name)

        cfg = self._config(max_file_count=3, max_age_days=999)
        # Use a "now" that makes all files young enough to not be age-purged
        deleted = enforce_retention(
            cfg, time_func=lambda: datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        )

        self.assertEqual(len(deleted), 2)
        self.assertIn(names[0], deleted)
        self.assertIn(names[1], deleted)
        # Verify files are actually gone
        for name in deleted:
            self.assertFalse(os.path.exists(os.path.join(self.tmpdir, name)))
        # Survivors still exist
        for name in names[2:]:
            self.assertTrue(os.path.exists(os.path.join(self.tmpdir, name)))

    def test_age_based_retention(self):
        old = f"{LOG_FILENAME}.20250101_100000_000000"
        young = f"{LOG_FILENAME}.20250114_100000_000000"
        self._touch(old)
        self._touch(young)

        cfg = self._config(max_file_count=100, max_age_days=7)
        deleted = enforce_retention(
            cfg, time_func=lambda: datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        )

        self.assertEqual(deleted, [old])
        self.assertFalse(os.path.exists(os.path.join(self.tmpdir, old)))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, young)))

    def test_age_then_count(self):
        # 4 files: 2 are too old, 2 are young. max_file_count=1.
        # Age purge removes 2, count purge removes 1 more, leaving 1.
        names = [
            f"{LOG_FILENAME}.20250101_100000_000000",  # old
            f"{LOG_FILENAME}.20250102_100000_000000",  # old
            f"{LOG_FILENAME}.20250114_100000_000000",  # young
            f"{LOG_FILENAME}.20250114_120000_000000",  # young
        ]
        for name in names:
            self._touch(name)

        cfg = self._config(max_file_count=1, max_age_days=7)
        deleted = enforce_retention(
            cfg, time_func=lambda: datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        )

        self.assertEqual(len(deleted), 3)
        # Only the newest should survive
        surviving = [n for n in names if n not in deleted]
        self.assertEqual(surviving, [names[-1]])

    def test_no_deletions_needed(self):
        self._touch(f"{LOG_FILENAME}.20250114_100000_000000")
        cfg = self._config(max_file_count=10, max_age_days=999)
        deleted = enforce_retention(
            cfg, time_func=lambda: datetime(2025, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
        )
        self.assertEqual(deleted, [])


if __name__ == "__main__":
    unittest.main()
