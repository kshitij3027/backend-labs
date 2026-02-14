"""Tests for the log persistence module."""

import os
import threading
import tempfile
import pytest
from src.persistence import LogPersistence


class TestDisabledMode:
    def test_write_returns_false(self):
        lp = LogPersistence("/tmp/unused", "unused.log", enabled=False)
        assert lp.write("ERROR", "should not write") is False

    def test_no_file_created(self, tmp_path):
        log_dir = str(tmp_path / "nologs")
        LogPersistence(log_dir, "unused.log", enabled=False)
        assert not os.path.exists(log_dir)

    def test_close_is_safe(self):
        lp = LogPersistence("/tmp/unused", "unused.log", enabled=False)
        lp.close()  # should not raise


class TestDirectoryCreation:
    def test_creates_directory(self, tmp_path):
        log_dir = str(tmp_path / "newdir" / "subdir")
        lp = LogPersistence(log_dir, "app.log", enabled=True)
        assert os.path.isdir(log_dir)
        lp.close()

    def test_creates_file(self, tmp_path):
        log_dir = str(tmp_path / "logs")
        lp = LogPersistence(log_dir, "app.log", enabled=True)
        assert os.path.isfile(os.path.join(log_dir, "app.log"))
        lp.close()


class TestContentFormat:
    def test_single_entry_format(self, tmp_path):
        log_dir = str(tmp_path)
        lp = LogPersistence(log_dir, "test.log", enabled=True)
        lp.write("ERROR", "disk full")
        lp.close()

        content = open(os.path.join(log_dir, "test.log")).read()
        lines = content.strip().split("\n")
        assert len(lines) == 1
        assert "[ERROR]" in lines[0]
        assert "disk full" in lines[0]
        # Check timestamp format: YYYY-MM-DDTHH:MM:SS
        assert "T" in lines[0].split(" ")[0]

    def test_multiple_entries(self, tmp_path):
        log_dir = str(tmp_path)
        lp = LogPersistence(log_dir, "test.log", enabled=True)
        lp.write("INFO", "starting")
        lp.write("WARNING", "low memory")
        lp.write("ERROR", "crash")
        lp.close()

        content = open(os.path.join(log_dir, "test.log")).read()
        lines = content.strip().split("\n")
        assert len(lines) == 3
        assert "[INFO]" in lines[0]
        assert "[WARNING]" in lines[1]
        assert "[ERROR]" in lines[2]

    def test_level_is_uppercased(self, tmp_path):
        log_dir = str(tmp_path)
        lp = LogPersistence(log_dir, "test.log", enabled=True)
        lp.write("error", "test")
        lp.close()

        content = open(os.path.join(log_dir, "test.log")).read()
        assert "[ERROR]" in content

    def test_write_returns_true(self, tmp_path):
        log_dir = str(tmp_path)
        lp = LogPersistence(log_dir, "test.log", enabled=True)
        assert lp.write("INFO", "hello") is True
        lp.close()


class TestConcurrentWrites:
    def test_concurrent_writes(self, tmp_path):
        """5 threads x 50 messages = 250 total lines, no data loss."""
        log_dir = str(tmp_path)
        lp = LogPersistence(log_dir, "concurrent.log", enabled=True)
        errors = []

        def writer(thread_id):
            for i in range(50):
                try:
                    lp.write("INFO", f"thread-{thread_id}-msg-{i}")
                except Exception as e:
                    errors.append(e)

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        lp.close()
        assert len(errors) == 0

        content = open(os.path.join(log_dir, "concurrent.log")).read()
        lines = content.strip().split("\n")
        assert len(lines) == 250


class TestCloseAndWrite:
    def test_write_after_close_returns_false(self, tmp_path):
        log_dir = str(tmp_path)
        lp = LogPersistence(log_dir, "test.log", enabled=True)
        lp.write("INFO", "before close")
        lp.close()
        assert lp.write("ERROR", "after close") is False
