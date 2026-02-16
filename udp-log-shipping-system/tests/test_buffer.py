"""Tests for the buffered writer."""

import json
import os
import time

import pytest

from src.buffer import BufferedWriter


def _make_entry(i: int) -> dict:
    return {"sequence": i, "level": "INFO", "message": f"test-{i}"}


class TestCountFlush:
    def test_flushes_at_count_threshold(self, tmp_path):
        writer = BufferedWriter(str(tmp_path), "test.log", flush_count=10, flush_timeout_sec=60)
        try:
            for i in range(10):
                writer.append(_make_entry(i))

            time.sleep(0.1)
            log_path = os.path.join(str(tmp_path), "test.log")
            assert os.path.isfile(log_path)

            with open(log_path) as f:
                lines = f.readlines()
            assert len(lines) == 10

            for i, line in enumerate(lines):
                entry = json.loads(line)
                assert entry["sequence"] == i
        finally:
            writer.close()

    def test_no_flush_below_threshold(self, tmp_path):
        writer = BufferedWriter(str(tmp_path), "test.log", flush_count=10, flush_timeout_sec=60)
        try:
            for i in range(5):
                writer.append(_make_entry(i))

            time.sleep(0.1)
            log_path = os.path.join(str(tmp_path), "test.log")
            assert not os.path.isfile(log_path)
        finally:
            writer.close()


class TestTimeoutFlush:
    def test_flushes_on_timeout(self, tmp_path):
        writer = BufferedWriter(str(tmp_path), "test.log", flush_count=1000, flush_timeout_sec=2)
        try:
            writer.append(_make_entry(0))

            log_path = os.path.join(str(tmp_path), "test.log")
            assert not os.path.isfile(log_path)

            time.sleep(3)
            assert os.path.isfile(log_path)

            with open(log_path) as f:
                lines = f.readlines()
            assert len(lines) == 1
        finally:
            writer.close()


class TestShutdownFlush:
    def test_close_flushes_remaining(self, tmp_path):
        writer = BufferedWriter(str(tmp_path), "test.log", flush_count=1000, flush_timeout_sec=60)

        for i in range(50):
            writer.append(_make_entry(i))

        writer.close()

        log_path = os.path.join(str(tmp_path), "test.log")
        assert os.path.isfile(log_path)

        with open(log_path) as f:
            lines = f.readlines()
        assert len(lines) == 50


class TestAppendMode:
    def test_multiple_flushes_append(self, tmp_path):
        writer = BufferedWriter(str(tmp_path), "test.log", flush_count=5, flush_timeout_sec=60)
        try:
            for i in range(15):
                writer.append(_make_entry(i))

            time.sleep(0.1)
            log_path = os.path.join(str(tmp_path), "test.log")

            with open(log_path) as f:
                lines = f.readlines()
            assert len(lines) == 15
        finally:
            writer.close()


class TestLogRotation:
    def test_rotation_triggered(self, tmp_path):
        """Write enough entries to exceed 1KB and verify .1 rotated file exists."""
        writer = BufferedWriter(
            str(tmp_path), "test.log",
            flush_count=5, flush_timeout_sec=60,
            max_log_size_mb=0.001,  # ~1 KB
        )
        try:
            # Each JSON entry is roughly 60-70 bytes; 50 entries > 1 KB.
            for i in range(50):
                writer.append(_make_entry(i))

            time.sleep(0.1)

            rotated_path = os.path.join(str(tmp_path), "test.log.1")
            assert os.path.isfile(rotated_path), "Rotated file .1 should exist after exceeding size limit"
        finally:
            writer.close()

    def test_rotation_preserves_data(self, tmp_path):
        """After rotation, new writes still go to the original log path."""
        writer = BufferedWriter(
            str(tmp_path), "test.log",
            flush_count=5, flush_timeout_sec=60,
            max_log_size_mb=0.001,  # ~1 KB
        )
        try:
            # Write enough to trigger at least one rotation.
            for i in range(50):
                writer.append(_make_entry(i))

            time.sleep(0.1)

            log_path = os.path.join(str(tmp_path), "test.log")
            rotated_path = os.path.join(str(tmp_path), "test.log.1")

            assert os.path.isfile(rotated_path), "Rotated file .1 should exist"

            # Write more entries — they should land in the original log path.
            for i in range(50, 55):
                writer.append(_make_entry(i))
            writer.flush()

            assert os.path.isfile(log_path), "Original log path should still accept new writes"
            with open(log_path) as f:
                lines = f.readlines()
            # At least the 5 new entries should be present in the current log.
            sequences = [json.loads(line)["sequence"] for line in lines]
            for seq in range(50, 55):
                assert seq in sequences, f"Entry {seq} should be in the current log file"
        finally:
            writer.close()
