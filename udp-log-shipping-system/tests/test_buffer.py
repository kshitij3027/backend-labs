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
