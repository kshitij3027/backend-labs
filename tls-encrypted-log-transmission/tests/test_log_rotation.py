"""Tests for rotating log writer."""

import json
import os
import pytest
from src.log_rotation import RotatingLogWriter


class TestRotatingLogWriter:
    def test_creates_first_file(self, tmp_path):
        writer = RotatingLogWriter(str(tmp_path), max_per_file=5)
        writer.write({"msg": "test"})
        writer.close()
        assert os.path.exists(tmp_path / "log_000.jsonl")

    def test_rotates_at_limit(self, tmp_path):
        writer = RotatingLogWriter(str(tmp_path), max_per_file=3)
        for i in range(7):
            writer.write({"msg": f"entry {i}"})
        writer.close()

        assert os.path.exists(tmp_path / "log_000.jsonl")
        assert os.path.exists(tmp_path / "log_001.jsonl")
        assert os.path.exists(tmp_path / "log_002.jsonl")

        # First file should have 3 entries
        with open(tmp_path / "log_000.jsonl") as f:
            lines = f.readlines()
        assert len(lines) == 3

    def test_jsonl_format(self, tmp_path):
        writer = RotatingLogWriter(str(tmp_path), max_per_file=10)
        writer.write({"level": "INFO", "msg": "test"})
        writer.close()

        with open(tmp_path / "log_000.jsonl") as f:
            entry = json.loads(f.readline())
        assert entry["level"] == "INFO"

    def test_file_index_tracks(self, tmp_path):
        writer = RotatingLogWriter(str(tmp_path), max_per_file=2)
        assert writer.current_file_index == 0
        writer.write({"a": 1})
        writer.write({"a": 2})
        writer.write({"a": 3})  # triggers rotation
        assert writer.current_file_index == 1
        writer.close()
