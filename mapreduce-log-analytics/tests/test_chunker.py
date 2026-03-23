import json
import os

import pytest

from src.chunker import read_chunk, split_file


class TestSplitFile:
    def test_small_file_single_chunk(self, sample_json_logs):
        """File smaller than chunk_size returns one chunk."""
        chunks = split_file(sample_json_logs, chunk_size=1_000_000)
        assert len(chunks) == 1
        assert chunks[0][0] == sample_json_logs
        assert chunks[0][1] == 0
        assert chunks[0][2] == os.path.getsize(sample_json_logs)

    def test_empty_file(self, tmp_output_dir):
        """Empty file returns no chunks."""
        path = os.path.join(tmp_output_dir, "empty.log")
        with open(path, "w"):
            pass
        chunks = split_file(path)
        assert len(chunks) == 0

    def test_multi_chunk_small_size(self, sample_json_logs):
        """Using a small chunk size produces multiple chunks."""
        chunks = split_file(sample_json_logs, chunk_size=500)
        assert len(chunks) > 1
        # Verify no gaps and no overlaps
        for i in range(1, len(chunks)):
            assert chunks[i][1] == chunks[i - 1][2]

    def test_line_boundary_integrity(self, sample_json_logs):
        """Each chunk starts and ends at line boundaries."""
        chunks = split_file(sample_json_logs, chunk_size=500)
        for file_path, start, end in chunks:
            lines = read_chunk(file_path, start, end)
            for line in lines:
                assert "\n" not in line.rstrip("\n")
                # Each line should be a valid JSON line
                parsed = json.loads(line)
                assert "timestamp" in parsed

    def test_all_lines_recovered(self, sample_json_logs):
        """All lines from the original file are present across chunks."""
        with open(sample_json_logs) as f:
            original_lines = [l.strip() for l in f if l.strip()]

        chunks = split_file(sample_json_logs, chunk_size=500)
        recovered = []
        for file_path, start, end in chunks:
            recovered.extend(read_chunk(file_path, start, end))

        recovered_stripped = [l.strip() for l in recovered if l.strip()]
        assert len(recovered_stripped) == len(original_lines)


class TestReadChunk:
    def test_read_full_file(self, sample_json_logs):
        """Reading entire file returns all lines."""
        size = os.path.getsize(sample_json_logs)
        lines = read_chunk(sample_json_logs, 0, size)
        assert len(lines) == 100  # sample_json_logs fixture generates 100 lines
