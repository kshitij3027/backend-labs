import json
import os

import pytest

from src.engine import (
    MapReduceEngine,
    _map_worker,
    _reduce_worker,
    _simple_word_count_map,
)


class TestMapWorker:
    def test_processes_json_chunk(self, sample_json_logs):
        """Map worker processes a JSON log file chunk."""
        size = os.path.getsize(sample_json_logs)
        args = (sample_json_logs, 0, size, "json", "word_count")
        pairs, processed, skipped = _map_worker(args)
        assert processed > 0
        assert skipped == 0
        assert len(pairs) > 0
        # All pairs should be (str, int)
        for key, value in pairs:
            assert isinstance(key, str)
            assert isinstance(value, int)

    def test_processes_apache_chunk(self, sample_apache_logs):
        """Map worker processes an Apache log file chunk."""
        size = os.path.getsize(sample_apache_logs)
        args = (sample_apache_logs, 0, size, "apache", "word_count")
        pairs, processed, skipped = _map_worker(args)
        assert processed > 0
        assert len(pairs) > 0

    def test_handles_malformed_lines(self, tmp_output_dir):
        """Map worker skips malformed lines without crashing."""
        path = os.path.join(tmp_output_dir, "mixed.jsonl")
        with open(path, "w") as f:
            f.write(
                json.dumps(
                    {
                        "timestamp": "t",
                        "level": "INFO",
                        "message": "hello world test",
                    }
                )
                + "\n"
            )
            f.write("this is not valid json\n")
            f.write(
                json.dumps(
                    {
                        "timestamp": "t",
                        "level": "ERROR",
                        "message": "error occurred",
                    }
                )
                + "\n"
            )

        size = os.path.getsize(path)
        args = (path, 0, size, "json", "word_count")
        pairs, processed, skipped = _map_worker(args)
        assert processed == 2
        assert skipped == 1


class TestReduceWorker:
    def test_reduces_groups(self):
        """Reduce worker sums values per key."""
        groups = [("hello", [1, 1, 1]), ("world", [1, 1])]
        args = (groups, "word_count")
        result = _reduce_worker(args)
        assert result["hello"] == 3
        assert result["world"] == 2


class TestShuffle:
    def test_groups_by_key(self):
        """Shuffle groups map output by key."""
        from collections import defaultdict

        pairs = [("a", 1), ("b", 1), ("a", 1), ("c", 1), ("b", 1)]
        grouped = defaultdict(list)
        for k, v in pairs:
            grouped[k].append(v)
        assert grouped["a"] == [1, 1]
        assert grouped["b"] == [1, 1]
        assert grouped["c"] == [1]


class TestMapReduceEngine:
    def test_full_pipeline_json(self, sample_json_logs):
        """Full MapReduce pipeline on JSON logs."""
        engine = MapReduceEngine(num_workers=2, chunk_size=500)
        results = engine.run([sample_json_logs], "word_count", "word_count")
        assert len(results) > 0
        # All values should be positive integers
        for key, value in results.items():
            assert isinstance(key, str)
            assert value > 0

    def test_full_pipeline_apache(self, sample_apache_logs):
        """Full MapReduce pipeline on Apache logs."""
        engine = MapReduceEngine(num_workers=2, chunk_size=500)
        results = engine.run([sample_apache_logs], "word_count", "word_count")
        assert len(results) > 0

    def test_full_pipeline_multiple_files(
        self, sample_json_logs, sample_apache_logs
    ):
        """Pipeline processes multiple input files."""
        engine = MapReduceEngine(num_workers=2, chunk_size=500)
        results = engine.run(
            [sample_json_logs, sample_apache_logs], "word_count", "word_count"
        )
        assert len(results) > 0

    def test_progress_callback(self, sample_json_logs):
        """Progress callback is called during execution."""
        phases_seen = []

        def callback(phase, progress, info):
            phases_seen.append(phase)

        engine = MapReduceEngine(num_workers=2, chunk_size=500)
        engine.run(
            [sample_json_logs],
            "word_count",
            "word_count",
            progress_callback=callback,
        )

        assert "mapping" in phases_seen
        assert "shuffling" in phases_seen
        assert "reducing" in phases_seen
        assert "completed" in phases_seen

    def test_empty_input(self, tmp_output_dir):
        """Pipeline handles empty file gracefully."""
        path = os.path.join(tmp_output_dir, "empty.jsonl")
        with open(path, "w"):
            pass
        engine = MapReduceEngine(num_workers=2)
        results = engine.run([path], "word_count", "word_count")
        assert results == {}

    def test_deterministic_results(self, sample_json_logs):
        """Running the same input twice produces identical results."""
        engine = MapReduceEngine(num_workers=1, chunk_size=500)
        r1 = engine.run([sample_json_logs], "word_count", "word_count")
        r2 = engine.run([sample_json_logs], "word_count", "word_count")
        assert r1 == r2
