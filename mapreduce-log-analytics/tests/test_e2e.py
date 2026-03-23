"""End-to-end integration tests for the full MapReduce pipeline."""

import os
import json
import pytest
from src.engine import MapReduceEngine
from src.generator import generate_json_logs, generate_apache_logs


class TestFullPipeline:
    def test_word_count_json(self, tmp_output_dir):
        path = os.path.join(tmp_output_dir, "logs.jsonl")
        generate_json_logs(path, num_lines=500, seed=42)
        engine = MapReduceEngine(num_workers=2, chunk_size=2000)
        results = engine.run([path], "word_count", "word_count")
        assert len(results) > 0
        for v in results.values():
            assert isinstance(v, int)
            assert v > 0

    def test_word_count_apache(self, tmp_output_dir):
        path = os.path.join(tmp_output_dir, "logs.log")
        generate_apache_logs(path, num_lines=500, seed=42)
        engine = MapReduceEngine(num_workers=2, chunk_size=2000)
        results = engine.run([path], "word_count", "word_count")
        assert len(results) > 0

    def test_pattern_frequency(self, tmp_output_dir):
        path = os.path.join(tmp_output_dir, "logs.jsonl")
        generate_json_logs(path, num_lines=500, seed=42)
        engine = MapReduceEngine(num_workers=2, chunk_size=2000)
        results = engine.run([path], "pattern_frequency", "pattern_frequency")
        # Should have IP, status, and error pattern keys
        ip_keys = [k for k in results if k.startswith("ip_address:")]
        status_keys = [k for k in results if k.startswith("http_status:")]
        assert len(ip_keys) > 0
        assert len(status_keys) > 0

    def test_service_distribution(self, tmp_output_dir):
        path = os.path.join(tmp_output_dir, "logs.jsonl")
        generate_json_logs(path, num_lines=500, seed=42)
        engine = MapReduceEngine(num_workers=2, chunk_size=2000)
        results = engine.run([path], "service_distribution", "service_distribution")
        service_keys = [k for k in results if k.startswith("service:")]
        level_keys = [k for k in results if k.startswith("level:")]
        assert len(service_keys) >= 3  # at least 3 services
        assert len(level_keys) >= 3  # at least 3 levels

    def test_security_analysis(self, tmp_output_dir):
        path = os.path.join(tmp_output_dir, "logs.jsonl")
        generate_json_logs(path, num_lines=500, seed=42)
        engine = MapReduceEngine(num_workers=2, chunk_size=2000)
        results = engine.run([path], "security", "security")
        assert "top_ips" in results
        assert "top_404_paths" in results
        assert "peak_hours" in results
        assert "top_user_agents" in results
        assert len(results["top_ips"]) > 0
        assert len(results["peak_hours"]) > 0

    def test_mixed_format_inputs(self, tmp_output_dir):
        json_path = os.path.join(tmp_output_dir, "logs.jsonl")
        apache_path = os.path.join(tmp_output_dir, "logs.log")
        generate_json_logs(json_path, num_lines=200, seed=42)
        generate_apache_logs(apache_path, num_lines=200, seed=42)
        engine = MapReduceEngine(num_workers=2, chunk_size=2000)
        results = engine.run([json_path, apache_path], "word_count", "word_count")
        assert len(results) > 0

    def test_malformed_lines_handled(self, tmp_output_dir):
        path = os.path.join(tmp_output_dir, "mixed.jsonl")
        with open(path, "w") as f:
            f.write(json.dumps({"timestamp": "t", "level": "INFO", "message": "good line one"}) + "\n")
            f.write("this is garbage\n")
            f.write("more garbage here\n")
            f.write(json.dumps({"timestamp": "t", "level": "ERROR", "message": "good line two"}) + "\n")
        engine = MapReduceEngine(num_workers=1, chunk_size=100000)
        results = engine.run([path], "word_count", "word_count")
        assert len(results) > 0  # Should process the valid lines

    def test_deterministic_across_runs(self, tmp_output_dir):
        path = os.path.join(tmp_output_dir, "logs.jsonl")
        generate_json_logs(path, num_lines=200, seed=42)
        engine = MapReduceEngine(num_workers=1, chunk_size=2000)
        r1 = engine.run([path], "word_count", "word_count")
        r2 = engine.run([path], "word_count", "word_count")
        assert r1 == r2

    def test_large_chunk_count(self, tmp_output_dir):
        """Force many small chunks to verify multi-chunk processing."""
        path = os.path.join(tmp_output_dir, "logs.jsonl")
        generate_json_logs(path, num_lines=500, seed=42)
        engine = MapReduceEngine(num_workers=2, chunk_size=200)  # Very small chunks
        results = engine.run([path], "word_count", "word_count")
        assert len(results) > 0
