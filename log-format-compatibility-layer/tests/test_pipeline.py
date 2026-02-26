"""Tests for the streaming pipeline."""
import os
import pytest
from src.pipeline import stream_lines, process_file


class TestPipeline:
    def test_stream_lines(self):
        """Test streaming lines from a sample file."""
        filepath = "logs/samples/mixed_sample.txt"
        lines = list(stream_lines(filepath))
        assert len(lines) == 10  # mixed_sample has 10 lines

    def test_process_file_json_output(self):
        """Test processing a file with JSON output format."""
        filepath = "logs/samples/json_sample.txt"
        results = list(process_file(filepath, output_format="json"))
        # Last result should be metrics
        assert results[-1][0] == "__metrics__"
        metrics = results[-1][1]
        assert metrics["successful"] >= 4
        assert metrics["success_rate_percent"] > 80

    def test_process_file_mixed(self):
        """Test processing mixed format file."""
        filepath = "logs/samples/mixed_sample.txt"
        results = list(process_file(filepath, output_format="json"))
        metrics = results[-1][1]
        assert metrics["total_lines"] == 10
        assert metrics["successful"] >= 8  # at least 8 out of 10
        assert len(metrics["format_distribution"]) >= 3

    def test_process_file_structured_output(self):
        """Test processing with structured output format."""
        filepath = "logs/samples/syslog_sample.txt"
        results = list(process_file(filepath, output_format="structured"))
        # Excluding metrics
        log_results = [r for r in results if r[0] != "__metrics__"]
        assert len(log_results) >= 4
        # Structured format should contain pipe separators
        assert " | " in log_results[0][0]

    def test_process_file_metrics(self):
        """Test that metrics contain expected fields."""
        filepath = "logs/samples/json_sample.txt"
        results = list(process_file(filepath))
        metrics = results[-1][1]
        assert "total_lines" in metrics
        assert "successful" in metrics
        assert "failed" in metrics
        assert "throughput_per_second" in metrics
        assert "success_rate_percent" in metrics
        assert "format_distribution" in metrics
