"""Tests for the Click CLI interface."""

import json
import os
import tempfile

import pytest
from click.testing import CliRunner

from src.cli import cli


@pytest.fixture
def runner():
    return CliRunner()


class TestVersion:
    def test_version_flag(self, runner):
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "1.0.0" in result.output


class TestEnrichCommand:
    def test_enrich_basic(self, runner):
        result = runner.invoke(cli, ["enrich", "INFO: test message"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "message" in data
        assert "hostname" in data
        assert "service_name" in data

    def test_enrich_error_message_has_cpu(self, runner):
        result = runner.invoke(cli, ["enrich", "ERROR: something failed"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "cpu_percent" in data

    def test_enrich_compact(self, runner):
        result = runner.invoke(cli, ["enrich", "test", "--compact"])
        assert result.exit_code == 0
        # Compact output should be a single line of JSON (no newlines except trailing)
        lines = result.output.strip().split("\n")
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert "message" in data

    def test_enrich_custom_source(self, runner):
        result = runner.invoke(cli, ["enrich", "test", "--source", "custom-source"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["source"] == "custom-source"


class TestBatchCommand:
    def test_batch_processes_lines(self, runner):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("INFO: first message\n")
            f.write("WARNING: second message\n")
            f.write("ERROR: third message\n")
            f.flush()
            tmp_path = f.name

        try:
            result = runner.invoke(cli, ["batch", tmp_path])
            assert result.exit_code == 0
            # Output contains JSON array followed by stats line on stderr.
            # Extract the JSON array (everything up to the closing bracket).
            output = result.output
            json_end = output.rindex("]") + 1
            json_part = output[:json_end]
            data = json.loads(json_part)
            assert isinstance(data, list)
            assert len(data) == 3
            # Stats summary is appended (via stderr) after the JSON
            assert "Processed: 3" in output
        finally:
            os.unlink(tmp_path)

    def test_batch_output_file(self, runner):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("INFO: test line 1\n")
            f.write("INFO: test line 2\n")
            f.flush()
            input_path = f.name

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as out:
            output_path = out.name

        try:
            result = runner.invoke(cli, ["batch", input_path, "--output", output_path])
            assert result.exit_code == 0

            with open(output_path, "r") as f:
                data = json.loads(f.read())
            assert isinstance(data, list)
            assert len(data) == 2
        finally:
            os.unlink(input_path)
            os.unlink(output_path)
