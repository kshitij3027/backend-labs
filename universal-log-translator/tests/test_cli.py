"""Tests for the CLI."""
import pytest
from click.testing import CliRunner

from src.cli import cli
import src.handlers  # noqa: F401


class TestCLI:
    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_translate_json_file(self, runner, tmp_path):
        f = tmp_path / "test.json"
        f.write_text(
            '{"timestamp": "2024-01-15T10:30:00", "level": "INFO", "message": "test"}'
        )
        result = runner.invoke(cli, ["translate", str(f)])
        assert result.exit_code == 0
        assert "test" in result.output

    def test_translate_text_file(self, runner, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("2024-01-15 10:30:00 INFO Application started")
        result = runner.invoke(cli, ["translate", str(f)])
        assert result.exit_code == 0

    def test_translate_with_format_hint(self, runner, tmp_path):
        f = tmp_path / "test.json"
        f.write_text(
            '{"timestamp": "2024-01-15T10:30:00", "level": "INFO", "message": "test"}'
        )
        result = runner.invoke(cli, ["translate", "--format", "json", str(f)])
        assert result.exit_code == 0

    def test_translate_text_output(self, runner, tmp_path):
        f = tmp_path / "test.json"
        f.write_text(
            '{"timestamp": "2024-01-15T10:30:00", "level": "INFO", "message": "test"}'
        )
        result = runner.invoke(cli, ["translate", "--output", "text", str(f)])
        assert result.exit_code == 0
        assert "INFO" in result.output

    def test_translate_stdin(self, runner):
        result = runner.invoke(
            cli,
            ["translate", "-"],
            input='{"timestamp": "2024-01-15T10:30:00", "level": "INFO", "message": "stdin test"}',
        )
        assert result.exit_code == 0
        assert "stdin test" in result.output

    def test_detect_json(self, runner, tmp_path):
        f = tmp_path / "test.json"
        f.write_text(
            '{"timestamp": "2024-01-15T10:30:00", "level": "INFO", "message": "test"}'
        )
        result = runner.invoke(cli, ["detect", str(f)])
        assert result.exit_code == 0
        assert "json" in result.output.lower()

    def test_detect_text(self, runner, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("2024-01-15 10:30:00 INFO Application started")
        result = runner.invoke(cli, ["detect", str(f)])
        assert result.exit_code == 0
        assert "text" in result.output.lower()

    def test_translate_nonexistent_file(self, runner):
        result = runner.invoke(cli, ["translate", "/nonexistent/file.log"])
        assert result.exit_code != 0

    def test_detect_nonexistent_file(self, runner):
        result = runner.invoke(cli, ["detect", "/nonexistent/file.log"])
        assert result.exit_code != 0

    def test_translate_json_output_has_fields(self, runner, tmp_path):
        f = tmp_path / "test.json"
        f.write_text(
            '{"timestamp": "2024-01-15T10:30:00", "level": "INFO", '
            '"message": "test msg", "hostname": "h1", "service": "svc1"}'
        )
        result = runner.invoke(cli, ["translate", str(f)])
        assert result.exit_code == 0
        assert '"timestamp"' in result.output
        assert '"level"' in result.output
        assert '"message"' in result.output
        assert "test msg" in result.output

    def test_translate_text_output_format(self, runner, tmp_path):
        f = tmp_path / "test.json"
        f.write_text(
            '{"timestamp": "2024-01-15T10:30:00", "level": "ERROR", "message": "bad thing"}'
        )
        result = runner.invoke(cli, ["translate", "--output", "text", str(f)])
        assert result.exit_code == 0
        assert "ERROR" in result.output
        assert "bad thing" in result.output
        assert "source=" in result.output
