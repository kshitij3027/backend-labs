"""Tests for the log level filtering module."""

import pytest
from src.filter import level_index, should_accept
from src.config import LOG_LEVELS


class TestLevelIndex:
    @pytest.mark.parametrize("level,expected", [
        ("DEBUG", 0),
        ("INFO", 1),
        ("WARNING", 2),
        ("ERROR", 3),
        ("CRITICAL", 4),
    ])
    def test_known_levels(self, level, expected):
        assert level_index(level) == expected

    @pytest.mark.parametrize("level,expected", [
        ("debug", 0),
        ("Info", 1),
        ("warning", 2),
        ("error", 3),
        ("CRITICAL", 4),
    ])
    def test_case_insensitive(self, level, expected):
        assert level_index(level) == expected

    def test_unknown_level(self):
        assert level_index("TRACE") == -1

    def test_empty_string(self):
        assert level_index("") == -1

    def test_whitespace(self):
        assert level_index("  ERROR  ") == 3


class TestShouldAccept:
    @pytest.mark.parametrize("msg_level,min_level,expected", [
        ("ERROR", "INFO", True),
        ("INFO", "INFO", True),
        ("DEBUG", "INFO", False),
        ("CRITICAL", "DEBUG", True),
        ("DEBUG", "CRITICAL", False),
        ("WARNING", "WARNING", True),
        ("INFO", "WARNING", False),
        ("ERROR", "WARNING", True),
    ])
    def test_level_comparison(self, msg_level, min_level, expected):
        assert should_accept(msg_level, min_level) == expected

    def test_unknown_message_level(self):
        assert should_accept("TRACE", "INFO") is False

    def test_unknown_min_level(self):
        assert should_accept("ERROR", "TRACE") is False

    def test_both_unknown(self):
        assert should_accept("FOO", "BAR") is False

    def test_case_insensitive(self):
        assert should_accept("error", "info") is True

    def test_boundary_debug_to_debug(self):
        assert should_accept("DEBUG", "DEBUG") is True

    def test_boundary_critical_to_critical(self):
        assert should_accept("CRITICAL", "CRITICAL") is True

    def test_all_levels_accepted_when_min_debug(self):
        for level in LOG_LEVELS:
            assert should_accept(level, "DEBUG") is True

    def test_only_critical_when_min_critical(self):
        for level in LOG_LEVELS:
            expected = level == "CRITICAL"
            assert should_accept(level, "CRITICAL") == expected
