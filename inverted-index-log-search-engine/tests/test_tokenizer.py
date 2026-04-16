"""Comprehensive tests for the LogTokenizer."""

import pytest

from backend.tokenizer import LogTokenizer


@pytest.fixture
def tokenizer():
    return LogTokenizer()


# ── IP extraction ─────────────────────────────────────────────────────

class TestIPExtraction:
    def test_extracts_ip_address(self, tokenizer):
        tokens = tokenizer.tokenize("Connection from 192.168.1.100 refused")
        assert "192.168.1.100" in tokens

    def test_extracts_multiple_ips(self, tokenizer):
        tokens = tokenizer.tokenize("Route from 10.0.0.1 to 10.0.0.2")
        assert "10.0.0.1" in tokens
        assert "10.0.0.2" in tokens


# ── Email extraction ──────────────────────────────────────────────────

class TestEmailExtraction:
    def test_extracts_email(self, tokenizer):
        tokens = tokenizer.tokenize("Login attempt by admin@corp.com")
        assert "admin@corp.com" in tokens

    def test_extracts_email_with_dots(self, tokenizer):
        tokens = tokenizer.tokenize("Email from john.doe@example.org")
        assert "john.doe@example.org" in tokens


# ── URL extraction ────────────────────────────────────────────────────

class TestURLExtraction:
    def test_extracts_url(self, tokenizer):
        tokens = tokenizer.tokenize(
            "Request to https://api.service.com/v2/users failed"
        )
        assert "https://api.service.com/v2/users" in tokens


# ── Timestamp extraction ──────────────────────────────────────────────

class TestTimestampExtraction:
    def test_extracts_iso_timestamp(self, tokenizer):
        tokens = tokenizer.tokenize("2024-01-15T10:30:00Z Error occurred")
        assert "2024-01-15t10:30:00z" in tokens  # lowercased

    def test_extracts_timestamp_with_offset(self, tokenizer):
        tokens = tokenizer.tokenize("2024-01-15T10:30:00+05:30 Started")
        assert "2024-01-15t10:30:00+05:30" in tokens


# ── Compound terms ────────────────────────────────────────────────────

class TestCompoundTerms:
    def test_extracts_compound_and_parts(self, tokenizer):
        tokens = tokenizer.tokenize("api.timeout exceeded limit")
        assert "api.timeout" in tokens
        assert "api" in tokens
        assert "timeout" in tokens

    def test_extracts_triple_compound(self, tokenizer):
        tokens = tokenizer.tokenize("com.example.service crashed")
        assert "com.example.service" in tokens
        assert "com" in tokens
        assert "example" in tokens
        assert "service" in tokens


# ── Stop words ────────────────────────────────────────────────────────

class TestStopWords:
    def test_filters_stop_words(self, tokenizer):
        tokens = tokenizer.tokenize(
            "the server is not responding to the request"
        )
        assert "the" not in tokens
        assert "is" not in tokens
        assert "not" not in tokens
        assert "to" not in tokens
        assert "server" in tokens
        assert "responding" in tokens
        assert "request" in tokens


# ── Case normalization ────────────────────────────────────────────────

class TestCaseNormalization:
    def test_lowercases_tokens(self, tokenizer):
        tokens = tokenizer.tokenize("ERROR Warning Info DEBUG")
        assert "error" in tokens
        assert "warning" in tokens
        assert "info" in tokens
        assert "debug" in tokens


# ── Edge cases ────────────────────────────────────────────────────────

class TestEdgeCases:
    def test_empty_string(self, tokenizer):
        assert tokenizer.tokenize("") == []

    def test_whitespace_only(self, tokenizer):
        assert tokenizer.tokenize("   ") == []

    def test_single_short_word(self, tokenizer):
        # Single-char words should be filtered (len < 2)
        tokens = tokenizer.tokenize("a b c")
        assert len(tokens) == 0


# ── Real-world log line ──────────────────────────────────────────────

class TestRealWorldLogLine:
    def test_full_log_line(self, tokenizer):
        log = (
            "2024-01-15T10:30:00Z ERROR [auth-service] "
            "Authentication failed for user admin@corp.com "
            "from 192.168.1.100"
        )
        tokens = tokenizer.tokenize(log)
        assert "2024-01-15t10:30:00z" in tokens
        assert "error" in tokens
        assert "auth" in tokens or "auth-service" in tokens
        assert "authentication" in tokens
        assert "failed" in tokens
        assert "admin@corp.com" in tokens
        assert "192.168.1.100" in tokens


# ── tokenize_with_positions ──────────────────────────────────────────

class TestTokenizeWithPositions:
    def test_returns_positions(self, tokenizer):
        positions = tokenizer.tokenize_with_positions(
            "error error warning error"
        )
        assert "error" in positions
        assert len(positions["error"]) == 3  # appears 3 times
        assert "warning" in positions
        assert len(positions["warning"]) == 1

    def test_positions_are_sequential(self, tokenizer):
        positions = tokenizer.tokenize_with_positions(
            "server crashed unexpectedly"
        )
        # Each term should have position 0, 1, 2
        all_positions = []
        for pos_list in positions.values():
            all_positions.extend(pos_list)
        assert sorted(all_positions) == list(range(len(all_positions)))
