"""Tests for ``src.index.tokenizer.LogTokenizer``.

Covers the happy-path (basic words, lowercasing), the structural
compound cases (IP, email, URL, UUID, ISO timestamp, dotted
identifier), the filtering rules (stop-words, min length, numbers
exempt from the alpha requirement), and a realistic mixed log line.

Tests are synchronous — tokenization has no I/O.
"""

from __future__ import annotations

import pytest

from src.index.tokenizer import LogTokenizer


@pytest.fixture
def tokenizer() -> LogTokenizer:
    """Default tokenizer used by most tests."""
    return LogTokenizer()


# ---------------------------------------------------------------------------
# Basics
# ---------------------------------------------------------------------------


def test_basic_words(tokenizer: LogTokenizer) -> None:
    assert tokenizer.tokenize("Hello World") == ["hello", "world"]


def test_lowercasing(tokenizer: LogTokenizer) -> None:
    tokens = tokenizer.tokenize("ERROR SomeService Failure")
    # Every emitted token must be lowercase.
    assert all(t == t.lower() for t in tokens)
    assert "error" in tokens
    assert "someservice" in tokens
    assert "failure" in tokens


def test_stop_words_filtered(tokenizer: LogTokenizer) -> None:
    tokens = tokenizer.tokenize("the quick brown fox is jumping")
    assert "the" not in tokens
    assert "is" not in tokens
    # Content words survive.
    assert {"quick", "brown", "fox", "jumping"} <= set(tokens)


def test_min_length_filter(tokenizer: LogTokenizer) -> None:
    tokens = tokenizer.tokenize("a bb ccc")
    assert "a" not in tokens
    assert "bb" in tokens
    assert "ccc" in tokens


# ---------------------------------------------------------------------------
# Compound structures
# ---------------------------------------------------------------------------


def test_ip_preserved(tokenizer: LogTokenizer) -> None:
    tokens = tokenizer.tokenize("connection from 192.168.1.100 failed")
    assert "192.168.1.100" in tokens


def test_ip_with_port(tokenizer: LogTokenizer) -> None:
    tokens = tokenizer.tokenize("connect to 192.168.1.100:8080 now")
    assert "192.168.1.100:8080" in tokens


def test_ipv4_components(tokenizer: LogTokenizer) -> None:
    tokens = tokenizer.tokenize("connection from 192.168.1.100 failed")
    # Compound and each octet (length >= 2 survives; "1" would be filtered).
    assert "192.168.1.100" in tokens
    assert "192" in tokens
    assert "168" in tokens
    assert "100" in tokens


def test_email_preserved(tokenizer: LogTokenizer) -> None:
    tokens = tokenizer.tokenize("user@example.com")
    assert "user@example.com" in tokens
    assert "user" in tokens
    assert "example" in tokens
    assert "com" in tokens


def test_url_preserved(tokenizer: LogTokenizer) -> None:
    tokens = tokenizer.tokenize("see https://api.example.com/v1/users for more")
    # Full URL preserved.
    assert "https://api.example.com/v1/users" in tokens
    # Component parts emitted too.
    assert {"api.example.com", "api", "example", "com", "v1", "users"} <= set(tokens)


def test_uuid_preserved_atomic(tokenizer: LogTokenizer) -> None:
    uuid = "550e8400-e29b-41d4-a716-446655440000"
    tokens = tokenizer.tokenize(f"request id {uuid} accepted")
    assert uuid in tokens
    # Hex groups should NOT leak out as independent tokens.
    assert "550e8400" not in tokens
    assert "e29b" not in tokens
    assert "446655440000" not in tokens


def test_iso_timestamp_preserved(tokenizer: LogTokenizer) -> None:
    tokens = tokenizer.tokenize("2026-04-19T12:34:56Z started")
    # The timestamp is lowercased by our upfront lower() call; the test
    # asserts the lowercased form (which is what the index stores).
    assert "2026-04-19t12:34:56z" in tokens
    assert "started" in tokens
    # Digit fragments from inside the timestamp must not leak.
    assert "2026" not in tokens
    assert "04" not in tokens


def test_dotted_identifier_preserved(tokenizer: LogTokenizer) -> None:
    tokens = tokenizer.tokenize("auth.service.login failed")
    assert "auth.service.login" in tokens
    assert "auth" in tokens
    assert "service" in tokens
    assert "login" in tokens
    assert "failed" in tokens


def test_numbers_kept(tokenizer: LogTokenizer) -> None:
    tokens = tokenizer.tokenize("HTTP 500 error")
    assert "http" in tokens
    assert "500" in tokens
    assert "error" in tokens


# ---------------------------------------------------------------------------
# Dedup + positions
# ---------------------------------------------------------------------------


def test_dedup_order(tokenizer: LogTokenizer) -> None:
    assert tokenizer.tokenize("error error ERROR") == ["error"]


def test_positions_basic(tokenizer: LogTokenizer) -> None:
    positions = tokenizer.tokenize_with_positions("error error warning")
    assert positions == {"error": [0, 1], "warning": [2]}


def test_positions_with_compound() -> None:
    # Use min_term_len=1 so the short "1" octets are kept, letting us
    # check that compound + sub-tokens occupy consecutive positions and
    # that repeat sub-tokens accumulate multiple positions.
    tk = LogTokenizer(min_term_len=1)
    positions = tk.tokenize_with_positions("192.168.1.1 failed")

    # Compound first.
    assert positions["192.168.1.1"] == [0]
    # First octet.
    assert positions["192"] == [1]
    # Second octet.
    assert positions["168"] == [2]
    # "1" appears at positions 3 and 4 (third and fourth octets).
    assert positions["1"] == [3, 4]
    # Then "failed" at position 5.
    assert positions["failed"] == [5]


# ---------------------------------------------------------------------------
# Custom configuration
# ---------------------------------------------------------------------------


def test_custom_stop_words() -> None:
    tk = LogTokenizer(stop_words={"foo", "bar"})
    assert tk.tokenize("foo alpha bar beta") == ["alpha", "beta"]


def test_custom_min_len() -> None:
    tk = LogTokenizer(min_term_len=4)
    assert tk.tokenize("abc abcd abcde") == ["abcd", "abcde"]


# ---------------------------------------------------------------------------
# Empty and degenerate input
# ---------------------------------------------------------------------------


def test_empty_input(tokenizer: LogTokenizer) -> None:
    assert tokenizer.tokenize("") == []
    assert tokenizer.tokenize("   ") == []


def test_only_stop_words(tokenizer: LogTokenizer) -> None:
    assert tokenizer.tokenize("the and of") == []


# ---------------------------------------------------------------------------
# Realistic end-to-end log line
# ---------------------------------------------------------------------------


def test_mixed_log_line(tokenizer: LogTokenizer) -> None:
    line = (
        "2026-04-19T12:34:56Z ERROR auth.service "
        "[req_id=550e8400-e29b-41d4-a716-446655440000] "
        "user user@example.com login failed from 192.168.1.100:8080 (took 250 ms)"
    )
    tokens = set(tokenizer.tokenize(line))

    # Compound tokens should all be present.
    assert "2026-04-19t12:34:56z" in tokens
    assert "550e8400-e29b-41d4-a716-446655440000" in tokens
    assert "user@example.com" in tokens
    assert "192.168.1.100:8080" in tokens

    # Expected content tokens.
    expected = {
        "error",
        "auth.service",
        "auth",
        "service",
        "user",
        "example",
        "com",
        "login",
        "failed",
        "192.168.1.100",
        "250",
        "ms",
    }
    assert expected <= tokens


# ---------------------------------------------------------------------------
# Type checks
# ---------------------------------------------------------------------------


def test_tokenize_returns_list(tokenizer: LogTokenizer) -> None:
    result = tokenizer.tokenize("hello world")
    assert isinstance(result, list)
    assert all(isinstance(t, str) for t in result)


def test_tokenize_with_positions_returns_dict(tokenizer: LogTokenizer) -> None:
    result = tokenizer.tokenize_with_positions("hello world")
    assert isinstance(result, dict)
    for term, positions in result.items():
        assert isinstance(term, str)
        assert isinstance(positions, list)
        assert all(isinstance(p, int) for p in positions)
