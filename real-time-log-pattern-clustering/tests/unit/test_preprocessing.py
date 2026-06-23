"""Unit tests for the log preprocessing module (masking / parsing / tokenization).

These tests pin the behavior downstream commits depend on: every variable-token mask,
the **idempotency** of :func:`~src.preprocessing.mask_log`, the all-important
**collision** property (two logs differing only in IP/number/UUID mask to one string),
stopword-aware tokenization that preserves placeholders, and typed field normalization
in :func:`~src.preprocessing.parse_log` / :func:`~src.preprocessing.preprocess`.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from src.preprocessing import (
    MASK_EMAIL,
    MASK_HEX,
    MASK_IP,
    MASK_NUM,
    MASK_PATH,
    MASK_TS,
    MASK_URL,
    MASK_UUID,
    mask_log,
    parse_log,
    preprocess,
    tokenize,
)
from src.schemas import LogEntry


# ---------------------------------------------------------------------------
# mask_log — per-pattern coverage
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text, placeholder",
    [
        ("event at 2026-06-23T12:00:00Z done", MASK_TS),
        ("event at 2026-06-23 12:00:00,123 done", MASK_TS),
        ("apache 23/Jun/2026:12:00:00 +0000 hit", MASK_TS),
        ("bare date 2026-06-23 today", MASK_TS),
        ("calling https://api.example.com/v1/users now", MASK_URL),
        ("notify user alice@example.com please", MASK_EMAIL),
        ("request from 192.168.1.10 blocked", MASK_IP),
        ("request from 10.0.0.1:8080 blocked", MASK_IP),
        ("peer 2001:0db8:85a3:0000:0000:8a2e:0370:7334 up", MASK_IP),
        ("trace 550e8400-e29b-41d4-a716-446655440000 seen", MASK_UUID),
        ("digest d41d8cd98f00b204e9800998ecf8427e ok", MASK_HEX),
        ("flag 0xDEADBEEF raised", MASK_HEX),
        ("reading /var/log/app/server.log now", MASK_PATH),
        ("latency was 123.45 ms", MASK_NUM),
        ("count is -42 today", MASK_NUM),
    ],
)
def test_mask_log_each_pattern(text: str, placeholder: str) -> None:
    """Each variable-token type is rewritten to its corresponding placeholder."""
    masked = mask_log(text)
    assert placeholder in masked
    # The original variable token should be gone.
    assert "192.168.1.10" not in masked
    assert "550e8400" not in masked


def test_mask_log_uuid_not_swallowed_by_hex() -> None:
    """A UUID masks to <UUID> (not partially to <HEX>) thanks to ordering."""
    masked = mask_log("id 550e8400-e29b-41d4-a716-446655440000 here")
    assert masked == f"id {MASK_UUID} here"
    assert MASK_HEX not in masked


def test_mask_log_timestamp_digits_not_left_as_num() -> None:
    """A timestamp is fully consumed as <TS>; no stray <NUM> from its digits."""
    masked = mask_log("2026-06-23T12:00:00Z request handled")
    assert masked.startswith(MASK_TS)
    assert MASK_NUM not in masked


def test_mask_log_collapses_whitespace() -> None:
    """Runs of whitespace collapse to single spaces and ends are stripped."""
    assert mask_log("  too    many   spaces  ") == "too many spaces"


def test_mask_log_empty_input() -> None:
    """Empty/falsy input returns an empty string."""
    assert mask_log("") == ""


def test_mask_log_rest_endpoint_not_masked_as_path() -> None:
    """A single-segment REST endpoint ('/users') is NOT treated as a file path."""
    masked = mask_log("GET /users handled")
    assert "/users" in masked
    assert MASK_PATH not in masked


# ---------------------------------------------------------------------------
# mask_log — idempotency & collision (the core clustering properties)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "text",
    [
        "user alice@example.com from 192.168.1.10 at 2026-06-23T12:00:00Z took 12.5ms",
        "trace 550e8400-e29b-41d4-a716-446655440000 hex 0xDEADBEEF path /var/log/x.log",
        "GET https://x.io/a 200 in 5ms from 10.0.0.1:443",
        "plain message with no variables at all",
    ],
)
def test_mask_log_is_idempotent(text: str) -> None:
    """Masking a masked string is a no-op: mask_log(mask_log(s)) == mask_log(s)."""
    once = mask_log(text)
    twice = mask_log(once)
    assert once == twice


def test_mask_log_collision_on_ip_and_number() -> None:
    """Two logs differing ONLY in IP and a number mask to the SAME string.

    This is the key property the clustering relies on: variable network/numeric tokens
    must not fragment otherwise-identical events into different clusters.
    """
    a = mask_log("request from 192.168.1.10 took 123 ms")
    b = mask_log("request from 10.0.0.254 took 999 ms")
    assert a == b
    assert a == f"request from {MASK_IP} took {MASK_NUM} ms"


def test_mask_log_collision_on_uuid() -> None:
    """Two logs differing only in a UUID mask identically."""
    a = mask_log("session 550e8400-e29b-41d4-a716-446655440000 opened")
    b = mask_log("session 123e4567-e89b-12d3-a456-426614174000 opened")
    assert a == b
    assert a == f"session {MASK_UUID} opened"


# ---------------------------------------------------------------------------
# tokenize
# ---------------------------------------------------------------------------
def test_tokenize_lowercases_and_removes_stopwords() -> None:
    """Tokens are lowercased and common stopwords (the/a/is) are dropped."""
    tokens = tokenize("The Server IS A Host")
    assert "the" not in tokens
    assert "a" not in tokens
    assert "is" not in tokens
    assert "server" in tokens
    assert "host" in tokens


def test_tokenize_preserves_mask_placeholder_as_single_token() -> None:
    """A placeholder like <IP> survives tokenization as exactly one token."""
    tokens = tokenize(f"request from {MASK_IP} blocked")
    assert MASK_IP in tokens
    # It must not be split into '<', 'ip', '>'.
    assert "<" not in tokens
    assert "ip" not in tokens


def test_tokenize_drops_pure_punctuation() -> None:
    """Pure-punctuation tokens are removed from the output."""
    tokens = tokenize("error :: occurred -- now !!!")
    assert "error" in tokens
    assert "occurred" in tokens
    for junk in ("::", "--", "!!!", ":", "!"):
        assert junk not in tokens


def test_tokenize_empty_input() -> None:
    """Empty input yields an empty token list."""
    assert tokenize("") == []


def test_tokenize_on_masked_message_keeps_all_placeholders() -> None:
    """Several placeholders in one masked message all survive as single tokens."""
    masked = mask_log("user alice@example.com from 10.0.0.1 took 12.5 ms")
    tokens = tokenize(masked)
    assert MASK_EMAIL in tokens
    assert MASK_IP in tokens
    assert MASK_NUM in tokens


# ---------------------------------------------------------------------------
# parse_log
# ---------------------------------------------------------------------------
def test_parse_log_from_log_entry_normalizes_fields() -> None:
    """parse_log on a LogEntry returns typed fields with cased service/level."""
    entry = LogEntry(
        timestamp="2026-06-23T12:30:00",
        service="Auth",
        level="error",
        message="Multiple failed login attempts",
        source_ip="10.0.0.1",
        endpoint="/login",
        response_time_ms=12.5,
        status_code=401,
    )
    parsed = parse_log(entry)
    assert isinstance(parsed["timestamp"], datetime)
    assert parsed["service"] == "auth"  # lowercased
    assert parsed["level"] == "ERROR"  # uppercased
    assert parsed["message"] == "Multiple failed login attempts"
    assert parsed["source_ip"] == "10.0.0.1"
    assert parsed["endpoint"] == "/login"
    assert isinstance(parsed["response_time_ms"], float)
    assert parsed["response_time_ms"] == 12.5
    assert isinstance(parsed["status_code"], int)
    assert parsed["status_code"] == 401


def test_parse_log_from_dict_normalizes_fields() -> None:
    """parse_log on a plain dict returns the same normalized shape and types."""
    parsed = parse_log(
        {
            "timestamp": "2026-06-23T12:30:00Z",
            "service": "API",
            "level": "info",
            "message": "GET /users 200",
            "source_ip": "10.0.0.2",
            "endpoint": "/users",
            "response_time_ms": "8.0",  # string -> float
            "status_code": "200",  # string -> int
        }
    )
    assert isinstance(parsed["timestamp"], datetime)
    assert parsed["service"] == "api"
    assert parsed["level"] == "INFO"
    assert parsed["response_time_ms"] == 8.0
    assert parsed["status_code"] == 200


def test_parse_log_defensive_on_missing_keys() -> None:
    """Missing keys default sensibly (empty strings / None), never raising."""
    parsed = parse_log({"message": "something happened"})
    assert parsed["service"] == ""
    assert parsed["level"] == ""
    assert parsed["timestamp"] is None
    assert parsed["source_ip"] is None
    assert parsed["response_time_ms"] is None
    assert parsed["status_code"] is None


def test_parse_log_derives_endpoint_from_path_in_message() -> None:
    """When endpoint is absent, a '/path' token is derived from the message."""
    parsed = parse_log(
        {"service": "api", "level": "info", "message": "GET /api/v1/orders ok"}
    )
    assert parsed["endpoint"] == "/api/v1/orders"


def test_parse_log_derives_component_from_dotted_token() -> None:
    """With no path token, a 'service.component' identifier is derived."""
    parsed = parse_log(
        {"service": "auth", "level": "error", "message": "auth.login failed for user"}
    )
    assert parsed["endpoint"] == "auth.login"


def test_parse_log_endpoint_none_when_undetectable() -> None:
    """No endpoint and no path/dotted token in the message yields None."""
    parsed = parse_log({"service": "auth", "level": "info", "message": "all good"})
    assert parsed["endpoint"] is None


# ---------------------------------------------------------------------------
# preprocess
# ---------------------------------------------------------------------------
def test_preprocess_returns_three_keys_with_sane_values() -> None:
    """preprocess bundles parsed fields, masked message, and tokens coherently."""
    entry = LogEntry(
        timestamp="2026-06-23T12:30:00",
        service="auth",
        level="ERROR",
        message="login failed from 192.168.1.10 in 12.5 ms",
        source_ip="192.168.1.10",
    )
    result = preprocess(entry)
    assert set(result.keys()) == {"parsed", "masked_message", "tokens"}

    # parsed is the normalized field dict.
    assert result["parsed"]["service"] == "auth"
    assert result["parsed"]["level"] == "ERROR"

    # masked_message had its IP and number masked.
    assert MASK_IP in result["masked_message"]
    assert MASK_NUM in result["masked_message"]

    # tokens are derived from the masked message, placeholders preserved, stopwords gone.
    assert MASK_IP in result["tokens"]
    assert "login" in result["tokens"]
    assert "failed" in result["tokens"]


def test_preprocess_accepts_plain_dict() -> None:
    """preprocess works on a plain dict input, not just a LogEntry."""
    result = preprocess(
        {
            "timestamp": "2026-06-23T12:30:00",
            "service": "db",
            "level": "warn",
            "message": "slow query took 5000 ms",
        }
    )
    assert result["parsed"]["service"] == "db"
    assert result["parsed"]["level"] == "WARN"
    assert MASK_NUM in result["masked_message"]
    assert isinstance(result["tokens"], list)
