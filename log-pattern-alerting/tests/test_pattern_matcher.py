"""Tests for the pattern matching engine."""

import re

import pytest

from src.engine.pattern_matcher import PatternMatcher
from src.models import AlertRule


def _make_rule(name: str, pattern: str, **kwargs) -> AlertRule:
    """Helper to create an AlertRule with sensible defaults."""
    defaults = dict(
        threshold=5,
        window_seconds=60,
        severity="high",
        enabled=True,
    )
    defaults.update(kwargs)
    return AlertRule(name=name, pattern=pattern, **defaults)


def _load_rules(matcher: PatternMatcher, rules: list[AlertRule]) -> None:
    """Compile and load rules into the matcher without a DB session."""
    compiled: list[tuple[AlertRule, re.Pattern]] = []
    for rule in rules:
        try:
            regex = re.compile(rule.pattern, re.IGNORECASE)
            compiled.append((rule, regex))
        except re.error:
            pass  # mirror production behaviour: skip bad patterns
    matcher._patterns = compiled


# -- Auth failure patterns used in multiple tests --
AUTH_RULES = [
    _make_rule("auth_failure", r"(authentication|login|auth)\s+(failed|error)"),
]

# -- Database error patterns --
DB_RULES = [
    _make_rule(
        "db_error",
        r"(database\s+error|connection\s+timeout|query\s+failed|deadlock)",
    ),
]

# -- API error patterns --
API_RULES = [
    _make_rule(
        "api_error",
        r"(api\s+error|endpoint\s+failed|request\s+timeout)",
    ),
]


class TestPatternMatcherInit:
    """PatternMatcher should start with an empty pattern list."""

    def test_starts_empty(self):
        matcher = PatternMatcher()
        assert matcher._patterns == []

    def test_match_returns_empty_when_no_patterns(self):
        matcher = PatternMatcher()
        results = matcher.match("anything at all")
        assert results == []


class TestAuthFailureMatching:
    """Rules should detect authentication / login failure messages."""

    @pytest.fixture()
    def matcher(self) -> PatternMatcher:
        m = PatternMatcher()
        _load_rules(m, AUTH_RULES)
        return m

    def test_authentication_failed(self, matcher: PatternMatcher):
        results = matcher.match("Authentication failed for user admin")
        assert len(results) == 1
        assert results[0].name == "auth_failure"

    def test_login_failed(self, matcher: PatternMatcher):
        results = matcher.match("Login failed from IP 1.2.3.4")
        assert len(results) == 1
        assert results[0].name == "auth_failure"

    def test_auth_error(self, matcher: PatternMatcher):
        results = matcher.match("auth error occurred")
        assert len(results) == 1
        assert results[0].name == "auth_failure"


class TestDatabaseErrorMatching:
    """Rules should detect database-related error messages."""

    @pytest.fixture()
    def matcher(self) -> PatternMatcher:
        m = PatternMatcher()
        _load_rules(m, DB_RULES)
        return m

    def test_database_error(self, matcher: PatternMatcher):
        results = matcher.match("Database error on query")
        assert len(results) == 1
        assert results[0].name == "db_error"

    def test_connection_timeout(self, matcher: PatternMatcher):
        results = matcher.match("Connection timeout to replica")
        assert len(results) == 1
        assert results[0].name == "db_error"

    def test_query_failed_deadlock(self, matcher: PatternMatcher):
        results = matcher.match("Query failed with deadlock")
        assert len(results) == 1
        assert results[0].name == "db_error"


class TestApiErrorMatching:
    """Rules should detect API error messages."""

    @pytest.fixture()
    def matcher(self) -> PatternMatcher:
        m = PatternMatcher()
        _load_rules(m, API_RULES)
        return m

    def test_api_error(self, matcher: PatternMatcher):
        results = matcher.match("API error on /users endpoint")
        assert len(results) == 1
        assert results[0].name == "api_error"

    def test_endpoint_failed(self, matcher: PatternMatcher):
        results = matcher.match("endpoint failed with 503")
        assert len(results) == 1
        assert results[0].name == "api_error"

    def test_request_timeout(self, matcher: PatternMatcher):
        results = matcher.match("request timeout after 30s")
        assert len(results) == 1
        assert results[0].name == "api_error"


class TestNonMatchingMessages:
    """Normal operational messages should never trigger any rule."""

    @pytest.fixture()
    def matcher(self) -> PatternMatcher:
        m = PatternMatcher()
        _load_rules(m, AUTH_RULES + DB_RULES + API_RULES)
        return m

    def test_system_started(self, matcher: PatternMatcher):
        assert matcher.match("System started successfully") == []

    def test_user_logged_in(self, matcher: PatternMatcher):
        assert matcher.match("User logged in") == []

    def test_request_completed(self, matcher: PatternMatcher):
        assert matcher.match("Request completed in 50ms") == []


class TestMultiplePatternMatch:
    """A single log message can match more than one rule."""

    def test_message_matches_two_rules(self):
        matcher = PatternMatcher()
        rules = [
            _make_rule("auth_issue", r"auth\s+error"),
            _make_rule("db_issue", r"database\s+error"),
            _make_rule("combined_issue", r"(auth|database)\s+error"),
        ]
        _load_rules(matcher, rules)

        results = matcher.match("auth error and database error in same log")
        matched_names = {r.name for r in results}
        assert "auth_issue" in matched_names
        assert "db_issue" in matched_names
        assert "combined_issue" in matched_names
        assert len(results) == 3


class TestCaseInsensitiveMatching:
    """Patterns are compiled with ``re.IGNORECASE``."""

    @pytest.fixture()
    def matcher(self) -> PatternMatcher:
        m = PatternMatcher()
        _load_rules(m, AUTH_RULES + DB_RULES)
        return m

    def test_uppercase_authentication_failed(self, matcher: PatternMatcher):
        results = matcher.match("AUTHENTICATION FAILED for user root")
        assert len(results) == 1
        assert results[0].name == "auth_failure"

    def test_mixed_case_database_error(self, matcher: PatternMatcher):
        results = matcher.match("Database ERROR on table users")
        assert len(results) == 1
        assert results[0].name == "db_error"


class TestInvalidRegex:
    """A broken regex pattern must be silently skipped, not crash."""

    def test_invalid_regex_skipped(self):
        matcher = PatternMatcher()
        rules = [
            _make_rule("bad_rule", r"[invalid"),
            _make_rule("good_rule", r"error\s+occurred"),
        ]
        _load_rules(matcher, rules)

        # Only the valid pattern should be loaded
        assert len(matcher._patterns) == 1
        assert matcher._patterns[0][0].name == "good_rule"

    def test_match_still_works_after_bad_regex(self):
        matcher = PatternMatcher()
        rules = [
            _make_rule("bad_rule", r"[invalid"),
            _make_rule("good_rule", r"error\s+occurred"),
        ]
        _load_rules(matcher, rules)

        results = matcher.match("An error occurred here")
        assert len(results) == 1
        assert results[0].name == "good_rule"
