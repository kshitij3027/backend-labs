"""Tests for :class:`src.query.intent.IntentDetector`.

The detector is a first-match-wins regex classifier, so the interesting
cases all live at the priority boundaries: queries that would match
more than one bucket must land in the highest-priority bucket, and
queries that match nothing must fall through to ``general_search``.

Per ``src/query/intent.py``, priority order is:

1. payment_analysis
2. user_activity
3. performance_analysis
4. troubleshooting
5. general_search (fallback)
"""

from __future__ import annotations

import re

from src.query.intent import IntentDetector


# ---------------------------------------------------------------------------
# Single-bucket matches — no priority contest
# ---------------------------------------------------------------------------


def test_payment_failed_is_payment_analysis():
    """``payment`` fires the narrowest bucket."""
    assert IntentDetector().detect("payment failed") == "payment_analysis"


def test_null_pointer_exception_is_troubleshooting():
    """``exception`` by itself hits the troubleshooting bucket."""
    assert IntentDetector().detect("null pointer exception") == "troubleshooting"


def test_slow_response_times_is_performance_analysis():
    """``slow`` fires the performance bucket."""
    assert (
        IntentDetector().detect("slow response times") == "performance_analysis"
    )


# ---------------------------------------------------------------------------
# Priority — narrower bucket wins over broader one
# ---------------------------------------------------------------------------


def test_user_login_timeout_prefers_user_activity_over_performance():
    """``user`` (user_activity) outranks ``timeout`` (performance_analysis)."""
    assert IntentDetector().detect("user login timeout") == "user_activity"


# ---------------------------------------------------------------------------
# Fallback
# ---------------------------------------------------------------------------


def test_unmatched_query_falls_through_to_general_search():
    """A free-form phrase with no trigger keywords gets the fallback."""
    assert (
        IntentDetector().detect("what happened to my dashboard")
        == "general_search"
    )


def test_empty_string_is_general_search():
    """Empty input must not raise; it takes the fallback."""
    assert IntentDetector().detect("") == "general_search"


# ---------------------------------------------------------------------------
# Case insensitivity
# ---------------------------------------------------------------------------


def test_detection_is_case_insensitive():
    """Uppercase keywords still trigger their bucket."""
    assert IntentDetector().detect("PAYMENT Failed") == "payment_analysis"


# ---------------------------------------------------------------------------
# Constructor override
# ---------------------------------------------------------------------------


def test_custom_patterns_override_defaults():
    """Injecting a custom pattern list bypasses the built-in buckets.

    Build a detector whose single pattern is ``rain|snow``. Queries
    that would normally hit a built-in bucket must return the custom
    label if they match, or ``general_search`` if they don't — the
    default patterns should not be consulted at all.
    """
    custom = [("weather", re.compile(r"rain|snow", re.IGNORECASE))]
    detector = IntentDetector(patterns=custom)
    assert detector.detect("heavy rain expected") == "weather"
    # A query that would normally hit ``troubleshooting`` under the
    # defaults must now fall through, because we replaced the patterns
    # entirely.
    assert detector.detect("null pointer exception") == "general_search"
