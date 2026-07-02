"""Unit tests for the feedback query-pattern bucket key (C10).

:func:`src.feedback.query_pattern` derives the ``"service|severity|tags"`` bucket a
vote is aggregated under. Its normalisation is what makes votes on semantically
similar queries (same contextual facets, possibly different wording / tag order)
accumulate in the *same* ``(pattern, incident)`` aggregate, so these tests pin the
normalisation contract exactly:

* ``service`` / ``severity`` — stripped + lower-cased; ``None`` / blank -> ``""``.
* ``tags`` — each stripped + lower-cased, blanks dropped, **de-duplicated**,
  **sorted**, comma-joined (order-independent).
* the three parts joined with ``"|"``; all-empty -> the catch-all ``"||"``.

These are pure functions (no DB / Redis), so they live in the unit suite.
"""

from __future__ import annotations

import pytest

from src.feedback import query_pattern


def test_query_pattern_normalizes_case_dedup_and_sort() -> None:
    """``("payments","high",["Timeout","db","db"])`` -> ``"payments|high|db,timeout"``.

    Exercises the whole contract at once: service/severity lower-cased, the tags
    lower-cased + de-duplicated (``db`` twice collapses) + **sorted** (``db`` before
    ``timeout`` regardless of input order).
    """
    assert (
        query_pattern("payments", "high", ["Timeout", "db", "db"])
        == "payments|high|db,timeout"
    )


def test_query_pattern_all_none_is_catch_all_bucket() -> None:
    """``(None, None, None)`` collapses to the catch-all ``"||"`` bucket."""
    assert query_pattern(None, None, None) == "||"


def test_query_pattern_strips_whitespace_and_dedups_case_insensitively() -> None:
    """``("  API "," HIGH ",[" x ","X"])`` -> ``"api|high|x"``.

    Whitespace is stripped from every facet; ``" x "`` and ``"X"`` normalise to the
    same ``x`` and collapse to a single tag.
    """
    assert query_pattern("  API ", " HIGH ", [" x ", "X"]) == "api|high|x"


def test_query_pattern_blank_facets_normalize_to_empty() -> None:
    """Blank / whitespace-only service & severity and all-blank tags -> ``"||"``."""
    assert query_pattern("   ", "", ["  ", ""]) == "||"


def test_query_pattern_empty_tag_list_leaves_tags_part_empty() -> None:
    """A non-empty service/severity with no tags leaves the third part empty."""
    assert query_pattern("payments", "high", []) == "payments|high|"
    assert query_pattern("payments", "high", None) == "payments|high|"


def test_query_pattern_tags_are_order_independent() -> None:
    """Two different tag orderings yield the *same* bucket (sort makes it stable)."""
    a = query_pattern("svc", "low", ["zeta", "alpha", "mike"])
    b = query_pattern("svc", "low", ["mike", "zeta", "alpha"])
    assert a == b == "svc|low|alpha,mike,zeta"


@pytest.mark.parametrize(
    ("service", "severity", "tags", "expected"),
    [
        ("payments", "high", ["Timeout", "db", "db"], "payments|high|db,timeout"),
        (None, None, None, "||"),
        ("  API ", " HIGH ", [" x ", "X"], "api|high|x"),
        ("only-service", None, None, "only-service||"),
        (None, "critical", None, "|critical|"),
        (None, None, ["B", "a"], "||a,b"),
    ],
)
def test_query_pattern_table(
    service: str | None,
    severity: str | None,
    tags: list[str] | None,
    expected: str,
) -> None:
    """Table of representative facet combinations -> exact bucket key."""
    assert query_pattern(service, severity, tags) == expected
