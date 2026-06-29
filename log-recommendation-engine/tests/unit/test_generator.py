"""Unit tests for the synthetic-incident generator (:mod:`src.generator`).

Pure — no DB. These assert:

* determinism: same ``seed`` + fixed ``end`` → byte-identical rows; a different
  ``seed`` → different rows;
* ``generate_default_corpus()`` returns 120 rows and covers all 10 families;
* every generated row is well-formed (required keys, valid severity, non-empty
  tags, and *no* ``embedding`` key — vectors are C5);
* ``created_at`` is timezone-aware and falls inside the ``[end - days_back, end]``
  window.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from src.generator import (
    FAMILY_COUNT,
    FAMILY_KEYS,
    generate_default_corpus,
    generate_incidents,
)
from src.schemas import SEVERITIES

_REQUIRED_KEYS = {
    "title",
    "description",
    "service",
    "severity",
    "tags",
    "resolution",
    "created_at",
}

# A fixed upper bound so timestamp comparisons are deterministic.
_FIXED_END = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


# --------------------------------------------------------------------------- #
# Family bookkeeping sanity
# --------------------------------------------------------------------------- #
def test_family_count_is_ten() -> None:
    """There are exactly 10 incident families."""
    assert FAMILY_COUNT == 10
    assert len(FAMILY_KEYS) == 10
    assert len(set(FAMILY_KEYS)) == 10  # keys are unique


# --------------------------------------------------------------------------- #
# Determinism
# --------------------------------------------------------------------------- #
def test_same_seed_and_end_is_byte_identical() -> None:
    """Same seed + same fixed ``end`` → identical output (timestamps included)."""
    a = generate_incidents(50, seed=42, end=_FIXED_END)
    b = generate_incidents(50, seed=42, end=_FIXED_END)
    assert a == b


def test_different_seed_differs() -> None:
    """A different seed produces a different corpus."""
    a = generate_incidents(50, seed=42, end=_FIXED_END)
    b = generate_incidents(50, seed=1234, end=_FIXED_END)
    assert a != b


def test_default_corpus_deterministic_content_excluding_time() -> None:
    """The default corpus is deterministic in its non-time fields across runs."""

    def _stripped(rows: list[dict]) -> list[dict]:
        return [{k: v for k, v in r.items() if k != "created_at"} for r in rows]

    assert _stripped(generate_default_corpus(seed=42)) == _stripped(
        generate_default_corpus(seed=42)
    )


# --------------------------------------------------------------------------- #
# Default corpus size + family coverage
# --------------------------------------------------------------------------- #
def test_default_corpus_size_is_120() -> None:
    """The default corpus is 12 × 10 families = 120 incidents."""
    rows = generate_default_corpus()
    assert len(rows) == 120


def test_default_corpus_covers_all_families() -> None:
    """Every family's service pool is represented in the default corpus.

    Round-robin over 120 rows / 10 families hits each family 12 times, so every
    family key's service set must intersect the generated services.
    """
    from src.generator import _FAMILIES  # noqa: PLC0415 - internal, test-only

    services_by_family = {f.key: set(f.services) for f in _FAMILIES}
    seen_services = {r["service"] for r in generate_default_corpus()}
    for key in FAMILY_KEYS:
        assert seen_services & services_by_family[key], (
            f"family {key} not represented in the default corpus"
        )


# --------------------------------------------------------------------------- #
# Row well-formedness
# --------------------------------------------------------------------------- #
def test_every_row_well_formed() -> None:
    """Each row has exactly the required keys, valid values, and NO embedding."""
    rows = generate_default_corpus()
    for r in rows:
        assert set(r.keys()) == _REQUIRED_KEYS
        assert "embedding" not in r
        assert isinstance(r["title"], str) and r["title"].strip()
        assert isinstance(r["description"], str) and r["description"].strip()
        assert isinstance(r["service"], str) and r["service"].strip()
        assert isinstance(r["resolution"], str) and r["resolution"].strip()
        assert r["severity"] in SEVERITIES
        assert isinstance(r["tags"], list) and len(r["tags"]) > 0
        assert all(isinstance(t, str) and t for t in r["tags"])


def test_created_at_tz_aware_and_within_window() -> None:
    """``created_at`` is tz-aware and inside ``[end - days_back, end]``."""
    days_back = 180
    rows = generate_incidents(200, seed=7, days_back=days_back, end=_FIXED_END)
    window_start = _FIXED_END - timedelta(days=days_back)
    for r in rows:
        created = r["created_at"]
        assert isinstance(created, datetime)
        assert created.tzinfo is not None  # timezone-aware
        assert window_start <= created <= _FIXED_END


def test_zero_or_negative_count_is_empty() -> None:
    """``count <= 0`` yields an empty list."""
    assert generate_incidents(0) == []
    assert generate_incidents(-5) == []
