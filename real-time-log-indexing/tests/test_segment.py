"""Unit tests for :class:`src.index.segment.Segment`.

These tests exercise the in-memory segment primitive in isolation —
no tokenizer, no persistence, no asyncio. Every invariant the
orchestrator relies on (monotone doc_ids, deduped per-doc terms,
sorted posting lists, non-decreasing memory estimate, full detection)
gets its own small deterministic check.

Tests run inside Docker via ``make test`` per the project rules.
"""

from __future__ import annotations

import pytest

from src.index.segment import Segment
from src.models import LogEntry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_entry(
    doc_id: int,
    msg: str = "hello world",
    level: str = "INFO",
    service: str = "svc",
) -> LogEntry:
    """Build a minimal ``LogEntry`` for segment tests."""
    return LogEntry(
        doc_id=doc_id,
        message=msg,
        timestamp=float(doc_id),
        service=service,
        level=level,  # type: ignore[arg-type]  # Literal covers common levels
    )


# ---------------------------------------------------------------------------
# Construction / empty state
# ---------------------------------------------------------------------------

def test_empty_segment_state() -> None:
    """A fresh segment reports empty, with no min/max doc_id set."""
    seg = Segment("seg-000001")
    assert seg.doc_count() == 0
    assert seg.term_count() == 0
    assert seg.memory_bytes() >= 0
    assert seg.min_doc_id is None
    assert seg.max_doc_id is None


def test_segment_id_preserved() -> None:
    """The constructor stores the provided id verbatim."""
    seg = Segment("seg-000042")
    assert seg.segment_id == "seg-000042"


# ---------------------------------------------------------------------------
# Single / multiple add
# ---------------------------------------------------------------------------

def test_add_single_document() -> None:
    """After one add, doc_count is 1 and min/max equal that doc_id."""
    seg = Segment("seg-1")
    seg.add(1, make_entry(1), ["error", "timeout"])
    assert seg.doc_count() == 1
    assert seg.min_doc_id == 1
    assert seg.max_doc_id == 1
    assert seg.search_term("error") == [1]
    assert seg.search_term("timeout") == [1]


def test_add_multiple_monotonic_docs() -> None:
    """Each posting list reflects exactly the docs that contained the term."""
    seg = Segment("seg-1")
    seg.add(1, make_entry(1), ["a", "b"])
    seg.add(2, make_entry(2), ["b", "c"])
    seg.add(3, make_entry(3), ["a", "c"])

    assert seg.search_term("a") == [1, 3]
    assert seg.search_term("b") == [1, 2]
    assert seg.search_term("c") == [2, 3]
    assert seg.doc_count() == 3
    assert seg.min_doc_id == 1
    assert seg.max_doc_id == 3


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

def test_reject_duplicate_doc_id() -> None:
    """Adding the same doc_id twice raises ValueError."""
    seg = Segment("seg-1")
    seg.add(1, make_entry(1), ["x"])
    with pytest.raises(ValueError):
        seg.add(1, make_entry(1), ["y"])


def test_reject_non_monotonic_doc_id() -> None:
    """Adding a smaller doc_id after a larger one raises ValueError."""
    seg = Segment("seg-1")
    seg.add(5, make_entry(5), ["x"])
    with pytest.raises(ValueError, match="monotonically increasing"):
        seg.add(3, make_entry(3), ["y"])


# ---------------------------------------------------------------------------
# search_term
# ---------------------------------------------------------------------------

def test_search_unknown_term() -> None:
    """Unknown terms return an empty list, not None."""
    seg = Segment("seg-1")
    seg.add(1, make_entry(1), ["known"])
    assert seg.search_term("missing") == []


def test_search_term_returns_copy() -> None:
    """Mutating the returned list must not affect the segment's state."""
    seg = Segment("seg-1")
    seg.add(1, make_entry(1), ["keep"])
    seg.add(2, make_entry(2), ["keep"])

    first = seg.search_term("keep")
    first.append(999)
    first.clear()

    # Re-query — the segment's own list should be untouched.
    assert seg.search_term("keep") == [1, 2]


def test_posting_list_sorted() -> None:
    """Posting lists are ascending by doc_id by virtue of monotone insertion."""
    seg = Segment("seg-1")
    for doc_id in range(1, 11):
        seg.add(doc_id, make_entry(doc_id), ["keep"])
    assert seg.search_term("keep") == list(range(1, 11))


# ---------------------------------------------------------------------------
# Per-doc term dedup
# ---------------------------------------------------------------------------

def test_doc_terms_deduped() -> None:
    """Duplicates in the input are collapsed before they hit any map."""
    seg = Segment("seg-1")
    seg.add(1, make_entry(1), ["a", "a", "b", "a"])

    # doc_terms stores each term at most once per doc.
    assert seg.doc_terms[1].count("a") == 1
    assert set(seg.doc_terms[1]) == {"a", "b"}

    # And the posting list has doc_id 1 exactly once.
    assert seg.search_term("a") == [1]
    assert seg.search_term("b") == [1]


# ---------------------------------------------------------------------------
# Memory accounting
# ---------------------------------------------------------------------------

def test_memory_bytes_grows_monotonically() -> None:
    """Memory estimate is non-decreasing across adds."""
    seg = Segment("seg-1")
    last = seg.memory_bytes()
    for doc_id in range(1, 21):
        seg.add(doc_id, make_entry(doc_id, msg=f"message {doc_id}"), ["t1", "t2"])
        current = seg.memory_bytes()
        assert current >= last, (
            f"memory_bytes dropped from {last} to {current} at doc {doc_id}"
        )
        last = current


# ---------------------------------------------------------------------------
# is_full
# ---------------------------------------------------------------------------

def test_is_full_by_docs() -> None:
    """Doc-count trigger fires exactly at max_docs."""
    seg = Segment("seg-1")
    seg.add(1, make_entry(1), ["t"])
    seg.add(2, make_entry(2), ["t"])
    # Below max — not full.
    assert seg.is_full(max_docs=3, max_bytes=10_000_000) is False

    seg.add(3, make_entry(3), ["t"])
    # At max — full.
    assert seg.is_full(max_docs=3, max_bytes=10_000_000) is True


def test_is_full_by_memory() -> None:
    """Memory trigger fires once the estimator crosses max_bytes."""
    seg = Segment("seg-1")
    # Large message to pump memory_bytes upward quickly.
    big_msg = "x" * 1024
    # With max_bytes=500 and each add contributing >500 bytes via message
    # alone, the first add is already enough to flip the flag.
    seg.add(1, make_entry(1, msg=big_msg), ["t1", "t2"])
    # If a single add didn't cross, a few more will; loop until true or
    # we give up. The bound keeps the test from hanging on regression.
    doc_id = 2
    while not seg.is_full(max_docs=100_000, max_bytes=500) and doc_id < 20:
        seg.add(doc_id, make_entry(doc_id, msg=big_msg), [f"t{doc_id}"])
        doc_id += 1

    assert seg.is_full(max_docs=100_000, max_bytes=500) is True


# ---------------------------------------------------------------------------
# iter_docs
# ---------------------------------------------------------------------------

def test_iter_docs_in_order() -> None:
    """iter_docs yields triples in ascending doc_id order."""
    seg = Segment("seg-1")
    # Insert monotonically but at irregular gaps to mimic real assignments.
    chosen = [2, 5, 9, 14, 15, 42]
    for doc_id in chosen:
        seg.add(doc_id, make_entry(doc_id), [f"tag{doc_id}"])

    observed_ids: list[int] = []
    for doc_id, entry, terms in seg.iter_docs():
        assert isinstance(entry, LogEntry)
        assert isinstance(terms, list)
        assert entry.doc_id == doc_id
        observed_ids.append(doc_id)

    assert observed_ids == sorted(chosen)


# ---------------------------------------------------------------------------
# Term count
# ---------------------------------------------------------------------------

def test_term_count_reflects_unique_terms() -> None:
    """term_count equals the size of the union of all docs' terms."""
    seg = Segment("seg-1")
    seg.add(1, make_entry(1), ["a", "b", "c"])
    seg.add(2, make_entry(2), ["b", "c", "d"])
    seg.add(3, make_entry(3), ["c", "d", "e"])

    # Union is {a, b, c, d, e} — five distinct terms.
    assert seg.term_count() == 5
