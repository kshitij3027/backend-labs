"""Unit tests for :class:`~src.index.inverted_index.InvertedIndex`.

Covers the invariants that commits 05+ rely on: monotonic doc ids,
append-only postings, concurrent-write safety, bulk-version bump
convention, candidate ordering, and the stats shape. Every test
constructs a fresh index so there is no state bleed between cases.
"""

from __future__ import annotations

import asyncio

import pytest

from src.config import get_settings
from src.index.inverted_index import InvertedIndex
from src.index.tokenizer import LogTokenizer
from src.models import LogEntry


def _fresh_index() -> InvertedIndex:
    """Factory for a fresh index backed by a fresh tokenizer.

    Pulled out of a fixture because a handful of tests want multiple
    indexes inside the same test body.
    """
    settings = get_settings()
    tokenizer = LogTokenizer(settings)
    return InvertedIndex(settings=settings, tokenizer=tokenizer)


@pytest.fixture
def idx() -> InvertedIndex:
    return _fresh_index()


# ---------------------------------------------------------------------------
# Empty-state invariants
# ---------------------------------------------------------------------------

def test_fresh_index_is_empty(idx: InvertedIndex) -> None:
    """A freshly-built index reports zero counts and version 0."""
    assert idx.total_docs == 0
    assert idx.version == 0
    stats = idx.stats()
    assert stats["total_docs"] == 0
    assert stats["unique_tokens"] == 0
    assert stats["version"] == 0
    assert stats["total_postings"] == 0


# ---------------------------------------------------------------------------
# Single add
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_single_add_assigns_zero_and_bumps_version(
    idx: InvertedIndex,
) -> None:
    """First admit gets ``doc_id == 0``, version transitions 0 -> 1."""
    entry = LogEntry(message="authentication error", timestamp=1.0)
    doc_id = await idx.add(entry)
    assert doc_id == 0
    assert idx.version == 1
    assert idx.total_docs == 1


@pytest.mark.asyncio
async def test_two_adds_yield_consecutive_doc_ids(
    idx: InvertedIndex,
) -> None:
    """Sequential adds get ``0`` then ``1`` and version lands on 2."""
    first = await idx.add(LogEntry(message="payment failed", timestamp=1.0))
    second = await idx.add(LogEntry(message="payment succeeded", timestamp=2.0))
    assert first == 0
    assert second == 1
    assert idx.version == 2


# ---------------------------------------------------------------------------
# Bulk add — version-bump convention
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_add_bulk_returns_consecutive_ids_and_bumps_once(
    idx: InvertedIndex,
) -> None:
    """Bulk ingest returns ids in order and bumps the version **once**.

    This pins the convention declared in ``InvertedIndex.add_bulk``:
    the batch is logically one write from a cache-coherence standpoint,
    so callers see a single version jump regardless of batch size.
    """
    entries = [
        LogEntry(message=f"log {i}", timestamp=float(i)) for i in range(5)
    ]
    ids = await idx.add_bulk(entries)
    assert ids == [0, 1, 2, 3, 4]
    # Single bump for the whole batch — not five.
    assert idx.version == 1
    assert idx.total_docs == 5


# ---------------------------------------------------------------------------
# retrieve_candidates
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_retrieve_candidates_returns_matching_docs(
    idx: InvertedIndex,
) -> None:
    """A token's postings flow through to the candidate list."""
    await idx.add(LogEntry(message="authentication error", timestamp=1.0))
    await idx.add(LogEntry(message="payment processed", timestamp=2.0))
    candidates = idx.retrieve_candidates(["authentication"])
    assert 0 in candidates
    assert 1 not in candidates


@pytest.mark.asyncio
async def test_retrieve_candidates_orders_by_distinct_match_count(
    idx: InvertedIndex,
) -> None:
    """A doc matching more query tokens outranks one matching fewer."""
    await idx.add(LogEntry(message="error", timestamp=1.0))
    await idx.add(LogEntry(message="authentication error", timestamp=2.0))
    cands = idx.retrieve_candidates(["authentication", "error"])
    # Doc 1 matches both query tokens, doc 0 matches only "error".
    assert cands[0] == 1
    assert 0 in cands


@pytest.mark.asyncio
async def test_retrieve_candidates_respects_top_k(
    idx: InvertedIndex,
) -> None:
    """Explicit ``top_k`` caps the returned list regardless of matches."""
    for i in range(50):
        await idx.add(LogEntry(message=f"error {i}", timestamp=float(i)))
    cands = idx.retrieve_candidates(["error"], top_k=10)
    assert len(cands) == 10


def test_retrieve_candidates_empty_query_returns_empty(
    idx: InvertedIndex,
) -> None:
    """An empty token list short-circuits to ``[]`` without a scan."""
    assert idx.retrieve_candidates([]) == []


def test_retrieve_candidates_no_postings_returns_empty(
    idx: InvertedIndex,
) -> None:
    """Unknown tokens in an empty index yield an empty candidate list."""
    assert idx.retrieve_candidates(["nope", "missing"]) == []


# ---------------------------------------------------------------------------
# doc() round-trip
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_doc_round_trips_entry_fields(idx: InvertedIndex) -> None:
    """Fields on an admitted entry come back unchanged from :meth:`doc`."""
    entry = LogEntry(
        message="hello", timestamp=1.0, service="auth", level="WARN"
    )
    doc_id = await idx.add(entry)
    retrieved = idx.doc(doc_id)
    assert retrieved is not None
    assert retrieved.message == "hello"
    assert retrieved.service == "auth"
    assert retrieved.level == "WARN"
    # The index assigns the id — callers get back their entry with
    # the server-side id populated.
    assert retrieved.id == doc_id


def test_doc_miss_returns_none(idx: InvertedIndex) -> None:
    """A non-existent doc id reads back as ``None``, not a raise."""
    assert idx.doc(9999) is None


# ---------------------------------------------------------------------------
# Concurrency — append-only invariants under gather()
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_concurrent_add_preserves_all_docs() -> None:
    """50 concurrent ``add`` calls leave a dense 0..49 doc-id range.

    Hammers the writer lock with a ``gather`` fan-out to confirm that
    the lock serialises the id-assignment + postings mutation and no
    writes are lost. Version must end at 50 (one bump per successful
    admit).
    """
    idx = _fresh_index()
    entries = [
        LogEntry(message=f"log {i}", timestamp=float(i)) for i in range(50)
    ]
    await asyncio.gather(*(idx.add(e) for e in entries))

    assert idx.total_docs == 50
    # Every id 0..49 should be present — no gaps, no duplicates.
    assert set(idx._docs.keys()) == set(range(50))
    # One version bump per ``add`` is the declared convention.
    assert idx.version == 50


# ---------------------------------------------------------------------------
# stats() shape
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_stats_structure_has_integer_fields(
    idx: InvertedIndex,
) -> None:
    """``stats()`` returns the four documented keys, all ints."""
    await idx.add(LogEntry(message="hello world", timestamp=1.0))
    stats = idx.stats()
    expected_keys = {"total_docs", "unique_tokens", "version", "total_postings"}
    assert set(stats.keys()) == expected_keys
    for key, value in stats.items():
        assert isinstance(value, int), f"stats[{key!r}] must be int, got {type(value)}"
    # Sanity: after one admit, at least these minima hold.
    assert stats["total_docs"] == 1
    assert stats["version"] == 1
    assert stats["unique_tokens"] >= 1
    assert stats["total_postings"] >= 1
