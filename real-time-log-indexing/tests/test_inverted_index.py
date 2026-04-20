"""Unit tests for :class:`src.index.inverted_index.InvertedIndex`.

These tests exercise the orchestrator end-to-end in isolation — we
build a real :class:`LogTokenizer`, a real on-disk segment directory
via pytest's ``tmp_path``, and drive the index through its write and
read APIs. The tests cover doc-id allocation, flush/spill thresholds,
cross-segment search + dedup, service/level filters, highlight
rendering, restart rehydration, concurrency, and error accounting.

Tests run inside Docker via ``make test`` per the project rules.
``pytest.ini`` sets ``asyncio_mode = auto`` so the ``async def`` tests
don't need explicit ``@pytest.mark.asyncio`` decoration.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Awaitable

import pytest

from src.config import Settings
from src.index.inverted_index import InvertedIndex
from src.index.tokenizer import LogTokenizer
from src.models import LogEntry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mk_entry(
    msg: str,
    service: str = "web",
    level: str = "INFO",
    timestamp: float | None = None,
) -> LogEntry:
    """Build a LogEntry without a doc_id (the index will assign one)."""
    return LogEntry(
        doc_id=0,  # placeholder — rewritten by add_document
        message=msg,
        timestamp=timestamp if timestamp is not None else time.time(),
        service=service,
        level=level,  # type: ignore[arg-type]
    )


def _mk_settings(
    tmp_path: Path,
    segment_max_docs: int = 10_000,
    segment_max_memory_mb: int = 50,
    max_memory_segments: int = 5,
) -> Settings:
    """Build a :class:`Settings` with the given overrides.

    We construct a fresh ``Settings`` each time rather than mutating
    the singleton so tests can run in any order without leaking state.
    """
    return Settings(
        disk_segment_dir=str(tmp_path / "segments"),
        segment_max_docs=segment_max_docs,
        segment_max_memory_mb=segment_max_memory_mb,
        max_memory_segments=max_memory_segments,
    )


def _mk_index(
    tmp_path: Path,
    **kwargs,
) -> InvertedIndex:
    """Build an InvertedIndex wired to a per-test disk directory."""
    settings = _mk_settings(tmp_path, **kwargs)
    return InvertedIndex(
        settings=settings,
        tokenizer=LogTokenizer(),
        disk_dir=Path(settings.disk_segment_dir),
    )


# ---------------------------------------------------------------------------
# 1. Initial state
# ---------------------------------------------------------------------------

async def test_initial_state_empty(tmp_path: Path) -> None:
    """A fresh index reports zero across every counter."""
    idx = _mk_index(tmp_path)
    s = idx.stats()
    assert s["docs_indexed"] == 0
    assert s["current_segment_docs"] == 0
    assert s["flushed_memory_segments"] == 0
    assert s["disk_segments"] == 0
    assert s["vocab_size"] == 0
    assert s["errors"] == 0


# ---------------------------------------------------------------------------
# 2. & 3. Doc-id allocation
# ---------------------------------------------------------------------------

async def test_add_single_document_assigns_doc_id_1(tmp_path: Path) -> None:
    """First document returns doc_id == 1 and stats reflect the add."""
    idx = _mk_index(tmp_path)
    doc_id = await idx.add_document(_mk_entry("hello world"))
    assert doc_id == 1
    s = idx.stats()
    assert s["docs_indexed"] == 1
    assert s["current_segment_docs"] == 1


async def test_doc_ids_monotonic(tmp_path: Path) -> None:
    """Sequential adds produce strictly increasing doc_ids."""
    idx = _mk_index(tmp_path)
    ids = []
    for i in range(5):
        ids.append(await idx.add_document(_mk_entry(f"msg {i}")))
    assert ids == [1, 2, 3, 4, 5]


# ---------------------------------------------------------------------------
# 4. Single-term search
# ---------------------------------------------------------------------------

async def test_search_finds_document(tmp_path: Path) -> None:
    """Single-term search returns exactly the matching docs."""
    idx = _mk_index(tmp_path)
    await idx.add_document(_mk_entry("auth failed"))
    await idx.add_document(_mk_entry("cache miss"))

    assert len(idx.search("auth")) == 1
    assert len(idx.search("miss")) == 1
    assert idx.search("nonexistent") == []


# ---------------------------------------------------------------------------
# 5. Multi-term AND
# ---------------------------------------------------------------------------

async def test_search_multi_term_and(tmp_path: Path) -> None:
    """Multi-term search intersects postings (AND)."""
    idx = _mk_index(tmp_path)
    await idx.add_document(_mk_entry("auth failed for user kshitij"))
    await idx.add_document(_mk_entry("db failed timeout"))
    await idx.add_document(_mk_entry("network unreachable"))

    results = idx.search("auth failed")
    assert len(results) == 1
    assert "auth failed" in results[0].message


# ---------------------------------------------------------------------------
# 6. / 7. Service & level filters
# ---------------------------------------------------------------------------

async def test_search_service_filter(tmp_path: Path) -> None:
    """service= narrows the hit set to that service only."""
    idx = _mk_index(tmp_path)
    await idx.add_document(_mk_entry("request failed", service="auth"))
    await idx.add_document(_mk_entry("request failed", service="payment"))
    await idx.add_document(_mk_entry("request failed", service="auth"))

    results = idx.search("request", service="auth")
    assert len(results) == 2
    assert all(r.service == "auth" for r in results)


async def test_search_level_filter(tmp_path: Path) -> None:
    """level= narrows the hit set to that level only."""
    idx = _mk_index(tmp_path)
    await idx.add_document(_mk_entry("deploy finished", level="INFO"))
    await idx.add_document(_mk_entry("deploy finished", level="ERROR"))
    await idx.add_document(_mk_entry("deploy finished", level="ERROR"))

    results = idx.search("deploy", level="ERROR")
    assert len(results) == 2
    assert all(r.level == "ERROR" for r in results)


# ---------------------------------------------------------------------------
# 8. limit
# ---------------------------------------------------------------------------

async def test_search_limit_applied(tmp_path: Path) -> None:
    """limit caps the returned list size even when more docs match."""
    idx = _mk_index(tmp_path)
    for i in range(20):
        await idx.add_document(_mk_entry(f"slowquery {i} timeout"))
    results = idx.search("timeout", limit=5)
    assert len(results) == 5


# ---------------------------------------------------------------------------
# 9. Newest-first ordering
# ---------------------------------------------------------------------------

async def test_search_newest_first(tmp_path: Path) -> None:
    """Results are sorted by timestamp descending."""
    idx = _mk_index(tmp_path)
    # Intentionally out of order on the write path.
    await idx.add_document(_mk_entry("needle", timestamp=100.0))
    await idx.add_document(_mk_entry("needle", timestamp=300.0))
    await idx.add_document(_mk_entry("needle", timestamp=200.0))

    results = idx.search("needle")
    assert [r.timestamp for r in results] == [300.0, 200.0, 100.0]


# ---------------------------------------------------------------------------
# 10. Highlighting
# ---------------------------------------------------------------------------

async def test_search_highlights_term(tmp_path: Path) -> None:
    """<mark> wraps the matched term regardless of original case."""
    idx = _mk_index(tmp_path)
    await idx.add_document(_mk_entry("Auth failed"))
    results = idx.search("auth")
    assert len(results) == 1
    # Case preserved in the original word; the mark wraps it as-is.
    assert "<mark>Auth</mark>" in results[0].highlighted_message


# ---------------------------------------------------------------------------
# 11. Rotation on SEGMENT_MAX_DOCS
# ---------------------------------------------------------------------------

async def test_flush_rotates_on_max_docs(tmp_path: Path) -> None:
    """Hitting segment_max_docs flushes the current segment to memory."""
    idx = _mk_index(tmp_path, segment_max_docs=3, max_memory_segments=5)
    for i in range(3):
        await idx.add_document(_mk_entry(f"row {i}"))

    s = idx.stats()
    assert s["flushed_memory_segments"] == 1
    assert s["current_segment_docs"] == 0

    await idx.add_document(_mk_entry("row after"))
    s = idx.stats()
    assert s["flushed_memory_segments"] == 1
    assert s["current_segment_docs"] == 1


# ---------------------------------------------------------------------------
# 12. Spill-to-disk when memory queue overflows
# ---------------------------------------------------------------------------

async def test_spill_to_disk_when_memory_queue_full(tmp_path: Path) -> None:
    """When the memory FIFO exceeds max_memory_segments, oldest spills to disk."""
    idx = _mk_index(tmp_path, segment_max_docs=2, max_memory_segments=2)
    for i in range(8):
        await idx.add_document(_mk_entry(f"row {i}"))

    s = idx.stats()
    assert s["flushed_memory_segments"] <= 2
    assert s["disk_segments"] >= 1
    # All 8 docs accounted for.
    assert s["docs_indexed"] == 8


# ---------------------------------------------------------------------------
# 13. Restart rehydration
# ---------------------------------------------------------------------------

async def test_load_from_disk_rehydrates(tmp_path: Path) -> None:
    """After 'restart', previously indexed docs are still searchable."""
    # First generation: spill everything immediately (cap = 0).
    idx1 = _mk_index(tmp_path, segment_max_docs=2, max_memory_segments=0)
    messages = [
        "auth failed for user alice",
        "auth granted for user bob",
        "payment timeout charlie",
        "payment success delta",
    ]
    for m in messages:
        await idx1.add_document(_mk_entry(m))
    assert idx1.stats()["disk_segments"] >= 1

    # Fresh index pointed at the same disk dir; load and check.
    idx2 = _mk_index(tmp_path, segment_max_docs=2, max_memory_segments=0)
    await idx2.load_from_disk()
    s = idx2.stats()
    assert s["disk_segments"] >= 1
    assert s["docs_indexed"] == 4

    # Previously added docs are searchable.
    results = idx2.search("auth")
    assert len(results) == 2

    # Next allocation continues the monotone sequence.
    new_id = await idx2.add_document(_mk_entry("post-restart"))
    assert new_id == 5


# ---------------------------------------------------------------------------
# 14. Cross-segment dedup
# ---------------------------------------------------------------------------

async def test_search_dedupes_across_segments(tmp_path: Path) -> None:
    """Each matching doc_id appears at most once in the results."""
    # Force a multi-segment layout (current + flushed + disk).
    idx = _mk_index(tmp_path, segment_max_docs=2, max_memory_segments=1)
    for i in range(10):
        await idx.add_document(_mk_entry(f"needle {i}"))

    results = idx.search("needle")
    assert len(results) == len({r.doc_id for r in results})


# ---------------------------------------------------------------------------
# 15. on_new_document callback
# ---------------------------------------------------------------------------

async def test_on_new_document_callback(tmp_path: Path) -> None:
    """Every successful add fires the configured async callback once."""
    seen: list[int] = []

    async def cb(entry: LogEntry) -> None:
        seen.append(entry.doc_id)

    settings = _mk_settings(tmp_path)
    idx = InvertedIndex(
        settings=settings,
        tokenizer=LogTokenizer(),
        disk_dir=Path(settings.disk_segment_dir),
        on_new_document=cb,
    )
    for i in range(3):
        await idx.add_document(_mk_entry(f"event {i}"))

    # Tasks are fire-and-forget — give the loop a moment to drain.
    await asyncio.sleep(0.05)
    assert sorted(seen) == [1, 2, 3]


# ---------------------------------------------------------------------------
# 16. Concurrent adds
# ---------------------------------------------------------------------------

async def test_concurrent_adds_unique_doc_ids(tmp_path: Path) -> None:
    """Concurrent add_document calls receive strictly unique ids."""
    idx = _mk_index(tmp_path)

    async def _add(i: int) -> int:
        return await idx.add_document(_mk_entry(f"message {i}"))

    ids = await asyncio.gather(*[_add(i) for i in range(50)])
    assert sorted(ids) == list(range(1, 51))
    assert len(set(ids)) == 50


# ---------------------------------------------------------------------------
# 17. Vocab size
# ---------------------------------------------------------------------------

async def test_stats_vocab_size(tmp_path: Path) -> None:
    """vocab_size equals the number of unique tokens seen across tiers."""
    idx = _mk_index(tmp_path)
    # Use a disabled stop-word set so we get predictable token counts.
    idx._tokenizer = LogTokenizer(stop_words=set(), min_term_len=1)

    await idx.add_document(_mk_entry("alpha beta gamma"))
    await idx.add_document(_mk_entry("beta delta"))

    # Unique terms: {alpha, beta, gamma, delta} -> 4.
    assert idx.stats()["vocab_size"] == 4


# ---------------------------------------------------------------------------
# 18. Errors counter
# ---------------------------------------------------------------------------

async def test_errors_counter_increments_on_tokenize_failure(tmp_path: Path) -> None:
    """A tokenizer that raises bumps _errors and re-raises."""
    idx = _mk_index(tmp_path)

    class _BoomTokenizer:
        def tokenize(self, _text: str) -> list[str]:
            raise RuntimeError("boom")

    idx._tokenizer = _BoomTokenizer()  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="boom"):
        await idx.add_document(_mk_entry("will explode"))

    assert idx.stats()["errors"] == 1


# ---------------------------------------------------------------------------
# Bonus: bulk add semantics (cheap, covers the batch path)
# ---------------------------------------------------------------------------

async def test_bulk_add_assigns_contiguous_ids(tmp_path: Path) -> None:
    """add_documents_bulk returns contiguous ids and is atomic-ish under lock."""
    idx = _mk_index(tmp_path)
    entries = [_mk_entry(f"bulk {i}") for i in range(4)]
    ids = await idx.add_documents_bulk(entries)
    assert ids == [1, 2, 3, 4]
    assert idx.stats()["docs_indexed"] == 4


async def test_bulk_add_empty_is_noop(tmp_path: Path) -> None:
    """Empty bulk add returns [] and doesn't mutate state."""
    idx = _mk_index(tmp_path)
    assert await idx.add_documents_bulk([]) == []
    assert idx.stats()["docs_indexed"] == 0


async def test_flush_current_helper_rotates(tmp_path: Path) -> None:
    """flush_current() forces rotation even when thresholds aren't hit."""
    idx = _mk_index(tmp_path, segment_max_docs=1000, max_memory_segments=5)
    await idx.add_document(_mk_entry("first"))
    flushed = await idx.flush_current()
    assert flushed is not None
    s = idx.stats()
    assert s["current_segment_docs"] == 0
    assert s["flushed_memory_segments"] == 1


async def test_flush_current_noop_when_empty(tmp_path: Path) -> None:
    """flush_current() on an empty current returns None."""
    idx = _mk_index(tmp_path)
    assert await idx.flush_current() is None


async def test_flush_all_to_disk_spills_everything(tmp_path: Path) -> None:
    """flush_all_to_disk drains current + flushed_memory to disk.

    Simulates the shutdown path: a small segment_max_docs forces many
    rotations into the memory FIFO, and max_memory_segments keeps a
    few segments in RAM that ``flush_current`` would leave behind.
    After ``flush_all_to_disk``, the memory queue must be empty and
    disk_segments must reflect every document ever written.
    """
    # Small per-segment cap + memory FIFO slack so the in-memory queue
    # is non-trivially full when we invoke the flush.
    idx = _mk_index(tmp_path, segment_max_docs=5, max_memory_segments=3)

    for i in range(50):
        await idx.add_document(_mk_entry(f"row {i}"))

    # Preconditions: some segments have spilled already (because the
    # FIFO overflowed), but the memory queue still holds some and the
    # current segment is non-empty.
    pre = idx.stats()
    assert pre["docs_indexed"] == 50
    assert pre["flushed_memory_segments"] > 0, (
        "test setup invalid: nothing left in memory FIFO to spill"
    )
    pre_disk = pre["disk_segments"]

    await idx.flush_all_to_disk()

    post = idx.stats()
    # Every memory-resident segment must now be on disk; the current
    # segment must be empty (rotated then spilled).
    assert post["current_segment_docs"] == 0
    assert post["flushed_memory_segments"] == 0
    assert post["disk_segments"] > pre_disk
    assert post["docs_indexed"] == 50

    # Search must still return all 50 matches — the on-disk caches
    # were primed during the spill, so everything is reachable.
    results = idx.search("row", limit=100)
    assert len(results) == 50


async def test_flush_all_to_disk_is_noop_when_empty(tmp_path: Path) -> None:
    """flush_all_to_disk on a brand-new index does not create disk segments."""
    idx = _mk_index(tmp_path)
    await idx.flush_all_to_disk()
    s = idx.stats()
    assert s["current_segment_docs"] == 0
    assert s["flushed_memory_segments"] == 0
    assert s["disk_segments"] == 0
