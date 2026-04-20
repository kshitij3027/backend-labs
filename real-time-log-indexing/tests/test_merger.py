"""Unit tests for the background segment merger.

The merger operates entirely on disk-resident segments, so these tests
build a real :class:`InvertedIndex` against a tmp segment directory and
exercise ``_compact_once`` / ``merge_loop`` directly. ``pytest.ini`` sets
``asyncio_mode = auto`` so async tests don't need explicit decoration.

Covered scenarios
-----------------
* Pure merge primitive produces the correct union of postings/entries.
* ``_compact_once`` is a no-op when there are fewer than two disk
  segments (no crash, no file churn).
* With >= 2 disk segments, ``_compact_once`` merges the two oldest into
  a single new segment file and searches still find the merged docs.
* A corrupt source segment is quarantined (renamed with ``.corrupt``)
  and its meta is dropped so the merger doesn't loop on it.
* ``merge_loop`` honours ``stop_event`` promptly.
* Concurrent searches during a merge observe a consistent view — they
  never see zero results mid-swap.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from src.config import Settings
from src.index import persistence
from src.index.inverted_index import InvertedIndex
from src.index.merger import _compact_once, _quarantine, merge_loop, merge_segments
from src.index.segment import Segment
from src.index.tokenizer import LogTokenizer
from src.models import LogEntry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_entry(doc_id: int, msg: str = "hello error") -> LogEntry:
    """Build a LogEntry. ``doc_id`` is just a placeholder — the index
    rewrites it on insertion, but Segment.add() still requires one."""
    return LogEntry(
        doc_id=doc_id,
        message=msg,
        timestamp=time.time(),
        service="svc",
        level="INFO",
    )


def _mk_settings(tmp_path: Path, **overrides) -> Settings:
    """Fresh Settings per test; avoids mutating the module singleton."""
    base = {"disk_segment_dir": str(tmp_path)}
    base.update(overrides)
    return Settings(**base)


def _mk_index(tmp_path: Path, **overrides) -> InvertedIndex:
    settings = _mk_settings(tmp_path, **overrides)
    return InvertedIndex(
        settings=settings,
        tokenizer=LogTokenizer(),
        disk_dir=Path(settings.disk_segment_dir),
    )


# ---------------------------------------------------------------------------
# Pure merge primitive
# ---------------------------------------------------------------------------

def test_merge_segments_union(tmp_path: Path) -> None:
    """Two disjoint segments merge into one with the union of their docs.

    We use four docs split across two segments, each with a distinctive
    term mix, so the resulting posting lists are easy to assert on.
    """
    s1 = Segment("seg-a")
    s2 = Segment("seg-b")
    s1.add(1, _make_entry(1), ["alpha"])
    s1.add(2, _make_entry(2), ["alpha", "beta"])
    s2.add(3, _make_entry(3), ["beta", "gamma"])
    s2.add(4, _make_entry(4), ["gamma"])

    merged = merge_segments(s1, s2, "seg-merged")

    assert merged.segment_id == "seg-merged"
    assert merged.doc_count() == 4
    assert merged.search_term("alpha") == [1, 2]
    assert merged.search_term("beta") == [2, 3]
    assert merged.search_term("gamma") == [3, 4]
    assert merged.min_doc_id == 1
    assert merged.max_doc_id == 4


def test_merge_segments_right_then_left_order(tmp_path: Path) -> None:
    """Sources given in arbitrary order still produce doc-id-sorted output."""
    left = Segment("seg-left")
    right = Segment("seg-right")
    # Right's ids are actually smaller than left's — merge must still
    # walk them in ascending order.
    right.add(1, _make_entry(1), ["x"])
    right.add(2, _make_entry(2), ["x"])
    left.add(3, _make_entry(3), ["x"])
    left.add(4, _make_entry(4), ["x"])

    merged = merge_segments(left, right, "seg-out")

    assert merged.search_term("x") == [1, 2, 3, 4]


# ---------------------------------------------------------------------------
# _compact_once behaviour
# ---------------------------------------------------------------------------

async def test_compact_once_noop_when_fewer_than_two_disk_segments(
    tmp_path: Path,
) -> None:
    """Empty index -> no work, no crash."""
    index = _mk_index(tmp_path)
    await _compact_once(index)
    assert len(index._disk_segments) == 0


async def test_compact_once_merges_two_oldest(tmp_path: Path) -> None:
    """With >=2 disk segments, one compaction step produces one fewer file.

    We force ``MAX_MEMORY_SEGMENTS=0`` so every flush spills immediately
    to disk, and set ``SEGMENT_MAX_DOCS=2`` so two adds trigger a flush.
    Six adds therefore produce three disk segments. One compaction merges
    the two oldest into one, leaving two total.
    """
    index = _mk_index(
        tmp_path,
        segment_max_docs=2,
        max_memory_segments=0,
    )
    for i in range(6):
        await index.add_document(_make_entry(0, f"term{i} common"))
    # Force last flush so current is empty and everything is on disk.
    await index.flush_current()
    assert len(index._disk_segments) >= 3, (
        f"expected >=3 disk segments, got {len(index._disk_segments)}"
    )

    before_count = len(index._disk_segments)
    before_files = len(list(tmp_path.glob("seg-*.jsonl.gz")))

    await _compact_once(index)

    after_count = len(index._disk_segments)
    after_files = len(list(tmp_path.glob("seg-*.jsonl.gz")))
    assert after_count == before_count - 1
    assert after_files == before_files - 1

    # Searches still find the common term across every surviving segment.
    results = index.search("common", limit=10)
    assert len(results) >= 3


async def test_compact_once_preserves_all_docs(tmp_path: Path) -> None:
    """Sum of doc_counts after merge matches sum before merge."""
    index = _mk_index(
        tmp_path,
        segment_max_docs=2,
        max_memory_segments=0,
    )
    for i in range(4):
        await index.add_document(_make_entry(0, f"token{i} common"))
    await index.flush_current()
    assert len(index._disk_segments) >= 2

    before_docs = sum(m.doc_count for m in index._disk_segments)
    await _compact_once(index)
    after_docs = sum(m.doc_count for m in index._disk_segments)
    assert before_docs == after_docs


# ---------------------------------------------------------------------------
# Corruption quarantine
# ---------------------------------------------------------------------------

async def test_merger_corrupt_source_quarantined(tmp_path: Path) -> None:
    """A source that fails to read is renamed with ``.corrupt`` and its
    meta is dropped from ``_disk_segments`` so the next pass picks up a
    different pair."""
    index = _mk_index(
        tmp_path,
        segment_max_docs=2,
        max_memory_segments=0,
    )
    for i in range(4):
        await index.add_document(_make_entry(0, f"msg{i}"))
    await index.flush_current()
    assert len(index._disk_segments) >= 2

    # Corrupt the oldest file — overwrite with non-gzip bytes so
    # ``read_segment`` raises.
    oldest = sorted(index._disk_segments, key=lambda m: m.created_at)[0]
    with open(oldest.path, "wb") as f:
        f.write(b"not a gzip")

    await _compact_once(index)

    # Quarantine outcomes: the original file is gone (or the .corrupt
    # sibling exists), and the meta is removed.
    corrupt_path = oldest.path.with_suffix(oldest.path.suffix + ".corrupt")
    assert (not oldest.path.exists()) or corrupt_path.exists()
    assert all(m.segment_id != oldest.segment_id for m in index._disk_segments)


async def test_quarantine_drops_meta_even_if_rename_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the rename throws, we still drop the bad meta so we don't loop."""
    index = _mk_index(tmp_path, segment_max_docs=2, max_memory_segments=0)
    await index.add_document(_make_entry(0, "msg"))
    await index.add_document(_make_entry(0, "msg2"))
    await index.flush_current()
    assert len(index._disk_segments) >= 1
    bad = index._disk_segments[0]

    # Force the rename to explode — Path.rename will raise.
    def _boom(*_a, **_kw):
        raise OSError("rename not allowed")

    monkeypatch.setattr(Path, "rename", _boom)

    await _quarantine(index, bad)
    assert all(m.segment_id != bad.segment_id for m in index._disk_segments)


# ---------------------------------------------------------------------------
# merge_loop lifecycle
# ---------------------------------------------------------------------------

async def test_merge_loop_respects_stop_event(tmp_path: Path) -> None:
    """Setting stop_event exits the loop promptly (well under 1 s)."""
    index = _mk_index(tmp_path)
    stop = asyncio.Event()
    task = asyncio.create_task(merge_loop(index, stop, interval=0.1))
    # Let a couple of ticks happen so we exercise the loop path, not just
    # the first ``wait_for``.
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(task, timeout=1.0)
    assert task.done()


async def test_merge_loop_swallows_compact_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An exception raised inside _compact_once must not kill the loop."""
    from src.index import merger as merger_module

    calls = {"n": 0}

    async def _flaky(_index):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")

    monkeypatch.setattr(merger_module, "_compact_once", _flaky)

    index = _mk_index(tmp_path)
    stop = asyncio.Event()
    task = asyncio.create_task(merge_loop(index, stop, interval=0.05))
    await asyncio.sleep(0.2)  # multiple ticks
    stop.set()
    await asyncio.wait_for(task, timeout=1.0)
    assert calls["n"] >= 2  # proved the loop kept running past the crash


# ---------------------------------------------------------------------------
# Concurrent search during a merge
# ---------------------------------------------------------------------------

async def _search_many(index: InvertedIndex, term: str, count: int) -> list[int]:
    """Run ``count`` searches interleaved with the caller's work."""
    out: list[int] = []
    for _ in range(count):
        out.append(len(index.search(term, limit=50)))
        await asyncio.sleep(0.01)
    return out


async def test_concurrent_search_during_merge_is_consistent(
    tmp_path: Path,
) -> None:
    """Searches running during ``_compact_once`` never see zero matches
    for a term that's present in every segment — i.e. the meta swap is
    atomic from the reader's point of view.
    """
    index = _mk_index(
        tmp_path,
        segment_max_docs=4,
        max_memory_segments=0,
    )
    for i in range(20):
        await index.add_document(_make_entry(0, f"token{i} common word"))
    await index.flush_current()
    assert len(index._disk_segments) >= 2

    search_task = asyncio.create_task(
        _search_many(index, "common", count=20)
    )
    await _compact_once(index)
    hits = await search_task

    # Every search, whether before / during / after the merge, must find
    # at least one doc with "common" — zero would mean the reader saw a
    # partial state mid-swap.
    assert all(h > 0 for h in hits), f"inconsistent reads: {hits}"
