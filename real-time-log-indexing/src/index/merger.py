"""Background segment merger.

Periodically compacts on-disk segments to avoid fragmentation. The algorithm
is straightforward size-tiered merging:

1. Pick the two oldest disk segments (lowest ``created_at``).
2. Read both into memory.
3. Build a new Segment whose doc_ids/postings/entries are the union of the
   two sources (in strict doc_id order, since source segments already have
   monotone doc_ids and no overlap).
4. Write the new segment to disk via ``persistence.write_segment``.
5. Atomically swap the :class:`InvertedIndex._disk_segments` list: remove
   the two merged metas, append the new one (under ``_write_lock`` so
   searches see a consistent view).
6. Delete the two source files.
7. Sleep until the next interval (or ``stop_event``).

Correctness notes
-----------------
* Merging touches only on-disk segments; live ingest is unaffected since
  new docs land in the current in-memory segment.
* The swap is atomic under ``_write_lock``; concurrent search will see
  either the pre-merge or post-merge layout, never a partial state.
* Corrupt source segments raise :class:`ValueError` from
  :func:`persistence.read_segment`; the merger logs, quarantines the file
  (renames it with a ``.corrupt`` suffix) and drops the meta so the next
  pass picks up a healthy pair instead.
"""

from __future__ import annotations

import asyncio
import logging

from src.index import persistence
from src.index.inverted_index import InvertedIndex, SegmentMeta
from src.index.segment import Segment


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pure merge primitive
# ---------------------------------------------------------------------------

def merge_segments(left: Segment, right: Segment, new_segment_id: str) -> Segment:
    """Merge two segments into one.

    Sources are assumed to have disjoint, monotone doc_ids (which is true
    for every segment produced by this pipeline — ids are assigned from
    a single monotone counter and each segment covers a contiguous range).
    We walk the two sources in ascending doc_id order and re-``add`` every
    doc into a fresh segment; this rebuilds the posting lists from scratch,
    preserving sort order without any clever splicing.
    """
    merged = Segment(segment_id=new_segment_id)

    left_docs = list(left.iter_docs())
    right_docs = list(right.iter_docs())
    i = j = 0
    while i < len(left_docs) and j < len(right_docs):
        if left_docs[i][0] <= right_docs[j][0]:
            doc_id, entry, terms = left_docs[i]
            i += 1
        else:
            doc_id, entry, terms = right_docs[j]
            j += 1
        merged.add(doc_id, entry, terms)
    while i < len(left_docs):
        doc_id, entry, terms = left_docs[i]
        i += 1
        merged.add(doc_id, entry, terms)
    while j < len(right_docs):
        doc_id, entry, terms = right_docs[j]
        j += 1
        merged.add(doc_id, entry, terms)

    return merged


# ---------------------------------------------------------------------------
# Merge loop
# ---------------------------------------------------------------------------

async def merge_loop(
    index: InvertedIndex, stop_event: asyncio.Event, interval: float
) -> None:
    """Every ``interval`` seconds, if >= 2 disk segments, merge the two oldest.

    The loop is driven by an interruptible sleep: ``asyncio.wait_for`` on
    ``stop_event.wait()`` returns as soon as the event is set (normal
    shutdown) or raises :class:`asyncio.TimeoutError` after the interval
    elapses (next compaction tick). Any exception raised by
    :func:`_compact_once` is logged and swallowed so one bad pass can't
    take down the merger for the lifetime of the process.
    """
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
            break  # stop_event set — unwind cleanly
        except asyncio.TimeoutError:
            pass
        try:
            await _compact_once(index)
        except Exception as exc:  # noqa: BLE001 — never kill the loop
            logger.exception("merger compaction error: %s", exc)


# ---------------------------------------------------------------------------
# Single compaction pass
# ---------------------------------------------------------------------------

async def _compact_once(index: InvertedIndex) -> None:
    """Run one compaction step: merge the two oldest disk segments if any.

    Blocks on :class:`asyncio.Lock` only for the snapshot read and the
    final meta swap; the heavy I/O and CPU work runs outside the lock via
    :func:`asyncio.to_thread` so searches and ingest remain responsive.
    """
    # Snapshot the disk-segment list under the lock so concurrent spills
    # can't slip a new segment in while we're choosing the oldest pair.
    async with index._write_lock:
        segments = list(index._disk_segments)
    if len(segments) < 2:
        return

    # Choose the two oldest by created_at — that's the canonical LSM-style
    # tiering heuristic for a simple two-way merge policy.
    segments_sorted = sorted(segments, key=lambda m: m.created_at)
    left_meta, right_meta = segments_sorted[0], segments_sorted[1]
    logger.info(
        "compacting %s + %s", left_meta.segment_id, right_meta.segment_id
    )

    # Load both segments from disk. gzip decompression + JSON parsing is
    # blocking CPU work, so we hand it off to a worker thread.
    try:
        left = await asyncio.to_thread(persistence.read_segment, left_meta.path)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "corrupt source %s; quarantining (%s)", left_meta.path, exc
        )
        await _quarantine(index, left_meta)
        return
    try:
        right = await asyncio.to_thread(persistence.read_segment, right_meta.path)
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "corrupt source %s; quarantining (%s)", right_meta.path, exc
        )
        await _quarantine(index, right_meta)
        return

    # Allocate a new id and do the merge + write. Both are blocking and
    # potentially long for big segments, so they go through to_thread too.
    new_id = await asyncio.to_thread(
        persistence.next_segment_id, index._disk_dir
    )
    merged = await asyncio.to_thread(merge_segments, left, right, new_id)
    new_path = await asyncio.to_thread(
        persistence.write_segment, index._disk_dir, merged
    )

    # Build the replacement meta. We eagerly cache the doc bodies the same
    # way ``InvertedIndex.load_from_disk`` / ``_spill_oldest_memory_to_disk``
    # do, so search stays warm without a re-read.
    new_meta = SegmentMeta(
        segment_id=merged.segment_id,
        path=new_path,
        min_doc_id=merged.min_doc_id,
        max_doc_id=merged.max_doc_id,
        term_postings=dict(merged.term_postings),
        doc_count=merged.doc_count(),
        created_at=merged.created_at,
        _docs_cache=dict(merged.doc_entries),
    )

    # Atomic swap: drop the two source metas, append the new one. Holding
    # the write lock here means search threads either see the pre-merge
    # list (both sources present) or the post-merge list (new meta only).
    async with index._write_lock:
        index._disk_segments = [
            m
            for m in index._disk_segments
            if m.segment_id not in {left_meta.segment_id, right_meta.segment_id}
        ] + [new_meta]

    # Delete old files *after* the swap so a crash between the write and
    # the unlink leaves an orphan file (recoverable) rather than a dangling
    # meta pointing at a missing file (unrecoverable for search).
    await asyncio.to_thread(
        persistence.delete_segments, [left_meta.path, right_meta.path]
    )
    logger.info(
        "merged %s and %s -> %s (%d docs)",
        left_meta.segment_id,
        right_meta.segment_id,
        merged.segment_id,
        merged.doc_count(),
    )


# ---------------------------------------------------------------------------
# Corruption handling
# ---------------------------------------------------------------------------

async def _quarantine(index: InvertedIndex, meta: SegmentMeta) -> None:
    """Move a bad segment aside so the merger stops trying to merge it.

    Renames the file with a ``.corrupt`` suffix appended to the existing
    extension (e.g. ``seg-000003.jsonl.gz`` -> ``seg-000003.jsonl.gz.corrupt``)
    so an operator can inspect it later, and removes the meta from
    ``_disk_segments`` under the write lock so search and subsequent
    compaction passes pretend the segment was never there. Any failure in
    the rename itself is logged at ``warning`` — the meta drop still
    happens so we don't loop on the same corrupt file forever.
    """
    try:
        quarantined = meta.path.with_suffix(meta.path.suffix + ".corrupt")
        await asyncio.to_thread(meta.path.rename, quarantined)
    except Exception:  # noqa: BLE001 — best-effort rename
        logger.warning("quarantine rename failed for %s", meta.path)
    async with index._write_lock:
        index._disk_segments = [
            m for m in index._disk_segments if m.segment_id != meta.segment_id
        ]
