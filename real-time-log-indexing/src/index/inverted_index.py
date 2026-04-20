"""Orchestrator for the hybrid inverted index.

This module owns the end-to-end write/read path:

* **Write path**: assign a monotonic ``doc_id``, tokenize the incoming
  :class:`~src.models.LogEntry`, append it to the *current* in-memory
  :class:`~src.index.segment.Segment`, and — once that segment crosses
  the doc-count or memory threshold — rotate it into a bounded
  in-memory FIFO of flushed segments. If the FIFO overflows, the
  oldest flushed segment is spilled to disk via the persistence layer.
* **Read path**: tokenize the query, fan out across every tier
  (``current`` + ``_flushed_memory`` + ``_disk_segments``), AND-intersect
  posting lists within each segment, dedupe matched doc_ids globally,
  apply service/level filters, sort newest-first, and return a bounded
  list of :class:`~src.models.SearchResult`.

Concurrency model
-----------------

All mutative APIs run under ``_write_lock`` so the segment state is
only touched by one coroutine at a time. :meth:`search` is *sync* and
reads from the same maps without locking — that's safe because the
writer only ever **appends** to posting lists (never re-sorts) and
only **adds** keys to dicts (never mutates existing values during
search). Under Python's GIL plus the asyncio single-threaded event
loop, a search walk cannot observe a torn write.

The ``_flushing`` lock is declared but unused in this commit; it's a
hook for the background merger added in Commit 12.

Startup
-------

On startup, :meth:`load_from_disk` rehydrates every finalised disk
segment found in ``_disk_dir``. We eagerly load the full doc bodies
into each :class:`SegmentMeta`'s ``_docs_cache`` — for the MVP that
keeps search <50 ms with zero additional I/O in the warm path. The
``SegmentMeta.docs`` helper is deliberately shaped like a lazy loader
so a later commit can swap in on-demand loading without changing the
search API.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Awaitable, Callable

from src.config import Settings
from src.index import persistence
from src.index.segment import Segment
from src.index.tokenizer import LogTokenizer
from src.models import LogEntry, SearchResult


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SegmentMeta — lightweight handle to a disk-resident segment
# ---------------------------------------------------------------------------

@dataclass
class SegmentMeta:
    """Lightweight metadata for a disk-resident segment.

    We hold the ``term -> doc_ids`` map in RAM so search fan-out stays
    sub-millisecond; only the full :class:`LogEntry` body is lazy-loaded
    from disk when a search hits this segment. For the simple JSONL
    format, lazy-loading means re-reading the file and picking out the
    requested doc_ids — acceptable because search hits on disk segments
    are rare in the warm path (most queries hit current / flushed
    memory).

    In this commit ``load_from_disk`` eagerly populates ``_docs_cache``
    so search always finds everything without extra I/O. That keeps the
    MVP simple while leaving the lazy-loader shape in place for a
    future optimisation.
    """

    segment_id: str
    path: Path
    min_doc_id: int | None
    max_doc_id: int | None
    term_postings: dict[str, list[int]]
    doc_count: int
    created_at: float
    _docs_cache: dict[int, LogEntry] | None = field(default=None, repr=False)

    def docs(self) -> dict[int, LogEntry]:
        """Return the full doc_id -> LogEntry map, loading from disk if needed.

        On the first call with no cache we re-read the segment from
        disk via :func:`persistence.read_segment` and memoise the
        result. Subsequent calls are O(1). In this commit the cache is
        usually primed eagerly by :meth:`InvertedIndex.load_from_disk`.
        """
        if self._docs_cache is not None:
            return self._docs_cache
        seg = persistence.read_segment(self.path)
        self._docs_cache = dict(seg.doc_entries)
        return self._docs_cache


# ---------------------------------------------------------------------------
# InvertedIndex orchestrator
# ---------------------------------------------------------------------------

class InvertedIndex:
    """The hybrid in-memory + on-disk inverted index.

    Exposes a minimal public API:

    * :meth:`load_from_disk` — called once at startup to rehydrate
      any segments left over from the previous run.
    * :meth:`add_document` / :meth:`add_documents_bulk` — write path.
    * :meth:`search` — read path (sync, no lock).
    * :meth:`stats` — counters for ``/api/stats``.
    * :meth:`flush_current` — test helper to force a rotation.

    ``on_new_document`` is an optional async callback (used by the
    WebSocket layer in Commit 11). When set, every successfully added
    document fires a fire-and-forget task so writes don't block on
    slow WS consumers.
    """

    def __init__(
        self,
        settings: Settings,
        tokenizer: LogTokenizer,
        disk_dir: Path | None = None,
        on_new_document: Callable[[LogEntry], Awaitable[None]] | None = None,
    ) -> None:
        self._settings = settings
        self._tokenizer = tokenizer
        self._disk_dir = (
            Path(disk_dir) if disk_dir else Path(settings.disk_segment_dir)
        )
        self._on_new_document = on_new_document

        # Tiered segment state.
        self._current_counter: int = 0
        self._current: Segment = Segment(segment_id=f"current-{self._current_counter}")
        self._flushed_memory: deque[Segment] = deque()
        self._disk_segments: list[SegmentMeta] = []

        # Monotone id allocator. Starts at 1; bumped past the largest
        # id on disk inside ``load_from_disk``.
        self._next_doc_id: int = 1

        # Locks. ``_flushing`` is a placeholder for Commit 12.
        self._write_lock: asyncio.Lock = asyncio.Lock()
        self._flushing: asyncio.Lock = asyncio.Lock()

        # Counters surfaced by :meth:`stats`.
        self._docs_indexed_total: int = 0
        self._errors: int = 0

        # Convert the MB-denominated setting to bytes once so the hot
        # path compares integers.
        self._max_bytes: int = settings.segment_max_memory_mb * 1024 * 1024

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def load_from_disk(self) -> None:
        """Rehydrate on-disk segments so search survives a restart.

        Lists every finalised ``seg-*.jsonl.gz`` in ``_disk_dir``,
        reads each one fully, and captures:

        * the term -> doc_ids map (always in RAM),
        * the doc_id -> LogEntry map (eagerly cached in this commit),
        * min/max/created_at metadata for the merger.

        Also bumps ``_next_doc_id`` past the largest id seen, so new
        writes continue the monotone sequence across restarts.

        Corrupt segments are logged and skipped — the checksum
        mismatch is raised as ``ValueError`` by
        :func:`persistence.read_segment`.
        """
        files = persistence.list_segment_files(self._disk_dir)
        if not files:
            logger.info("no existing segments in %s", self._disk_dir)
            return

        max_doc_id_seen = 0
        total_docs = 0
        for path in files:
            try:
                seg = persistence.read_segment(path)
            except Exception as exc:  # noqa: BLE001 — broad on-disk failures
                logger.error(
                    "failed to load segment %s: %s; skipping", path, exc
                )
                continue

            meta = SegmentMeta(
                segment_id=seg.segment_id,
                path=path,
                min_doc_id=seg.min_doc_id,
                max_doc_id=seg.max_doc_id,
                term_postings=dict(seg.term_postings),
                doc_count=seg.doc_count(),
                created_at=seg.created_at,
                _docs_cache=dict(seg.doc_entries),
            )
            self._disk_segments.append(meta)
            total_docs += meta.doc_count
            if meta.max_doc_id is not None and meta.max_doc_id > max_doc_id_seen:
                max_doc_id_seen = meta.max_doc_id

        self._next_doc_id = max_doc_id_seen + 1
        self._docs_indexed_total += total_docs
        logger.info(
            "loaded %d segments (%d docs); next_doc_id=%d",
            len(self._disk_segments),
            total_docs,
            self._next_doc_id,
        )

    async def flush_current(self) -> Segment | None:
        """Force the current segment out even if it hasn't hit thresholds.

        Returns the flushed segment (now at the tail of
        ``_flushed_memory`` or already spilled to disk) so tests and
        shutdown hooks can assert on it. Returns ``None`` if the
        current segment was empty.
        """
        async with self._write_lock:
            if self._current.doc_count() == 0:
                return None
            flushed = self._current
            await self._rotate_current_to_memory_queue()
            # After rotation, the old current sits at the tail of the
            # memory queue; spill if we exceeded the bound.
            if len(self._flushed_memory) > self._settings.max_memory_segments:
                await self._spill_oldest_memory_to_disk()
            return flushed

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    async def add_document(self, entry: LogEntry) -> int:
        """Index a single document and return its assigned doc_id.

        Steps (all under ``_write_lock``):

        1. Allocate the next doc_id.
        2. Tokenize the message.
        3. Stamp the doc_id on a copy of the entry.
        4. Insert into the current segment.
        5. Maybe rotate + spill.
        6. Fire the optional ``on_new_document`` callback
           (fire-and-forget so a slow WS consumer can't stall ingest).

        Errors bump ``_errors`` and re-raise — the caller (Redis
        consumer) is expected to XACK and move on.
        """
        async with self._write_lock:
            doc_id = self._next_doc_id
            try:
                stamped = entry.model_copy(update={"doc_id": doc_id})
                terms = self._tokenizer.tokenize(stamped.message)
                self._current.add(doc_id, stamped, terms)
                self._next_doc_id += 1
                self._docs_indexed_total += 1
                await self._maybe_flush_locked()
            except Exception:
                self._errors += 1
                raise

        # Notify *after* releasing the write lock so a slow callback
        # can never block the next writer.
        if self._on_new_document is not None:
            try:
                asyncio.create_task(self._on_new_document(stamped))
            except Exception as exc:  # noqa: BLE001 — don't let WS break ingest
                logger.warning("on_new_document scheduling failed: %s", exc)

        return doc_id

    async def add_documents_bulk(self, entries: list[LogEntry]) -> list[int]:
        """Index a batch of documents under a single lock acquisition.

        The lock is held for the whole batch so ids are strictly
        increasing and no other writer can interleave. Threshold
        checks run after *each* add so a 100 k-entry batch still
        rotates/spills at the right boundaries — the alternative
        (check once at the end) would let one "current" segment blow
        past the configured max_docs.

        Callback notifications are scheduled *after* the lock is
        released, one task per accepted document, matching the
        single-doc path.
        """
        if not entries:
            return []

        assigned: list[int] = []
        stamped_entries: list[LogEntry] = []
        async with self._write_lock:
            for entry in entries:
                doc_id = self._next_doc_id
                try:
                    stamped = entry.model_copy(update={"doc_id": doc_id})
                    terms = self._tokenizer.tokenize(stamped.message)
                    self._current.add(doc_id, stamped, terms)
                    self._next_doc_id += 1
                    self._docs_indexed_total += 1
                    assigned.append(doc_id)
                    stamped_entries.append(stamped)
                    await self._maybe_flush_locked()
                except Exception:
                    self._errors += 1
                    raise

        if self._on_new_document is not None:
            for stamped in stamped_entries:
                try:
                    asyncio.create_task(self._on_new_document(stamped))
                except Exception as exc:  # noqa: BLE001
                    logger.warning("on_new_document scheduling failed: %s", exc)

        return assigned

    # ------------------------------------------------------------------
    # Internal flush / spill helpers — callers must hold _write_lock.
    # ------------------------------------------------------------------

    async def _maybe_flush_locked(self) -> None:
        """Rotate + spill if the current segment is at/over its limits.

        ``max_memory_segments`` is interpreted as an *inclusive* cap:
        the queue may hold up to that many flushed segments. We spill
        only when a rotation would push it strictly past the cap.
        That lets ``MAX_MEMORY_SEGMENTS=0`` act as "every flush spills
        immediately" without introducing a special case.
        """
        if not self._current.is_full(
            self._settings.segment_max_docs, self._max_bytes
        ):
            return
        await self._rotate_current_to_memory_queue()
        if len(self._flushed_memory) > self._settings.max_memory_segments:
            await self._spill_oldest_memory_to_disk()

    async def _rotate_current_to_memory_queue(self) -> None:
        """Move the current segment onto the in-memory FIFO and start a new one.

        The segment is queued under its existing ``current-N`` id; it
        only gets renamed to ``seg-NNNNNN`` when (and if) it later
        spills to disk.
        """
        self._flushed_memory.append(self._current)
        self._current_counter += 1
        self._current = Segment(
            segment_id=f"current-{self._current_counter}"
        )

    async def _spill_oldest_memory_to_disk(self) -> None:
        """Persist the oldest in-memory flushed segment and drop it from RAM.

        The segment is renamed to the next monotonic ``seg-NNNNNN`` id
        right before the write, and a fresh ``created_at`` stamp is
        applied so merger ordering reflects the spill time, not the
        original memtable creation time.

        Writing goes through ``asyncio.to_thread`` so the event loop
        isn't blocked on gzip / disk I/O.
        """
        if not self._flushed_memory:
            return
        old = self._flushed_memory.popleft()
        disk_id = persistence.next_segment_id(self._disk_dir)
        old.segment_id = disk_id
        old.created_at = time.time()

        path = await asyncio.to_thread(
            persistence.write_segment, self._disk_dir, old
        )

        meta = SegmentMeta(
            segment_id=old.segment_id,
            path=path,
            min_doc_id=old.min_doc_id,
            max_doc_id=old.max_doc_id,
            term_postings=dict(old.term_postings),
            doc_count=old.doc_count(),
            created_at=old.created_at,
            _docs_cache=dict(old.doc_entries),
        )
        self._disk_segments.append(meta)
        logger.debug(
            "spilled segment %s to %s (docs=%d)",
            old.segment_id,
            path,
            meta.doc_count,
        )

    # ------------------------------------------------------------------
    # Read path — sync, lock-free, reads append-only structures.
    # ------------------------------------------------------------------

    def search(
        self,
        q: str,
        service: str | None = None,
        level: str | None = None,
        limit: int = 50,
    ) -> list[SearchResult]:
        """Search every tier, dedupe, filter, sort newest-first.

        Within each segment, posting lists for every query term are
        intersected (AND semantics). Across segments, doc_ids are
        deduplicated globally — the first segment to claim a doc_id
        wins, and the search order is ``current`` -> flushed memory
        (newest first) -> disk segments (newest first), because newer
        segments are more likely to carry the authoritative copy.

        Returns up to ``limit`` :class:`SearchResult` instances sorted
        by ``timestamp`` descending, each with ``<mark>`` tags wrapped
        around matched terms in ``highlighted_message``.
        """
        query_terms = self._tokenizer.tokenize(q)
        if not query_terms:
            return []

        # Fan-out order: current (freshest), then flushed memory
        # (newest first), then disk segments (newest first). This
        # ordering means first-seen wins during dedup, which is the
        # right semantics if the same doc_id somehow appears twice —
        # the freshest copy (newer segment) is preferred.
        ordered_sources: list[Segment | SegmentMeta] = [self._current]
        ordered_sources.extend(reversed(self._flushed_memory))
        ordered_sources.extend(reversed(self._disk_segments))

        # doc_id -> (segment_source, LogEntry)
        winners: dict[int, tuple[Segment | SegmentMeta, LogEntry]] = {}

        for src in ordered_sources:
            matched_ids = self._intersect_terms_in_segment(src, query_terms)
            if not matched_ids:
                continue
            docs_map = self._docs_map_of(src)
            for doc_id in matched_ids:
                if doc_id in winners:
                    continue
                entry = docs_map.get(doc_id)
                if entry is None:
                    # Posting claimed this id but the body isn't there;
                    # shouldn't happen in practice. Skip defensively.
                    continue
                winners[doc_id] = (src, entry)

        # Apply post-filters (service / level) before sorting so we
        # don't waste work on entries that would be discarded anyway.
        filtered: list[LogEntry] = []
        for _src, entry in winners.values():
            if service is not None and entry.service != service:
                continue
            if level is not None and entry.level != level:
                continue
            filtered.append(entry)

        # Newest first. ``reverse=True`` on timestamp is cheaper than
        # negating and also handles floats with mixed resolutions.
        filtered.sort(key=lambda e: e.timestamp, reverse=True)
        top = filtered[:limit]

        # Build the single regex once per search so per-result
        # substitution is fast.
        mark_re = self._compile_highlight_regex(query_terms)

        return [
            SearchResult(
                doc_id=e.doc_id,
                message=e.message,
                highlighted_message=mark_re.sub(
                    lambda m: f"<mark>{m.group(0)}</mark>", e.message
                ),
                timestamp=e.timestamp,
                service=e.service,
                level=e.level,
                score=1.0,
            )
            for e in top
        ]

    # ------------------------------------------------------------------
    # Search helpers (private)
    # ------------------------------------------------------------------

    @staticmethod
    def _intersect_terms_in_segment(
        src: Segment | SegmentMeta, query_terms: list[str]
    ) -> set[int]:
        """AND-intersect every query term's posting list inside one segment.

        Returns an empty set if any term is missing — the AND
        semantics mean a single miss eliminates the whole segment from
        consideration.
        """
        first = True
        acc: set[int] = set()
        for term in query_terms:
            if isinstance(src, Segment):
                bucket = src.term_postings.get(term)
            else:
                bucket = src.term_postings.get(term)
            if not bucket:
                return set()
            ids = set(bucket)
            if first:
                acc = ids
                first = False
            else:
                acc &= ids
                if not acc:
                    return set()
        return acc

    @staticmethod
    def _docs_map_of(src: Segment | SegmentMeta) -> dict[int, LogEntry]:
        """Return the ``doc_id -> LogEntry`` map for a segment source."""
        if isinstance(src, Segment):
            return src.doc_entries
        return src.docs()

    @staticmethod
    def _compile_highlight_regex(query_terms: list[str]) -> re.Pattern[str]:
        """Build a single case-insensitive regex over all query terms.

        One regex + one ``sub`` call is far cheaper than looping once
        per term — especially since tokenizer output can expand a URL
        or IP query into half a dozen sub-tokens.
        """
        escaped = sorted((re.escape(t) for t in query_terms), key=len, reverse=True)
        pattern = "|".join(escaped)
        return re.compile(pattern, re.IGNORECASE)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        """Return indexer-internal counters for :class:`StatsResponse`.

        The API layer fills in outside-of-index fields
        (``throughput_1m``, ``ingest_lag``, ``query_p95_ms``,
        ``uptime_s``) — those depend on the Redis consumer / request
        timers which this module deliberately knows nothing about.
        """
        # Unique terms across all tiers. Using a set union is O(V)
        # rather than summing term_count() per segment (which would
        # overcount shared terms).
        vocab: set[str] = set()
        vocab.update(self._current.term_postings.keys())
        for seg in self._flushed_memory:
            vocab.update(seg.term_postings.keys())
        for meta in self._disk_segments:
            vocab.update(meta.term_postings.keys())

        memory_bytes = self._current.memory_bytes()
        for seg in self._flushed_memory:
            memory_bytes += seg.memory_bytes()

        return {
            "docs_indexed": self._docs_indexed_total,
            "current_segment_docs": self._current.doc_count(),
            "flushed_memory_segments": len(self._flushed_memory),
            "disk_segments": len(self._disk_segments),
            "vocab_size": len(vocab),
            "memory_bytes": memory_bytes,
            "errors": self._errors,
        }
