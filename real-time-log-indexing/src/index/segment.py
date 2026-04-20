"""In-memory segment — the memtable of the LSM-style inverted index.

A ``Segment`` is a single tier's worth of the inverted index held
entirely in RAM. It stores three parallel maps:

* ``term_postings`` — every term that appears in any document in this
  segment, pointing at the sorted list of doc_ids that contain it.
* ``doc_entries``   — the original :class:`LogEntry` keyed by doc_id,
  so search hits can be rehydrated into a response without extra I/O.
* ``doc_terms``     — per-document term list (deduped), useful during
  flush/merge when we need to rewrite the postings from scratch.

Segments are designed for **single-writer, many-reader** access. They
don't carry their own lock; :class:`InvertedIndex` serialises writes
behind an ``asyncio.Lock``. The pure-sync surface here keeps Segment
easy to unit-test in isolation.

Once flushed, a segment is treated as immutable by convention — the
API doesn't *prevent* mutation, but no caller should be adding more
documents once the segment is handed to the disk-writer or merger.

Memory accounting is deliberately approximate. The goal is to trigger
flushes within the right order of magnitude when payload sizes vary,
not to match actual Python object overhead byte-for-byte. Tests only
assert direction (monotonic growth) rather than exact values.
"""

from __future__ import annotations

from time import time
from typing import Iterator

from src.models import LogEntry


# Per-document and per-term overhead constants used by the memory
# estimator. These are rough approximations of Python object / dict
# bucket overhead; exact values aren't load-bearing for correctness.
_ENTRY_CONST_OVERHEAD: int = 64       # doc_id int, timestamp float, stream_id slot
_PER_TERM_POSTING_ENTRY: int = 16     # int in a list + pointer overhead
_NEW_TERM_BUCKET_OVERHEAD: int = 48   # amortised hash-bucket cost for a new term


class Segment:
    """An in-memory, append-only segment of the inverted index.

    Writes must come through :meth:`add` in monotonically increasing
    doc_id order — the segment relies on that invariant to keep each
    term's posting list sorted via plain ``append``. Reads
    (:meth:`search_term`, :meth:`doc_count`, :meth:`memory_bytes`)
    never mutate state, so they can safely race with a single writer.

    Attributes
    ----------
    segment_id:
        Stable identifier, e.g. ``"seg-000001"``. Echoed into the
        on-disk file name at flush time.
    created_at:
        Unix epoch seconds at construction — used by the merger when
        choosing the oldest segment to compact.
    min_doc_id / max_doc_id:
        Populated by the first and last :meth:`add` respectively.
        Both are ``None`` for an empty segment.
    term_postings:
        Maps a term to the sorted list of doc_ids that contain it.
    doc_entries:
        Maps a doc_id to its :class:`LogEntry`.
    doc_terms:
        Maps a doc_id to the deduped list of terms for that document.
    """

    def __init__(self, segment_id: str) -> None:
        """Construct an empty segment with the given id."""
        self.segment_id: str = segment_id
        self.created_at: float = time()
        self.min_doc_id: int | None = None
        self.max_doc_id: int | None = None
        self.term_postings: dict[str, list[int]] = {}
        self.doc_entries: dict[int, LogEntry] = {}
        self.doc_terms: dict[int, list[str]] = {}
        self._memory_bytes: int = 0

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def add(self, doc_id: int, entry: LogEntry, terms: list[str]) -> None:
        """Admit *entry* into the segment under *doc_id* with *terms*.

        Parameters
        ----------
        doc_id:
            Monotonically increasing document id assigned by the
            orchestrator. Must be strictly greater than the current
            ``max_doc_id`` if one exists.
        entry:
            The :class:`LogEntry` to store verbatim.
        terms:
            Pre-tokenized terms for this document. Duplicates within
            the list are collapsed (a document contributes to a term's
            posting list exactly once). An empty list is legal — the
            document will still be retrievable by doc_id.

        Raises
        ------
        ValueError:
            If *doc_id* has already been added, or if it is not
            greater than the current ``max_doc_id`` (we enforce strict
            monotone insertion so posting lists stay sorted without a
            re-sort step).
        """
        if doc_id in self.doc_entries:
            raise ValueError(f"duplicate doc_id: {doc_id}")

        if self.max_doc_id is not None and doc_id <= self.max_doc_id:
            raise ValueError("doc_ids must be monotonically increasing")

        # Dedupe terms while preserving first-seen order. Using an
        # explicit seen-set rather than ``dict.fromkeys`` makes the
        # intent obvious and avoids a second pass.
        seen: set[str] = set()
        deduped: list[str] = []
        for t in terms:
            if t in seen:
                continue
            seen.add(t)
            deduped.append(t)

        # Posting membership — append keeps each list sorted because
        # doc_id is strictly greater than any doc_id seen before.
        new_term_count = 0
        for t in deduped:
            bucket = self.term_postings.get(t)
            if bucket is None:
                self.term_postings[t] = [doc_id]
                new_term_count += 1
            else:
                bucket.append(doc_id)

        # Persist the entry + its terms.
        self.doc_entries[doc_id] = entry
        self.doc_terms[doc_id] = deduped

        # Track min/max for segment-range metadata.
        if self.min_doc_id is None:
            self.min_doc_id = doc_id
        self.max_doc_id = doc_id

        # Update memory estimate. Kept as a running counter so
        # ``memory_bytes()`` is O(1).
        entry_bytes = (
            len(entry.message.encode("utf-8"))
            + len(entry.service)
            + len(entry.level)
            + _ENTRY_CONST_OVERHEAD
        )
        terms_bytes = (
            sum(len(t) for t in deduped)
            + _PER_TERM_POSTING_ENTRY * len(deduped)
        )
        bucket_bytes = _NEW_TERM_BUCKET_OVERHEAD * new_term_count
        self._memory_bytes += entry_bytes + terms_bytes + bucket_bytes

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def search_term(self, term: str) -> list[int]:
        """Return a copy of the posting list for *term*.

        A copy is returned so callers can freely mutate the list
        (e.g. merge it with postings from another segment) without
        corrupting this segment's index. Unknown terms yield an empty
        list.
        """
        bucket = self.term_postings.get(term)
        if bucket is None:
            return []
        # ``list.copy`` here avoids aliasing even though Python lists
        # of ints are only shallowly copied — ints are immutable.
        return list(bucket)

    def doc_count(self) -> int:
        """Number of documents currently in the segment."""
        return len(self.doc_entries)

    def term_count(self) -> int:
        """Number of unique terms indexed across all documents."""
        return len(self.term_postings)

    def memory_bytes(self) -> int:
        """Approximate memory footprint in bytes.

        The estimator sums per-entry overhead, per-term posting cost,
        and a one-time bucket overhead for each new term. It is not
        exact — it deliberately biases high enough that the 50 MB
        flush trigger fires before we actually OOM.
        """
        return self._memory_bytes

    def is_full(self, max_docs: int, max_bytes: int) -> bool:
        """Return True when either limit has been reached.

        Both limits are checked with ``>=`` so a segment at exactly
        the threshold is considered full and gets flushed on the next
        write-attempt. ``max_bytes`` is expected in bytes; the caller
        converts from the MB-denominated setting.
        """
        return self.doc_count() >= max_docs or self.memory_bytes() >= max_bytes

    def iter_docs(self) -> Iterator[tuple[int, LogEntry, list[str]]]:
        """Yield ``(doc_id, entry, terms)`` in ascending doc_id order.

        Used by the persistence layer to write the segment out and by
        the merger to stream docs into a combined segment. Sorting is
        unavoidable only if upstream invariants were violated — in
        practice the dict is already insertion-ordered and insertion
        is monotone, but we sort defensively so callers don't need to
        worry about that.
        """
        for doc_id in sorted(self.doc_entries.keys()):
            yield doc_id, self.doc_entries[doc_id], self.doc_terms[doc_id]
