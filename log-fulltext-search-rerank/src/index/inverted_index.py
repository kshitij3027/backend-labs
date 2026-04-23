"""In-memory inverted index with append-only postings and lock-free reads.

The index is the hottest data path in the service: every ingest mutates
it, every search reads from it. The design splits the two sides cleanly
so they don't contend:

* **Writers** ingest sequentially behind an :class:`asyncio.Lock`. Each
  admitted :class:`~src.models.LogEntry` gets a monotonic ``doc_id``,
  tokenization happens once, and the postings map is mutated with a
  fresh ``doc_id`` that no prior state references.
* **Readers** — :meth:`retrieve_candidates`, :meth:`doc`, :meth:`stats`,
  and the :attr:`version` property — run lock-free. The correctness of
  this relies on the index being **append-only**: new ``doc_id`` values
  land in brand-new entries inside ``_postings[token]``, and no existing
  ``(token, doc_id, tf)`` triple is ever rewritten. Under the CPython
  GIL, a dict ``setitem`` is atomic, so a reader may see a token that
  some writer is about to add as absent one tick and present the next,
  but it never observes a half-built intermediate state.

The version counter backs commit-09's query cache: cache keys include
``index_version`` so a ``put`` before a write and a ``get`` after a
write miss cleanly without an explicit invalidation pass.

Candidate retrieval is deliberately synchronous. Commit 08's reranker
pushes the retrieval + rescoring block onto :func:`asyncio.to_thread`
so the event loop keeps accepting new requests while the rescoring
work runs on a worker thread. Keeping :meth:`retrieve_candidates` pure
Python (no ``await``) lets that offload be a trivial call.
"""

import asyncio
from collections import Counter
from typing import TYPE_CHECKING, Iterator

from src.models import LogEntry

if TYPE_CHECKING:  # pragma: no cover - import-cycle guard
    from src.config import Settings
    from src.index.tokenizer import LogTokenizer


class InvertedIndex:
    """Append-only inverted index with concurrency-safe ingestion.

    Data layout:

    * ``_postings: dict[str, dict[int, int]]`` — token to ``{doc_id: tf}``.
    * ``_docs: dict[int, LogEntry]`` — raw documents keyed by assigned id.
    * ``_doc_token_counts: dict[int, int]`` — pre-computed length used by
      TF-IDF normalisation in commit 07.
    * ``_version: int`` — monotonic write counter surfaced to cache keys.

    The index owns its :class:`~src.index.tokenizer.LogTokenizer` so the
    factory can inject a settings-matched instance and callers never
    have to pick one.
    """

    def __init__(self, settings: "Settings", tokenizer: "LogTokenizer") -> None:
        self._settings = settings
        self._tokenizer = tokenizer

        # Append-only core state. All four dicts are initialised here so
        # the shape is obvious at a glance — no lazy allocation trickery.
        self._postings: dict[str, dict[int, int]] = {}
        self._docs: dict[int, LogEntry] = {}
        self._doc_token_counts: dict[int, int] = {}

        # Monotonic counters. ``_next_doc_id`` is bumped under the writer
        # lock so two concurrent ``add`` calls cannot collide on the
        # same id. ``_version`` is a plain ``int``; CPython guarantees
        # single-opcode assignment under the GIL, so readers can poll it
        # without the lock — they will just see the old value briefly.
        self._next_doc_id: int = 0
        self._version: int = 0

        # Writers-only lock. Readers do not acquire it, per the
        # append-only invariants described in the module docstring.
        self._writer_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Writers
    # ------------------------------------------------------------------

    async def add(self, entry: LogEntry) -> int:
        """Admit a single log entry and return its assigned ``doc_id``.

        Holds the writer lock for the duration of the write so two
        concurrent ``add`` coroutines serialise their postings
        mutations and doc-id assignment. The lock is released before
        returning, so callers never observe a running writer.
        """
        async with self._writer_lock:
            doc_id = self._admit_one(entry)
            # Bump the version once per admitted entry. Readers that
            # cache against (query, version) see a new key on every
            # successful write, which is the point of a per-doc bump.
            self._version += 1
        return doc_id

    async def add_bulk(self, entries: list[LogEntry]) -> list[int]:
        """Admit a batch of entries atomically and return their ``doc_id``s.

        Design choice: the version is bumped **once** at the end of the
        batch, not once per entry inside it. Rationale — a bulk ingest
        is logically one write from a cache-coherence standpoint, so
        callers should see a single version jump after the whole batch
        lands rather than mid-batch stale views. Tests assert this
        convention.
        """
        async with self._writer_lock:
            doc_ids = [self._admit_one(entry) for entry in entries]
            if doc_ids:
                # Only bump when we actually wrote something — an empty
                # bulk (blocked at the pydantic layer, but defence in
                # depth is cheap) should not look like a write.
                self._version += 1
        return doc_ids

    def _admit_one(self, entry: LogEntry) -> int:
        """Assign an id, tokenize, write postings. Caller holds the lock.

        Factored out so ``add`` and ``add_bulk`` share exactly the same
        per-entry logic — drift between the two is a classic source of
        subtle bugs (one path bumps a counter the other doesn't).
        """
        doc_id = self._next_doc_id
        self._next_doc_id += 1

        # Tokenize through the owned tokenizer so the index + search
        # pipeline always see identical tokens for the same text. Any
        # tokenization change therefore propagates consistently.
        tokens = self._tokenizer.tokenize(entry.message)

        # Count term-frequency in one pass. ``Counter`` returns ints
        # directly so the inner dict stays lean.
        tf_counts = Counter(tokens)
        for token, tf in tf_counts.items():
            # ``setdefault`` keeps the outer dict allocation cheap —
            # we only pay the empty-dict cost the first time a token
            # is seen. The inner ``[doc_id] = tf`` is a fresh key
            # (doc_id is new) so no existing entry is ever overwritten.
            self._postings.setdefault(token, {})[doc_id] = tf

        # Persist the entry with its assigned id so readers can round-
        # trip it later. Using ``model_copy`` keeps the stored object
        # immutable from the caller's perspective — they don't get to
        # mutate what's in the index after ingest.
        stored = entry.model_copy(update={"id": doc_id})
        self._docs[doc_id] = stored
        self._doc_token_counts[doc_id] = len(tokens)
        return doc_id

    # ------------------------------------------------------------------
    # Readers (lock-free)
    # ------------------------------------------------------------------

    def retrieve_candidates(
        self, tokens: list[str], top_k: int | None = None
    ) -> list[int]:
        """Return up to ``top_k`` candidate ``doc_id``s for ``tokens``.

        Heuristic ordering: primary key is the number of **distinct**
        query tokens the doc matches (a 3-of-4 match outranks any
        1-of-4 match); secondary key is total term-frequency across
        the query tokens (when two docs match the same query words,
        the one that mentions them more often wins); tiebreak by
        newer ``doc_id`` so recent logs bubble up ahead of ancient
        ones when the rest is equal.

        This is the cheap **recall** stage — commit 08's reranker
        does the expensive TF-IDF + temporal + severity + service +
        context scoring on this shortlist. The ``candidate_top_k``
        default (200) is the spec's budget for how many docs the
        reranker is willing to score per query.

        Synchronous on purpose: the caller offloads the whole block
        via :func:`asyncio.to_thread` so the event loop stays
        responsive. Adding awaits here would defeat that pattern.
        """
        if not tokens:
            return []
        cap = top_k if top_k is not None else self._settings.candidate_top_k

        # Deduplicate query tokens before the union so a query that
        # says "error error" doesn't double-count matches.
        distinct_tokens = set(tokens)

        # Snapshot the posting refs under the current reader view.
        # Because postings are append-only, the reference we capture
        # here may grow concurrently, but existing (doc_id, tf) pairs
        # are never rewritten — so a stable iteration is safe.
        per_doc_matches: dict[int, int] = {}
        per_doc_tf_sum: dict[int, int] = {}
        for token in distinct_tokens:
            postings_for_token = self._postings.get(token)
            if not postings_for_token:
                continue
            for doc_id, tf in postings_for_token.items():
                per_doc_matches[doc_id] = per_doc_matches.get(doc_id, 0) + 1
                per_doc_tf_sum[doc_id] = per_doc_tf_sum.get(doc_id, 0) + tf

        if not per_doc_matches:
            return []

        # Sort by (distinct-matches desc, tf-sum desc, doc_id desc).
        # Using a three-tuple key keeps the sort stable and avoids
        # the overhead of running three separate passes.
        ordered = sorted(
            per_doc_matches.keys(),
            key=lambda d: (
                -per_doc_matches[d],
                -per_doc_tf_sum[d],
                -d,
            ),
        )
        return ordered[:cap]

    def unique_tokens(self) -> Iterator[str]:
        """Iterate distinct tokens currently in the index.

        Used by the trie in commit 05 to rebuild autocomplete state lazily.
        Safe for concurrent use because postings is append-only and new
        keys become visible atomically under the GIL.
        """
        return iter(self._postings.keys())

    def doc_frequency(self, token: str) -> int:
        """Return the number of documents containing ``token``.

        Reads the length of the token's posting list. Zero if the token
        is unknown. Used by the suggestions route so the trie's per-
        terminal frequency mirrors real-world popularity rather than a
        flat count.
        """
        postings = self._postings.get(token)
        return len(postings) if postings else 0

    def token_frequency(self, token: str, doc_id: int) -> int:
        """TF for ``token`` in ``doc_id``. Returns 0 when either is absent."""
        return self._postings.get(token, {}).get(doc_id, 0)

    def doc_length(self, doc_id: int) -> int:
        """Total tokens in ``doc_id`` (pre-lemmatization sum). Returns 0 when absent."""
        return self._doc_token_counts.get(doc_id, 0)

    def doc(self, doc_id: int) -> LogEntry | None:
        """Return the stored entry for ``doc_id`` or ``None`` if absent.

        Synchronous dict lookup — the caller is the reranker/serializer,
        both CPU-bound paths that should not pay an event-loop hop for a
        pointer chase. A miss is expected (e.g. the reranker may filter
        out a doc_id before calling this) and returns ``None`` rather
        than raising.
        """
        return self._docs.get(doc_id)

    def stats(self) -> dict:
        """Return a small dict of index counters.

        Cheap — no locks, no iteration over postings, just property
        reads and three ``len()`` calls. The shape deliberately mirrors
        the subset of :class:`~src.models.StatsResponse` the index owns;
        commit 09's ``/api/search/stats`` handler merges these with
        cache + latency metrics.
        """
        # ``total_postings`` counts ``(token, doc_id, tf)`` triples.
        # It's a handy proxy for how much storage the postings map is
        # using and how dense the inverted structure is. Computed on
        # demand because the alternative — a running counter — would
        # need lock coordination with the writers.
        total_postings = sum(len(bucket) for bucket in self._postings.values())
        return {
            "total_docs": len(self._docs),
            "unique_tokens": len(self._postings),
            "version": self._version,
            "total_postings": total_postings,
        }

    @property
    def version(self) -> int:
        """Return the current write version.

        Incremented once per successful ``add`` and once per non-empty
        ``add_bulk``. Used by the query cache as part of its lookup key
        so cached results silently expire the moment the corpus changes.
        """
        return self._version

    @property
    def total_docs(self) -> int:
        """Return the number of documents currently indexed."""
        return len(self._docs)
