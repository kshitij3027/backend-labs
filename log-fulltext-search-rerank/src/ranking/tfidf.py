"""Hand-rolled TF-IDF scorer on top of :class:`InvertedIndex`.

The scorer maintains a lazy ``idf_cache: dict[str, float]`` that
stays in sync with the underlying index via a version+time-based
rebuild policy described in ``plan.md`` section 4. Reads go through
:meth:`idf`, which returns the cached value when present and falls
back to a live computation otherwise; writes only happen inside
:meth:`maybe_rebuild`, which atomically swaps the whole cache dict
so in-flight readers never observe a half-built state.

The :meth:`score` entry point is pure-sync and hot — commit 08 calls
it inside :func:`asyncio.to_thread` from the reranker, and any
awaitable or lazy import here would bust that pattern.
"""

from __future__ import annotations

import math
import time

from src.config import Settings
from src.index.inverted_index import InvertedIndex


class TfIdfScorer:
    """TF-IDF on the :class:`InvertedIndex` with a lazy, batched IDF rebuild.

    The ``idf_cache`` maps ``token -> idf value`` and stays in sync
    with the underlying index by rebuilding when **either**:

    * ``index.version`` has advanced by at least
      ``settings.idf_rebuild_every_n_docs`` since the last rebuild, or
    * ``settings.idf_rebuild_every_s`` seconds have elapsed **and** the
      version has advanced at all.

    Readers take a cheap reference to ``self.idf_cache`` for the
    duration of a query; a subsequent rebuild atomically replaces the
    dict, so the reader's reference remains valid.
    """

    def __init__(self, index: InvertedIndex, settings: Settings) -> None:
        self._index = index
        self._settings = settings
        self.idf_cache: dict[str, float] = {}
        self._idf_version: int = 0       # bumped each rebuild
        self._last_built_version: int = -1
        self._last_built_time: float = 0.0

    @property
    def idf_version(self) -> int:
        """Monotonic counter bumped once per successful rebuild.

        Commit 09's query cache folds this into its key so cached
        responses silently expire when the corpus shifts under them.
        """
        return self._idf_version

    def idf(self, token: str) -> float:
        """Return the inverse-document-frequency for ``token``.

        Prefers the cached value when present; otherwise computes on
        the fly from the live index. The smoothed formula
        ``log((N+1)/(df+1)) + 1`` never returns a negative score and
        handles the empty-corpus edge case (``N == 0``) gracefully.
        """
        cached = self.idf_cache.get(token)
        if cached is not None:
            return cached
        df = self._index.doc_frequency(token)
        n_total = self._index.total_docs
        return math.log((n_total + 1) / (df + 1)) + 1.0

    def maybe_rebuild(self) -> None:
        """Rebuild ``idf_cache`` if the version/time thresholds are hit.

        Cheap no-op when the thresholds are not met — a single int
        compare and a monotonic clock read. Callers invoke this from
        the search path so the cache catches up incrementally without
        requiring a dedicated maintenance thread.
        """
        idx_ver = self._index.version
        now = time.monotonic()
        versions_since = idx_ver - self._last_built_version
        elapsed = now - self._last_built_time
        should_rebuild = (
            idx_ver > 0
            and (
                versions_since >= self._settings.idf_rebuild_every_n_docs
                or (versions_since > 0 and elapsed >= self._settings.idf_rebuild_every_s)
            )
        )
        if not should_rebuild:
            return
        n_total = self._index.total_docs
        new_cache: dict[str, float] = {}
        for tok in self._index.unique_tokens():
            df = self._index.doc_frequency(tok)
            new_cache[tok] = math.log((n_total + 1) / (df + 1)) + 1.0
        # Atomic swap so concurrent readers either see the whole old
        # cache or the whole new one, never a half-built dict.
        self.idf_cache = new_cache
        self._idf_version += 1
        self._last_built_version = idx_ver
        self._last_built_time = now

    def score(self, doc_id: int, tokens: list[str]) -> float:
        """Return TF-IDF score for ``doc_id`` against the query ``tokens``.

        Uses raw TF (no sublinear scaling) normalised by the document
        length, summed across query tokens. Cheap and predictable for
        the ranking-by-order comparisons the rest of the pipeline
        actually cares about.

        Zero short-circuits: empty token list or a zero-length doc
        (defensive default) collapse to ``0.0`` instantly.
        """
        if not tokens:
            return 0.0
        doc_len = self._index.doc_length(doc_id) or 1
        total = 0.0
        for tok in tokens:
            tf = self._index.token_frequency(tok, doc_id)
            if tf == 0:
                continue
            total += (tf / doc_len) * self.idf(tok)
        return total
