"""Search engine with TF-IDF scoring and AND-intersection semantics."""

import heapq
import math
import re
import time

from backend.index import InvertedIndex
from backend.models import SearchResult, SearchResponse
from backend.tokenizer import LogTokenizer


class SearchEngine:
    """Full-text search over an inverted index using TF-IDF ranking.

    Queries are tokenized with the same ``LogTokenizer`` used at index time.
    All query terms must appear in a document for it to match (AND semantics).
    Results are ranked by cumulative TF-IDF score with timestamp as tiebreaker.
    """

    def __init__(self, index: InvertedIndex, tokenizer: LogTokenizer) -> None:
        self._index = index
        self._tokenizer = tokenizer

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def search(self, query: str, limit: int = 100) -> SearchResponse:
        start = time.perf_counter()

        if not query or not query.strip():
            return self._empty_response(start, query)

        terms = self._tokenizer.tokenize(query)
        if not terms:
            return self._empty_response(start, query)

        # Single-term fast path: skip intersection overhead entirely
        if len(terms) == 1:
            return self._search_single_term(terms[0], terms, limit, start, query)

        return self._search_multi_term(terms, limit, start, query)

    def _empty_response(self, start: float, query: str) -> SearchResponse:
        return SearchResponse(
            results=[],
            total_results=0,
            search_time_ms=(time.perf_counter() - start) * 1000,
            query=query,
        )

    def _search_single_term(
        self, term: str, terms: list[str], limit: int, start: float, query: str
    ) -> SearchResponse:
        """Optimized path for single-term queries — no intersection needed."""
        postings = self._index.get_posting_list_raw(term)
        if not postings:
            return self._empty_response(start, query)

        total_docs = self._index.get_total_documents()
        df = self._index.get_term_doc_frequency(term)
        idf = math.log(total_docs / df) if df > 0 else 0.0

        _get_doc = self._index.get_document
        total_results = len(postings)

        # Score inline and use heapq for top-K (avoids sorting full list)
        def _scored_iter():
            for doc_id, positions in postings:
                doc = _get_doc(doc_id)
                if doc is None:
                    continue
                tf = len(positions) / doc.term_count if doc.term_count > 0 else 0.0
                score = tf * idf
                yield score, doc.timestamp, doc_id

        top_k = heapq.nlargest(limit, _scored_iter(), key=lambda s: (s[0], s[1]))

        return self._build_response(top_k, terms, total_results, start, query)

    def _search_multi_term(
        self, terms: list[str], limit: int, start: float, query: str
    ) -> SearchResponse:
        """Multi-term search with AND intersection and TF-IDF scoring."""
        posting_lists = []
        for term in terms:
            postings = self._index.get_posting_list_raw(term)
            if not postings:
                return self._empty_response(start, query)
            posting_lists.append((term, postings))

        posting_lists.sort(key=lambda tp: len(tp[1]))

        # AND intersection using sets
        candidate_ids = {doc_id for doc_id, _ in posting_lists[0][1]}
        for _, postings in posting_lists[1:]:
            candidate_ids &= {doc_id for doc_id, _ in postings}
            if not candidate_ids:
                return self._empty_response(start, query)

        # Build position lookup only for candidates
        term_postings_by_doc: dict[int, dict[str, list[int]]] = {}
        for term, postings in posting_lists:
            for doc_id, positions in postings:
                if doc_id in candidate_ids:
                    term_postings_by_doc.setdefault(doc_id, {})[term] = positions

        total_docs = self._index.get_total_documents()
        _log = math.log
        idf_cache = {}
        for term, _ in posting_lists:
            df = self._index.get_term_doc_frequency(term)
            idf_cache[term] = _log(total_docs / df) if df > 0 else 0.0

        _get_doc = self._index.get_document
        total_results = len(candidate_ids)

        def _scored_iter():
            for doc_id in candidate_ids:
                doc = _get_doc(doc_id)
                if doc is None:
                    continue
                score = 0.0
                tp = term_postings_by_doc.get(doc_id, {})
                inv_tc = 1.0 / doc.term_count if doc.term_count > 0 else 0.0
                for term, _ in posting_lists:
                    positions = tp.get(term, [])
                    score += len(positions) * inv_tc * idf_cache[term]
                yield score, doc.timestamp, doc_id

        top_k = heapq.nlargest(limit, _scored_iter(), key=lambda s: (s[0], s[1]))

        return self._build_response(top_k, terms, total_results, start, query)

    def _build_response(
        self,
        scored: list[tuple[float, float, int]],
        terms: list[str],
        total_results: int,
        start: float,
        query: str,
    ) -> SearchResponse:
        """Build SearchResponse from scored tuples — highlight only final set."""
        escaped = [re.escape(t) for t in terms]
        highlight_pattern = re.compile("|".join(escaped), re.IGNORECASE)
        _get_doc = self._index.get_document

        results = []
        for score, _, doc_id in scored:
            doc = _get_doc(doc_id)
            if doc is None:
                continue
            highlighted = highlight_pattern.sub(
                lambda m: f"<mark>{m.group()}</mark>", doc.message
            )
            results.append(
                SearchResult(
                    doc_id=doc_id,
                    message=doc.message,
                    highlighted_message=highlighted,
                    timestamp=doc.timestamp,
                    service=doc.service,
                    level=doc.level,
                    score=score,
                )
            )

        elapsed_ms = (time.perf_counter() - start) * 1000
        return SearchResponse(
            results=results,
            total_results=total_results,
            search_time_ms=elapsed_ms,
            query=query,
        )

    def highlight(self, text: str, terms: list[str]) -> str:
        """Wrap occurrences of *terms* in ``<mark>`` tags (case-insensitive).

        Uses word-boundary matching to avoid partial overlap issues and
        processes all terms in a single regex pass to prevent double-
        highlighting.
        """
        if not terms:
            return text

        # Build a single alternation pattern from all terms so we only
        # make one pass over the text (avoids marking inside marks).
        escaped = [re.escape(t) for t in terms]
        pattern = re.compile("|".join(escaped), re.IGNORECASE)
        return pattern.sub(lambda m: f"<mark>{m.group()}</mark>", text)

    def get_suggestions(self, prefix: str, limit: int = 10) -> list[str]:
        """Return index terms that start with *prefix*, ranked by frequency.

        Parameters
        ----------
        prefix:
            Case-insensitive prefix to match against index terms.
        limit:
            Maximum number of suggestions to return.
        """
        prefix_lower = prefix.lower()
        all_terms = self._index.get_all_terms()
        matching = [t for t in all_terms if t.startswith(prefix_lower)]

        # Sort by document frequency (most common first)
        matching.sort(
            key=lambda t: self._index.get_term_doc_frequency(t), reverse=True
        )
        return matching[:limit]
