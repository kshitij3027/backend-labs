"""Search engine with TF-IDF scoring and AND-intersection semantics."""

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
        """Execute a search query and return ranked results.

        Parameters
        ----------
        query:
            Free-text search string.  Tokenized with the same pipeline as
            indexed documents.
        limit:
            Maximum number of results to return.

        Returns
        -------
        SearchResponse
            Ranked results with TF-IDF scores and timing information.
        """
        start = time.perf_counter()

        # Empty / whitespace query -> empty response
        if not query or not query.strip():
            elapsed_ms = (time.perf_counter() - start) * 1000
            return SearchResponse(
                results=[],
                total_results=0,
                search_time_ms=elapsed_ms,
                query=query,
            )

        terms = self._tokenizer.tokenize(query)
        if not terms:
            elapsed_ms = (time.perf_counter() - start) * 1000
            return SearchResponse(
                results=[],
                total_results=0,
                search_time_ms=elapsed_ms,
                query=query,
            )

        # Gather posting lists for every query term
        posting_lists = []
        for term in terms:
            postings = self._index.get_posting_list(term)
            if not postings:
                # AND semantics: if any term is absent, no documents match
                elapsed_ms = (time.perf_counter() - start) * 1000
                return SearchResponse(
                    results=[],
                    total_results=0,
                    search_time_ms=elapsed_ms,
                    query=query,
                )
            posting_lists.append((term, postings))

        # ----------------------------------------------------------
        # AND intersection: sort posting lists smallest-first, then
        # keep only doc_ids present in every list.
        # ----------------------------------------------------------
        posting_lists.sort(key=lambda tp: len(tp[1]))

        # Seed with doc_ids from the smallest posting list
        candidate_ids = {doc_id for doc_id, _ in posting_lists[0][1]}

        for _, postings in posting_lists[1:]:
            other_ids = {doc_id for doc_id, _ in postings}
            candidate_ids &= other_ids
            if not candidate_ids:
                elapsed_ms = (time.perf_counter() - start) * 1000
                return SearchResponse(
                    results=[],
                    total_results=0,
                    search_time_ms=elapsed_ms,
                    query=query,
                )

        # ----------------------------------------------------------
        # Build a lookup: doc_id -> { term -> positions } for scoring
        # ----------------------------------------------------------
        term_postings_by_doc: dict[int, dict[str, list[int]]] = {}
        for term, postings in posting_lists:
            for doc_id, positions in postings:
                if doc_id in candidate_ids:
                    term_postings_by_doc.setdefault(doc_id, {})[term] = positions

        total_docs = self._index.get_total_documents()

        # ----------------------------------------------------------
        # Score each candidate document using TF-IDF
        # ----------------------------------------------------------
        scored: list[tuple[float, float, int]] = []
        for doc_id in candidate_ids:
            doc = self._index.get_document(doc_id)
            if doc is None:
                continue

            score = 0.0
            term_positions = term_postings_by_doc.get(doc_id, {})

            for term, _ in posting_lists:
                positions = term_positions.get(term, [])
                # TF = positions count / total unique terms in document
                tf = len(positions) / doc.term_count if doc.term_count > 0 else 0.0
                # IDF = log(N / df)
                df = self._index.get_term_doc_frequency(term)
                idf = math.log(total_docs / df) if df > 0 else 0.0
                score += tf * idf

            scored.append((score, doc.timestamp, doc_id))

        # Sort: highest score first, then newest timestamp first (tiebreaker)
        scored.sort(key=lambda s: (-s[0], -s[1]))

        total_results = len(scored)
        scored = scored[:limit]

        # ----------------------------------------------------------
        # Build response objects with highlighting
        # ----------------------------------------------------------
        results: list[SearchResult] = []
        for score, _, doc_id in scored:
            doc = self._index.get_document(doc_id)
            if doc is None:
                continue
            highlighted = self.highlight(doc.message, terms)
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
