"""Thread-safe inverted index with positional posting lists."""

import asyncio
from typing import Dict, List, Optional, Tuple

from backend.models import DocumentInput, DocumentMeta
from backend.tokenizer import LogTokenizer


class InvertedIndex:
    """Thread-safe inverted index with positional posting lists.

    Write operations (add_document, add_documents_bulk, clear) acquire an
    asyncio.Lock to guarantee consistency.  Read operations are lock-free
    because they either return copies or read a single atomic attribute.
    """

    def __init__(self, tokenizer: LogTokenizer) -> None:
        self._tokenizer = tokenizer
        # term -> sorted list of (doc_id, [positions])
        self._postings: Dict[str, List[Tuple[int, List[int]]]] = {}
        # doc_id -> document metadata
        self._documents: Dict[int, DocumentMeta] = {}
        # term -> number of documents containing this term (document frequency)
        self._term_frequencies: Dict[str, int] = {}
        self._next_doc_id: int = 0
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _index_one(
        self, message: str, timestamp: float, service: str, level: str
    ) -> int:
        """Index a single document.  Caller must hold ``self._lock``."""
        doc_id = self._next_doc_id
        self._next_doc_id += 1

        term_positions = self._tokenizer.tokenize_with_positions(message)

        for term, positions in term_positions.items():
            self._postings.setdefault(term, []).append((doc_id, positions))
            self._term_frequencies[term] = self._term_frequencies.get(term, 0) + 1

        self._documents[doc_id] = DocumentMeta(
            doc_id=doc_id,
            message=message,
            timestamp=timestamp,
            service=service,
            level=level,
            term_count=len(term_positions),
        )

        return doc_id

    # ------------------------------------------------------------------
    # Write operations (lock-protected)
    # ------------------------------------------------------------------

    async def add_document(
        self,
        message: str,
        timestamp: float,
        service: str,
        level: str,
    ) -> int:
        """Index a single log document and return its assigned doc_id."""
        async with self._lock:
            return self._index_one(message, timestamp, service, level)

    async def add_documents_bulk(self, documents: list[DocumentInput]) -> list[int]:
        """Index multiple documents in one lock acquisition.

        Parameters
        ----------
        documents:
            A list of ``DocumentInput`` objects to index.

        Returns
        -------
        list[int]
            The doc_ids assigned to each document, in input order.
        """
        async with self._lock:
            return [
                self._index_one(
                    doc.message, doc.timestamp, doc.service, doc.level
                )
                for doc in documents
            ]

    # ------------------------------------------------------------------
    # Read operations (lock-free)
    # ------------------------------------------------------------------

    def get_posting_list(self, term: str) -> List[Tuple[int, List[int]]]:
        """Return a *copy* of the posting list for *term* (or empty list)."""
        return list(self._postings.get(term.lower(), []))

    def get_posting_list_raw(self, term: str) -> List[Tuple[int, List[int]]]:
        """Return a *reference* to the internal posting list for *term*.

        This avoids the copy overhead of ``get_posting_list`` and is safe
        for read-only iteration (e.g. AND-intersection, scoring).  Callers
        must **never** mutate the returned list.

        Returns the internal list directly, or an empty list (singleton)
        if the term is absent.
        """
        return self._postings.get(term.lower(), [])

    def get_document(self, doc_id: int) -> Optional[DocumentMeta]:
        """Return the metadata for *doc_id*, or ``None`` if not found."""
        return self._documents.get(doc_id)

    def get_stats(self) -> dict:
        """Return aggregate index statistics."""
        total_docs = len(self._documents)
        total_terms = len(self._postings)
        if total_docs > 0:
            avg_terms = sum(d.term_count for d in self._documents.values()) / total_docs
        else:
            avg_terms = 0.0
        return {
            "total_documents": total_docs,
            "total_terms": total_terms,
            "avg_terms_per_doc": avg_terms,
        }

    def get_all_terms(self) -> List[str]:
        """Return a sorted list of every term in the index."""
        return sorted(self._postings.keys())

    def get_term_doc_frequency(self, term: str) -> int:
        """Return the number of documents that contain *term*."""
        return self._term_frequencies.get(term.lower(), 0)

    def get_total_documents(self) -> int:
        """Return the total number of indexed documents."""
        return len(self._documents)

    def clear(self) -> None:
        """Reset all data structures to their initial empty state."""
        self._postings.clear()
        self._documents.clear()
        self._term_frequencies.clear()
        self._next_doc_id = 0
