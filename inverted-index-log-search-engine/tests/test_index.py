"""Comprehensive tests for the InvertedIndex class."""

import asyncio

import pytest

from backend.models import DocumentInput
from backend.tokenizer import LogTokenizer
from backend.index import InvertedIndex


@pytest.fixture
def tokenizer():
    return LogTokenizer()


@pytest.fixture
def index(tokenizer):
    return InvertedIndex(tokenizer)


# ------------------------------------------------------------------
# Adding documents
# ------------------------------------------------------------------


class TestAddDocument:
    async def test_add_single_document(self, index):
        doc_id = await index.add_document(
            message="Error in authentication service",
            timestamp=1700000000.0,
            service="auth-service",
            level="ERROR",
        )
        assert doc_id == 0
        assert index.get_total_documents() == 1

    async def test_add_returns_incremental_ids(self, index):
        id1 = await index.add_document("msg1", 1.0, "svc", "INFO")
        id2 = await index.add_document("msg2", 2.0, "svc", "INFO")
        assert id1 == 0
        assert id2 == 1

    async def test_document_metadata_stored(self, index):
        await index.add_document("Test message", 1700000000.0, "test-svc", "WARN")
        doc = index.get_document(0)
        assert doc is not None
        assert doc.message == "Test message"
        assert doc.service == "test-svc"
        assert doc.level == "WARN"
        assert doc.timestamp == 1700000000.0

    async def test_nonexistent_document_returns_none(self, index):
        assert index.get_document(999) is None


# ------------------------------------------------------------------
# Posting lists
# ------------------------------------------------------------------


class TestPostingList:
    async def test_posting_list_contains_doc(self, index):
        await index.add_document("Server error occurred", 1.0, "svc", "ERROR")
        postings = index.get_posting_list("error")
        assert len(postings) == 1
        assert postings[0][0] == 0  # doc_id

    async def test_posting_list_has_positions(self, index):
        await index.add_document("error after error", 1.0, "svc", "ERROR")
        postings = index.get_posting_list("error")
        assert len(postings) == 1
        doc_id, positions = postings[0]
        assert len(positions) >= 2  # "error" appears at least twice

    async def test_multiple_docs_in_posting_list(self, index):
        await index.add_document("Error one", 1.0, "svc", "ERROR")
        await index.add_document("Error two", 2.0, "svc", "ERROR")
        postings = index.get_posting_list("error")
        assert len(postings) == 2
        doc_ids = [p[0] for p in postings]
        assert 0 in doc_ids
        assert 1 in doc_ids

    async def test_nonexistent_term_returns_empty(self, index):
        assert index.get_posting_list("nonexistent") == []

    async def test_posting_list_returns_copy(self, index):
        await index.add_document("Error here", 1.0, "svc", "ERROR")
        postings_a = index.get_posting_list("error")
        postings_b = index.get_posting_list("error")
        assert postings_a is not postings_b  # distinct list objects

    async def test_case_insensitive_lookup(self, index):
        await index.add_document("Error occurred", 1.0, "svc", "ERROR")
        assert len(index.get_posting_list("ERROR")) == 1
        assert len(index.get_posting_list("Error")) == 1
        assert len(index.get_posting_list("error")) == 1


# ------------------------------------------------------------------
# Bulk add
# ------------------------------------------------------------------


class TestBulkAdd:
    async def test_bulk_add_multiple(self, index):
        docs = [
            DocumentInput(message="Error one", timestamp=1.0, service="svc1", level="ERROR"),
            DocumentInput(message="Warning two", timestamp=2.0, service="svc2", level="WARN"),
            DocumentInput(message="Info three", timestamp=3.0, service="svc3", level="INFO"),
        ]
        ids = await index.add_documents_bulk(docs)
        assert len(ids) == 3
        assert index.get_total_documents() == 3

    async def test_bulk_add_ids_are_sequential(self, index):
        docs = [
            DocumentInput(message="msg1", timestamp=1.0, service="svc", level="INFO"),
            DocumentInput(message="msg2", timestamp=2.0, service="svc", level="INFO"),
        ]
        ids = await index.add_documents_bulk(docs)
        assert ids == [0, 1]

    async def test_bulk_add_empty_list(self, index):
        ids = await index.add_documents_bulk([])
        assert ids == []
        assert index.get_total_documents() == 0


# ------------------------------------------------------------------
# Stats
# ------------------------------------------------------------------


class TestStats:
    async def test_empty_stats(self, index):
        stats = index.get_stats()
        assert stats["total_documents"] == 0
        assert stats["total_terms"] == 0
        assert stats["avg_terms_per_doc"] == 0.0

    async def test_stats_after_add(self, index):
        await index.add_document("Error in authentication", 1.0, "auth", "ERROR")
        stats = index.get_stats()
        assert stats["total_documents"] == 1
        assert stats["total_terms"] > 0
        assert stats["avg_terms_per_doc"] > 0.0


# ------------------------------------------------------------------
# get_all_terms
# ------------------------------------------------------------------


class TestGetAllTerms:
    async def test_returns_sorted_terms(self, index):
        await index.add_document("zebra alpha beta", 1.0, "svc", "INFO")
        terms = index.get_all_terms()
        assert terms == sorted(terms)
        assert "alpha" in terms
        assert "beta" in terms
        assert "zebra" in terms

    async def test_empty_index_returns_empty(self, index):
        assert index.get_all_terms() == []


# ------------------------------------------------------------------
# Term document frequency
# ------------------------------------------------------------------


class TestTermDocFrequency:
    async def test_single_doc(self, index):
        await index.add_document("error occurred", 1.0, "svc", "ERROR")
        assert index.get_term_doc_frequency("error") == 1

    async def test_multiple_docs(self, index):
        await index.add_document("error one", 1.0, "svc", "ERROR")
        await index.add_document("error two", 2.0, "svc", "ERROR")
        assert index.get_term_doc_frequency("error") == 2

    async def test_unknown_term(self, index):
        assert index.get_term_doc_frequency("nonexistent") == 0


# ------------------------------------------------------------------
# Clear
# ------------------------------------------------------------------


class TestClear:
    async def test_clear_resets_everything(self, index):
        await index.add_document("Test message", 1.0, "svc", "INFO")
        assert index.get_total_documents() == 1
        index.clear()
        assert index.get_total_documents() == 0
        assert index.get_stats()["total_terms"] == 0

    async def test_ids_restart_after_clear(self, index):
        await index.add_document("first", 1.0, "svc", "INFO")
        index.clear()
        doc_id = await index.add_document("second", 2.0, "svc", "INFO")
        assert doc_id == 0


# ------------------------------------------------------------------
# Concurrency
# ------------------------------------------------------------------


class TestConcurrency:
    async def test_concurrent_adds_no_data_loss(self, index):
        """50 concurrent add_document calls should not lose any documents."""
        tasks = [
            index.add_document(f"Message {i}", float(i), "svc", "INFO")
            for i in range(50)
        ]
        ids = await asyncio.gather(*tasks)
        assert len(set(ids)) == 50  # all unique IDs
        assert index.get_total_documents() == 50

    async def test_concurrent_adds_and_reads(self, index):
        """Reads during concurrent writes should not raise."""
        async def writer(n: int):
            await index.add_document(f"Message {n}", float(n), "svc", "INFO")

        async def reader():
            index.get_stats()
            index.get_all_terms()
            index.get_total_documents()

        tasks = [writer(i) for i in range(20)] + [reader() for _ in range(20)]
        await asyncio.gather(*tasks)
        assert index.get_total_documents() == 20
