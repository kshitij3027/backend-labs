"""Tests for the IndexPersistence class."""

import os
import tempfile

import pytest

from backend.index import InvertedIndex
from backend.persistence import IndexPersistence
from backend.tokenizer import LogTokenizer


@pytest.fixture
def tokenizer():
    return LogTokenizer()


@pytest.fixture
def index(tokenizer):
    return InvertedIndex(tokenizer)


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def persistence(tmp_dir):
    return IndexPersistence(tmp_dir)


class TestSaveLoad:
    async def test_round_trip(self, index, persistence):
        # Add docs
        await index.add_document("Error in auth service", 1000.0, "auth", "ERROR")
        await index.add_document("Payment processed", 2000.0, "payment", "INFO")

        # Save
        persistence.save(index)

        # Create fresh index and load
        tokenizer = LogTokenizer()
        new_index = InvertedIndex(tokenizer)
        loaded = persistence.load(new_index)

        assert loaded is True
        assert new_index.get_total_documents() == 2
        assert new_index.get_stats()["total_terms"] == index.get_stats()["total_terms"]

        # Verify documents are identical
        for doc_id in range(2):
            orig = index.get_document(doc_id)
            restored = new_index.get_document(doc_id)
            assert orig.message == restored.message
            assert orig.service == restored.service
            assert orig.level == restored.level

    async def test_posting_lists_preserved(self, index, persistence):
        await index.add_document("Error error error", 1.0, "svc", "ERROR")
        persistence.save(index)

        new_index = InvertedIndex(LogTokenizer())
        persistence.load(new_index)

        postings = new_index.get_posting_list("error")
        orig_postings = index.get_posting_list("error")
        assert len(postings) == len(orig_postings)

    async def test_load_nonexistent_returns_false(self, persistence):
        new_index = InvertedIndex(LogTokenizer())
        assert persistence.load(new_index) is False

    async def test_next_doc_id_preserved(self, index, persistence):
        await index.add_document("msg1", 1.0, "svc", "INFO")
        await index.add_document("msg2", 2.0, "svc", "INFO")
        persistence.save(index)

        new_index = InvertedIndex(LogTokenizer())
        persistence.load(new_index)
        doc_id = await new_index.add_document("msg3", 3.0, "svc", "INFO")
        assert doc_id == 2  # continues from where it left off


class TestCompression:
    async def test_file_is_compressed(self, index, persistence):
        # Add several documents
        for i in range(20):
            await index.add_document(
                f"Log message number {i} with error details",
                float(i),
                "svc",
                "ERROR",
            )

        persistence.save(index)
        file_size = persistence.get_file_size()
        assert file_size > 0
        # Compressed file should exist
        assert persistence.exists()


class TestFileSize:
    def test_no_file_returns_zero(self, persistence):
        assert persistence.get_file_size() == 0
