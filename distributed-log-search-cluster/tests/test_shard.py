"""Unit tests for NodeShard against fakeredis."""

from __future__ import annotations

import pytest
import pytest_asyncio
from fakeredis import aioredis

from node.shard import NodeShard
from shared.models import TermTF


@pytest_asyncio.fixture
async def shard():
    redis = aioredis.FakeRedis(decode_responses=True)
    yield NodeShard("node-1", redis)
    await redis.aclose()


@pytest.mark.asyncio
async def test_index_single_doc(shard: NodeShard):
    count = await shard.index_terms("d1", [TermTF(term="error", tf=3)])
    assert count == 1
    stats = await shard.stats()
    assert stats == {"term_count": 1, "document_count": 1}


@pytest.mark.asyncio
async def test_index_multiple_docs_same_term(shard: NodeShard):
    await shard.index_terms("d1", [TermTF(term="error", tf=2)])
    await shard.index_terms("d2", [TermTF(term="error", tf=5)])
    doc_ids, df = await shard.get_posting_list("error")
    assert df == 2
    assert doc_ids == ["d1", "d2"]


@pytest.mark.asyncio
async def test_get_posting_list_missing(shard: NodeShard):
    doc_ids, df = await shard.get_posting_list("nope")
    assert doc_ids == []
    assert df == 0


@pytest.mark.asyncio
async def test_get_postings_batch_mixed(shard: NodeShard):
    await shard.index_terms(
        "d1",
        [TermTF(term="error", tf=3), TermTF(term="timeout", tf=1)],
    )
    await shard.index_terms("d2", [TermTF(term="error", tf=1)])
    results = await shard.get_postings_batch(["error", "timeout", "missing"])
    by_term = {t: (dids, df) for t, dids, df in results}
    assert by_term["error"] == (["d1", "d2"], 2)
    assert by_term["timeout"] == (["d1"], 1)
    assert by_term["missing"] == ([], 0)
    assert [t for t, _, _ in results] == ["error", "timeout", "missing"]


@pytest.mark.asyncio
async def test_term_frequency_roundtrip(shard: NodeShard):
    await shard.index_terms("d1", [TermTF(term="error", tf=7)])
    assert await shard.get_term_frequency("d1", "error") == 7
    assert await shard.get_term_frequency("d1", "absent") == 0
    assert await shard.get_term_frequency("missing-doc", "error") == 0


@pytest.mark.asyncio
async def test_stats_empty(shard: NodeShard):
    stats = await shard.stats()
    assert stats == {"term_count": 0, "document_count": 0}
