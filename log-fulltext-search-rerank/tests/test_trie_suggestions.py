"""Tests for the prefix trie and the ``/api/search/suggestions`` route.

Unit tests cover the trie's behavioural contract: insert/suggest,
frequency-based ranking with alphabetical tiebreaker, empty-prefix
semantics, missing-prefix behaviour, ``clear()`` semantics, and a
generous latency sanity check at 50k tokens (the real target is
<5ms, but CI sandboxes are slow — 100ms is a safety net, not a
performance gate).

Route tests cover the HTTP surface: empty-index returns ``[]``,
ingest-then-suggest reflects the doc-frequency ordering, ``limit``
above the max trips pydantic 422, and the trie is rebuilt lazily
when the index version advances between calls.
"""

from __future__ import annotations

import random
import string
import time

import pytest
import pytest_asyncio

from src.config import get_settings
from src.index.inverted_index import InvertedIndex
from src.index.tokenizer import LogTokenizer
from src.index.trie import PrefixTrie
from src.main import app


# ---------------------------------------------------------------------------
# Autouse: fresh index + trie on every test
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(autouse=True)
async def _fresh_state():
    """Reset ``app.state.index`` and ``app.state.trie`` per test.

    Same pattern ``test_api_logs.py`` uses for isolation — without it,
    a suggestion test that ingests 500 docs would leak them into the
    next test's trie.
    """
    settings = get_settings()
    tokenizer = LogTokenizer(settings)
    app.state.index = InvertedIndex(settings, tokenizer)
    app.state.tokenizer = tokenizer
    app.state.trie = PrefixTrie()
    yield


# ---------------------------------------------------------------------------
# Unit tests on PrefixTrie
# ---------------------------------------------------------------------------

def test_fresh_trie_is_empty() -> None:
    """An untouched trie reports zero tokens and returns no suggestions."""
    t = PrefixTrie()
    assert t.token_count == 0
    assert t.suggest("auth", 5) == []


def test_insert_and_exact_prefix_suggest() -> None:
    """A single inserted token is surfaced by its prefix."""
    t = PrefixTrie()
    t.insert("authentication")
    assert t.suggest("auth", 5) == ["authentication"]
    assert t.token_count == 1


def test_frequency_based_ordering() -> None:
    """Higher-freq tokens appear first; alphabetical breaks ties."""
    t = PrefixTrie()
    t.insert("auth", freq=3)
    t.insert("authenticate", freq=7)
    t.insert("authentication", freq=1)
    out = t.suggest("auth", 10)
    assert out == ["authenticate", "auth", "authentication"]


def test_alphabetical_tiebreaker_for_equal_freq() -> None:
    """Tokens with identical freq order alphabetically."""
    t = PrefixTrie()
    t.insert("apple", freq=1)
    t.insert("apricot", freq=1)
    assert t.suggest("ap", 10) == ["apple", "apricot"]


def test_missing_prefix_returns_empty() -> None:
    """A prefix no token starts with yields an empty list."""
    t = PrefixTrie()
    t.insert("authentication")
    assert t.suggest("xyz", 5) == []


def test_empty_prefix_returns_empty() -> None:
    """Empty prefix is treated as "no query" — returns ``[]``."""
    t = PrefixTrie()
    t.insert("authentication")
    assert t.suggest("", 5) == []


def test_limit_is_respected() -> None:
    """Inserting 20 ``e*`` tokens and asking for 5 returns exactly 5."""
    t = PrefixTrie()
    # Use distinct suffixes so all 20 tokens share the ``e`` prefix.
    for i in range(20):
        t.insert(f"event{i:02d}", freq=1)
    out = t.suggest("e", 5)
    assert len(out) == 5


def test_clear_resets_trie() -> None:
    """``clear()`` empties the trie and drops the token count."""
    t = PrefixTrie()
    t.insert("authentication")
    assert t.token_count == 1
    t.clear()
    assert t.token_count == 0
    assert t.suggest("auth", 5) == []


def test_repeated_insert_does_not_inflate_token_count() -> None:
    """Reinserting the same token bumps freq but not the distinct count.

    Important for the lazy-rebuild path — the route shouldn't have to
    worry about seeing the same token twice.
    """
    t = PrefixTrie()
    t.insert("error", freq=2)
    t.insert("error", freq=3)
    assert t.token_count == 1
    # The first suggestion should be ``error`` with the combined 5 freq,
    # verifiable indirectly by comparing against a lower-freq sibling.
    t.insert("errata", freq=4)
    assert t.suggest("err", 5) == ["error", "errata"]


def test_latency_under_100ms_at_50k_tokens() -> None:
    """Sanity: suggest on a 50k-token trie stays below 100ms.

    The real perf target is <5ms, but CI sandboxes are noisy enough
    that asserting 5ms would flake. 100ms is a very generous upper
    bound — any regression that blows past it (e.g. an accidental
    full-trie scan per call) will trip this assertion reliably.
    """
    t = PrefixTrie()
    rng = random.Random(42)
    alphabet = string.ascii_lowercase
    for _ in range(50_000):
        length = rng.randint(4, 12)
        token = "".join(rng.choice(alphabet) for _ in range(length))
        t.insert(token, freq=rng.randint(1, 10))

    start = time.perf_counter()
    t.suggest("a", 10)
    elapsed_ms = (time.perf_counter() - start) * 1000
    assert elapsed_ms < 100, f"suggest took {elapsed_ms:.2f}ms, expected <100ms"


# ---------------------------------------------------------------------------
# Route tests: /api/search/suggestions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_suggestions_empty_index_returns_empty_list(async_client) -> None:
    """With no ingested logs the suggestions endpoint returns ``[]``."""
    resp = await async_client.get("/api/search/suggestions?q=err&limit=5")
    assert resp.status_code == 200
    assert resp.json() == {"suggestions": []}


@pytest.mark.asyncio
async def test_suggestions_reflect_ingested_tokens(async_client) -> None:
    """After ingesting logs, suggestions include tokens from the corpus.

    Ingest several entries that share a prefix. The suggestions for
    that prefix must be non-empty and each suggestion must start with
    the prefix.
    """
    bulk = {
        "entries": [
            {"message": "authentication failed for user", "timestamp": 1.0},
            {"message": "authorization error on service", "timestamp": 2.0},
            {"message": "database timeout occurred", "timestamp": 3.0},
        ]
    }
    resp = await async_client.post("/api/logs/bulk", json=bulk)
    assert resp.status_code == 202

    resp = await async_client.get("/api/search/suggestions?q=auth&limit=10")
    assert resp.status_code == 200
    suggestions = resp.json()["suggestions"]
    assert len(suggestions) > 0
    assert all(s.startswith("auth") for s in suggestions)


@pytest.mark.asyncio
async def test_suggestions_ordered_by_doc_frequency(async_client) -> None:
    """Tokens appearing in more docs rank above rarer ones.

    Insert one message mentioning ``erroneous`` and three messages
    mentioning ``error`` — the doc frequency of ``error`` is strictly
    larger so it must come first. (We avoid plural nouns like ``errata``
    here because the shared tokenizer would lemmatize them, collapsing
    the comparison.)
    """
    bulk = {
        "entries": [
            {"message": "error first", "timestamp": 1.0},
            {"message": "error second", "timestamp": 2.0},
            {"message": "error third", "timestamp": 3.0},
            {"message": "erroneous once", "timestamp": 4.0},
        ]
    }
    resp = await async_client.post("/api/logs/bulk", json=bulk)
    assert resp.status_code == 202

    resp = await async_client.get("/api/search/suggestions?q=err&limit=10")
    assert resp.status_code == 200
    suggestions = resp.json()["suggestions"]
    # Both tokens land in the result; ``error`` must precede ``erroneous``.
    assert "error" in suggestions
    assert "erroneous" in suggestions
    assert suggestions.index("error") < suggestions.index("erroneous")


@pytest.mark.asyncio
async def test_suggestions_limit_above_max_rejected(async_client) -> None:
    """``limit`` above 100 trips the pydantic/FastAPI validator."""
    resp = await async_client.get("/api/search/suggestions?q=a&limit=101")
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_suggestions_case_insensitive(async_client) -> None:
    """Uppercase query is lowercased before lookup.

    The tokenizer stores lowercased tokens, and the route lowercases
    ``q`` to match. ``AUTH`` should surface ``authentication``.
    """
    resp = await async_client.post(
        "/api/logs",
        json={"message": "authentication failed", "timestamp": 1.0},
    )
    assert resp.status_code == 202

    resp = await async_client.get("/api/search/suggestions?q=AUTH&limit=5")
    assert resp.status_code == 200
    suggestions = resp.json()["suggestions"]
    assert any(s.startswith("auth") for s in suggestions)


@pytest.mark.asyncio
async def test_suggestions_reflect_index_version_bump(async_client) -> None:
    """A second ingest updates the suggestions seen by a later call.

    Validates the lazy-rebuild path: first call populates the trie
    from version N, second ingest bumps the index version, third
    call must see the new tokens.
    """
    resp = await async_client.post(
        "/api/logs",
        json={"message": "database failure", "timestamp": 1.0},
    )
    assert resp.status_code == 202

    # First suggestion pass — trie gets populated from the first
    # ingest's tokens.
    resp = await async_client.get("/api/search/suggestions?q=data&limit=10")
    assert resp.status_code == 200
    first = resp.json()["suggestions"]
    assert any(s.startswith("data") for s in first)

    # Second ingest introduces a new ``database-scale`` token that
    # lemmatises/tokenises under ``data*`` — verify the trie picks
    # it up after the index version advances.
    resp = await async_client.post(
        "/api/logs",
        json={"message": "datacenter outage", "timestamp": 2.0},
    )
    assert resp.status_code == 202

    resp = await async_client.get("/api/search/suggestions?q=data&limit=10")
    assert resp.status_code == 200
    second = resp.json()["suggestions"]
    # The second call must reflect ``datacenter`` (or at least be a
    # superset of the first — lemmatisation may map to stems). The
    # strictest invariant we can assert without coupling to the
    # tokenizer's WordNet behaviour is that the union of tokens
    # grew.
    assert len(second) >= len(first)
    # And specifically, *some* token starting with ``datac`` now
    # shows up — that's the ingest-just-happened signal.
    assert any(s.startswith("datac") for s in second)
