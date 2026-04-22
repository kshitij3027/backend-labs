"""Tests for :class:`src.index.tokenizer.LogTokenizer`.

The tokenizer is deterministic given a pinned ``nltk==3.9.1`` and a
baked WordNet corpus, so these tests assert exact tokens for cases
that are unambiguous. For cases where WordNet lemma drift could bite
(e.g. verb-vs-noun lemmatization with default noun POS), the
assertions use containment rather than equality so the suite stays
stable across micro patches.

All tests reuse the ``settings`` fixture from ``conftest.py``; the
tokenizer is built fresh per test so stopword caches and lemma caches
never cross pollute assertions.
"""

from __future__ import annotations

import pytest

from src.index.tokenizer import LogTokenizer


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_string_returns_empty_list(settings):
    """Empty input must yield an empty token list, not raise."""
    tokenizer = LogTokenizer(settings)
    assert tokenizer.tokenize("") == []
    assert tokenizer.tokenize_query("") == []


def test_whitespace_only_input_returns_empty_list(settings):
    """Whitespace-only input has no matchable tokens."""
    tokenizer = LogTokenizer(settings)
    assert tokenizer.tokenize("   \t\n  ") == []


# ---------------------------------------------------------------------------
# Compound preservation
# ---------------------------------------------------------------------------


def test_ipv4_preserved_verbatim(settings):
    """``10.0.0.1`` must stay fused; four separate ints would be useless."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("server 10.0.0.1 down")
    assert "10.0.0.1" in tokens
    # And the splatter forms must NOT appear.
    assert "10" not in tokens
    assert "0" not in tokens
    assert "1" not in tokens


def test_uuid_preserved_verbatim(settings):
    """Canonical UUIDs stay as a single token."""
    tokenizer = LogTokenizer(settings)
    uuid = "550e8400-e29b-41d4-a716-446655440000"
    tokens = tokenizer.tokenize(f"request {uuid} failed")
    assert uuid in tokens


def test_url_preserved_verbatim(settings):
    """URLs keep their scheme, host, path, and query string intact."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("see https://example.com/path?x=1 for details")
    assert "https://example.com/path?x=1" in tokens


def test_email_preserved_verbatim(settings):
    """Email addresses stay whole — no splitting on ``@`` or ``.``."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("user@example.com logged in")
    assert "user@example.com" in tokens


def test_dotted_identifier_preserved_and_lowercased(settings):
    """Dotted identifiers (Java-style package paths) stay fused, lowercased."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("com.example.App failed")
    assert "com.example.app" in tokens


def test_iso_timestamp_preserved(settings):
    """ISO-8601 timestamps with a ``Z`` suffix stay as one token."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("at 2025-07-07T10:23:45Z error")
    assert "2025-07-07t10:23:45z" in tokens


def test_iso_timestamp_with_offset_preserved(settings):
    """ISO-8601 timestamps with a numeric offset stay fused."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("ts 2025-01-15T08:30:00+05:30 info")
    # Accept either ``+05:30`` or ``+0530`` depending on regex class;
    # this assertion uses startswith to stay tolerant.
    assert any(t.startswith("2025-01-15t08:30:00") for t in tokens)


def test_http_status_code_preserved(settings):
    """3-digit HTTP codes become first-class tokens of their own."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("returned 500 on /orders")
    assert "500" in tokens


def test_multiple_compound_types_in_one_line(settings):
    """A realistic log line with several structured fragments survives."""
    tokenizer = LogTokenizer(settings)
    line = (
        "2025-07-07T10:23:45Z host 10.0.0.1 "
        "user@example.com GET https://api.example.com/v1 500"
    )
    tokens = tokenizer.tokenize(line)
    assert "2025-07-07t10:23:45z" in tokens
    assert "10.0.0.1" in tokens
    assert "user@example.com" in tokens
    assert "https://api.example.com/v1" in tokens
    assert "500" in tokens


# ---------------------------------------------------------------------------
# Stopword filtering
# ---------------------------------------------------------------------------


def test_english_stopwords_filtered(settings):
    """With the default English stopword list, ``the``/``is`` drop out."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("the error is here")
    # ``the`` and ``is`` are canonical English stopwords.
    assert "the" not in tokens
    assert "is" not in tokens
    # The content word survives.
    assert "error" in tokens


# ---------------------------------------------------------------------------
# Minimum-length filter
# ---------------------------------------------------------------------------


def test_short_word_tokens_dropped_in_tokenize(settings):
    """With ``min_token_length=3`` (default), ``it``/``is``/``an`` drop."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("it is an error")
    # All three are shorter than 3 characters OR are stopwords.
    assert "it" not in tokens
    assert "is" not in tokens
    assert "an" not in tokens
    assert "error" in tokens


def test_tokenize_query_keeps_short_tokens(settings):
    """``tokenize_query`` must preserve intent-critical short words."""
    tokenizer = LogTokenizer(settings)
    # "go" is two characters — tokenize drops it, tokenize_query keeps it.
    assert tokenizer.tokenize("go") == []
    query_tokens = tokenizer.tokenize_query("go")
    assert "go" in query_tokens


def test_tokenize_query_preserves_three_char_intent_word(settings):
    """Exact-threshold words like ``api`` are present in both modes."""
    tokenizer = LogTokenizer(settings)
    # ``api`` is 3 chars, so with min_token_length=3 it survives in
    # both tokenize (>=3) and tokenize_query (>=1).
    assert "api" in tokenizer.tokenize("api")
    assert "api" in tokenizer.tokenize_query("api")


# ---------------------------------------------------------------------------
# Lemmatization
# ---------------------------------------------------------------------------


def test_plural_noun_lemmatized_to_singular(settings):
    """WordNet default POS (noun) turns ``tests`` into ``test``."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("running tests passed")
    assert "test" in tokens


def test_compound_tokens_not_lemmatized(settings):
    """IPs/URLs/etc. must bypass the lemmatizer — they are emitted raw."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("hit https://example.com/orders")
    # If the lemmatizer touched the URL, it would not round-trip.
    assert "https://example.com/orders" in tokens


# ---------------------------------------------------------------------------
# Lowercasing
# ---------------------------------------------------------------------------


def test_uppercase_word_lowercased(settings):
    """Plain words get lowercased regardless of source casing."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("ERROR on path")
    assert "error" in tokens


def test_mixed_case_dotted_identifier_lowercased(settings):
    """Compound tokens get lowercased too so case never breaks equality."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("com.Example.MyService booted")
    assert "com.example.myservice" in tokens


# ---------------------------------------------------------------------------
# Punctuation handling
# ---------------------------------------------------------------------------


def test_edge_punctuation_stripped_from_words(settings):
    """Leading/trailing parens and commas stripped; ``error`` emerges clean."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("(error),")
    assert "error" in tokens
    # And the punctuated form is NOT present.
    assert "(error)," not in tokens


# ---------------------------------------------------------------------------
# Idempotency / cache behavior
# ---------------------------------------------------------------------------


def test_repeated_tokenize_is_idempotent(settings):
    """Two back-to-back calls on the same text produce identical lists."""
    tokenizer = LogTokenizer(settings)
    first = tokenizer.tokenize("error error error")
    second = tokenizer.tokenize("error error error")
    assert first == second
    # Internal check: the lemma cache has at least the ``error`` entry.
    assert "error" in tokenizer._lemma_cache


def test_lemma_cache_bounded(settings):
    """Once the cache is at the cap, further inserts evict oldest first.

    Seed the cache up to the cap, then force one real insert through
    ``_lemma`` and verify the size stayed at the cap — the oldest
    entry was evicted to make room. This is the invariant that keeps
    adversarial tokenizations from blowing up memory.
    """
    tokenizer = LogTokenizer(settings)
    # Pre-seed exactly up to the cap without going over. Synthesizing
    # cheap keys avoids 50 000 lemmatizer calls in the test itself.
    for i in range(50_000):
        tokenizer._lemma_cache[f"token{i}"] = f"token{i}"
    assert len(tokenizer._lemma_cache) == 50_000
    # Force one real insert — eviction must fire before the new entry lands.
    _ = tokenizer._lemma("freshword")
    assert len(tokenizer._lemma_cache) == 50_000
    # The freshly-inserted key is present; the oldest seeded key is gone.
    assert "freshword" in tokenizer._lemma_cache
    assert "token0" not in tokenizer._lemma_cache


# ---------------------------------------------------------------------------
# Non-empty output sanity
# ---------------------------------------------------------------------------


def test_tokens_are_strings(settings):
    """All yielded tokens must be ``str`` — no bytes, ints, or Matches."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize("server 10.0.0.1 returned 500 error")
    assert tokens, "expected at least one token"
    assert all(isinstance(t, str) for t in tokens)


@pytest.mark.parametrize(
    "text,expected_present",
    [
        ("authentication failed for user", "authentication"),
        ("database connection timeout", "database"),
        ("request 12345 succeeded", "12345"),
    ],
)
def test_sample_log_lines_produce_expected_tokens(
    settings, text: str, expected_present: str
):
    """Smoke-test a few realistic log lines for the obvious content word."""
    tokenizer = LogTokenizer(settings)
    tokens = tokenizer.tokenize(text)
    assert expected_present in tokens
