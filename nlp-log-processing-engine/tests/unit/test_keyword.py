"""Unit tests for the YAKE keyword extractor and trending aggregator (:mod:`src.nlp.keyword`).

These pin the behaviour C7 (the orchestrator) and C8 (``/api/stats`` trending) rely on:
``extract`` returns a small, best-first, de-duplicated list of surface keyphrases drawn from
the line; it is deterministic and capped at ``top_k``; it never raises on empty / degenerate
input (returning ``[]``); and — the whole point of choosing YAKE over rake-nltk — it works
with **no NLTK data present**. :class:`TrendingKeywords` counts correctly, normalises case,
breaks ties deterministically and resets cleanly.

Constructing the extractor loads YAKE's stop-word list, so a single default
:class:`KeywordAnalyzer` is shared across the module.
"""

from __future__ import annotations

import string

import pytest

from src.generators import sample_messages
from src.nlp.keyword import DEFAULT_TOP_K, KeywordAnalyzer, TrendingKeywords

#: A realistic, salient-word-rich log line reused by several extraction tests.
SALIENT_LINE = "auth-svc rejected login: invalid token from 10.0.0.1"

#: Salient content-word substrings from ``SALIENT_LINE`` (``token`` / ``auth`` / ``login`` are
#: the obvious ones; ``invalid`` / ``rejected`` round out the set). The assertion only requires
#: that *at least one* appears, so it is robust to YAKE's exact ranking / phrase choice while
#: still proving the output is real salient text from the line, not the IP or noise.
SALIENT_TERMS = ("token", "auth", "login", "invalid", "rejected")


@pytest.fixture(scope="module")
def analyzer() -> KeywordAnalyzer:
    """One default analyzer for the whole module (amortises the extractor construction)."""
    return KeywordAnalyzer()


def _words(phrase: str) -> list[str]:
    """Lower-case words of ``phrase`` with surrounding punctuation stripped (drops empties)."""
    out = []
    for raw in phrase.lower().split():
        word = raw.strip(string.punctuation)
        if word:
            out.append(word)
    return out


def _assert_valid_keyword_list(result, top_k: int = DEFAULT_TOP_K) -> None:
    """Shared shape check: a de-duplicated ``list[str]`` of length ``<= top_k``, no blanks."""
    assert isinstance(result, list)
    assert len(result) <= top_k
    assert all(isinstance(phrase, str) and phrase.strip() for phrase in result)
    lowered = [phrase.casefold() for phrase in result]
    assert len(lowered) == len(set(lowered)), f"case-insensitive duplicate in {result!r}"


# --------------------------------------------------------------------------------------
# Salient phrases: real keyphrases, drawn from the input, at least one obviously-salient
# --------------------------------------------------------------------------------------
def test_extract_returns_salient_phrases(analyzer):
    result = analyzer.extract(SALIENT_LINE)

    _assert_valid_keyword_list(result)
    assert result, "expected at least one keyphrase for a rich log line"

    # Every returned phrase is drawn from the input surface text: each of its words appears
    # (case-insensitively) in the line. YAKE returns surface n-grams, never stemmed tokens.
    lowered_line = SALIENT_LINE.lower()
    for phrase in result:
        for word in _words(phrase):
            assert word in lowered_line, f"{word!r} (from {phrase!r}) not in the input line"

    # At least one intuitively-salient term shows up somewhere in the results. Membership is
    # loose (substring, case-insensitive) so the test is robust to YAKE's exact ordering.
    joined = " ".join(result).casefold()
    assert any(term in joined for term in SALIENT_TERMS), (
        f"none of {SALIENT_TERMS} found in {result!r}"
    )


# --------------------------------------------------------------------------------------
# Determinism: same input -> identical output, across calls and across instances
# --------------------------------------------------------------------------------------
def test_determinism_across_calls_and_instances(analyzer):
    first = analyzer.extract(SALIENT_LINE)
    second = analyzer.extract(SALIENT_LINE)
    fresh = KeywordAnalyzer().extract(SALIENT_LINE)
    assert first == second == fresh

    # Scores are deterministic too (identical float values, not just the phrases).
    assert analyzer.extract_scored(SALIENT_LINE) == KeywordAnalyzer().extract_scored(SALIENT_LINE)


# --------------------------------------------------------------------------------------
# top_k is respected
# --------------------------------------------------------------------------------------
def test_top_k_is_respected():
    # A line with many distinct content words so YAKE genuinely has > 3 candidates to cap.
    line = (
        "database connection pool exhausted while replaying write-ahead log on host db-03 "
        "causing elevated query latency and dropped client connections"
    )
    small = KeywordAnalyzer(top_k=3)
    result = small.extract(line)
    assert len(result) <= 3
    _assert_valid_keyword_list(result, top_k=3)


# --------------------------------------------------------------------------------------
# De-duplication: no case-insensitive duplicate phrases in the output
# --------------------------------------------------------------------------------------
def test_no_case_insensitive_duplicates(analyzer):
    # Repeating a salient token in mixed case is the situation a case-insensitive dedupe must
    # collapse; whatever YAKE returns, the output must not contain two case-variants.
    result = analyzer.extract("TOKEN token Token invalid token expired token refresh token")
    lowered = [phrase.casefold() for phrase in result]
    assert len(lowered) == len(set(lowered)), f"duplicate case-variant survived in {result!r}"


# --------------------------------------------------------------------------------------
# Short / degenerate input: always [] (or a clean short list), never an exception
# --------------------------------------------------------------------------------------
@pytest.mark.parametrize("text", ["", "   ", "\n\t ", "!!!", "...", "-"])
def test_degenerate_input_returns_empty(analyzer, text):
    # Empty / whitespace / punctuation-only input must yield [] and must NOT raise — YAKE can
    # blow up on such input (statistics over an empty candidate set), so extract() guards it.
    assert analyzer.extract(text) == []
    assert analyzer.extract_scored(text) == []


def test_single_token_is_safe(analyzer):
    # A lone token may legitimately yield [] or ["ok"]; the contract is only "a clean list,
    # no exception". (Some YAKE builds raise on a single token — the guard turns that into [].)
    result = analyzer.extract("ok")
    _assert_valid_keyword_list(result)


# --------------------------------------------------------------------------------------
# No-NLTK smoke: a passing extraction *is* the proof YAKE needs no downloaded corpora
# --------------------------------------------------------------------------------------
def test_no_nltk_data_dependency(analyzer):
    # The Docker image installs NO nltk_data (that is precisely why YAKE was chosen over
    # rake-nltk). YAKE reads its stop-word list from inside its own wheel, so the mere fact
    # that this end-to-end extraction runs and returns keywords proves no NLTK data is needed.
    result = analyzer.extract("database connection pool exhausted on host db-03")
    _assert_valid_keyword_list(result)
    assert result, "a normal multi-word log line should yield at least one keyphrase"


# --------------------------------------------------------------------------------------
# extract_scored: (phrase, score) pairs, best-first (non-decreasing score), agrees w/ extract
# --------------------------------------------------------------------------------------
def test_extract_scored_shape_and_ordering(analyzer):
    scored = analyzer.extract_scored(SALIENT_LINE)
    assert scored, "expected scored keyphrases for a rich log line"

    for phrase, score in scored:
        assert isinstance(phrase, str) and phrase.strip()
        assert isinstance(score, float)

    # Lower YAKE score == more relevant, and results are best-first: scores never decrease.
    scores = [score for _, score in scored]
    assert scores == sorted(scores), f"scores not non-decreasing (best-first): {scores}"

    # extract() is exactly the phrase projection of extract_scored().
    assert analyzer.extract(SALIENT_LINE) == [phrase for phrase, _ in scored]


# --------------------------------------------------------------------------------------
# Realistic eyeball: run over varied generated log lines and assert every result is valid
# --------------------------------------------------------------------------------------
def test_over_sample_messages_never_crashes(analyzer):
    # Push ~10 realistic, mixed-intent lines (with timestamp/level noise, IPs, paths, etc.)
    # through extract() and assert each result is a clean, de-duplicated list[str] <= top_k.
    for sample in sample_messages(10, seed=1):
        result = analyzer.extract(sample.message)
        _assert_valid_keyword_list(result)


# --------------------------------------------------------------------------------------
# TrendingKeywords: counts, ordering, tie-breaking, case-normalisation, reset
# --------------------------------------------------------------------------------------
def test_trending_counts_and_ordering():
    trending = TrendingKeywords()
    trending.add(["timeout", "auth"])
    trending.add(["timeout", "deploy"])
    trending.add(["timeout"])
    trending.add(["auth"])

    top = trending.top(2)
    assert top == [("timeout", 3), ("auth", 2)]

    # top() with no arg returns everything seen, still most-frequent-first.
    assert trending.top() == [("timeout", 3), ("auth", 2), ("deploy", 1)]


def test_trending_ties_broken_alphabetically():
    trending = TrendingKeywords()
    # Added out of alphabetical order; each ends with count 1 -> ties must sort alphabetically
    # regardless of insertion order (the determinism guarantee the dashboard relies on).
    trending.add(["zebra", "mango", "apple"])
    top = trending.top(3)
    assert [kw for kw, _ in top] == ["apple", "mango", "zebra"]
    assert all(count == 1 for _, count in top)


def test_trending_normalises_case():
    trending = TrendingKeywords()
    trending.add(["Timeout", "TIMEOUT", "timeout"])
    # All three collapse to one lower-cased tally.
    assert trending.top() == [("timeout", 3)]


def test_trending_ignores_blank_and_respects_k_bounds():
    trending = TrendingKeywords()
    trending.add(["  ", "", "auth", "  auth  "])  # blanks ignored; "auth" counted twice
    assert trending.top() == [("auth", 2)]
    assert trending.top(0) == []
    assert trending.top(-5) == []


def test_trending_reset_clears_counts():
    trending = TrendingKeywords()
    trending.add(["a", "b", "a"])
    assert trending.top()  # non-empty before reset
    trending.reset()
    assert trending.top() == []
    # Usable again after reset.
    trending.add(["c"])
    assert trending.top() == [("c", 1)]
