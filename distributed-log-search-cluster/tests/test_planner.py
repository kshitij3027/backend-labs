"""Pure unit tests for QueryPlanner merge/score logic (no network)."""

from __future__ import annotations

from coordinator.planner import QueryPlanner
from shared.models import PostingEntry


def _planner() -> QueryPlanner:
    return QueryPlanner(
        registry=None, ring=None, tokenizer=None, client=None, shared_redis=None
    )


def _p(term: str, doc_ids: list[str]) -> PostingEntry:
    return PostingEntry(term=term, doc_ids=doc_ids, doc_frequency=len(doc_ids))


def test_merge_and_intersection():
    planner = _planner()
    postings = {
        "error": [_p("error", ["d1", "d2", "d3"])],
        "timeout": [_p("timeout", ["d2", "d3", "d4"])],
    }
    result = planner.merge(postings, "AND", query_terms=["error", "timeout"])
    assert result == {"d2", "d3"}


def test_merge_or_union():
    planner = _planner()
    postings = {
        "error": [_p("error", ["d1", "d2"])],
        "timeout": [_p("timeout", ["d3"])],
    }
    result = planner.merge(postings, "OR", query_terms=["error", "timeout"])
    assert result == {"d1", "d2", "d3"}


def test_merge_and_with_missing_term_returns_empty():
    planner = _planner()
    postings = {
        "error": [_p("error", ["d1", "d2"])],
        # "timeout" missing entirely
    }
    result = planner.merge(postings, "AND", query_terms=["error", "timeout"])
    assert result == set()


def test_merge_or_skips_missing_terms():
    planner = _planner()
    postings = {
        "error": [_p("error", ["d1", "d2"])],
        # "timeout" missing
    }
    result = planner.merge(postings, "OR", query_terms=["error", "timeout"])
    assert result == {"d1", "d2"}


def test_score_favors_rare_terms():
    planner = _planner()
    # "rare" has df=1 (only d1), "common" has df=500 (d1..d500)
    common_docs = [f"d{i}" for i in range(1, 501)]
    postings = {
        "rare": [PostingEntry(term="rare", doc_ids=["d1"], doc_frequency=1)],
        "common": [
            PostingEntry(term="common", doc_ids=common_docs, doc_frequency=500)
        ],
    }
    # d1 matches both; d2 matches only common.
    scored = planner.score(
        {"d1", "d2"},
        postings,
        query_terms=["rare", "common"],
        total_docs_hint=1000,
    )
    lookup = dict(scored)
    # d1 benefits from both rare idf + common idf; d2 only common idf.
    assert lookup["d1"] > lookup["d2"]
    # And the first in sorted order should be d1.
    assert scored[0][0] == "d1"


def test_score_returns_sorted_desc():
    planner = _planner()
    postings = {
        "a": [PostingEntry(term="a", doc_ids=["d1"], doc_frequency=1)],
        "b": [PostingEntry(term="b", doc_ids=["d1", "d2"], doc_frequency=2)],
    }
    scored = planner.score(
        {"d1", "d2"}, postings, query_terms=["a", "b"], total_docs_hint=1000
    )
    # d1 hits both, d2 hits only b → d1 should come first.
    assert scored[0][0] == "d1"
    assert scored[0][1] >= scored[1][1]
