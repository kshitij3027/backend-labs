"""Unit tests for :class:`src.stats.StatsAggregator` — the in-memory rolling aggregate stats.

These pin the behaviour ``GET /api/stats`` (and the C11/C12 dashboard) relies on: the
distributions and trending counts reflect exactly the results fed in; ``recent`` is a bounded,
newest-first buffer; :meth:`snapshot` always carries the full documented shape with the right
types; :meth:`update` is robust to malformed results and safe under concurrent threads (the
lock); and :meth:`reset` clears everything. No HTTP layer and no NLP models are involved —
results are hand-built dicts in the :meth:`~src.nlp.NLPEngine.analyze` schema.
"""

from __future__ import annotations

import threading

from src.stats import StatsAggregator

#: The exact set of keys every snapshot must expose (the frozen /api/stats body shape).
SNAPSHOT_KEYS = {
    "total_analyzed",
    "intent_distribution",
    "sentiment_distribution",
    "entity_type_distribution",
    "trending_keywords",
    "recent",
    "throughput_per_sec",
}


def _result(
    message: str = "some log line",
    intent: str | None = "error_report",
    sentiment: str | None = "negative",
    entities: list[tuple[str, str]] | None = None,
    keywords: list[str] | None = None,
):
    """Build a minimal, valid analyze-result dict in the NLPEngine.analyze schema.

    ``entities`` is given as ``(text, label)`` pairs; ``intent`` / ``sentiment`` are label
    strings (or ``None`` to omit the facet's label). Only the fields StatsAggregator reads are
    populated with realistic-but-minimal values.
    """
    entity_dicts = [
        {"text": text, "label": label, "start": 0, "end": len(text)}
        for text, label in (entities or [])
    ]
    result: dict = {
        "message": message,
        "entities": entity_dicts,
        "intent": {"label": intent, "confidence": 0.9},
        "sentiment": {"label": sentiment, "score": -0.5},
        "keywords": list(keywords or []),
    }
    return result


# --------------------------------------------------------------------------------------
# Aggregation math: distributions, trending, and total reflect the fed results exactly
# --------------------------------------------------------------------------------------
def test_update_accumulates_distributions_trending_and_total():
    agg = StatsAggregator(window=100, trending_top_k=10)
    agg.update(
        _result(
            intent="error_report",
            sentiment="negative",
            entities=[("db-01", "HOST"), ("4821", "USER")],
            keywords=["disk full", "timeout"],
        )
    )
    agg.update(
        _result(
            intent="error_report",
            sentiment="neutral",
            entities=[("db-01", "HOST")],
            keywords=["timeout"],
        )
    )
    agg.update(
        _result(
            intent="deploy_event",
            sentiment="positive",
            entities=[],
            keywords=["deploy"],
        )
    )

    snap = agg.snapshot()
    assert snap["total_analyzed"] == 3
    assert snap["intent_distribution"] == {"error_report": 2, "deploy_event": 1}
    assert snap["sentiment_distribution"] == {"negative": 1, "neutral": 1, "positive": 1}
    # Each entity's label is counted (HOST appears in two results, USER in one).
    assert snap["entity_type_distribution"] == {"HOST": 2, "USER": 1}

    trending = dict(tuple(pair) for pair in snap["trending_keywords"])
    assert trending["timeout"] == 2
    assert trending["disk full"] == 1
    assert trending["deploy"] == 1


# --------------------------------------------------------------------------------------
# recent: bounded by window, newest-first, message truncated
# --------------------------------------------------------------------------------------
def test_recent_is_bounded_by_window_and_newest_first():
    window = 5
    agg = StatsAggregator(window=window, trending_top_k=10)
    pushed = window + 3  # push MORE than the window so the oldest get evicted
    for i in range(pushed):
        agg.update(_result(message=f"line-{i}"))

    snap = agg.snapshot()
    recent = snap["recent"]
    assert len(recent) == window  # capped at the window size
    # Newest-first: the last pushed line is first, the oldest *retained* line is last.
    assert recent[0]["message"] == f"line-{pushed - 1}"
    assert recent[-1]["message"] == f"line-{pushed - window}"
    # total counts EVERY update, not just the retained recent items.
    assert snap["total_analyzed"] == pushed


def test_recent_message_is_truncated():
    agg = StatsAggregator(window=10)
    agg.update(_result(message="x" * 500))
    item = agg.snapshot()["recent"][0]
    assert len(item["message"]) <= 200
    assert item["message"] == "x" * 200


# --------------------------------------------------------------------------------------
# snapshot shape and types (throughput is a non-negative float)
# --------------------------------------------------------------------------------------
def test_snapshot_shape_and_types():
    agg = StatsAggregator()
    agg.update(_result(entities=[("h", "HOST")], keywords=["alpha", "beta"]))

    snap = agg.snapshot()
    assert set(snap) == SNAPSHOT_KEYS

    assert isinstance(snap["total_analyzed"], int)
    assert isinstance(snap["intent_distribution"], dict)
    assert isinstance(snap["sentiment_distribution"], dict)
    assert isinstance(snap["entity_type_distribution"], dict)

    assert isinstance(snap["trending_keywords"], list)
    for pair in snap["trending_keywords"]:
        assert isinstance(pair, list) and len(pair) == 2
        keyword, count = pair
        assert isinstance(keyword, str) and isinstance(count, int)

    assert isinstance(snap["recent"], list)
    for item in snap["recent"]:
        assert set(item) == {"message", "intent", "sentiment", "ts"}
        assert isinstance(item["message"], str)
        assert isinstance(item["ts"], float)

    assert isinstance(snap["throughput_per_sec"], float)
    assert snap["throughput_per_sec"] >= 0.0


# --------------------------------------------------------------------------------------
# Empty aggregator: zeros / empties, still a valid snapshot
# --------------------------------------------------------------------------------------
def test_empty_aggregator_snapshot_is_zeroed():
    snap = StatsAggregator().snapshot()
    assert set(snap) == SNAPSHOT_KEYS
    assert snap["total_analyzed"] == 0
    assert snap["intent_distribution"] == {}
    assert snap["sentiment_distribution"] == {}
    assert snap["entity_type_distribution"] == {}
    assert snap["trending_keywords"] == []
    assert snap["recent"] == []
    assert isinstance(snap["throughput_per_sec"], float)
    assert snap["throughput_per_sec"] == 0.0


# --------------------------------------------------------------------------------------
# Robustness: malformed / partial results never raise; other facets still update
# --------------------------------------------------------------------------------------
def test_malformed_result_does_not_raise_and_updates_other_facets():
    agg = StatsAggregator()
    # 'sentiment' missing entirely; 'entities' wrong-typed; keywords mix in non-strings.
    bad = {
        "message": "partial line",
        "intent": {"label": "error_report", "confidence": 0.4},
        "entities": None,
        "keywords": ["ok", 123, None, "ok2"],
    }
    agg.update(bad)  # must NOT raise

    snap = agg.snapshot()
    assert snap["total_analyzed"] == 1
    assert snap["intent_distribution"] == {"error_report": 1}
    assert snap["sentiment_distribution"] == {}  # skipped facet, no crash
    assert snap["entity_type_distribution"] == {}  # entities not a list -> skipped
    trending = dict(tuple(pair) for pair in snap["trending_keywords"])
    assert trending.get("ok") == 1 and trending.get("ok2") == 1  # non-strings filtered out
    # The recent item still records, with the missing sentiment label as None.
    assert snap["recent"][0]["sentiment"] is None
    assert snap["recent"][0]["intent"] == "error_report"


def test_missing_and_non_dict_facets_are_skipped():
    agg = StatsAggregator()
    agg.update({"message": "m", "keywords": ["k"]})  # no intent/sentiment/entities keys
    agg.update({"intent": "not-a-dict", "sentiment": ["bad"], "message": 999})  # wrong types

    snap = agg.snapshot()
    assert snap["total_analyzed"] == 2
    assert snap["intent_distribution"] == {}
    assert snap["sentiment_distribution"] == {}
    # newest-first: the second (message=999, non-str) is first and coerced to "".
    assert snap["recent"][0]["message"] == ""
    assert snap["recent"][0]["intent"] is None
    assert snap["recent"][1]["message"] == "m"


def test_non_dict_result_is_ignored():
    agg = StatsAggregator()
    for garbage in (None, "garbage", 42, ["not", "a", "dict"]):
        agg.update(garbage)  # type: ignore[arg-type]  # must not raise, must not count
    snap = agg.snapshot()
    assert snap["total_analyzed"] == 0
    assert snap["recent"] == []


# --------------------------------------------------------------------------------------
# Thread-safety: concurrent updates (+ a concurrent reader) stay exact and never raise
# --------------------------------------------------------------------------------------
def test_concurrent_updates_are_thread_safe():
    # window large enough to retain every update, so recent length is checkable too.
    agg = StatsAggregator(window=100_000, trending_top_k=10)
    thread_count = 8
    per_thread = 500
    errors: list[Exception] = []

    def worker() -> None:
        try:
            for _ in range(per_thread):
                agg.update(
                    _result(
                        intent="error_report",
                        sentiment="negative",
                        entities=[("h", "HOST")],
                        keywords=["kw"],
                    )
                )
        except Exception as exc:  # pragma: no cover - only hit if the lock is broken
            errors.append(exc)

    stop = threading.Event()

    def reader() -> None:
        # Snapshot in a tight loop concurrently with the writers: proves the lock protects the
        # copy-out (a bare dict(counter) would raise "changed size during iteration" otherwise).
        try:
            while not stop.is_set():
                agg.snapshot()
        except Exception as exc:  # pragma: no cover - only hit if the lock is broken
            errors.append(exc)

    reader_thread = threading.Thread(target=reader)
    reader_thread.start()
    workers = [threading.Thread(target=worker) for _ in range(thread_count)]
    for t in workers:
        t.start()
    for t in workers:
        t.join()
    stop.set()
    reader_thread.join()

    assert not errors, f"threads raised: {errors!r}"

    expected = thread_count * per_thread
    snap = agg.snapshot()
    # Exact counts prove no lost read-modify-writes under contention (the whole point of the lock).
    assert snap["total_analyzed"] == expected
    assert snap["intent_distribution"] == {"error_report": expected}
    assert snap["sentiment_distribution"] == {"negative": expected}
    assert snap["entity_type_distribution"] == {"HOST": expected}
    assert len(snap["recent"]) == expected
    assert dict(tuple(p) for p in snap["trending_keywords"])["kw"] == expected


# --------------------------------------------------------------------------------------
# reset clears everything and the aggregator is usable again afterwards
# --------------------------------------------------------------------------------------
def test_reset_clears_everything_and_stays_usable():
    agg = StatsAggregator()
    for _ in range(5):
        agg.update(_result(entities=[("h", "HOST")], keywords=["k"]))

    agg.reset()
    snap = agg.snapshot()
    assert snap["total_analyzed"] == 0
    assert snap["intent_distribution"] == {}
    assert snap["sentiment_distribution"] == {}
    assert snap["entity_type_distribution"] == {}
    assert snap["trending_keywords"] == []
    assert snap["recent"] == []
    assert snap["throughput_per_sec"] == 0.0

    # Still works after a reset.
    agg.update(_result())
    assert agg.snapshot()["total_analyzed"] == 1
