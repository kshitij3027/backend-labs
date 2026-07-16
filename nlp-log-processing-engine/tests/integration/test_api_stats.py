"""Integration tests for ``GET /api/stats`` against a fully loaded NLPEngine.

Drives the real HTTP surface via the session-scoped ``loaded_client`` (``conftest.py``):
posting analyses must be reflected in the rolling aggregates, and the ``/api/stats`` body must
always carry the full documented shape with the right types.

Because ``loaded_client`` is **session-scoped and shared** (other tests post analyses through
it too), these tests assert RELATIVE deltas on ``total_analyzed`` — never an absolute zero.
"""

from __future__ import annotations

#: The exact set of keys the /api/stats body must always expose.
STATS_KEYS = {
    "total_analyzed",
    "intent_distribution",
    "sentiment_distribution",
    "entity_type_distribution",
    "trending_keywords",
    "recent",
    "throughput_per_sec",
}

#: Varied, keyword-rich lines across several intents so the distributions and trending panel
#: get real content to reflect.
MESSAGES = [
    "auth-svc rejected login for user 4821 from 10.0.0.1: invalid token",
    "deployment of payments-api succeeded on gateway",
    "cpu usage high on db-01 worker",
    "disk full on host web-03, write failed",
    "connection pool exhausted on db-02",
]


def _get_stats(loaded_client) -> dict:
    """GET /api/stats and assert it is a 200 with the full documented key set."""
    response = loaded_client.get("/api/stats")
    assert response.status_code == 200
    body = response.json()
    assert set(body) == STATS_KEYS
    return body


# --------------------------------------------------------------------------------------
# Shape: /api/stats always returns the full documented body with correct types
# --------------------------------------------------------------------------------------
def test_stats_shape_is_complete(loaded_client):
    body = _get_stats(loaded_client)

    assert isinstance(body["total_analyzed"], int)
    assert isinstance(body["intent_distribution"], dict)
    assert isinstance(body["sentiment_distribution"], dict)
    assert isinstance(body["entity_type_distribution"], dict)

    assert isinstance(body["trending_keywords"], list)
    for pair in body["trending_keywords"]:
        assert isinstance(pair, list) and len(pair) == 2
        assert isinstance(pair[0], str) and isinstance(pair[1], int)

    assert isinstance(body["recent"], list)
    for item in body["recent"]:
        assert set(item) == {"message", "intent", "sentiment", "ts"}
        assert isinstance(item["message"], str)
        assert isinstance(item["ts"], float)

    # Through JSON a whole-number rate could arrive as int, so accept either — it is a
    # non-negative number regardless.
    assert isinstance(body["throughput_per_sec"], (int, float))
    assert body["throughput_per_sec"] >= 0.0


# --------------------------------------------------------------------------------------
# Posting analyses updates the rolling stats by exactly the number posted (relative delta)
# --------------------------------------------------------------------------------------
def test_posting_analyses_updates_stats_by_delta(loaded_client):
    before = _get_stats(loaded_client)
    t0 = before["total_analyzed"]

    posted_intents: list[str] = []
    posted_sentiments: list[str] = []
    for message in MESSAGES:
        response = loaded_client.post("/api/analyze", json={"message": message})
        assert response.status_code == 200
        result = response.json()
        posted_intents.append(result["intent"]["label"])
        posted_sentiments.append(result["sentiment"]["label"])

    after = _get_stats(loaded_client)

    # total_analyzed advanced by exactly the number of messages we posted.
    assert after["total_analyzed"] == t0 + len(MESSAGES)

    # Every intent / sentiment label we actually got back is present in the distributions.
    for intent_label in set(posted_intents):
        assert after["intent_distribution"].get(intent_label, 0) >= 1
    for sentiment_label in set(posted_sentiments):
        assert after["sentiment_distribution"].get(sentiment_label, 0) >= 1

    # Trending and recent are populated after real analyses.
    assert after["trending_keywords"], "expected a non-empty trending panel after analyses"
    assert after["recent"], "expected a non-empty recent feed after analyses"

    # recent is newest-first: the last line we posted is the most recent entry (short enough
    # to survive the ~200-char truncation intact).
    assert after["recent"][0]["message"] == MESSAGES[-1]


# --------------------------------------------------------------------------------------
# A batch analyze also feeds the same rolling stats
# --------------------------------------------------------------------------------------
def test_batch_analyze_updates_stats_by_delta(loaded_client):
    t0 = _get_stats(loaded_client)["total_analyzed"]

    response = loaded_client.post("/api/analyze/batch", json={"messages": MESSAGES})
    assert response.status_code == 200

    after = _get_stats(loaded_client)
    assert after["total_analyzed"] == t0 + len(MESSAGES)
