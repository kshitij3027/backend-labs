"""Integration tests for the analyze API against a fully loaded NLPEngine.

Every test drives the real HTTP surface through the session-scoped ``loaded_client``
(``conftest.py``): the response schema of ``/api/analyze`` and ``/api/analyze/batch``, request
validation, the readiness-reflecting health probe, and the debug memory endpoint.
"""

from src.generators import INTENTS
from src.nlp.intent import OTHER_LABEL
from src.nlp.sentiment import SENTIMENT_LABELS

#: Every label the intent field may legitimately carry — a real intent or the reject bucket.
VALID_INTENTS = set(INTENTS) | {OTHER_LABEL}

MESSAGE = "auth-svc rejected login for user 4821 from 10.0.0.1: invalid token"


def _assert_analysis(body: dict, expected_message: str) -> None:
    """Assert ``body`` is a full, well-typed, in-range AnalysisResponse for ``expected_message``."""
    assert set(body) == {"message", "entities", "intent", "sentiment", "keywords"}
    assert body["message"] == expected_message

    assert isinstance(body["entities"], list)
    for entity in body["entities"]:
        assert isinstance(entity["text"], str)
        assert isinstance(entity["label"], str)

    intent = body["intent"]
    assert set(intent) == {"label", "confidence"}
    assert intent["label"] in VALID_INTENTS
    assert isinstance(intent["confidence"], float)
    assert 0.0 <= intent["confidence"] <= 1.0

    sentiment = body["sentiment"]
    assert set(sentiment) == {"label", "score"}
    assert sentiment["label"] in SENTIMENT_LABELS
    assert isinstance(sentiment["score"], float)
    assert -1.0 <= sentiment["score"] <= 1.0

    assert isinstance(body["keywords"], list)
    assert all(isinstance(keyword, str) for keyword in body["keywords"])


# --------------------------------------------------------------------------------------
# POST /api/analyze
# --------------------------------------------------------------------------------------
def test_analyze_returns_full_schema(loaded_client):
    response = loaded_client.post("/api/analyze", json={"message": MESSAGE})
    assert response.status_code == 200
    _assert_analysis(response.json(), MESSAGE)


def test_analyze_missing_message_is_422(loaded_client):
    # `message` is required — omitting it is a request-validation error, not a 500.
    response = loaded_client.post("/api/analyze", json={})
    assert response.status_code == 422


# --------------------------------------------------------------------------------------
# POST /api/analyze/batch
# --------------------------------------------------------------------------------------
def test_analyze_batch_returns_envelope_and_preserves_order(loaded_client):
    messages = [
        MESSAGE,
        "deployment of payments-api succeeded on gateway",
        "cpu usage high on db-01 worker",
    ]
    response = loaded_client.post("/api/analyze/batch", json={"messages": messages})
    assert response.status_code == 200

    body = response.json()
    assert set(body) == {"results", "count"}
    assert body["count"] == len(messages)
    assert len(body["results"]) == len(messages)
    # Order preserved and each result is a full analysis of its input line.
    assert [result["message"] for result in body["results"]] == messages
    for message, result in zip(messages, body["results"]):
        _assert_analysis(result, message)


def test_analyze_batch_empty_messages_is_200_with_empty_results(loaded_client):
    # An empty batch is a valid (no-op) request: 200 with an empty envelope, not a 422.
    response = loaded_client.post("/api/analyze/batch", json={"messages": []})
    assert response.status_code == 200
    assert response.json() == {"results": [], "count": 0}


# --------------------------------------------------------------------------------------
# GET /api/health — reflects real readiness on the loaded app
# --------------------------------------------------------------------------------------
def test_health_reports_ready_when_engine_loaded(loaded_client):
    response = loaded_client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy", "analyzer_ready": True}


# --------------------------------------------------------------------------------------
# GET /api/debug/memory
# --------------------------------------------------------------------------------------
def test_debug_memory_reports_positive_rss(loaded_client):
    response = loaded_client.get("/api/debug/memory")
    assert response.status_code == 200
    body = response.json()
    assert isinstance(body["memory_mb"], (int, float))
    assert body["memory_mb"] > 0
