"""Integration tests for the C5 incident HTTP surface.

Drive ``POST /api/analyze-incident`` and ``GET /api/incidents[/{id}]`` end-to-end
through the injected-runtime :class:`~fastapi.testclient.TestClient` (the ``client``
fixture in ``tests/conftest.py`` wires a fresh :class:`~src.main.Runtime` and skips the
lifespan, so each test starts from an empty history). Coverage:

* the happy path against a generated ground-truth cascade — a fully-assembled report
  whose top root cause is the injected root and whose impact has a real blast radius;
* history growth, newest-first ordering, and the ``?limit`` query param;
* single-incident lookup by id, and a 404 for an unknown id;
* a malformed body (missing required fields) -> 422 from pydantic;
* a structurally-valid body with an unparseable ``timestamp`` -> 422 from the analyzer's
  ``ValueError`` (never a 500);
* the bounded history — an analyzer with a small ``max_incident_history`` retains only
  the most recent reports.
"""

from fastapi.testclient import TestClient

from src.api import create_app
from src.config import Settings
from src.generators import generate_incident
from src.main import Runtime


def _events_json(seed: int) -> list[dict]:
    """The generated scenario's events as JSON-ready dicts (the POST body)."""
    return [event.model_dump() for event in generate_incident(seed=seed).events]


# --- Happy path ------------------------------------------------------------------


def test_analyze_incident_returns_full_report(client):
    scenario = generate_incident(seed=1)
    payload = [event.model_dump() for event in scenario.events]

    response = client.post("/api/analyze-incident", json=payload)

    assert response.status_code == 200
    report = response.json()
    # Every stage populated the report.
    assert report["timeline"], "timeline must be reconstructed"
    assert report["root_causes"], "root causes must be ranked"
    assert report["causal_graph"]["nodes"], "causal graph must have nodes"

    impact = report["impact_analysis"]
    assert impact["blast_radius"] > 0
    assert impact["affected_services"], "affected services must be non-empty"

    # The injected ground-truth root ranks #1.
    assert report["root_causes"][0]["event_id"] == scenario.root_cause_event_id


# --- History listing -------------------------------------------------------------


def test_incidents_history_grows_and_is_newest_first(client):
    assert client.get("/api/incidents").json() == []  # empty before any analysis

    first = client.post("/api/analyze-incident", json=_events_json(1)).json()
    second = client.post("/api/analyze-incident", json=_events_json(2)).json()

    listing = client.get("/api/incidents").json()
    assert len(listing) == 2
    # Newest-first: the most recently analyzed incident comes first.
    assert listing[0]["incident_id"] == second["incident_id"]
    assert listing[1]["incident_id"] == first["incident_id"]

    # ?limit=1 returns only the single most recent report.
    limited = client.get("/api/incidents", params={"limit": 1}).json()
    assert len(limited) == 1
    assert limited[0]["incident_id"] == second["incident_id"]


# --- Single-incident lookup ------------------------------------------------------


def test_get_incident_by_id_returns_same_report(client):
    posted = client.post("/api/analyze-incident", json=_events_json(3)).json()
    incident_id = posted["incident_id"]

    fetched = client.get(f"/api/incidents/{incident_id}")

    assert fetched.status_code == 200
    body = fetched.json()
    assert body["incident_id"] == incident_id
    assert body["root_causes"][0]["event_id"] == posted["root_causes"][0]["event_id"]


def test_get_unknown_incident_is_404(client):
    response = client.get("/api/incidents/does-not-exist")
    assert response.status_code == 404


# --- Bad input (422, never 500) --------------------------------------------------


def test_malformed_body_is_422(client):
    # Missing the required timestamp/level/message fields -> pydantic rejects it.
    response = client.post("/api/analyze-incident", json=[{"service": "x"}])
    assert response.status_code == 422


def test_unparseable_timestamp_is_422(client):
    # Structurally valid LogEvent, but the timeline stage cannot parse the timestamp;
    # the analyzer's ValueError is mapped to 422 rather than surfacing as a 500.
    bad = [
        {
            "timestamp": "not-a-date",
            "service": "database",
            "level": "ERROR",
            "message": "x",
        }
    ]
    response = client.post("/api/analyze-incident", json=bad)
    assert response.status_code == 422


# --- Bounded history -------------------------------------------------------------


def test_history_is_bounded_by_max_incident_history():
    # A dedicated app whose analyzer caps history at 3 (bypass any ambient .env).
    app = create_app(
        runtime=Runtime.build(Settings(_env_file=None, max_incident_history=3))
    )
    client = TestClient(app)

    posted_ids: list[str] = []
    for seed in range(5):
        response = client.post("/api/analyze-incident", json=_events_json(seed))
        assert response.status_code == 200
        posted_ids.append(response.json()["incident_id"])

    listing = client.get("/api/incidents").json()
    assert len(listing) <= 3
    # Exactly the 3 most recent, newest-first (the oldest two were trimmed).
    returned_ids = [report["incident_id"] for report in listing]
    assert returned_ids == list(reversed(posted_ids))[:3]
