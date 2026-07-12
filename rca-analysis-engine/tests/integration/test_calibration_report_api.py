"""Integration tests for the C9 post-mortem + calibration HTTP surface.

Drive the three new routes end-to-end through the injected-runtime
:class:`~fastapi.testclient.TestClient` (the ``client`` fixture wires a fresh
:class:`~src.main.Runtime` and skips the lifespan, so each test starts from an empty
history *and* an unfitted calibrator). Coverage:

* ``GET /api/incidents/{id}/report`` returns 200 with non-empty ``markdown`` +
  ``recovery_points`` + ``classifications`` for an analyzed incident, and 404 for an
  unknown id;
* ``GET /api/calibration`` returns the stats shape, unfitted initially;
* ``POST /api/incidents/{id}/feedback`` accumulates one calibration sample per ranked
  candidate — feeding back a handful of generated incidents (each with its known
  ground-truth root) exceeds ``calibration_min_samples`` with both outcome classes, so a
  follow-up ``GET /api/calibration`` shows ``fitted: true`` with numeric Brier scores;
* an unknown incident -> 404 and a malformed feedback body -> 422.
"""

from src.generators import generate_incident


def _events_json(seed: int) -> list[dict]:
    """The generated scenario's events as JSON-ready dicts (the POST body)."""
    return [event.model_dump() for event in generate_incident(seed=seed).events]


# --- Report export ---------------------------------------------------------------


def test_incident_report_export_returns_full_post_mortem(client):
    scenario = generate_incident(seed=1)
    posted = client.post(
        "/api/analyze-incident", json=[e.model_dump() for e in scenario.events]
    ).json()
    incident_id = posted["incident_id"]

    response = client.get(f"/api/incidents/{incident_id}/report")

    assert response.status_code == 200
    body = response.json()
    assert body["incident_id"] == incident_id
    assert isinstance(body["markdown"], str) and body["markdown"].strip()
    assert incident_id in body["markdown"]
    assert "Recovery" in body["markdown"]
    # The structured artifacts are present and non-empty for a multi-level cascade.
    assert body["recovery_points"]
    assert body["classifications"]
    # Every event is classified exactly once.
    assert set(body["classifications"]) == {event["event_id"] for event in posted["events"]}


def test_report_unknown_incident_is_404(client):
    assert client.get("/api/incidents/does-not-exist/report").status_code == 404


# --- Calibration stats -----------------------------------------------------------


def test_calibration_stats_shape_unfitted_initially(client):
    response = client.get("/api/calibration")

    assert response.status_code == 200
    stats = response.json()
    assert set(stats) >= {
        "method",
        "n_samples",
        "fitted",
        "brier_raw",
        "brier_calibrated",
        "reliability_bins",
    }
    # A fresh analyzer has recorded no outcomes yet.
    assert stats["fitted"] is False
    assert stats["n_samples"] == 0
    assert stats["brier_raw"] is None
    assert stats["brier_calibrated"] is None
    assert stats["reliability_bins"] == []


# --- Feedback loop -> fitted calibration ------------------------------------------


def test_feedback_accumulates_samples_and_fits_calibration(client):
    # Analyze several incidents and feed back each one's KNOWN ground-truth root cause.
    # Each feedback records one sample per ranked candidate (the true root is the positive,
    # the rest negatives), so a few incidents exceed calibration_min_samples with both
    # classes present and the calibrator fits.
    for seed in range(4):
        scenario = generate_incident(seed=seed)
        posted = client.post(
            "/api/analyze-incident", json=[e.model_dump() for e in scenario.events]
        ).json()
        feedback = client.post(
            f"/api/incidents/{posted['incident_id']}/feedback",
            json={"true_root_cause_event_id": scenario.root_cause_event_id},
        )
        assert feedback.status_code == 200
        # The feedback endpoint returns the freshly-updated calibration stats.
        assert set(feedback.json()) >= {"fitted", "n_samples", "brier_raw"}

    final = client.get("/api/calibration").json()
    assert final["fitted"] is True
    assert final["n_samples"] >= 10
    # Both Brier scores are numeric once fitted with both classes present.
    assert isinstance(final["brier_raw"], float)
    assert isinstance(final["brier_calibrated"], float)
    assert len(final["reliability_bins"]) == 10


def test_feedback_unknown_incident_is_404(client):
    response = client.post(
        "/api/incidents/does-not-exist/feedback",
        json={"true_root_cause_event_id": "evt-0-000"},
    )
    assert response.status_code == 404


def test_feedback_malformed_body_is_422(client):
    posted = client.post("/api/analyze-incident", json=_events_json(1)).json()
    # Missing the required true_root_cause_event_id field -> pydantic rejects it.
    response = client.post(
        f"/api/incidents/{posted['incident_id']}/feedback", json={"wrong": "field"}
    )
    assert response.status_code == 422
