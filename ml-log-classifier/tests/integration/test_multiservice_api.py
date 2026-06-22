"""Integration tests for the multi-service HTTP surface (Commit 11).

Drives the real wired app (:func:`src.api.create_app`) through Starlette's
:class:`~fastapi.testclient.TestClient`, so the FastAPI lifespan runs and
``app.state.multiservice`` is trained (tiny estimators) before any request:

* ``POST /classify/service`` returns the 8-key :class:`MultiServiceResponse`,
* the metrics aggregator's ``service_distribution`` is populated by those calls
  (and grows multiple service keys once varied logs are posted),
* ``GET /services`` reports ``status == "ready"`` and the three services,
* a **regression** guard: the base ``POST /classify`` still returns exactly its
  five keys (multi-service did not leak fields into it), and
* the untrained path (``auto_train=False`` + empty model dir) surfaces ``503`` on
  ``/classify/service`` and ``status == "untrained"`` on ``/services``.

A module-scoped tiny-estimator app keeps first-boot training (base + multi-service)
fast; a separate function-scoped untrained app covers the 503/untrained path.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.api import create_app
from src.config import Settings
from src.log_generator import SERVICES

CANONICAL_LOG = "Database connection failed with timeout error"

# The exact keys the base ``POST /classify`` (ClassifyResponse) must still emit —
# multi-service must NOT leak its extra fields (service/anomaly_score/...) into it.
BASE_CLASSIFY_KEYS = {
    "severity",
    "category",
    "confidence",
    "severity_confidence",
    "category_confidence",
}

# The keys ``POST /classify/service`` (MultiServiceResponse) is contracted to emit.
MULTISERVICE_KEYS = {
    "service",
    "service_confidence",
    "severity",
    "severity_confidence",
    "category",
    "category_confidence",
    "confidence",
    "anomaly_score",
}

# A web / database / cache flavoured log apiece, to spread the service distribution.
SERVICE_LOGS = [
    "GET /api/v1/users 200 OK in 12ms req_id=abc",
    "Slow query detected took 4200ms query_id=deadbeef rows=512",
    "High cache eviction rate 900 keys/s, memory pressure detected",
]


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    """A module-scoped TestClient whose app trained tiny base + multi-service models.

    Tiny estimators + an isolated tmp ``model_dir`` keep first-boot training (now
    base *and* multi-service) fast; the ``with`` block drives the lifespan so both
    models are ready before the first request.
    """
    model_dir = tmp_path_factory.mktemp("ms_models")
    app = create_app(
        Settings(rf_n_estimators=4, gb_n_estimators=4, model_dir=str(model_dir)),
        auto_train=True,
    )
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def untrained_client(tmp_path):
    """A TestClient for an app started with no model and ``auto_train=False``.

    Startup leaves both the base and the multi-service model unloaded, so
    ``POST /classify/service`` must 503 and ``/services`` must report untrained.
    """
    empty_dir = tmp_path / "empty_ms_models"
    empty_dir.mkdir()
    app = create_app(Settings(model_dir=str(empty_dir)), auto_train=False)
    with TestClient(app) as test_client:
        yield test_client


# --------------------------------------------------------------------------- #
# 1) POST /classify/service — happy path / shape
# --------------------------------------------------------------------------- #


def test_classify_service_shape(client):
    """A successful /classify/service returns all 8 keys with valid values."""
    resp = client.post("/classify/service", json={"raw_log": CANONICAL_LOG})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == MULTISERVICE_KEYS, f"unexpected keys: {sorted(body)}"

    assert body["service"] in SERVICES
    assert isinstance(body["severity"], str) and body["severity"]
    assert isinstance(body["category"], str) and body["category"]
    for key in (
        "service_confidence",
        "severity_confidence",
        "category_confidence",
        "confidence",
        "anomaly_score",
    ):
        assert isinstance(body[key], float)
        assert 0.0 <= body[key] <= 1.0
    assert 0.0 <= body["anomaly_score"] <= 1.0


# --------------------------------------------------------------------------- #
# 2) /classify/service populates metrics service_distribution
# --------------------------------------------------------------------------- #


def test_service_distribution_populated(client):
    """After /classify/service calls, /metrics service_distribution is non-empty."""
    for _ in range(3):
        resp = client.post("/classify/service", json={"raw_log": CANONICAL_LOG})
        assert resp.status_code == 200, resp.text

    snapshot = client.get("/metrics")
    assert snapshot.status_code == 200, snapshot.text
    dist = snapshot.json().get("service_distribution", {})
    assert isinstance(dist, dict)
    assert dist, "service_distribution should be non-empty after /classify/service"
    assert sum(dist.values()) >= 3


# --------------------------------------------------------------------------- #
# 3) GET /services
# --------------------------------------------------------------------------- #


def test_services_ready(client):
    """``GET /services`` reports ready status and lists the three services."""
    resp = client.get("/services")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["status"] == "ready"
    assert set(body["services"]) == set(SERVICES)
    # Each service exposes the severity classes its model can emit.
    per_service = body["per_service_severity_classes"]
    assert set(per_service) == set(SERVICES)
    for service in SERVICES:
        assert per_service[service], f"no severity classes for {service}"


# --------------------------------------------------------------------------- #
# 4) regression: base /classify is unchanged (5 keys, no leaked fields)
# --------------------------------------------------------------------------- #


def test_base_classify_shape_unchanged(client):
    """The base POST /classify still returns exactly its five keys (no leakage)."""
    resp = client.post("/classify", json={"raw_log": CANONICAL_LOG})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert set(body) == BASE_CLASSIFY_KEYS, f"unexpected keys: {sorted(body)}"
    # Explicitly assert the multi-service-only fields did NOT leak in.
    for leaked in ("service", "service_confidence", "anomaly_score"):
        assert leaked not in body


# --------------------------------------------------------------------------- #
# 5) untrained path: 503 on /classify/service, "untrained" on /services
# --------------------------------------------------------------------------- #


def test_untrained_classify_service_503_and_services_untrained(untrained_client):
    """With no model + auto_train=False: /classify/service 503s, /services untrained."""
    classify = untrained_client.post(
        "/classify/service", json={"raw_log": CANONICAL_LOG}
    )
    assert classify.status_code == 503, classify.text

    services = untrained_client.get("/services")
    assert services.status_code == 200, services.text
    body = services.json()
    assert body["status"] == "untrained"
    assert body["services"] == []


# --------------------------------------------------------------------------- #
# 6) varied logs -> multiple service keys in the distribution
# --------------------------------------------------------------------------- #


def test_multiple_services_in_distribution(client):
    """Posting web/database/cache logs grows service_distribution to >= 2 keys."""
    for raw_log in SERVICE_LOGS:
        resp = client.post("/classify/service", json={"raw_log": raw_log})
        assert resp.status_code == 200, resp.text

    snapshot = client.get("/metrics")
    assert snapshot.status_code == 200, snapshot.text
    dist = snapshot.json().get("service_distribution", {})
    assert len(dist) >= 2, f"expected >= 2 services in distribution, got {dist}"
    # Every key present must be a real service label.
    for key in dist:
        assert key in SERVICES
