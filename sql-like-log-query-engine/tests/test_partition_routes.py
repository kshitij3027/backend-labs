from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.parser import parse_sql
from src.partition.app import create_partition_app
from src.planner.planner import serialize_ast
from src.shared.config import PartitionSettings


@pytest.fixture
def settings() -> PartitionSettings:
    return PartitionSettings(
        partition_id="partition-test",
        partition_port=8199,
        partition_time_start="2026-04-01T00:00:00",
        partition_time_end="2026-04-07T23:59:59",
        indexed_fields="level,service,timestamp",
        log_sample_count=100,
        log_level="INFO",
    )


@pytest.fixture
def client(settings: PartitionSettings):
    app = create_partition_app(settings)
    # TestClient runs the lifespan (FastAPI >=0.100) so app.state is
    # populated before the first request reaches a route.
    with TestClient(app) as c:
        yield c


# --- health / metadata ----------------------------------------------------


def test_health_ok(client: TestClient) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["partition_id"] == "partition-test"


def test_metadata_shape(client: TestClient) -> None:
    resp = client.get("/metadata")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == "partition-test"
    assert data["healthy"] is True
    assert "time_range" in data
    assert data["time_range"]["start"].startswith("2026-04-01")
    # Pydantic serializes the end datetime via .isoformat — matches planner
    # expectations and ISO-8601 literal parsing.
    assert data["time_range"]["end"].startswith("2026-04-07")
    assert set(data["indexed_fields"]) == {"level", "service", "timestamp"}


# --- execute --------------------------------------------------------------


def test_execute_without_filter_returns_rows(client: TestClient) -> None:
    resp = client.post("/execute", json={"filter_ast_json": None})
    assert resp.status_code == 200
    body = resp.json()
    assert body["records_scanned"] == 100
    assert len(body["rows"]) == 100
    assert body["partial_aggregate"] is None
    # Spot-check the generated row shape.
    row = body["rows"][0]
    for required in ("timestamp", "level", "service", "message", "duration_ms"):
        assert required in row


def test_execute_respects_limit(client: TestClient) -> None:
    resp = client.post(
        "/execute", json={"filter_ast_json": None, "limit": 10}
    )
    assert resp.status_code == 200
    assert len(resp.json()["rows"]) == 10


def test_execute_with_level_filter(client: TestClient) -> None:
    # Planner emits this exact serialized shape for `level = 'ERROR'`.
    node = parse_sql("SELECT * FROM logs WHERE level = 'ERROR'")
    filt = serialize_ast(node.where)

    resp = client.post("/execute", json={"filter_ast_json": filt})
    assert resp.status_code == 200
    body = resp.json()
    assert body["records_scanned"] == 100
    assert all(row["level"] == "ERROR" for row in body["rows"])


def test_execute_with_select_fields_projects(client: TestClient) -> None:
    resp = client.post(
        "/execute",
        json={
            "filter_ast_json": None,
            "limit": 5,
            "select_fields": ["level", "service"],
        },
    )
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert rows
    for row in rows:
        assert set(row.keys()) == {"level", "service"}


def test_execute_with_aggregation_returns_partial(
    client: TestClient,
) -> None:
    node = parse_sql("SELECT COUNT(*) FROM logs")
    _ = node  # planner derives aggregation off the whole Select
    agg_payload = {"functions": [["COUNT", "*"]], "group_by": []}

    resp = client.post(
        "/execute",
        json={"filter_ast_json": None, "aggregation": agg_payload},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["rows"] == []
    assert body["partial_aggregate"] is not None

    partial = body["partial_aggregate"]
    assert partial["aggregates"] == {"COUNT(*)": 100}
    assert partial["record_count"] == 100
    assert partial["groups"] is None


def test_execute_with_group_by_returns_buckets(client: TestClient) -> None:
    agg = {"functions": [["COUNT", "*"]], "group_by": ["service"]}
    resp = client.post(
        "/execute",
        json={"filter_ast_json": None, "aggregation": agg},
    )
    assert resp.status_code == 200
    partial = resp.json()["partial_aggregate"]
    assert partial["aggregates"] is None
    assert partial["groups"] is not None
    total = sum(bucket["count"] for bucket in partial["groups"].values())
    assert total == 100


def test_execute_malformed_filter_returns_400(client: TestClient) -> None:
    # A node whose "kind" is unknown triggers our ValueError → 400 path.
    resp = client.post(
        "/execute",
        json={"filter_ast_json": {"kind": "not_a_real_node"}},
    )
    assert resp.status_code == 400
