"""Integration tests for the Flask API endpoints."""

import json


class TestDashboard:
    """Tests for the dashboard and health endpoints."""

    def test_dashboard_title(self, client):
        """GET / should return 200 with the dashboard title."""
        resp = client.get("/")
        assert resp.status_code == 200
        assert "Avro Schema Evolution Dashboard" in resp.data.decode()

    def test_health(self, client):
        """GET /health should return {"status": "healthy"}."""
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data == {"status": "healthy"}


class TestSchemaEndpoints:
    """Tests for schema listing and detail endpoints."""

    def test_schemas_list_api(self, client):
        """GET /api/schemas should return 3 schemas."""
        resp = client.get("/api/schemas")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "success"
        assert len(data["data"]["schemas"]) == 3

    def test_schema_detail_api(self, client):
        """GET /api/schemas/v1 should return version=v1."""
        resp = client.get("/api/schemas/v1")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "success"
        assert data["data"]["version"] == "v1"

    def test_schema_not_found(self, client):
        """GET /api/schemas/v99 should return 404."""
        resp = client.get("/api/schemas/v99")
        assert resp.status_code == 404
        data = resp.get_json()
        assert data["status"] == "error"

    def test_schema_info_api(self, client):
        """GET /api/schema-info should return schemas and full compatibility matrix."""
        resp = client.get("/api/schema-info")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "success"

        payload = data["data"]
        assert payload["available_schemas"] == ["v1", "v2", "v3"]

        # Verify all 9 matrix entries are True
        matrix = payload["compatibility_matrix"]
        true_count = sum(
            1
            for writer in matrix
            for reader in matrix[writer]
            if matrix[writer][reader]
        )
        assert true_count == 9


class TestCompatibilityEndpoints:
    """Tests for compatibility check and test endpoints."""

    def test_test_compatibility_api(self, client):
        """POST /api/test-compatibility should succeed and include size info."""
        resp = client.post(
            "/api/test-compatibility",
            data=json.dumps({"schema_version": "v2"}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "success"
        assert "Size:" in data["message"]

    def test_compatibility_check_api(self, client):
        """POST /api/compatibility/check with backward mode should return compatible=True."""
        resp = client.post(
            "/api/compatibility/check",
            data=json.dumps({
                "writer_schema": "v1",
                "reader_schema": "v2",
                "mode": "backward",
            }),
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "success"
        assert data["data"]["compatible"] is True

    def test_compatibility_check_forward(self, client):
        """POST /api/compatibility/check with forward mode should return compatible=True."""
        resp = client.post(
            "/api/compatibility/check",
            data=json.dumps({
                "writer_schema": "v2",
                "reader_schema": "v1",
                "mode": "forward",
            }),
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["data"]["compatible"] is True

    def test_compatibility_check_full(self, client):
        """POST /api/compatibility/check with full mode should return compatible=True."""
        resp = client.post(
            "/api/compatibility/check",
            data=json.dumps({
                "writer_schema": "v1",
                "reader_schema": "v2",
                "mode": "full",
            }),
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["data"]["compatible"] is True

    def test_compatibility_check_missing_fields(self, client):
        """POST /api/compatibility/check without required fields should return 400."""
        resp = client.post(
            "/api/compatibility/check",
            data=json.dumps({}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_compatibility_check_invalid_version(self, client):
        """POST /api/compatibility/check with invalid version should return 400."""
        resp = client.post(
            "/api/compatibility/check",
            data=json.dumps({
                "writer_schema": "v99",
                "reader_schema": "v1",
                "mode": "backward",
            }),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_compatibility_check_invalid_mode(self, client):
        """POST /api/compatibility/check with invalid mode should return 400."""
        resp = client.post(
            "/api/compatibility/check",
            data=json.dumps({
                "writer_schema": "v1",
                "reader_schema": "v2",
                "mode": "invalid",
            }),
            content_type="application/json",
        )
        assert resp.status_code == 400


class TestGenerateEndpoints:
    """Tests for event generation endpoints."""

    def test_generate_sample_api(self, client):
        """GET /api/generate-sample/v1 should return a sample event."""
        resp = client.get("/api/generate-sample/v1")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "success"
        assert data["data"]["version"] == "v1"
        assert "sample" in data["data"]

    def test_generate_sample_not_found(self, client):
        """GET /api/generate-sample/v99 should return 404."""
        resp = client.get("/api/generate-sample/v99")
        assert resp.status_code == 404

    def test_generate_events_api(self, client):
        """POST /api/generate with count=2 should return 2 events."""
        resp = client.post(
            "/api/generate",
            data=json.dumps({"schema_version": "v1", "count": 2}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "success"
        assert data["data"]["count"] == 2
        assert len(data["data"]["events"]) == 2

    def test_generate_events_invalid_version(self, client):
        """POST /api/generate with invalid version should return 400."""
        resp = client.post(
            "/api/generate",
            data=json.dumps({"schema_version": "v99"}),
            content_type="application/json",
        )
        assert resp.status_code == 400


class TestDeserializeEndpoint:
    """Tests for the /api/deserialize endpoint."""

    def test_deserialize_api_json(self, client):
        """POST /api/deserialize with base64 container data should return records."""
        import base64
        from src.schema_registry import SchemaRegistry
        from src.serializer import AvroSerializer
        from src.log_event import LogEvent

        reg = SchemaRegistry()
        ser = AvroSerializer(reg)
        sample = LogEvent.generate_sample("v2")
        container = ser.serialize_to_container([sample.to_dict("v2")], "v2")
        b64 = base64.b64encode(container).decode()

        resp = client.post(
            "/api/deserialize",
            data=json.dumps({"schema_version": "v2", "data": b64}),
            content_type="application/json",
        )
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["status"] == "success"
        assert data["data"]["record_count"] == 1

    def test_deserialize_api_missing_data(self, client):
        """POST /api/deserialize without data should return 400."""
        resp = client.post(
            "/api/deserialize",
            data=json.dumps({"schema_version": "v1"}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_deserialize_api_invalid_version(self, client):
        """POST /api/deserialize with invalid version should return 400."""
        import base64
        resp = client.post(
            "/api/deserialize",
            data=json.dumps({"schema_version": "v99", "data": base64.b64encode(b"fake").decode()}),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_deserialize_api_corrupt_data(self, client):
        """POST /api/deserialize with corrupt data should return 400."""
        import base64
        resp = client.post(
            "/api/deserialize",
            data=json.dumps({
                "schema_version": "v1",
                "data": base64.b64encode(b"not valid avro data").decode(),
            }),
            content_type="application/json",
        )
        assert resp.status_code == 400

    def test_test_compatibility_invalid_version(self, client):
        """POST /api/test-compatibility with invalid version should return 400."""
        resp = client.post(
            "/api/test-compatibility",
            data=json.dumps({"schema_version": "v99"}),
            content_type="application/json",
        )
        assert resp.status_code == 400
