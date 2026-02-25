"""Tests for API endpoints."""


class TestHealth:
    def test_health_returns_200(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_health_returns_healthy(self, client):
        resp = client.get("/health")
        data = resp.get_json()
        assert data["status"] == "healthy"

    def test_health_includes_counts(self, client):
        resp = client.get("/health")
        data = resp.get_json()
        assert "schema_count" in data
        assert "subject_count" in data
        assert data["schema_count"] == 0
        assert data["subject_count"] == 0


class TestRegistration:
    def test_register_schema(self, client):
        resp = client.post("/schemas", json={
            "subject": "user-events",
            "schema": {"type": "object", "properties": {"name": {"type": "string"}}},
        })
        assert resp.status_code == 201
        data = resp.get_json()
        assert data["status"] == "success"
        assert data["data"]["subject"] == "user-events"
        assert data["data"]["version"] == 1

    def test_register_dedup_returns_200(self, client):
        schema = {"type": "object", "properties": {"name": {"type": "string"}}}
        client.post("/schemas", json={"subject": "dedup-test", "schema": schema})
        resp = client.post("/schemas", json={"subject": "dedup-test", "schema": schema})
        assert resp.status_code == 200

    def test_register_missing_fields(self, client):
        resp = client.post("/schemas", json={"subject": "test"})
        assert resp.status_code == 400

    def test_register_invalid_schema_type(self, client):
        resp = client.post("/schemas", json={
            "subject": "test",
            "schema": {"type": "object"},
            "schema_type": "xml",
        })
        assert resp.status_code == 400


class TestRetrieval:
    def _register(self, client, subject, schema):
        return client.post("/schemas", json={"subject": subject, "schema": schema})

    def test_list_subjects(self, client):
        self._register(client, "b-subject", {"type": "object"})
        self._register(client, "a-subject", {"type": "object"})
        resp = client.get("/schemas/subjects")
        data = resp.get_json()["data"]
        assert data == ["a-subject", "b-subject"]

    def test_get_latest(self, client):
        self._register(client, "s", {"type": "object", "properties": {"a": {"type": "string"}}})
        self._register(client, "s", {"type": "object", "properties": {"b": {"type": "string"}}})
        resp = client.get("/schemas/subjects/s")
        assert resp.get_json()["data"]["version"] == 2

    def test_get_latest_404(self, client):
        resp = client.get("/schemas/subjects/nonexistent")
        assert resp.status_code == 404

    def test_list_versions(self, client):
        self._register(client, "v", {"type": "object", "properties": {"x": {"type": "string"}}})
        self._register(client, "v", {"type": "object", "properties": {"y": {"type": "string"}}})
        resp = client.get("/schemas/subjects/v/versions")
        assert resp.get_json()["data"] == [1, 2]

    def test_get_specific_version(self, client):
        self._register(client, "v", {"type": "object", "properties": {"x": {"type": "string"}}})
        self._register(client, "v", {"type": "object", "properties": {"y": {"type": "string"}}})
        resp = client.get("/schemas/subjects/v/versions/1")
        assert resp.get_json()["data"]["version"] == 1

    def test_get_version_404(self, client):
        self._register(client, "v", {"type": "object"})
        resp = client.get("/schemas/subjects/v/versions/99")
        assert resp.status_code == 404


class TestValidation:
    def _register(self, client, subject, schema, schema_type="json"):
        return client.post("/schemas", json={
            "subject": subject, "schema": schema, "schema_type": schema_type,
        })

    def test_validate_valid_data(self, client):
        schema = {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}
        self._register(client, "val-test", schema)
        resp = client.post("/validate", json={"subject": "val-test", "data": {"name": "Alice"}})
        data = resp.get_json()["data"]
        assert data["valid"] is True
        assert data["errors"] == []

    def test_validate_invalid_data(self, client):
        schema = {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}
        self._register(client, "val-test", schema)
        resp = client.post("/validate", json={"subject": "val-test", "data": {"name": 123}})
        data = resp.get_json()["data"]
        assert data["valid"] is False
        assert len(data["errors"]) > 0

    def test_validate_specific_version(self, client):
        schema1 = {"type": "object", "properties": {"a": {"type": "string"}}, "required": ["a"]}
        schema2 = {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "integer"}}, "required": ["a", "b"]}
        self._register(client, "ver-test", schema1)
        self._register(client, "ver-test", schema2)
        # Valid for v1, would fail v2
        resp = client.post("/validate", json={"subject": "ver-test", "data": {"a": "hello"}, "version": 1})
        assert resp.get_json()["data"]["valid"] is True
        assert resp.get_json()["data"]["schema_version"] == 1

    def test_validate_missing_subject(self, client):
        resp = client.post("/validate", json={"subject": "nope", "data": {}})
        assert resp.status_code == 404

    def test_validate_missing_fields(self, client):
        resp = client.post("/validate", json={"subject": "test"})
        assert resp.status_code == 400


class TestCompatibility:
    def _register(self, client, subject, schema, schema_type="json"):
        return client.post("/schemas", json={
            "subject": subject, "schema": schema, "schema_type": schema_type,
        })

    def test_compatible_change(self, client):
        old = {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}
        self._register(client, "compat-test", old)
        new = {"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}, "required": ["name"]}
        resp = client.post("/compatibility/subjects/compat-test", json={"schema": new})
        data = resp.get_json()["data"]
        assert data["compatible"] is True
        assert data["issues"] == []

    def test_breaking_change(self, client):
        old = {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}
        self._register(client, "compat-test", old)
        new = {"type": "object", "properties": {"name": {"type": "string"}, "email": {"type": "string"}}, "required": ["name", "email"]}
        resp = client.post("/compatibility/subjects/compat-test", json={"schema": new})
        data = resp.get_json()["data"]
        assert data["compatible"] is False
        assert len(data["issues"]) > 0

    def test_compat_missing_subject(self, client):
        resp = client.post("/compatibility/subjects/nonexistent", json={"schema": {"type": "object"}})
        assert resp.status_code == 404

    def test_compat_missing_schema(self, client):
        old = {"type": "object", "properties": {"name": {"type": "string"}}}
        self._register(client, "compat-test", old)
        resp = client.post("/compatibility/subjects/compat-test", json={})
        assert resp.status_code == 400


class TestAvroRegistration:
    def test_register_avro_schema(self, client):
        schema = {
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "event_type", "type": "string"},
            ]
        }
        resp = client.post("/schemas", json={
            "subject": "avro-test",
            "schema": schema,
            "schema_type": "avro",
        })
        assert resp.status_code == 201
        assert resp.get_json()["data"]["schema_type"] == "avro"

    def test_validate_avro_data(self, client):
        schema = {
            "type": "record",
            "name": "UserEvent",
            "fields": [
                {"name": "user_id", "type": "string"},
                {"name": "event_type", "type": "string"},
            ]
        }
        client.post("/schemas", json={"subject": "avro-val", "schema": schema, "schema_type": "avro"})
        resp = client.post("/validate", json={
            "subject": "avro-val",
            "data": {"user_id": "u1", "event_type": "click"},
        })
        assert resp.get_json()["data"]["valid"] is True

    def test_avro_compat_check(self, client):
        old = {"type": "record", "name": "T", "fields": [{"name": "a", "type": "string"}]}
        client.post("/schemas", json={"subject": "avro-compat", "schema": old, "schema_type": "avro"})
        new = {"type": "record", "name": "T", "fields": [{"name": "a", "type": "string"}, {"name": "b", "type": "string", "default": ""}]}
        resp = client.post("/compatibility/subjects/avro-compat", json={"schema": new, "schema_type": "avro"})
        assert resp.get_json()["data"]["compatible"] is True


class TestMetrics:
    def _register(self, client, subject, schema):
        return client.post("/schemas", json={"subject": subject, "schema": schema})

    def test_metrics_initial_zeros(self, client):
        resp = client.get("/metrics")
        data = resp.get_json()["data"]
        assert data["total_validations"] == 0
        assert data["total_registrations"] == 0
        assert data["success_rate"] == 0.0

    def test_metrics_after_registration(self, client):
        self._register(client, "m-test", {"type": "object"})
        resp = client.get("/metrics")
        assert resp.get_json()["data"]["total_registrations"] == 1

    def test_metrics_after_validation(self, client):
        schema = {"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"]}
        self._register(client, "m-val", schema)
        client.post("/validate", json={"subject": "m-val", "data": {"x": "ok"}})
        client.post("/validate", json={"subject": "m-val", "data": {"x": 123}})
        resp = client.get("/metrics")
        data = resp.get_json()["data"]
        assert data["total_validations"] == 2
        assert data["successful_validations"] == 1
        assert data["failed_validations"] == 1
        assert data["success_rate"] == 50.0

    def test_metrics_per_subject(self, client):
        schema = {"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"]}
        self._register(client, "subj-a", schema)
        client.post("/validate", json={"subject": "subj-a", "data": {"x": "ok"}})
        resp = client.get("/metrics")
        per = resp.get_json()["data"]["per_subject"]
        assert "subj-a" in per
        assert per["subj-a"]["validations"] == 1


class TestDashboard:
    def test_dashboard_returns_200(self, client):
        resp = client.get("/")
        assert resp.status_code == 200

    def test_dashboard_contains_html(self, client):
        resp = client.get("/")
        assert b"Schema Registry Service" in resp.data


class TestPerformance:
    """Performance benchmarks to ensure acceptable response times."""

    def _register(self, client, subject, schema):
        return client.post("/schemas", json={"subject": subject, "schema": schema})

    def test_1000_validations_under_5_seconds(self, client):
        import time
        schema = {
            "type": "object",
            "properties": {
                "user_id": {"type": "string"},
                "event_type": {"type": "string"},
                "timestamp": {"type": "string"},
            },
            "required": ["user_id", "event_type", "timestamp"],
        }
        self._register(client, "perf-test", schema)

        start = time.time()
        for i in range(1000):
            client.post("/validate", json={
                "subject": "perf-test",
                "data": {"user_id": f"u{i}", "event_type": "click", "timestamp": "2024-01-01T00:00:00Z"},
            })
        elapsed = time.time() - start
        assert elapsed < 5.0, f"1000 validations took {elapsed:.2f}s (target: <5s)"

    def test_100_lookups_under_1_second(self, client):
        import time
        self._register(client, "lookup-test", {"type": "object", "properties": {"x": {"type": "string"}}})

        start = time.time()
        for _ in range(100):
            client.get("/schemas/subjects/lookup-test")
        elapsed = time.time() - start
        assert elapsed < 1.0, f"100 lookups took {elapsed:.2f}s (target: <1s)"
