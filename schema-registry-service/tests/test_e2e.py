"""End-to-end tests for the full schema registry workflow."""
import json


class TestFullWorkflow:
    """Test the complete register -> validate -> evolve -> compat -> validate flow."""

    def test_full_json_workflow(self, client):
        # 1. Register initial schema
        schema_v1 = {
            "type": "object",
            "properties": {
                "user_id": {"type": "string"},
                "event_type": {"type": "string"},
                "timestamp": {"type": "string"},
            },
            "required": ["user_id", "event_type", "timestamp"],
        }
        resp = client.post("/schemas", json={"subject": "user-events", "schema": schema_v1})
        assert resp.status_code == 201
        v1_data = resp.get_json()["data"]
        assert v1_data["version"] == 1

        # 2. Validate data against v1
        resp = client.post("/validate", json={
            "subject": "user-events",
            "data": {"user_id": "u1", "event_type": "click", "timestamp": "2024-01-01T00:00:00Z"},
        })
        assert resp.get_json()["data"]["valid"] is True

        # 3. Validate invalid data
        resp = client.post("/validate", json={
            "subject": "user-events",
            "data": {"user_id": 123},
        })
        assert resp.get_json()["data"]["valid"] is False

        # 4. Check compatibility of evolved schema (add optional field)
        schema_v2 = {
            "type": "object",
            "properties": {
                "user_id": {"type": "string"},
                "event_type": {"type": "string"},
                "timestamp": {"type": "string"},
                "metadata": {"type": "object"},
            },
            "required": ["user_id", "event_type", "timestamp"],
        }
        resp = client.post("/compatibility/subjects/user-events", json={"schema": schema_v2})
        assert resp.get_json()["data"]["compatible"] is True

        # 5. Register evolved schema
        resp = client.post("/schemas", json={"subject": "user-events", "schema": schema_v2})
        assert resp.status_code == 201
        v2_data = resp.get_json()["data"]
        assert v2_data["version"] == 2

        # 6. Validate against specific version
        resp = client.post("/validate", json={
            "subject": "user-events",
            "data": {"user_id": "u1", "event_type": "click", "timestamp": "2024-01-01T00:00:00Z"},
            "version": 1,
        })
        assert resp.get_json()["data"]["valid"] is True
        assert resp.get_json()["data"]["schema_version"] == 1

        # 7. Check versions list
        resp = client.get("/schemas/subjects/user-events/versions")
        assert resp.get_json()["data"] == [1, 2]

        # 8. Check metrics
        resp = client.get("/metrics")
        metrics = resp.get_json()["data"]
        assert metrics["total_registrations"] == 2
        assert metrics["total_validations"] == 3

        # 9. Dedup: re-register same schema
        resp = client.post("/schemas", json={"subject": "user-events", "schema": schema_v1})
        assert resp.status_code == 200  # Not 201

        # 10. Health check shows correct counts
        resp = client.get("/health")
        health = resp.get_json()
        assert health["schema_count"] == 2
        assert health["subject_count"] == 1

    def test_full_avro_workflow(self, client):
        schema_v1 = {
            "type": "record",
            "name": "LogEntry",
            "fields": [
                {"name": "level", "type": "string"},
                {"name": "message", "type": "string"},
            ],
        }
        resp = client.post("/schemas", json={
            "subject": "log-entries", "schema": schema_v1, "schema_type": "avro",
        })
        assert resp.status_code == 201

        # Validate
        resp = client.post("/validate", json={
            "subject": "log-entries",
            "data": {"level": "INFO", "message": "test"},
        })
        assert resp.get_json()["data"]["valid"] is True

        # Evolve with default
        schema_v2 = {
            "type": "record",
            "name": "LogEntry",
            "fields": [
                {"name": "level", "type": "string"},
                {"name": "message", "type": "string"},
                {"name": "source", "type": "string", "default": "unknown"},
            ],
        }
        resp = client.post("/compatibility/subjects/log-entries", json={
            "schema": schema_v2, "schema_type": "avro",
        })
        assert resp.get_json()["data"]["compatible"] is True

    def test_multiple_subjects(self, client):
        for name in ["alpha", "beta", "gamma"]:
            client.post("/schemas", json={
                "subject": name,
                "schema": {"type": "object", "properties": {name: {"type": "string"}}},
            })
        resp = client.get("/schemas/subjects")
        subjects = resp.get_json()["data"]
        assert subjects == ["alpha", "beta", "gamma"]


class TestPersistence:
    """Test that data survives across app instances (same storage file)."""

    def test_data_persists_across_instances(self, tmp_storage):
        from src.app import create_app

        # Instance 1: register schemas
        app1 = create_app(storage_path=tmp_storage)
        app1.config["TESTING"] = True
        client1 = app1.test_client()

        schema = {"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"]}
        resp = client1.post("/schemas", json={"subject": "persist-test", "schema": schema})
        assert resp.status_code == 201

        # Instance 2: read from same storage file
        app2 = create_app(storage_path=tmp_storage)
        app2.config["TESTING"] = True
        client2 = app2.test_client()

        resp = client2.get("/schemas/subjects")
        subjects = resp.get_json()["data"]
        assert "persist-test" in subjects

        resp = client2.get("/schemas/subjects/persist-test")
        assert resp.get_json()["data"]["version"] == 1

    def test_validators_recompiled_on_restart(self, tmp_storage):
        from src.app import create_app

        # Instance 1: register + validate
        app1 = create_app(storage_path=tmp_storage)
        app1.config["TESTING"] = True
        client1 = app1.test_client()

        schema = {"type": "object", "properties": {"x": {"type": "string"}}, "required": ["x"]}
        client1.post("/schemas", json={"subject": "recompile-test", "schema": schema})

        # Instance 2: should be able to validate (validators recompiled on startup)
        app2 = create_app(storage_path=tmp_storage)
        app2.config["TESTING"] = True
        client2 = app2.test_client()

        resp = client2.post("/validate", json={"subject": "recompile-test", "data": {"x": "hello"}})
        assert resp.get_json()["data"]["valid"] is True
