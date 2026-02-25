"""Tests for SchemaRegistry."""
from src.storage import FileStorage
from src.registry import SchemaRegistry


class TestSchemaRegistry:
    def _make_registry(self, tmp_path):
        path = str(tmp_path / "test.json")
        storage = FileStorage(path)
        return SchemaRegistry(storage)

    def test_register_creates_record(self, tmp_path):
        reg = self._make_registry(tmp_path)
        schema = {"type": "object", "properties": {"name": {"type": "string"}}}
        record, created = reg.register("test-subject", schema)
        assert created is True
        assert record["id"] == 1
        assert record["subject"] == "test-subject"
        assert record["version"] == 1
        assert record["schema_type"] == "json"
        assert record["schema"] == schema
        assert "hash" in record
        assert "registered_at" in record

    def test_register_increments_version(self, tmp_path):
        reg = self._make_registry(tmp_path)
        schema1 = {"type": "object", "properties": {"name": {"type": "string"}}}
        schema2 = {"type": "object", "properties": {"name": {"type": "string"}, "age": {"type": "integer"}}}
        r1, _ = reg.register("test-subject", schema1)
        r2, _ = reg.register("test-subject", schema2)
        assert r1["version"] == 1
        assert r2["version"] == 2
        assert r2["id"] == 2

    def test_register_deduplicates(self, tmp_path):
        reg = self._make_registry(tmp_path)
        schema = {"type": "object", "properties": {"name": {"type": "string"}}}
        r1, created1 = reg.register("test-subject", schema)
        r2, created2 = reg.register("test-subject", schema)
        assert created1 is True
        assert created2 is False
        assert r1["id"] == r2["id"]

    def test_dedup_same_schema_different_subject(self, tmp_path):
        """Same schema under different subjects should NOT dedup -- each gets its own record."""
        reg = self._make_registry(tmp_path)
        schema = {"type": "object", "properties": {"name": {"type": "string"}}}
        r1, c1 = reg.register("subject-a", schema)
        r2, c2 = reg.register("subject-b", schema)
        assert c1 is True
        assert c2 is True
        assert r1["id"] != r2["id"]

    def test_get_latest(self, tmp_path):
        reg = self._make_registry(tmp_path)
        schema1 = {"type": "object", "properties": {"a": {"type": "string"}}}
        schema2 = {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "integer"}}}
        reg.register("my-subject", schema1)
        reg.register("my-subject", schema2)
        latest = reg.get_latest("my-subject")
        assert latest["version"] == 2

    def test_get_latest_missing_subject(self, tmp_path):
        reg = self._make_registry(tmp_path)
        import pytest
        with pytest.raises(KeyError):
            reg.get_latest("nonexistent")

    def test_get_version(self, tmp_path):
        reg = self._make_registry(tmp_path)
        schema1 = {"type": "object", "properties": {"x": {"type": "string"}}}
        schema2 = {"type": "object", "properties": {"x": {"type": "string"}, "y": {"type": "integer"}}}
        reg.register("versioned", schema1)
        reg.register("versioned", schema2)
        v1 = reg.get_version("versioned", 1)
        v2 = reg.get_version("versioned", 2)
        assert v1["version"] == 1
        assert v2["version"] == 2

    def test_get_version_missing(self, tmp_path):
        reg = self._make_registry(tmp_path)
        schema = {"type": "object"}
        reg.register("s", schema)
        import pytest
        with pytest.raises(KeyError):
            reg.get_version("s", 99)

    def test_list_subjects(self, tmp_path):
        reg = self._make_registry(tmp_path)
        reg.register("beta", {"type": "object"})
        reg.register("alpha", {"type": "object"})
        subjects = reg.list_subjects()
        assert subjects == ["alpha", "beta"]

    def test_list_versions(self, tmp_path):
        reg = self._make_registry(tmp_path)
        reg.register("s", {"type": "object", "properties": {"a": {"type": "string"}}})
        reg.register("s", {"type": "object", "properties": {"b": {"type": "string"}}})
        versions = reg.list_versions("s")
        assert versions == [1, 2]

    def test_get_by_id(self, tmp_path):
        reg = self._make_registry(tmp_path)
        schema = {"type": "object"}
        record, _ = reg.register("s", schema)
        fetched = reg.get_by_id(record["id"])
        assert fetched["id"] == record["id"]

    def test_get_by_id_missing(self, tmp_path):
        reg = self._make_registry(tmp_path)
        import pytest
        with pytest.raises(KeyError):
            reg.get_by_id(999)
