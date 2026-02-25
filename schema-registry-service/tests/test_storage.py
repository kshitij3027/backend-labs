"""Tests for FileStorage."""
import json
import os
from src.storage import FileStorage


class TestFileStorage:
    def test_initial_state_empty(self, tmp_path):
        path = str(tmp_path / "test.json")
        storage = FileStorage(path)
        state = storage.get_state()
        assert state["next_id"] == 1
        assert state["schemas"] == {}
        assert state["subjects"] == {}

    def test_set_and_get_state(self, tmp_path):
        path = str(tmp_path / "test.json")
        storage = FileStorage(path)
        new_state = {"next_id": 2, "schemas": {"1": {"id": 1}}, "subjects": {}}
        storage.set_state(new_state)
        result = storage.get_state()
        assert result["next_id"] == 2
        assert "1" in result["schemas"]

    def test_persistence_across_instances(self, tmp_path):
        path = str(tmp_path / "test.json")
        storage1 = FileStorage(path)
        state = storage1.get_state()
        state["next_id"] = 5
        state["schemas"]["1"] = {"id": 1, "subject": "test"}
        storage1.set_state(state)

        storage2 = FileStorage(path)
        loaded = storage2.get_state()
        assert loaded["next_id"] == 5
        assert loaded["schemas"]["1"]["subject"] == "test"

    def test_atomic_write_creates_file(self, tmp_path):
        path = str(tmp_path / "subdir" / "test.json")
        storage = FileStorage(path)
        storage.set_state({"next_id": 1, "schemas": {}, "subjects": {}})
        assert os.path.exists(path)

    def test_get_state_returns_copy(self, tmp_path):
        path = str(tmp_path / "test.json")
        storage = FileStorage(path)
        state1 = storage.get_state()
        state1["next_id"] = 999
        state2 = storage.get_state()
        assert state2["next_id"] == 1  # Original unchanged
