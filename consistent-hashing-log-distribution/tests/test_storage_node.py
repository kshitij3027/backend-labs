"""Tests for the StorageNode in-memory log storage."""

import threading

from src.storage_node import StorageNode


class TestStorageNode:
    """Tests for StorageNode."""

    def test_store_adds_metadata(self, storage_node):
        """Stored entry should have stored_at and node_id fields."""
        entry = {"message": "test log", "source": "app1", "level": "INFO"}
        result = storage_node.store(entry)

        assert "stored_at" in result
        assert result["node_id"] == "test-node"
        assert result["message"] == "test log"

    def test_store_does_not_mutate_original(self, storage_node):
        """Original dict passed to store should remain unchanged."""
        original = {"message": "test log", "source": "app1"}
        original_copy = dict(original)
        storage_node.store(original)

        assert original == original_copy
        assert "stored_at" not in original
        assert "node_id" not in original

    def test_get_logs_returns_all(self, storage_node):
        """After storing 5 entries, get_logs should return 5."""
        for i in range(5):
            storage_node.store({"message": f"log {i}", "source": "app1"})

        logs = storage_node.get_logs()
        assert len(logs) == 5

    def test_get_log_count(self, storage_node):
        """get_log_count should match number of stored entries."""
        assert storage_node.get_log_count() == 0
        storage_node.store({"message": "log 1"})
        assert storage_node.get_log_count() == 1
        storage_node.store({"message": "log 2"})
        assert storage_node.get_log_count() == 2

    def test_get_logs_returns_copy(self, storage_node):
        """Modifying the returned list should not affect internal state."""
        storage_node.store({"message": "log 1"})
        logs = storage_node.get_logs()
        logs.clear()

        assert storage_node.get_log_count() == 1
        assert len(storage_node.get_logs()) == 1

    def test_remove_logs(self, storage_node):
        """remove_logs should remove matching entries and return them."""
        storage_node.store({"message": "keep", "source": "app1"})
        storage_node.store({"message": "remove", "source": "app2"})
        storage_node.store({"message": "keep too", "source": "app1"})

        removed = storage_node.remove_logs(lambda log: log.get("source") == "app2")

        assert len(removed) == 1
        assert removed[0]["message"] == "remove"
        assert storage_node.get_log_count() == 2

        remaining = storage_node.get_logs()
        sources = [log["source"] for log in remaining]
        assert all(s == "app1" for s in sources)

    def test_remove_logs_empty_match(self, storage_node):
        """When no logs match predicate, return empty list and keep all logs."""
        storage_node.store({"message": "log 1", "source": "app1"})
        storage_node.store({"message": "log 2", "source": "app1"})

        removed = storage_node.remove_logs(lambda log: log.get("source") == "app99")

        assert removed == []
        assert storage_node.get_log_count() == 2

    def test_add_logs(self, storage_node):
        """Bulk add_logs should increase count by the number of logs added."""
        storage_node.store({"message": "existing"})

        new_logs = [
            {"message": "migrated 1", "node_id": "old-node"},
            {"message": "migrated 2", "node_id": "old-node"},
            {"message": "migrated 3", "node_id": "old-node"},
        ]
        added = storage_node.add_logs(new_logs)

        assert added == 3
        assert storage_node.get_log_count() == 4

    def test_add_logs_empty(self, storage_node):
        """Adding an empty list should be fine and return 0."""
        storage_node.store({"message": "existing"})
        added = storage_node.add_logs([])

        assert added == 0
        assert storage_node.get_log_count() == 1

    def test_get_stats(self, storage_node):
        """get_stats should return node_id, log_count, sources, and levels."""
        storage_node.store({"message": "a", "source": "web", "level": "INFO"})
        storage_node.store({"message": "b", "source": "web", "level": "ERROR"})
        storage_node.store({"message": "c", "source": "api", "level": "INFO"})

        stats = storage_node.get_stats()

        assert stats["node_id"] == "test-node"
        assert stats["log_count"] == 3
        assert stats["sources"] == {"web": 2, "api": 1}
        assert stats["levels"] == {"INFO": 2, "ERROR": 1}

    def test_thread_safety(self):
        """10 threads storing simultaneously should result in correct total count."""
        node = StorageNode("concurrent-node")
        logs_per_thread = 100
        num_threads = 10
        errors = []

        def store_logs(thread_id):
            try:
                for i in range(logs_per_thread):
                    node.store({
                        "message": f"thread-{thread_id}-log-{i}",
                        "source": f"thread-{thread_id}",
                        "level": "INFO",
                    })
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=store_logs, args=(t,))
            for t in range(num_threads)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Errors during concurrent stores: {errors}"
        assert node.get_log_count() == num_threads * logs_per_thread
        assert len(node.get_logs()) == num_threads * logs_per_thread
