import time
from src.storage.store import StorageStore


class TestStorageStore:
    def test_put_and_get(self):
        """Put a value and get it back, verifying value, version, and timestamp."""
        store = StorageStore(node_id="test-node")
        before = time.time()
        entry = store.put("k1", "v1")
        after = time.time()
        assert entry.value == "v1"
        assert entry.version == 1
        assert before <= entry.timestamp <= after

        retrieved = store.get("k1")
        assert retrieved is not None
        assert retrieved.value == "v1"
        assert retrieved.version == 1

    def test_put_auto_increments_version(self):
        """Putting the same key twice auto-increments the version from 1 to 2."""
        store = StorageStore()
        e1 = store.put("k1", "v1")
        assert e1.version == 1
        e2 = store.put("k1", "v2")
        assert e2.version == 2

    def test_put_explicit_version(self):
        """Putting with an explicit version uses that version."""
        store = StorageStore()
        entry = store.put("k1", "v1", version=5)
        assert entry.version == 5

    def test_get_nonexistent(self):
        """Getting a key that doesn't exist returns None."""
        store = StorageStore()
        assert store.get("no-such-key") is None

    def test_get_all(self):
        """Put multiple entries and get_all returns all of them."""
        store = StorageStore()
        store.put("a", "1")
        store.put("b", "2")
        store.put("c", "3")
        all_data = store.get_all()
        assert len(all_data) == 3
        assert "a" in all_data
        assert "b" in all_data
        assert "c" in all_data
        assert all_data["a"].value == "1"

    def test_keys(self):
        """Keys returns a list of all stored keys."""
        store = StorageStore()
        store.put("x", "1")
        store.put("y", "2")
        keys = store.keys()
        assert set(keys) == {"x", "y"}

    def test_merkle_root_changes(self):
        """Putting a value changes the merkle root from the empty-tree root."""
        store = StorageStore()
        empty_root = store.get_merkle_root()
        store.put("k1", "v1")
        new_root = store.get_merkle_root()
        assert empty_root != new_root
        assert len(new_root) == 64  # SHA-256 hex digest

    def test_merkle_leaves(self):
        """Putting 3 values produces 3 merkle leaf entries."""
        store = StorageStore()
        store.put("a", "1")
        store.put("b", "2")
        store.put("c", "3")
        leaves = store.get_merkle_leaves()
        assert len(leaves) == 3
        assert "a" in leaves
        assert "b" in leaves
        assert "c" in leaves

    def test_merkle_root_deterministic(self):
        """Same data in two stores produces the same merkle root."""
        store_a = StorageStore(node_id="a")
        store_b = StorageStore(node_id="b")
        for k, v in [("x", "1"), ("y", "2"), ("z", "3")]:
            store_a.put(k, v)
            store_b.put(k, v)
        assert store_a.get_merkle_root() == store_b.get_merkle_root()
