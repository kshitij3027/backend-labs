import time

from src.merkle.tree import MerkleTree


class TestMerkleTree:
    def test_identical_trees_same_hash(self, sample_data):
        """Two trees with the same data must produce the same root hash."""
        tree_a = MerkleTree(sample_data)
        tree_b = MerkleTree(dict(sample_data))  # fresh copy
        assert tree_a.root_hash == tree_b.root_hash

    def test_different_trees_different_hash(self, sample_data):
        """Two trees with different data must produce different root hashes."""
        data_b = dict(sample_data)
        data_b["key-000"] = "changed-value"
        tree_a = MerkleTree(sample_data)
        tree_b = MerkleTree(data_b)
        assert tree_a.root_hash != tree_b.root_hash

    def test_diff_finds_changed_keys(self, sample_data):
        """Modifying one value should cause diff to find exactly that key."""
        tree_a = MerkleTree(sample_data)
        data_b = dict(sample_data)
        data_b["key-005"] = "modified"
        tree_b = MerkleTree(data_b)
        diff = MerkleTree.diff_leaf_hashes(
            tree_a.get_leaf_hashes(), tree_b.get_leaf_hashes()
        )
        assert diff == {"key-005"}

    def test_diff_finds_missing_keys(self, sample_data):
        """One tree with an extra key should show that key in the diff."""
        data_b = dict(sample_data)
        data_b["key-extra"] = "extra-value"
        tree_a = MerkleTree(sample_data)
        tree_b = MerkleTree(data_b)
        diff = MerkleTree.diff_leaf_hashes(
            tree_a.get_leaf_hashes(), tree_b.get_leaf_hashes()
        )
        assert "key-extra" in diff

    def test_diff_identical_empty(self, sample_data):
        """Identical trees should produce an empty diff set."""
        tree_a = MerkleTree(sample_data)
        tree_b = MerkleTree(dict(sample_data))
        diff = MerkleTree.diff_leaf_hashes(
            tree_a.get_leaf_hashes(), tree_b.get_leaf_hashes()
        )
        assert diff == set()

    def test_odd_leaf_count(self):
        """A tree with 3 entries should build successfully and have a valid root hash."""
        data = {"a": "1", "b": "2", "c": "3"}
        tree = MerkleTree(data)
        assert len(tree.root_hash) == 64  # SHA-256 hex digest length

    def test_single_entry(self):
        """A tree with exactly 1 entry should work correctly."""
        data = {"only-key": "only-value"}
        tree = MerkleTree(data)
        assert len(tree.root_hash) == 64
        leaves = tree.get_leaf_hashes()
        assert len(leaves) == 1
        assert "only-key" in leaves

    def test_empty_tree(self):
        """A tree with 0 entries should have a valid root hash (SHA-256 of empty string)."""
        tree = MerkleTree({})
        assert len(tree.root_hash) == 64
        # SHA-256 of empty string
        import hashlib
        expected = hashlib.sha256(b"").hexdigest()
        assert tree.root_hash == expected

    def test_leaf_hashes_match_data(self, sample_data):
        """The number of leaf hashes should equal the number of data entries."""
        tree = MerkleTree(sample_data)
        leaves = tree.get_leaf_hashes()
        assert len(leaves) == len(sample_data)
        for key in sample_data:
            assert key in leaves

    def test_deterministic(self, sample_data):
        """Building a tree multiple times from the same data always produces the same root hash."""
        hashes = set()
        for _ in range(5):
            tree = MerkleTree(dict(sample_data))
            hashes.add(tree.root_hash)
        assert len(hashes) == 1

    def test_performance_1000_entries(self, sample_data_large):
        """Building a tree from 1000 entries should take less than 50ms."""
        start = time.time()
        tree = MerkleTree(sample_data_large)
        elapsed_ms = (time.time() - start) * 1000
        assert elapsed_ms < 50, f"Tree build took {elapsed_ms:.1f}ms, expected < 50ms"
        assert len(tree.root_hash) == 64
