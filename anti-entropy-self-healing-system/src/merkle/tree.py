import hashlib
from src.merkle.node import MerkleNode


class MerkleTree:
    """A Merkle tree built from a dictionary of key-value pairs."""

    def __init__(self, data: dict[str, str]):
        self._data = data
        self._leaves: dict[str, str] = {}
        self._root: MerkleNode | None = None
        self._build(data)

    def _hash(self, value: str) -> str:
        """Return the SHA-256 hex digest of a string."""
        return hashlib.sha256(value.encode()).hexdigest()

    def _build(self, data: dict[str, str]) -> None:
        """Build the Merkle tree bottom-up from sorted key-value pairs."""
        if not data:
            self._root = MerkleNode(hash_value=self._hash(""))
            return

        # Sort keys alphabetically and create leaf nodes
        sorted_keys = sorted(data.keys())
        nodes: list[MerkleNode] = []
        for key in sorted_keys:
            leaf_hash = self._hash(key + ":" + data[key])
            node = MerkleNode(hash_value=leaf_hash, key=key)
            nodes.append(node)
            self._leaves[key] = leaf_hash

        # If odd number of leaves, duplicate the last one
        if len(nodes) > 1 and len(nodes) % 2 != 0:
            nodes.append(MerkleNode(
                hash_value=nodes[-1].hash_value,
                key=nodes[-1].key,
            ))

        # Build tree bottom-up, pairing nodes level by level
        while len(nodes) > 1:
            next_level: list[MerkleNode] = []
            for i in range(0, len(nodes), 2):
                if i + 1 < len(nodes):
                    left = nodes[i]
                    right = nodes[i + 1]
                    parent_hash = self._hash(left.hash_value + right.hash_value)
                    parent = MerkleNode(
                        hash_value=parent_hash,
                        left=left,
                        right=right,
                    )
                    next_level.append(parent)
                else:
                    # Odd node at this level, carry it up
                    next_level.append(nodes[i])
            nodes = next_level

        self._root = nodes[0]

    @property
    def root_hash(self) -> str:
        """Return the root hash of the tree."""
        if self._root is None:
            return self._hash("")
        return self._root.hash_value

    def get_leaf_hashes(self) -> dict[str, str]:
        """Return a dict mapping each key to its leaf hash."""
        return dict(self._leaves)

    @staticmethod
    def diff_leaf_hashes(leaves_a: dict[str, str], leaves_b: dict[str, str]) -> set[str]:
        """Compare two leaf hash dicts and return the set of keys that differ or are missing in either."""
        all_keys = set(leaves_a.keys()) | set(leaves_b.keys())
        diff_keys: set[str] = set()
        for key in all_keys:
            if key not in leaves_a or key not in leaves_b:
                diff_keys.add(key)
            elif leaves_a[key] != leaves_b[key]:
                diff_keys.add(key)
        return diff_keys
