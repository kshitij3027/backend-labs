import time
from dataclasses import dataclass, field
from src.merkle.tree import MerkleTree


@dataclass
class Entry:
    value: str
    version: int
    timestamp: float


class StorageStore:
    def __init__(self, node_id: str = "unknown"):
        self.node_id = node_id
        self._data: dict[str, Entry] = {}

    def put(self, key: str, value: str, version: int | None = None, timestamp: float | None = None) -> Entry:
        """Store a key-value pair. If version is None, auto-increment from current version."""
        if version is None:
            current = self._data.get(key)
            version = (current.version + 1) if current else 1
        if timestamp is None:
            timestamp = time.time()
        entry = Entry(value=value, version=version, timestamp=timestamp)
        self._data[key] = entry
        return entry

    def get(self, key: str) -> Entry | None:
        return self._data.get(key)

    def get_all(self) -> dict[str, Entry]:
        return dict(self._data)

    def keys(self) -> list[str]:
        return list(self._data.keys())

    def build_merkle_tree(self) -> MerkleTree:
        data = {k: e.value for k, e in self._data.items()}
        return MerkleTree(data)

    def get_merkle_root(self) -> str:
        return self.build_merkle_tree().root_hash

    def get_merkle_leaves(self) -> dict[str, str]:
        return self.build_merkle_tree().get_leaf_hashes()
