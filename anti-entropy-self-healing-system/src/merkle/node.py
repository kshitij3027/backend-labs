from dataclasses import dataclass
from typing import Optional


@dataclass
class MerkleNode:
    hash_value: str
    left: Optional['MerkleNode'] = None
    right: Optional['MerkleNode'] = None
    key: Optional[str] = None  # Only set for leaf nodes
