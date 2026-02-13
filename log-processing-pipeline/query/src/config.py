"""Query configuration loaded from the YAML query section."""

from dataclasses import dataclass


@dataclass(frozen=True)
class QueryConfig:
    storage_dir: str

    @classmethod
    def from_dict(cls, d: dict) -> "QueryConfig":
        return cls(storage_dir=d["storage_dir"])
