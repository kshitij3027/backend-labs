"""Storage configuration loaded from the YAML storage section."""

from dataclasses import dataclass


@dataclass(frozen=True)
class StorageConfig:
    input_dir: str
    storage_dir: str
    poll_interval: int
    rotation_size_mb: float
    rotation_hours: int
    state_file: str
    compression_enabled: bool

    @classmethod
    def from_dict(cls, d: dict) -> "StorageConfig":
        return cls(
            input_dir=d["input_dir"],
            storage_dir=d["storage_dir"],
            poll_interval=d.get("poll_interval", 2),
            rotation_size_mb=d.get("rotation_size_mb", 5),
            rotation_hours=d.get("rotation_hours", 24),
            state_file=d["state_file"],
            compression_enabled=d.get("compression_enabled", True),
        )
