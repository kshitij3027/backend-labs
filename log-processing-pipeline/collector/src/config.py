"""Collector configuration loaded from the YAML collector section."""

from dataclasses import dataclass


@dataclass(frozen=True)
class CollectorConfig:
    source_file: str
    output_dir: str
    poll_interval: int
    batch_size: int
    state_file: str

    @classmethod
    def from_dict(cls, d: dict) -> "CollectorConfig":
        return cls(
            source_file=d["source_file"],
            output_dir=d["output_dir"],
            poll_interval=d.get("poll_interval", 2),
            batch_size=d.get("batch_size", 100),
            state_file=d["state_file"],
        )
