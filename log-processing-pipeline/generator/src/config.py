"""Generator configuration loaded from the YAML generator section."""

from dataclasses import dataclass


@dataclass(frozen=True)
class GeneratorConfig:
    log_file: str
    rate: int
    format: str

    @classmethod
    def from_dict(cls, d: dict) -> "GeneratorConfig":
        return cls(
            log_file=d["log_file"],
            rate=d.get("rate", 10),
            format=d.get("format", "apache"),
        )
