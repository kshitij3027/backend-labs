"""Parser configuration loaded from the YAML parser section."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ParserConfig:
    input_dir: str
    output_dir: str
    poll_interval: int
    state_file: str
    filters: tuple

    @classmethod
    def from_dict(cls, d: dict) -> "ParserConfig":
        return cls(
            input_dir=d["input_dir"],
            output_dir=d["output_dir"],
            poll_interval=d.get("poll_interval", 2),
            state_file=d["state_file"],
            filters=tuple(d.get("filters", [])),
        )
