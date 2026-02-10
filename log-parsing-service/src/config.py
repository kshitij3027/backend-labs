"""Configuration loading from environment variables."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    input_dir: str = "./logs"
    output_dir: str = "./parsed_logs"


def load_config() -> Config:
    """Build Config from environment variables with sensible defaults."""
    return Config(
        input_dir=os.environ.get("LOG_INPUT_DIR", "./logs"),
        output_dir=os.environ.get("LOG_OUTPUT_DIR", "./parsed_logs"),
    )
