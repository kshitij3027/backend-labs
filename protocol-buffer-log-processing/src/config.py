"""Application configuration with environment variable overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """Immutable application configuration.

    All numeric fields can be overridden via environment variables
    of the same name. Directory paths use sensible defaults.
    """

    NUM_SERVICES: int = 5
    NUM_LOGS: int = 1000
    BENCHMARK_ITERATIONS: int = 100
    DAILY_LOG_VOLUME: int = 10_000_000
    HIGH_SCALE_RATE: int = 1000
    HIGH_LOAD_DURATION: int = 30
    JSON_LOG_DIR: str = "logs/json"
    PROTOBUF_LOG_DIR: str = "logs/protobuf"

    @classmethod
    def from_env(cls) -> Config:
        """Create a Config instance, overriding defaults with env vars."""
        return cls(
            NUM_SERVICES=int(os.environ.get("NUM_SERVICES", 5)),
            NUM_LOGS=int(os.environ.get("NUM_LOGS", 1000)),
            BENCHMARK_ITERATIONS=int(os.environ.get("BENCHMARK_ITERATIONS", 100)),
            DAILY_LOG_VOLUME=int(os.environ.get("DAILY_LOG_VOLUME", 10_000_000)),
            HIGH_SCALE_RATE=int(os.environ.get("HIGH_SCALE_RATE", 1000)),
            HIGH_LOAD_DURATION=int(os.environ.get("HIGH_LOAD_DURATION", 30)),
            JSON_LOG_DIR=os.environ.get("JSON_LOG_DIR", "logs/json"),
            PROTOBUF_LOG_DIR=os.environ.get("PROTOBUF_LOG_DIR", "logs/protobuf"),
        )
