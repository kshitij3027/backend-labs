"""Configuration loading from environment variables with sensible defaults."""

import os
import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

DEFAULT_DISTRIBUTION = {"INFO": 0.70, "WARNING": 0.20, "ERROR": 0.05, "DEBUG": 0.05}
DEFAULT_SERVICES = [
    "user-service",
    "payment-service",
    "inventory-service",
    "notification-service",
]
VALID_FORMATS = ("text", "json", "csv")


def _parse_bool(val: str | None, default: bool) -> bool:
    if val is None:
        return default
    return val.strip().lower() in ("true", "1", "yes")


def _parse_distribution(val: str | None) -> dict[str, float]:
    if val is None:
        return DEFAULT_DISTRIBUTION.copy()
    try:
        pairs = [p.strip() for p in val.split(",")]
        dist = {}
        for pair in pairs:
            level, weight = pair.split(":")
            dist[level.strip().upper()] = float(weight.strip())
        total = sum(dist.values())
        if abs(total - 1.0) > 0.01:
            logger.warning(
                "LOG_DISTRIBUTION weights sum to %.2f (expected ~1.0), using defaults",
                total,
            )
            return DEFAULT_DISTRIBUTION.copy()
        return dist
    except (ValueError, KeyError):
        logger.warning("Invalid LOG_DISTRIBUTION format, using defaults")
        return DEFAULT_DISTRIBUTION.copy()


@dataclass(frozen=True)
class Config:
    log_rate: int = 10
    output_file: str = "logs/app.log"
    console_output: bool = True
    log_format: str = "text"
    log_distribution: dict = field(default_factory=lambda: DEFAULT_DISTRIBUTION.copy())
    services: list = field(default_factory=lambda: DEFAULT_SERVICES.copy())
    enable_bursts: bool = True
    burst_frequency: float = 0.05
    burst_multiplier: int = 5
    burst_duration: int = 3
    enable_patterns: bool = True


def load_config() -> Config:
    """Load configuration from environment variables, falling back to defaults."""
    log_format = os.environ.get("LOG_FORMAT", "text").strip().lower()
    if log_format not in VALID_FORMATS:
        logger.warning(
            "Invalid LOG_FORMAT '%s', falling back to 'text'", log_format
        )
        log_format = "text"

    return Config(
        log_rate=int(os.environ.get("LOG_RATE", "10")),
        output_file=os.environ.get("OUTPUT_FILE", "logs/app.log"),
        console_output=_parse_bool(os.environ.get("CONSOLE_OUTPUT"), True),
        log_format=log_format,
        log_distribution=_parse_distribution(os.environ.get("LOG_DISTRIBUTION")),
        services=[
            s.strip()
            for s in os.environ.get("SERVICES", ",".join(DEFAULT_SERVICES)).split(",")
        ],
        enable_bursts=_parse_bool(os.environ.get("ENABLE_BURSTS"), True),
        burst_frequency=float(os.environ.get("BURST_FREQUENCY", "0.05")),
        burst_multiplier=int(os.environ.get("BURST_MULTIPLIER", "5")),
        burst_duration=int(os.environ.get("BURST_DURATION", "3")),
        enable_patterns=_parse_bool(os.environ.get("ENABLE_PATTERNS"), True),
    )
