"""Application configuration loaded from environment variables.

All knobs the operator can tune at startup live here. The defaults match the
spec in `project_requirements.md` §7 (and `plan.md` Commit 1).

The `from_env` classmethod reads ``os.environ`` once and returns an immutable
``AppConfig`` instance — keeping config gathering out of the hot path and
making the values easy to reason about in tests.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List


def _parse_csv_list(raw: str | None, default: List[str]) -> List[str]:
    """Parse a comma-separated environment value into a list of trimmed strings.

    Empty string and ``None`` both fall back to ``default``.
    """
    if raw is None:
        return list(default)
    items = [token.strip() for token in raw.split(",") if token.strip()]
    return items if items else list(default)


def _parse_int(raw: str | None, default: int) -> int:
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _parse_float(raw: str | None, default: float) -> float:
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _parse_bool(raw: str | None, default: bool) -> bool:
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


# Defaults pulled out of the dataclass so tests can reference them without
# instantiating an AppConfig.
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000
DEFAULT_REGIONS: List[str] = ["us-east", "europe", "asia"]
DEFAULT_PRIMARY_PREFERENCE: List[str] = ["us-east", "europe", "asia"]
DEFAULT_REPLICATION_LAG_TARGET_MS = 100
DEFAULT_WEBSOCKET_PUSH_INTERVAL_SEC = 5.0
DEFAULT_HEALTH_CHECK_INTERVAL_SEC = 1.0
DEFAULT_FAILOVER_TIMEOUT_SEC = 5.0
DEFAULT_MAX_LOGS_RETURNED = 25
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_ALLOW_KILL_ENDPOINT = True


@dataclass(frozen=True)
class AppConfig:
    """Runtime configuration for the multi-region log replication service."""

    host: str = DEFAULT_HOST
    port: int = DEFAULT_PORT
    regions: List[str] = field(default_factory=lambda: list(DEFAULT_REGIONS))
    primary_preference: List[str] = field(
        default_factory=lambda: list(DEFAULT_PRIMARY_PREFERENCE)
    )
    replication_lag_target_ms: int = DEFAULT_REPLICATION_LAG_TARGET_MS
    websocket_push_interval_sec: float = DEFAULT_WEBSOCKET_PUSH_INTERVAL_SEC
    health_check_interval_sec: float = DEFAULT_HEALTH_CHECK_INTERVAL_SEC
    failover_timeout_sec: float = DEFAULT_FAILOVER_TIMEOUT_SEC
    max_logs_returned: int = DEFAULT_MAX_LOGS_RETURNED
    log_level: str = DEFAULT_LOG_LEVEL
    allow_kill_endpoint: bool = DEFAULT_ALLOW_KILL_ENDPOINT

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "AppConfig":
        """Construct an ``AppConfig`` by reading environment variables.

        Pass an explicit ``env`` mapping for tests; otherwise ``os.environ``
        is used.
        """
        e = env if env is not None else os.environ
        return cls(
            host=e.get("HOST", DEFAULT_HOST) or DEFAULT_HOST,
            port=_parse_int(e.get("PORT"), DEFAULT_PORT),
            regions=_parse_csv_list(e.get("REGIONS"), DEFAULT_REGIONS),
            primary_preference=_parse_csv_list(
                e.get("PRIMARY_PREFERENCE"), DEFAULT_PRIMARY_PREFERENCE
            ),
            replication_lag_target_ms=_parse_int(
                e.get("REPLICATION_LAG_TARGET_MS"),
                DEFAULT_REPLICATION_LAG_TARGET_MS,
            ),
            websocket_push_interval_sec=_parse_float(
                e.get("WEBSOCKET_PUSH_INTERVAL_SEC"),
                DEFAULT_WEBSOCKET_PUSH_INTERVAL_SEC,
            ),
            health_check_interval_sec=_parse_float(
                e.get("HEALTH_CHECK_INTERVAL_SEC"),
                DEFAULT_HEALTH_CHECK_INTERVAL_SEC,
            ),
            failover_timeout_sec=_parse_float(
                e.get("FAILOVER_TIMEOUT_SEC"), DEFAULT_FAILOVER_TIMEOUT_SEC
            ),
            max_logs_returned=_parse_int(
                e.get("MAX_LOGS_RETURNED"), DEFAULT_MAX_LOGS_RETURNED
            ),
            log_level=(e.get("LOG_LEVEL") or DEFAULT_LOG_LEVEL).upper(),
            allow_kill_endpoint=_parse_bool(
                e.get("ALLOW_KILL_ENDPOINT"), DEFAULT_ALLOW_KILL_ENDPOINT
            ),
        )
