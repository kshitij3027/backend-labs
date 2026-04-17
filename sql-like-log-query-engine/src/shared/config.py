from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class PartitionSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    partition_id: str = "partition-1"
    partition_port: int = 8101
    partition_time_start: str = "2026-04-01T00:00:00"
    partition_time_end: str = "2026-04-07T23:59:59"
    indexed_fields: str = "level,service,timestamp"
    log_sample_count: int = 5000
    log_level: str = "INFO"

    def indexed_fields_list(self) -> list[str]:
        return [f.strip() for f in self.indexed_fields.split(",") if f.strip()]


class CoordinatorSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    coordinator_port: int = 8000
    partition_urls: str = (
        "partition-1=http://partition-1:8101,"
        "partition-2=http://partition-2:8102,"
        "partition-3=http://partition-3:8103"
    )
    request_timeout: float = 5.0
    default_limit: int = 1000
    max_concurrent_queries: int = 100
    query_timeout: float = 30.0
    log_level: str = "INFO"

    def partition_urls_dict(self) -> dict[str, str]:
        result: dict[str, str] = {}
        for entry in self.partition_urls.split(","):
            entry = entry.strip()
            if not entry or "=" not in entry:
                continue
            key, _, url = entry.partition("=")
            key = key.strip()
            url = url.strip()
            if key and url:
                result[key] = url
        return result
