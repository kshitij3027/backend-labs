import os
import json
from dataclasses import dataclass, field


@dataclass
class CoordinatorConfig:
    host: str = "0.0.0.0"
    port: int = 8080
    partition_urls: list[str] = field(default_factory=lambda: ["http://localhost:8081", "http://localhost:8082"])
    query_timeout: float = 5.0
    max_cache_size: int = 1000
    max_merge_size: int = 10000


@dataclass
class PartitionConfig:
    host: str = "0.0.0.0"
    port: int = 8081
    partition_id: str = "partition_1"
    log_count: int = 5000
    days_back: int = 7


def load_coordinator_config() -> CoordinatorConfig:
    partition_urls_raw = os.environ.get("PARTITION_URLS", "")
    if partition_urls_raw:
        partition_urls = [u.strip() for u in partition_urls_raw.split(",")]
    else:
        partition_urls = ["http://localhost:8081", "http://localhost:8082"]
    return CoordinatorConfig(
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", 8080)),
        partition_urls=partition_urls,
        query_timeout=float(os.environ.get("QUERY_TIMEOUT", 5.0)),
        max_cache_size=int(os.environ.get("MAX_CACHE_SIZE", 1000)),
        max_merge_size=int(os.environ.get("MAX_MERGE_SIZE", 10000)),
    )


def load_partition_config() -> PartitionConfig:
    return PartitionConfig(
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", 8081)),
        partition_id=os.environ.get("PARTITION_ID", "partition_1"),
        log_count=int(os.environ.get("LOG_COUNT", 5000)),
        days_back=int(os.environ.get("DAYS_BACK", 7)),
    )
