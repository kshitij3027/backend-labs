"""Environment-driven settings for index nodes and the coordinator."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class NodeSettings(BaseSettings):
    """Settings for a single index node service."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    node_id: str = "node-1"
    node_port: int = 8101
    redis_host: str = "redis"
    redis_port: int = 6379
    redis_db: int = 0


class CoordinatorSettings(BaseSettings):
    """Settings for the coordinator service."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    coordinator_port: int = 8000
    # Comma-separated list like: "node-1=http://node-1:8101,node-2=http://node-2:8102"
    node_urls: str = ""
    virtual_nodes: int = 100
    redis_host: str = "redis"
    redis_port: int = 6379
    request_timeout: float = 5.0
    retry_count: int = 3
    retry_base_delay: float = 0.1
    cache_size: int = 1000
    cache_ttl: int = 60

    def parsed_node_urls(self) -> dict[str, str]:
        """Parse ``node_urls`` into a ``{node_id: url}`` dict."""
        result: dict[str, str] = {}
        if not self.node_urls:
            return result
        for entry in self.node_urls.split(","):
            entry = entry.strip()
            if not entry:
                continue
            if "=" not in entry:
                continue
            node_id, url = entry.split("=", 1)
            node_id = node_id.strip()
            url = url.strip()
            if node_id and url:
                result[node_id] = url
        return result
