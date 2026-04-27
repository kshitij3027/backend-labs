from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    PROJECT_NAME: str = "Log Search API"
    API_V1_PREFIX: str = "/api/v1"

    SECRET_KEY: str = Field(..., min_length=8)
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_TTL_MINUTES: int = 15

    RATE_LIMIT_REQUESTS: int = 100
    RATE_LIMIT_WINDOW_SECONDS: int = 60

    REDIS_URL: str = "redis://redis:6379"
    CACHE_REDIS_DB: int = 0
    RATE_LIMIT_REDIS_DB: int = 1

    ELASTICSEARCH_URL: str = "http://elasticsearch:9200"
    ELASTICSEARCH_INDEX: str = "logs"

    SEARCH_CACHE_TTL_SECONDS: int = 300
    DEFAULT_SEARCH_LIMIT: int = 100
    MAX_SEARCH_LIMIT: int = 1000
    DEFAULT_SEARCH_OFFSET: int = 0
    DEFAULT_SORT_BY: str = "relevance"
    DEFAULT_SORT_ORDER: str = "desc"
    DEFAULT_INCLUDE_CONTENT: bool = True

    CORS_ALLOWED_ORIGINS: str = "http://localhost:3000,http://localhost:8000"

    SEED_USERNAME: str = "demo"
    SEED_PASSWORD_HASH: str = ""

    LOG_LEVEL: str = "INFO"

    def cors_origins_list(self) -> list[str]:
        return [origin.strip() for origin in self.CORS_ALLOWED_ORIGINS.split(",") if origin.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
