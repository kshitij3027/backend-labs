"""Application configuration using Pydantic BaseModel."""

from pydantic import BaseModel


class Settings(BaseModel):
    """Application settings with sensible defaults."""

    BACKEND_PORT: int = 8000
    STORAGE_DIR: str = "./storage"
    FLUSH_INTERVAL: int = 60
    SEARCH_RESULT_LIMIT: int = 100


settings = Settings()
