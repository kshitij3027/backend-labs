from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://alertuser:alertpass@postgres:5432/alertdb"
    sync_database_url: str = "postgresql://alertuser:alertpass@postgres:5432/alertdb"
    redis_url: str = "redis://redis:6379/0"
    correlation_window: int = 300
    max_alerts_per_minute: int = 10
    auto_escalation_timeout: int = 900

    class Config:
        env_file = ".env.example"


def get_settings() -> Settings:
    return Settings()
