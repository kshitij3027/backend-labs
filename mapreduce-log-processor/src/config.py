from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    POSTGRES_URL: str = "postgresql+asyncpg://mapreduce:mapreduce@postgres:5432/mapreduce"
    POSTGRES_SYNC_URL: str = "postgresql://mapreduce:mapreduce@postgres:5432/mapreduce"
    REDIS_URL: str = "redis://redis:6379/0"
    COORDINATOR_HOST: str = "coordinator"
    COORDINATOR_PORT: int = 8000
    HEARTBEAT_INTERVAL: int = 5
    HEARTBEAT_TIMEOUT: int = 15
    MAX_RETRIES: int = 3
    REDIS_TTL: int = 3600
    MAX_CONCURRENT_TASKS: int = 8
    BACKPRESSURE_THRESHOLD: int = 80
    WORKER_ID: str = ""

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
