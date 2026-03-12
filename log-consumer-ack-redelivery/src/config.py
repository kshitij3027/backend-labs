"""Application configuration loaded from environment variables."""

from typing import List

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Settings loaded from environment variables with sensible defaults."""

    # RabbitMQ connection
    RABBITMQ_HOST: str = "rabbitmq"
    RABBITMQ_PORT: int = 5672
    RABBITMQ_USER: str = "guest"
    RABBITMQ_PASS: str = "guest"

    # Queue / exchange names
    MAIN_QUEUE: str = "logs.incoming"
    MAIN_EXCHANGE: str = "logs.main"

    # Retry configuration
    RETRY_EXCHANGE: str = "logs.retry"
    RETRY_DELAYS: List[int] = [1000, 2000, 4000, 8000]  # milliseconds

    # Dead-letter queue
    DLQ_QUEUE: str = "logs.dead_letter"
    DLQ_EXCHANGE: str = "logs.dlx"

    # Processing behaviour
    MAX_RETRIES: int = 5
    ACK_TIMEOUT_SEC: int = 30
    PREFETCH_COUNT: int = 10

    # Dashboard / testing
    DASHBOARD_PORT: int = 8000
    FAILURE_RATE: float = 0.2
    TIMEOUT_RATE: float = 0.1

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
    }


def get_settings() -> Settings:
    """Return a cached Settings instance."""
    return Settings()
