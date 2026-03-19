"""Application configuration with environment variable overrides."""

import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    """Central configuration for the exactly-once transaction processor."""

    # Kafka
    bootstrap_servers: str = "kafka1:29092,kafka2:29092,kafka3:29092"
    topic_pending: str = "transactions.pending"
    topic_completed: str = "transactions.completed"
    topic_dlq: str = "transactions.dlq"

    # Database
    db_url: str = "postgresql://postgres:postgres@postgres:5432/transactions"

    # Producer
    producer_interval: float = 2.0
    transactional_id_prefix: str = "txn-processor"

    # Consumer
    consumer_group_id: str = "exactly-once-consumer-group"

    # Monitor
    monitor_interval: int = 15

    # Dashboard
    dashboard_port: int = 5050


def load_config() -> Settings:
    """Load configuration from environment variables with dataclass defaults as fallback."""
    return Settings(
        bootstrap_servers=os.environ.get(
            "BOOTSTRAP_SERVERS", Settings.bootstrap_servers
        ),
        topic_pending=os.environ.get("TOPIC_PENDING", Settings.topic_pending),
        topic_completed=os.environ.get("TOPIC_COMPLETED", Settings.topic_completed),
        topic_dlq=os.environ.get("TOPIC_DLQ", Settings.topic_dlq),
        db_url=os.environ.get("DB_URL", Settings.db_url),
        producer_interval=float(
            os.environ.get("PRODUCER_INTERVAL", str(Settings.producer_interval))
        ),
        transactional_id_prefix=os.environ.get(
            "TRANSACTIONAL_ID_PREFIX", Settings.transactional_id_prefix
        ),
        consumer_group_id=os.environ.get(
            "CONSUMER_GROUP_ID", Settings.consumer_group_id
        ),
        monitor_interval=int(
            os.environ.get("MONITOR_INTERVAL", str(Settings.monitor_interval))
        ),
        dashboard_port=int(
            os.environ.get("DASHBOARD_PORT", str(Settings.dashboard_port))
        ),
    )
