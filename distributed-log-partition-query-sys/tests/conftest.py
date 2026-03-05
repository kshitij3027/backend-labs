import pytest
from datetime import datetime, timedelta, timezone
from src.config import CoordinatorConfig, PartitionConfig
from src.models import LogEntry, Query, QueryFilter, TimeRange


@pytest.fixture
def coordinator_config():
    return CoordinatorConfig(
        partition_urls=["http://partition-1:8081", "http://partition-2:8082"],
    )


@pytest.fixture
def partition_config():
    return PartitionConfig(
        partition_id="test_partition",
        log_count=100,
        days_back=7,
    )


@pytest.fixture
def sample_log_entries():
    now = datetime.now(tz=timezone.utc)
    return [
        LogEntry(timestamp=now - timedelta(minutes=i), level=level, service=service, message=f"Test message {i}", partition_id="partition_1")
        for i, (level, service) in enumerate([
            ("ERROR", "auth-service"),
            ("INFO", "api-gateway"),
            ("WARN", "payment-service"),
            ("DEBUG", "user-service"),
            ("INFO", "auth-service"),
        ])
    ]


@pytest.fixture
def sample_query():
    return Query(limit=10)
