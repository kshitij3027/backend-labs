from __future__ import annotations

from datetime import datetime

import pytest

from src.shared.config import CoordinatorSettings, PartitionSettings
from src.shared.models import PartitionMetadata, TimeRange


@pytest.fixture
def coordinator_settings() -> CoordinatorSettings:
    return CoordinatorSettings(
        coordinator_port=8000,
        partition_urls=(
            "partition-1=http://partition-1:8101,"
            "partition-2=http://partition-2:8102,"
            "partition-3=http://partition-3:8103"
        ),
        request_timeout=5.0,
        default_limit=1000,
        max_concurrent_queries=100,
        query_timeout=30.0,
        log_level="INFO",
    )


@pytest.fixture
def partition_settings() -> PartitionSettings:
    return PartitionSettings(
        partition_id="partition-1",
        partition_port=8101,
        partition_time_start="2026-04-01T00:00:00",
        partition_time_end="2026-04-07T23:59:59",
        indexed_fields="level,service,timestamp",
        log_sample_count=5000,
        log_level="INFO",
    )


@pytest.fixture
def sample_partitions() -> list[PartitionMetadata]:
    return [
        PartitionMetadata(
            id="partition-1",
            url="http://partition-1:8101",
            time_range=TimeRange(
                start=datetime(2026, 4, 1, 0, 0, 0),
                end=datetime(2026, 4, 7, 23, 59, 59),
            ),
            indexed_fields=["level", "service", "timestamp"],
            healthy=True,
        ),
        PartitionMetadata(
            id="partition-2",
            url="http://partition-2:8102",
            time_range=TimeRange(
                start=datetime(2026, 4, 8, 0, 0, 0),
                end=datetime(2026, 4, 14, 23, 59, 59),
            ),
            indexed_fields=["level", "service", "timestamp"],
            healthy=True,
        ),
        PartitionMetadata(
            id="partition-3",
            url="http://partition-3:8103",
            time_range=TimeRange(
                start=datetime(2026, 4, 15, 0, 0, 0),
                end=datetime(2026, 4, 21, 23, 59, 59),
            ),
            indexed_fields=["level", "service", "timestamp"],
            healthy=True,
        ),
    ]
