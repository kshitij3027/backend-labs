"""Partition configuration for the Smart Log Partitioning System."""

import os
from dataclasses import dataclass


@dataclass
class PartitionConfig:
    """Configuration for log partitioning behavior."""

    strategy: str = "source"  # "source", "time", or "hybrid"
    num_nodes: int = 3
    time_bucket_hours: int = 1
    data_dir: str = "data"
    host: str = "0.0.0.0"
    port: int = 5000


def load_config() -> PartitionConfig:
    """Load partition configuration from environment variables with defaults."""
    return PartitionConfig(
        strategy=os.environ.get("PARTITION_STRATEGY", "source"),
        num_nodes=int(os.environ.get("PARTITION_NUM_NODES", "3")),
        time_bucket_hours=int(os.environ.get("PARTITION_TIME_BUCKET_HOURS", "1")),
        data_dir=os.environ.get("PARTITION_DATA_DIR", "data"),
        host=os.environ.get("PARTITION_HOST", "0.0.0.0"),
        port=int(os.environ.get("PARTITION_PORT", "5000")),
    )
