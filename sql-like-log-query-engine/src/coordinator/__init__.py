"""Coordinator package — scatter/gather API server."""

from .aggregator import merge
from .app import create_coordinator_app
from .executor import QueryExecutor
from .node_client import check_health, fetch_metadata, post_execute
from .progress import ProgressEmitter, ProgressRegistry, default_registry
from .registry import PartitionRegistry

__all__ = [
    "PartitionRegistry",
    "ProgressEmitter",
    "ProgressRegistry",
    "QueryExecutor",
    "check_health",
    "create_coordinator_app",
    "default_registry",
    "fetch_metadata",
    "merge",
    "post_execute",
]
