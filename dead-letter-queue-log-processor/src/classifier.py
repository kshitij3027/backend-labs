"""Failure classification for the Dead Letter Queue Log Processor."""

import json

from src.models import FailureType


class FailureClassifier:
    """Maps exception types to FailureType with per-type retry limits."""

    RETRY_LIMITS: dict[FailureType, int] = {
        FailureType.PARSING: 1,
        FailureType.NETWORK: 5,
        FailureType.RESOURCE: 3,
        FailureType.UNKNOWN: 2,
    }

    @staticmethod
    def classify(error: Exception) -> FailureType:
        """Classify an exception into a FailureType.

        Mapping:
        - json.JSONDecodeError, KeyError, ValueError -> PARSING
        - ConnectionError, TimeoutError, OSError -> NETWORK
        - MemoryError, OverflowError -> RESOURCE
        - Everything else -> UNKNOWN
        """
        if isinstance(error, (json.JSONDecodeError, KeyError, ValueError)):
            return FailureType.PARSING
        if isinstance(error, (ConnectionError, TimeoutError, OSError)):
            return FailureType.NETWORK
        if isinstance(error, (MemoryError, OverflowError)):
            return FailureType.RESOURCE
        return FailureType.UNKNOWN

    @classmethod
    def get_max_retries(cls, failure_type: FailureType) -> int:
        """Return the max retry count for a given failure type."""
        return cls.RETRY_LIMITS.get(failure_type, 2)
