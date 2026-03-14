"""Tests for FailureClassifier."""

import json

import pytest

from src.classifier import FailureClassifier
from src.models import FailureType


class TestClassify:
    """Tests for FailureClassifier.classify."""

    def test_classify_json_error(self):
        error = json.JSONDecodeError("bad json", "", 0)
        assert FailureClassifier.classify(error) == FailureType.PARSING

    def test_classify_key_error(self):
        error = KeyError("missing_key")
        assert FailureClassifier.classify(error) == FailureType.PARSING

    def test_classify_value_error(self):
        error = ValueError("bad value")
        assert FailureClassifier.classify(error) == FailureType.PARSING

    def test_classify_connection_error(self):
        error = ConnectionError("refused")
        assert FailureClassifier.classify(error) == FailureType.NETWORK

    def test_classify_timeout_error(self):
        error = TimeoutError("timed out")
        assert FailureClassifier.classify(error) == FailureType.NETWORK

    def test_classify_os_error(self):
        error = OSError("disk I/O")
        assert FailureClassifier.classify(error) == FailureType.NETWORK

    def test_classify_memory_error(self):
        error = MemoryError("oom")
        assert FailureClassifier.classify(error) == FailureType.RESOURCE

    def test_classify_overflow_error(self):
        error = OverflowError("too big")
        assert FailureClassifier.classify(error) == FailureType.RESOURCE

    def test_classify_runtime_error(self):
        error = RuntimeError("something went wrong")
        assert FailureClassifier.classify(error) == FailureType.UNKNOWN

    def test_classify_generic_exception(self):
        error = Exception("generic")
        assert FailureClassifier.classify(error) == FailureType.UNKNOWN


class TestRetryLimits:
    """Tests for RETRY_LIMITS and get_max_retries."""

    def test_retry_limits(self):
        assert FailureClassifier.RETRY_LIMITS[FailureType.PARSING] == 1
        assert FailureClassifier.RETRY_LIMITS[FailureType.NETWORK] == 5
        assert FailureClassifier.RETRY_LIMITS[FailureType.RESOURCE] == 3
        assert FailureClassifier.RETRY_LIMITS[FailureType.UNKNOWN] == 2

    def test_get_max_retries_per_type(self):
        assert FailureClassifier.get_max_retries(FailureType.PARSING) == 1
        assert FailureClassifier.get_max_retries(FailureType.NETWORK) == 5
        assert FailureClassifier.get_max_retries(FailureType.RESOURCE) == 3
        assert FailureClassifier.get_max_retries(FailureType.UNKNOWN) == 2
