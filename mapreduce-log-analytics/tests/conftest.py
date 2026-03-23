import os
import shutil
import tempfile

import pytest

from src.generator import generate_apache_logs, generate_json_logs


@pytest.fixture
def tmp_output_dir():
    """Create a temporary directory for test output, clean up after."""
    d = tempfile.mkdtemp(prefix="mapreduce_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture
def sample_json_logs(tmp_output_dir):
    """Generate 100 JSON log lines to a temp file, return the path."""
    path = os.path.join(tmp_output_dir, "test-logs.jsonl")
    generate_json_logs(path, num_lines=100, seed=42)
    return path


@pytest.fixture
def sample_apache_logs(tmp_output_dir):
    """Generate 50 Apache log lines to a temp file, return the path."""
    path = os.path.join(tmp_output_dir, "test-apache.log")
    generate_apache_logs(path, num_lines=50, seed=42)
    return path
