"""Shared fixtures for integration tests.

These tests run INSIDE the chaos-framework container (so the test process
can reach the target stack over chaos-net). They expect the compose stack
to already be up (the runner shell brings it up before invoking pytest).
"""

import pytest


@pytest.fixture(scope="module")
def latency_target() -> str:
    return "log-consumer"


@pytest.fixture(scope="module")
def latency_target_url() -> str:
    return "http://log-consumer:8000/health"
