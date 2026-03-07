import pytest


@pytest.fixture
def sample_data():
    return {f"key-{i:03d}": f"value-{i}" for i in range(10)}


@pytest.fixture
def sample_data_large():
    return {f"key-{i:04d}": f"value-{i}" for i in range(1000)}
