import os
import tempfile
import pytest
from src.app import create_app


@pytest.fixture
def tmp_storage(tmp_path):
    """Provide a temporary storage file path."""
    return str(tmp_path / "test_registry.json")


@pytest.fixture
def app(tmp_storage):
    """Create app with temporary storage for testing."""
    app = create_app(storage_path=tmp_storage)
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(app):
    """Flask test client."""
    return app.test_client()
