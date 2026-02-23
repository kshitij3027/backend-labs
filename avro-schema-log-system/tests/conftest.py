import pytest
from src.schema_registry import SchemaRegistry
from src.serializer import AvroSerializer
from src.deserializer import AvroDeserializer
from src.compatibility import CompatibilityChecker
from src.log_event import LogEvent
from src.app import create_app

@pytest.fixture
def registry():
    return SchemaRegistry()

@pytest.fixture
def serializer(registry):
    return AvroSerializer(registry)

@pytest.fixture
def deserializer(registry):
    return AvroDeserializer(registry)

@pytest.fixture
def checker(registry, serializer, deserializer):
    return CompatibilityChecker(registry, serializer, deserializer)

@pytest.fixture
def sample_events():
    return {v: LogEvent.generate_sample(v) for v in ["v1", "v2", "v3"]}

@pytest.fixture
def app():
    app = create_app()
    app.config["TESTING"] = True
    return app

@pytest.fixture
def client(app):
    return app.test_client()
