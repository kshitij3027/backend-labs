"""Unit tests for the ConnectionHandler."""

import asyncio
import json

import pytest
import pytest_asyncio

from server.config import ServerConfig
from server.handler import ConnectionHandler
from server.persistence import LogPersistence


class MockStreamReader:
    """Mock asyncio.StreamReader for testing."""

    def __init__(self, data: list[bytes]):
        self._data = data
        self._index = 0

    async def readline(self) -> bytes:
        if self._index >= len(self._data):
            return b""
        line = self._data[self._index]
        self._index += 1
        return line


class MockStreamWriter:
    """Mock asyncio.StreamWriter for testing."""

    def __init__(self):
        self.written = []
        self._closed = False

    def write(self, data: bytes):
        self.written.append(data)

    async def drain(self):
        pass

    def close(self):
        self._closed = True

    async def wait_closed(self):
        pass

    def get_extra_info(self, key, default=None):
        if key == "peername":
            return ("127.0.0.1", 12345)
        return default


@pytest.fixture
def config(tmp_path):
    """Create a test config with temp log directory."""
    return ServerConfig(
        SERVER_HOST="127.0.0.1",
        SERVER_PORT=9000,
        BUFFER_SIZE=65536,
        ENABLE_TLS=False,
        CERT_DIR=str(tmp_path / "certs"),
        ENABLE_UDP=False,
        UDP_PORT=9001,
        ENABLE_PERSISTENCE=True,
        LOG_DIR=str(tmp_path / "logs"),
        MIN_LOG_LEVEL="DEBUG",
        CIRCUIT_BREAKER_ENABLED=False,
        BATCH_SIZE=500,
        BATCH_FLUSH_MS=100,
    )


@pytest.fixture
def persistence(config):
    return LogPersistence(config.LOG_DIR)


@pytest.mark.asyncio
async def test_valid_message(config, persistence):
    """Test that a valid log message is accepted."""
    msg = json.dumps({"level": "INFO", "message": "test message"}) + "\n"
    reader = MockStreamReader([msg.encode()])
    writer = MockStreamWriter()

    handler = ConnectionHandler(config, persistence)
    await handler.handle_client(reader, writer)

    assert len(writer.written) == 1
    response = json.loads(writer.written[0].decode())
    assert response["status"] == "ok"


@pytest.mark.asyncio
async def test_invalid_json(config, persistence):
    """Test that invalid JSON gets an error response."""
    reader = MockStreamReader([b"not valid json\n"])
    writer = MockStreamWriter()

    handler = ConnectionHandler(config, persistence)
    await handler.handle_client(reader, writer)

    assert len(writer.written) == 1
    response = json.loads(writer.written[0].decode())
    assert response["status"] == "error"


@pytest.mark.asyncio
async def test_missing_fields(config, persistence):
    """Test that messages missing required fields get an error."""
    msg = json.dumps({"level": "INFO"}) + "\n"  # missing "message"
    reader = MockStreamReader([msg.encode()])
    writer = MockStreamWriter()

    handler = ConnectionHandler(config, persistence)
    await handler.handle_client(reader, writer)

    assert len(writer.written) == 1
    response = json.loads(writer.written[0].decode())
    assert response["status"] == "error"


@pytest.mark.asyncio
async def test_log_level_filter(config, persistence):
    """Test that messages below MIN_LOG_LEVEL are filtered."""
    filtered_config = ServerConfig(
        SERVER_HOST="127.0.0.1",
        SERVER_PORT=9000,
        BUFFER_SIZE=65536,
        ENABLE_TLS=False,
        CERT_DIR=config.CERT_DIR,
        ENABLE_UDP=False,
        UDP_PORT=9001,
        ENABLE_PERSISTENCE=True,
        LOG_DIR=config.LOG_DIR,
        MIN_LOG_LEVEL="ERROR",
        CIRCUIT_BREAKER_ENABLED=False,
        BATCH_SIZE=500,
        BATCH_FLUSH_MS=100,
    )
    msg = json.dumps({"level": "DEBUG", "message": "debug msg"}) + "\n"
    reader = MockStreamReader([msg.encode()])
    writer = MockStreamWriter()

    handler = ConnectionHandler(filtered_config, persistence)
    await handler.handle_client(reader, writer)

    # Should still respond with ok, but not persist
    assert len(writer.written) == 1
    response = json.loads(writer.written[0].decode())
    assert response["status"] == "ok"


@pytest.mark.asyncio
async def test_multiple_messages(config, persistence):
    """Test handling multiple messages in sequence."""
    messages = [
        json.dumps({"level": "INFO", "message": f"msg {i}"}).encode() + b"\n"
        for i in range(5)
    ]
    reader = MockStreamReader(messages)
    writer = MockStreamWriter()

    handler = ConnectionHandler(config, persistence)
    await handler.handle_client(reader, writer)

    assert len(writer.written) == 5
    for data in writer.written:
        response = json.loads(data.decode())
        assert response["status"] == "ok"


@pytest.mark.asyncio
async def test_persistence_writes(config, persistence, tmp_path):
    """Test that messages are persisted to log file."""
    msg = json.dumps({"level": "ERROR", "message": "critical failure"}) + "\n"
    reader = MockStreamReader([msg.encode()])
    writer = MockStreamWriter()

    handler = ConnectionHandler(config, persistence)
    await handler.handle_client(reader, writer)

    # Check log file exists and has content
    log_file = tmp_path / "logs" / "server.log"
    assert log_file.exists()
    content = log_file.read_text()
    assert "critical failure" in content
    assert "[ERROR]" in content
