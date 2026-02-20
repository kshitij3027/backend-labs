"""Integration tests — spin up a real async TCP server and connect to it."""

import asyncio
import json

import pytest
import pytest_asyncio

from server.config import ServerConfig
from server.tcp_server import TCPServer


@pytest.fixture
def config(tmp_path):
    return ServerConfig(
        SERVER_HOST="127.0.0.1",
        SERVER_PORT=0,  # Let OS pick a free port
        BUFFER_SIZE=65536,
        ENABLE_TLS=False,
        CERT_DIR=str(tmp_path / "certs"),
        ENABLE_UDP=False,
        UDP_PORT=0,
        ENABLE_PERSISTENCE=True,
        LOG_DIR=str(tmp_path / "logs"),
        MIN_LOG_LEVEL="DEBUG",
        CIRCUIT_BREAKER_ENABLED=False,
        BATCH_SIZE=500,
        BATCH_FLUSH_MS=100,
    )


@pytest_asyncio.fixture
async def server(config):
    """Start a TCP server for testing, yield its (host, port), then stop."""
    srv = TCPServer(config)
    task = asyncio.create_task(srv.start())

    # Wait for server to be ready
    for _ in range(50):
        if srv.server is not None and srv.server.is_serving():
            break
        await asyncio.sleep(0.05)
    else:
        raise RuntimeError("Server did not start in time")

    port = srv.server.sockets[0].getsockname()[1]
    yield ("127.0.0.1", port)

    await srv.stop()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_single_message(server):
    """Send a single NDJSON message and verify response."""
    host, port = server
    reader, writer = await asyncio.open_connection(host, port)

    msg = json.dumps({"level": "INFO", "message": "hello"}) + "\n"
    writer.write(msg.encode())
    await writer.drain()

    data = await asyncio.wait_for(reader.readline(), timeout=5.0)
    response = json.loads(data.decode())
    assert response["status"] == "ok"

    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_multiple_messages(server):
    """Send multiple messages on one connection."""
    host, port = server
    reader, writer = await asyncio.open_connection(host, port)

    for i in range(10):
        msg = json.dumps({"level": "WARNING", "message": f"warning {i}"}) + "\n"
        writer.write(msg.encode())
        await writer.drain()

        data = await asyncio.wait_for(reader.readline(), timeout=5.0)
        response = json.loads(data.decode())
        assert response["status"] == "ok"

    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_concurrent_connections(server):
    """Open multiple concurrent connections."""
    host, port = server

    async def send_message(msg_id: int):
        reader, writer = await asyncio.open_connection(host, port)
        msg = json.dumps({"level": "ERROR", "message": f"error {msg_id}"}) + "\n"
        writer.write(msg.encode())
        await writer.drain()

        data = await asyncio.wait_for(reader.readline(), timeout=5.0)
        response = json.loads(data.decode())
        assert response["status"] == "ok"

        writer.close()
        await writer.wait_closed()

    await asyncio.gather(*[send_message(i) for i in range(10)])


@pytest.mark.asyncio
async def test_invalid_json(server):
    """Send invalid JSON and verify error response."""
    host, port = server
    reader, writer = await asyncio.open_connection(host, port)

    writer.write(b"not valid json\n")
    await writer.drain()

    data = await asyncio.wait_for(reader.readline(), timeout=5.0)
    response = json.loads(data.decode())
    assert response["status"] == "error"

    writer.close()
    await writer.wait_closed()


@pytest.mark.asyncio
async def test_persistence(server, tmp_path):
    """Verify messages are persisted to log file."""
    host, port = server
    reader, writer = await asyncio.open_connection(host, port)

    msg = json.dumps({"level": "CRITICAL", "message": "server on fire"}) + "\n"
    writer.write(msg.encode())
    await writer.drain()

    data = await asyncio.wait_for(reader.readline(), timeout=5.0)
    response = json.loads(data.decode())
    assert response["status"] == "ok"

    writer.close()
    await writer.wait_closed()

    # Give persistence a moment to flush
    await asyncio.sleep(0.5)

    log_file = tmp_path / "logs" / "server.log"
    assert log_file.exists()
    content = log_file.read_text()
    assert "server on fire" in content
