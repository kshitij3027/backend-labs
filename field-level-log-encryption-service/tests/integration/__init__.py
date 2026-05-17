"""Integration tests for the field-level log encryption service.

Distinct from ``tests/unit/`` in that these tests build the full FastAPI
application (including the lifespan handler that mints the initial DEK and
spawns the background rotation task) and exercise it via an in-process
httpx ``ASGITransport`` client — no real network sockets.

Marked with ``@pytest.mark.integration`` so a future ``-m "not integration"``
selector can run unit tests in isolation.
"""
