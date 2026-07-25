"""Unit tests: no container, no socket, no database, no Redis.

Everything here runs against directly-constructed objects or against the ASGI app through
``TestClient`` **without** entering the lifespan, so a unit test can never be slow for a reason
that is not its own fault. Anything that needs a live dependency belongs in ``tests/integration``.
"""
