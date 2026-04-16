"""Shared async httpx client for the coordinator.

A single pooled ``httpx.AsyncClient`` is reused for all fan-out requests so
connections to the index nodes are kept warm.
"""

from __future__ import annotations

import httpx

_client: httpx.AsyncClient | None = None


def get_client(timeout: float = 5.0) -> httpx.AsyncClient:
    """Return (creating if needed) the process-wide AsyncClient."""
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout),
            limits=httpx.Limits(
                max_connections=200, max_keepalive_connections=100
            ),
        )
    return _client


async def close_client() -> None:
    """Close the shared client (safe to call when uninitialized)."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
