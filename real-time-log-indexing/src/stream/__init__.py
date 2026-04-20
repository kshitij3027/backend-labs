"""Redis stream ingest package.

Thin re-export layer so callers can do
``from src.stream import RedisStreamConsumer`` instead of reaching
into the submodule. Keeping the submodule pattern matches the rest of
``src/`` (``src.index``, ``src.api``) and leaves room for future
streaming transports (e.g. a WebSocket producer) without reshuffling
imports.
"""

from __future__ import annotations

from src.stream.redis_consumer import RedisStreamConsumer

__all__ = ["RedisStreamConsumer"]
