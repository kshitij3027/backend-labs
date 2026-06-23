"""External-service client factories for the Real-Time Log Pattern Clustering engine.

Currently houses the Redis client factory (:mod:`src.clients.redis`). Each factory is
defensive: a missing or unreachable backend yields ``None`` rather than raising, so the
engine can degrade gracefully (e.g. fall back to in-memory state) without Redis.
"""
