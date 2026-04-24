"""Cache layer for the log-fulltext-search-rerank service.

Currently houses the in-process :class:`QueryCache` used by the
:class:`SearchService` to avoid re-running the full
parse -> retrieve -> rerank pipeline for repeat queries. Commit 09
deliberately keeps the cache in-process (no Redis) so the stack stays
simple; the cache key encodes the index version so stale entries
expire silently rather than requiring explicit invalidation.
"""
