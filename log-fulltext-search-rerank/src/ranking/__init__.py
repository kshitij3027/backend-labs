"""Ranking primitives for the log-fulltext-search-rerank service.

This package holds the pure scoring modules consumed by the
MultiFactorReranker (commit 08). Each module is intentionally
isolated so the tests can drive it without spinning up the rest of
the stack, and so commit 08's composition layer stays legible.
"""
