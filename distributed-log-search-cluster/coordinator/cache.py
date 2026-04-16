"""In-memory TTL LRU cache for coordinator search responses."""

from __future__ import annotations

import hashlib
import json

from cachetools import TTLCache

from shared.models import SearchRequest, SearchResponse


class ResultCache:
    """Thin wrapper around ``cachetools.TTLCache`` keyed by search request.

    Keys normalize the query to lowercase + stripped whitespace so trivial
    variations hit the same entry, but honor AND/OR and the requested limit.
    """

    def __init__(self, size: int = 1000, ttl: int = 60) -> None:
        self._c: TTLCache = TTLCache(maxsize=size, ttl=ttl)

    @staticmethod
    def key_for(req: SearchRequest) -> str:
        payload = json.dumps(
            {
                "q": req.query.strip().lower(),
                "op": req.op,
                "limit": req.limit,
            },
            sort_keys=True,
        )
        return hashlib.sha1(payload.encode()).hexdigest()

    def get(self, req: SearchRequest) -> SearchResponse | None:
        return self._c.get(self.key_for(req))

    def put(self, req: SearchRequest, resp: SearchResponse) -> None:
        self._c[self.key_for(req)] = resp

    def clear(self) -> None:
        self._c.clear()

    def __len__(self) -> int:  # pragma: no cover - trivial
        return len(self._c)
