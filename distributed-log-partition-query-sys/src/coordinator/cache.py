import hashlib
import json
from collections import OrderedDict

from src.models import Query, QueryResponse


class QueryCache:
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._cache: OrderedDict[str, QueryResponse] = OrderedDict()
        self._hits = 0
        self._misses = 0

    @staticmethod
    def _make_key(query: Query) -> str:
        """Create cache key from query params, excluding query_id."""
        key_data = {
            "time_range": (
                query.time_range.model_dump(mode="json") if query.time_range else None
            ),
            "filters": [f.model_dump() for f in query.filters],
            "sort_field": query.sort_field,
            "sort_order": query.sort_order,
            "limit": query.limit,
            "page": query.page,
            "page_size": query.page_size,
        }
        key_str = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.sha256(key_str.encode()).hexdigest()[:16]

    def get(self, query: Query) -> QueryResponse | None:
        """Get cached response for query. Returns None on miss."""
        key = self._make_key(query)
        if key in self._cache:
            self._hits += 1
            self._cache.move_to_end(key)
            response = self._cache[key]
            return response
        self._misses += 1
        return None

    def put(self, query: Query, response: QueryResponse) -> None:
        """Cache a query response. Evicts LRU if at capacity."""
        key = self._make_key(query)
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = response
        while len(self._cache) > self.max_size:
            self._cache.popitem(last=False)

    @property
    def stats(self) -> dict:
        total = self._hits + self._misses
        return {
            "cache_size": len(self._cache),
            "max_size": self.max_size,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": round(self._hits / total, 4) if total > 0 else 0.0,
        }

    def clear(self) -> None:
        self._cache.clear()
        self._hits = 0
        self._misses = 0
