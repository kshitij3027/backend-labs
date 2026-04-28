import hashlib
import json
import logging
from typing import Any

import redis.asyncio as redis

from src.schemas.search import SearchRequest, SearchResponse

logger = logging.getLogger(__name__)

CACHE_KEY_PREFIX = "logs:search:v1:"


def _normalize_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _normalize_payload(v) for k, v in value.items()}
    if isinstance(value, list):
        normalized = [_normalize_payload(v) for v in value]
        try:
            return sorted(normalized, key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":")))
        except TypeError:
            return normalized
    return value


def canonical_request_payload(req: SearchRequest) -> dict[str, Any]:
    payload = req.model_dump(exclude_none=True, mode="json")
    return _normalize_payload(payload)


def canonical_key(req: SearchRequest) -> str:
    payload = canonical_request_payload(req)
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(blob.encode()).hexdigest()[:16]
    return f"{CACHE_KEY_PREFIX}{digest}"


class CacheCounters:
    def __init__(self) -> None:
        self.hits: int = 0
        self.misses: int = 0
        self.errors: int = 0

    def as_dict(self) -> dict[str, Any]:
        total = self.hits + self.misses
        hit_rate = (self.hits / total) if total > 0 else 0.0
        return {
            "hits": self.hits,
            "misses": self.misses,
            "errors": self.errors,
            "hit_rate": round(hit_rate, 4),
        }


class SearchCache:
    def __init__(
        self,
        redis_client: redis.Redis,
        ttl_seconds: int,
        counters: CacheCounters,
    ) -> None:
        self.redis = redis_client
        self.ttl_seconds = ttl_seconds
        self.counters = counters

    async def get(self, key: str) -> SearchResponse | None:
        try:
            raw = await self.redis.get(key)
        except Exception as exc:
            self.counters.errors += 1
            logger.warning("cache get failed for key=%s: %s", key, exc)
            return None

        if raw is None:
            self.counters.misses += 1
            return None

        try:
            response = SearchResponse.model_validate_json(raw)
        except Exception as exc:
            self.counters.errors += 1
            logger.warning("cache deserialize failed for key=%s: %s", key, exc)
            return None

        response = response.model_copy(update={"cache_hit": True})
        self.counters.hits += 1
        return response

    async def set(self, key: str, value: SearchResponse) -> None:
        try:
            normalized = value.model_copy(update={"cache_hit": False})
            payload = normalized.model_dump_json().encode()
            await self.redis.setex(key, self.ttl_seconds, payload)
        except Exception as exc:
            self.counters.errors += 1
            logger.warning("cache set failed for key=%s: %s", key, exc)
