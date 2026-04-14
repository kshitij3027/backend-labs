"""Redis storage layer with injectable client for testing."""

from __future__ import annotations

import json
import time

import redis.asyncio as aioredis

from src.models import AnomalyRecord, MetricPoint


class RedisStorage:
    """Async Redis wrapper with injectable client support.

    When ``client`` is provided (e.g. a fakeredis instance for tests),
    the :meth:`connect` call becomes a no-op — the injected client is
    used directly.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        metric_ttl_seconds: int = 3600,
        client: aioredis.Redis | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._metric_ttl = metric_ttl_seconds
        self._client = client

    @property
    def client(self) -> aioredis.Redis | None:
        return self._client

    async def connect(self) -> None:
        """Open a Redis connection if no client was injected."""
        if self._client is None:
            self._client = aioredis.Redis(
                host=self._host,
                port=self._port,
                decode_responses=True,
            )

    async def close(self) -> None:
        """Gracefully close the Redis connection."""
        if self._client:
            await self._client.aclose()

    async def ping(self) -> bool:
        """Return True if Redis is reachable, False otherwise."""
        try:
            return await self._client.ping()
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Metric CRUD
    # ------------------------------------------------------------------

    async def store_metric(self, point: MetricPoint) -> None:
        """Store a single metric point in a sorted set. Score = timestamp."""
        key = f"metrics:{point.service}:{point.metric_name}"
        member = json.dumps({
            "value": point.value,
            "tags": point.tags,
            "timestamp": point.timestamp,
        })
        await self._client.zadd(key, {member: point.timestamp})
        await self._client.expire(key, self._metric_ttl)
        await self._client.sadd("services", point.service)

    async def store_metrics_batch(self, points: list[MetricPoint]) -> None:
        """Store multiple metrics using a pipeline for efficiency."""
        if not points:
            return
        pipe = self._client.pipeline()
        services: set[str] = set()
        keys_seen: set[str] = set()
        for p in points:
            key = f"metrics:{p.service}:{p.metric_name}"
            member = json.dumps({
                "value": p.value,
                "tags": p.tags,
                "timestamp": p.timestamp,
            })
            pipe.zadd(key, {member: p.timestamp})
            keys_seen.add(key)
            services.add(p.service)
        # Set TTL on all keys
        for key in keys_seen:
            pipe.expire(key, self._metric_ttl)
        # Track services
        for svc in services:
            pipe.sadd("services", svc)
        await pipe.execute()

    async def get_metrics(
        self,
        service: str,
        metric_name: str,
        start_time: float,
        end_time: float,
    ) -> list[MetricPoint]:
        """Retrieve metric points within a time range using ZRANGEBYSCORE."""
        key = f"metrics:{service}:{metric_name}"
        members = await self._client.zrangebyscore(key, start_time, end_time)
        points: list[MetricPoint] = []
        for m in members:
            data = json.loads(m)
            points.append(MetricPoint(
                service=service,
                metric_name=metric_name,
                value=data["value"],
                timestamp=data["timestamp"],
                tags=data.get("tags", {}),
            ))
        return points

    # ------------------------------------------------------------------
    # Anomaly CRUD
    # ------------------------------------------------------------------

    async def store_anomaly(self, record: AnomalyRecord) -> None:
        """Store a detected anomaly in a sorted set."""
        key = f"anomalies:{record.service}:{record.metric_name}"
        member = json.dumps(record.model_dump())
        await self._client.zadd(key, {member: record.timestamp})
        # Keep anomalies longer than regular metrics
        await self._client.expire(key, self._metric_ttl * 2)

    async def get_anomalies(self, hours: float = 1.0) -> list[AnomalyRecord]:
        """Get all anomalies from the last N hours across all services/metrics."""
        cutoff = time.time() - (hours * 3600)
        anomalies: list[AnomalyRecord] = []
        # SCAN for anomaly keys
        cursor = 0
        while True:
            cursor, keys = await self._client.scan(
                cursor, match="anomalies:*", count=100,
            )
            for key in keys:
                members = await self._client.zrangebyscore(key, cutoff, "+inf")
                for m in members:
                    data = json.loads(m)
                    anomalies.append(AnomalyRecord(**data))
            if cursor == 0:
                break
        anomalies.sort(key=lambda a: a.timestamp, reverse=True)
        return anomalies

    # ------------------------------------------------------------------
    # Discovery helpers
    # ------------------------------------------------------------------

    async def get_services(self) -> list[str]:
        """Get all known service names."""
        members = await self._client.smembers("services")
        return sorted(members)

    async def get_metric_names(self, service: str) -> list[str]:
        """Get all metric names for a given service by scanning keys."""
        prefix = f"metrics:{service}:"
        names: set[str] = set()
        cursor = 0
        while True:
            cursor, keys = await self._client.scan(
                cursor, match=f"{prefix}*", count=100,
            )
            for key in keys:
                # key is "metrics:service:metric_name"
                name = key[len(prefix):]
                names.add(name)
            if cursor == 0:
                break
        return sorted(names)
