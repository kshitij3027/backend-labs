"""Redis-backed session storage with connection pooling and pipeline batching."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import redis.asyncio as aioredis

from src.config import Config
from src.models import Event, Session, SessionState

logger = logging.getLogger(__name__)


class RedisStore:
    """Manages session persistence in Redis using hash-per-session pattern."""

    def __init__(self, config: Config):
        self._config = config
        self._redis: aioredis.Redis | None = None

    async def connect(self) -> None:
        """Establish Redis connection with pooling."""
        self._redis = aioredis.from_url(
            self._config.redis_url,
            max_connections=30,
            decode_responses=True,
            retry_on_timeout=True,
        )
        # Test connection
        await self._redis.ping()
        logger.info("Redis connected: %s", self._config.redis_url)

    async def close(self) -> None:
        """Close Redis connection."""
        if self._redis:
            await self._redis.close()
            logger.info("Redis connection closed")

    async def save_session(self, session: Session) -> None:
        """Persist session as a Redis HASH with TTL."""
        key = f"session:{session.session_id}"
        ttl = int(self._config.session_timeout_seconds * 2)  # keep expired sessions for 2x timeout

        data = {
            "session_id": session.session_id,
            "user_id": session.user_id,
            "state": session.state.value,
            "start_time": session.start_time.isoformat(),
            "last_event_time": session.last_event_time.isoformat(),
            "event_count": str(session.event_count),
            "device_type": session.device_type,
            "pages_visited": json.dumps(session.pages_visited),
            "event_types": json.dumps(session.event_types),
            "quality_score": str(session.quality_score),
            "engagement": session.engagement,
            "events": json.dumps([e.model_dump(mode="json") for e in session.events]),
        }

        pipe = self._redis.pipeline(transaction=False)
        pipe.hset(key, mapping=data)
        pipe.expire(key, ttl)
        # Maintain user session index
        user_key = f"user_sessions:{session.user_id}"
        pipe.sadd(user_key, session.session_id)
        pipe.expire(user_key, ttl)
        await pipe.execute()

    async def load_session(self, session_id: str) -> Session | None:
        """Load a session from Redis by session_id."""
        key = f"session:{session_id}"
        data = await self._redis.hgetall(key)
        if not data:
            return None
        return self._deserialize_session(data)

    async def get_user_sessions(self, user_id: str) -> list[Session]:
        """Get all sessions for a user from Redis."""
        user_key = f"user_sessions:{user_id}"
        session_ids = await self._redis.smembers(user_key)
        if not session_ids:
            return []

        sessions = []
        for sid in session_ids:
            session = await self.load_session(sid)
            if session:
                sessions.append(session)
        return sessions

    async def delete_session(self, session_id: str, user_id: str) -> None:
        """Delete a session and remove from user index."""
        pipe = self._redis.pipeline(transaction=False)
        pipe.delete(f"session:{session_id}")
        pipe.srem(f"user_sessions:{user_id}", session_id)
        await pipe.execute()

    async def get_all_active_session_ids(self) -> list[str]:
        """Get all session IDs from Redis (via key scan)."""
        keys = []
        async for key in self._redis.scan_iter(match="session:*", count=100):
            sid = key.replace("session:", "")
            keys.append(sid)
        return keys

    @property
    def redis(self) -> aioredis.Redis:
        """Expose the raw Redis client for direct access (e.g., in E2E tests)."""
        return self._redis

    @staticmethod
    def _deserialize_session(data: dict) -> Session:
        """Convert Redis hash data back to a Session object."""
        events_raw = json.loads(data.get("events", "[]"))
        events = [Event(**e) for e in events_raw]

        return Session(
            session_id=data["session_id"],
            user_id=data["user_id"],
            state=SessionState(data["state"]),
            start_time=datetime.fromisoformat(data["start_time"]),
            last_event_time=datetime.fromisoformat(data["last_event_time"]),
            event_count=int(data.get("event_count", 0)),
            events=events,
            device_type=data.get("device_type", "desktop"),
            pages_visited=json.loads(data.get("pages_visited", "[]")),
            event_types=json.loads(data.get("event_types", "[]")),
            quality_score=float(data.get("quality_score", 0.0)),
            engagement=data.get("engagement", "bounce"),
        )
