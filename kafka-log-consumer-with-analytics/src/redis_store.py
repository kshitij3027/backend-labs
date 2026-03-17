"""Redis persistence for analytics snapshots."""
import json
import logging
import threading
import time

import redis

logger = logging.getLogger(__name__)


class RedisStore:
    """Persists analytics snapshots to Redis for crash recovery.

    Degrades gracefully if Redis is unavailable.
    """

    SNAPSHOT_KEY = "analytics:snapshot"

    def __init__(self, host: str = "redis", port: int = 6379, db: int = 0) -> None:
        self._host = host
        self._port = port
        self._db = db
        self._client: redis.Redis | None = None
        self._connected = False
        self._connect()

    def _connect(self) -> None:
        """Attempt to connect to Redis."""
        try:
            self._client = redis.Redis(
                host=self._host, port=self._port, db=self._db,
                socket_timeout=5, socket_connect_timeout=5,
                decode_responses=True,
            )
            self._client.ping()
            self._connected = True
            logger.info("Redis connected at %s:%d", self._host, self._port)
        except Exception as exc:
            logger.warning("Redis unavailable (%s) — running without persistence", exc)
            self._connected = False

    @property
    def is_connected(self) -> bool:
        return self._connected

    def save_snapshot(self, data: dict) -> bool:
        """Save analytics snapshot to Redis. Returns True on success."""
        if not self._connected:
            return False
        try:
            serialized = {k: json.dumps(v) if isinstance(v, (dict, list)) else str(v)
                          for k, v in data.items()}
            self._client.hset(self.SNAPSHOT_KEY, mapping=serialized)
            self._client.set(f"{self.SNAPSHOT_KEY}:timestamp", str(time.time()))
            return True
        except Exception as exc:
            logger.error("Failed to save snapshot: %s", exc)
            self._connected = False
            return False

    def load_snapshot(self) -> dict | None:
        """Load the last saved analytics snapshot from Redis."""
        if not self._connected:
            return None
        try:
            data = self._client.hgetall(self.SNAPSHOT_KEY)
            if not data:
                return None
            result = {}
            for k, v in data.items():
                try:
                    result[k] = json.loads(v)
                except (json.JSONDecodeError, TypeError):
                    result[k] = v
            return result
        except Exception as exc:
            logger.error("Failed to load snapshot: %s", exc)
            return None

    def ping(self) -> bool:
        """Check Redis connectivity."""
        if not self._connected:
            return False
        try:
            return self._client.ping()
        except Exception:
            self._connected = False
            return False
