"""Collects queue and exchange statistics from the RabbitMQ Management API."""

import threading
import time

import requests

from src.config import Config


class StatsCollector:
    """Polls the RabbitMQ Management HTTP API for queue, exchange, and overview stats."""

    def __init__(self, config=None):
        if config is None:
            config = Config()
        self._config = config
        self._base_url = f"http://{config.host}:{config.management_port}/api"
        self._auth = (config.username, config.password)

        # TTL cache
        self._cache = {}
        self._cache_ttl = 1  # seconds
        self._cache_time = 0
        self._lock = threading.Lock()

    def _get_cached(self, key, fetcher):
        """Return cached data if still valid, otherwise call fetcher and cache the result."""
        with self._lock:
            now = time.time()
            if now - self._cache_time < self._cache_ttl and key in self._cache:
                return self._cache[key]
            data = fetcher()
            self._cache[key] = data
            self._cache_time = now
            return data

    def get_queue_stats(self):
        """Query the management API for all queue statistics."""
        def _fetch():
            try:
                resp = requests.get(
                    f"{self._base_url}/queues/%2f",
                    auth=self._auth,
                    timeout=5,
                )
                resp.raise_for_status()
                queues = resp.json()
            except (requests.RequestException, ValueError):
                return []

            results = []
            for q in queues:
                msg_stats = q.get("message_stats", {})
                results.append({
                    "name": q.get("name", ""),
                    "messages": q.get("messages", 0),
                    "messages_ready": q.get("messages_ready", 0),
                    "consumers": q.get("consumers", 0),
                    "message_stats": {
                        "publish_rate": msg_stats.get("publish_details", {}).get("rate", 0),
                        "deliver_rate": msg_stats.get("deliver_get_details", {}).get("rate", 0),
                    },
                })
            return results

        return self._get_cached("queues", _fetch)

    def get_exchange_stats(self):
        """Query the management API for exchange information."""
        def _fetch():
            try:
                resp = requests.get(
                    f"{self._base_url}/exchanges/%2f",
                    auth=self._auth,
                    timeout=5,
                )
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError):
                return []

        return self._get_cached("exchanges", _fetch)

    def get_overview(self):
        """Query the management API for cluster-level overview stats."""
        def _fetch():
            try:
                resp = requests.get(
                    f"{self._base_url}/overview",
                    auth=self._auth,
                    timeout=5,
                )
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError):
                return {}

        return self._get_cached("overview", _fetch)
