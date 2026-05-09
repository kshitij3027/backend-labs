"""Log processor that orchestrates DB / queue / external-API calls."""
from __future__ import annotations
import logging
import random
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Optional

from src.services.database import DatabaseService
from src.services.queue import MessageQueueService
from src.services.external_api import ExternalAPIService
from src.state import CircuitState

logger = logging.getLogger(__name__)


@dataclass
class ProcessingStats:
    total_processed: int = 0
    successful_processed: int = 0
    failed_processed: int = 0
    fallback_responses: int = 0
    start_time: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            **asdict(self),
            "uptime_seconds": time.time() - self.start_time,
        }


class LogProcessorService:
    """Orchestrates one log entry through DB (with backup failover), queue, and enrichment.

    Never raises — every call returns a dict with sub-results, even on failure.
    """

    def __init__(
        self,
        primary_db: DatabaseService,
        backup_db: DatabaseService,
        queue: MessageQueueService,
        ext_api: ExternalAPIService,
    ):
        self.primary_db = primary_db
        self.backup_db = backup_db
        self.queue = queue
        self.ext_api = ext_api
        self.stats = ProcessingStats()
        # Cache of last successful sub-step responses, keyed by step_key
        # ("db_primary", "db_backup", "queue", "enrich"). When a sub-step
        # falls back, we enrich the static fallback dict with this cached
        # payload (tagged ``from_cache: True``) so downstream consumers see a
        # realistic shape rather than the bare static fallback.
        self._last_good: dict[str, dict] = {}

    def _apply_cached_fallback(self, step_key: str, result: dict) -> dict:
        """If ``result`` is a static fallback and we have a cached good payload
        for ``step_key``, merge the cached payload into the fallback so callers
        see a realistic response shape tagged ``from_cache: True``.
        """
        if result.get("status") != "fallback":
            return result
        cached = self._last_good.get(step_key)
        if cached is None:
            return result
        # Cached payload wins so nested real data (e.g. enrichment fields)
        # is preserved. Only the fallback's service identifier and the
        # explicit status/from_cache markers override the cache.
        merged = {**cached, "status": "fallback", "from_cache": True}
        if "service" in result:
            merged["service"] = result["service"]
        return merged

    async def process_log(self, log_entry: dict) -> dict:
        """Run a single log through the pipeline. Never raises."""
        result: dict = {"log": log_entry}
        had_failure = False
        had_fallback = False

        # 1. Database — failover from primary to backup if primary breaker is OPEN.
        try:
            if self.primary_db.breaker.state == CircuitState.OPEN:
                db_result = await self.backup_db.insert_log(log_entry)
                step_key = "db_backup"
                result["used_backup"] = True
            else:
                db_result = await self.primary_db.insert_log(log_entry)
                step_key = "db_primary"
                result["used_backup"] = False

            if db_result.get("status") == "ok":
                # Cache by the path we actually took. Also mirror into the
                # "other" key so a subsequent failover can hand back a
                # sensible cached response too.
                self._last_good[step_key] = db_result
                if step_key == "db_primary":
                    self._last_good.setdefault("db_backup", db_result)
                else:
                    self._last_good.setdefault("db_primary", db_result)
            elif db_result.get("status") == "fallback":
                # Try to enrich with cache from either db key.
                cache_key = step_key if step_key in self._last_good else (
                    "db_primary" if "db_primary" in self._last_good else "db_backup"
                )
                db_result = self._apply_cached_fallback(cache_key, db_result)
                had_fallback = True

            result["db"] = db_result
        except Exception as exc:
            logger.warning("processor: db sub-step failed: %s", exc)
            had_failure = True
            result["db"] = {"status": "error", "error": str(exc)}

        # 2. Queue.
        try:
            queue_result = await self.queue.publish(log_entry)
            if queue_result.get("status") == "ok":
                self._last_good["queue"] = queue_result
            elif queue_result.get("status") == "fallback":
                queue_result = self._apply_cached_fallback("queue", queue_result)
                had_fallback = True
            result["queue"] = queue_result
        except Exception as exc:
            logger.warning("processor: queue sub-step failed: %s", exc)
            had_failure = True
            result["queue"] = {"status": "error", "error": str(exc)}

        # 3. External API enrichment.
        try:
            enrich_result = await self.ext_api.enrich(log_entry)
            if enrich_result.get("status") == "ok":
                self._last_good["enrich"] = enrich_result
            elif enrich_result.get("status") == "fallback":
                enrich_result = self._apply_cached_fallback("enrich", enrich_result)
                had_fallback = True
            result["enrich"] = enrich_result
        except Exception as exc:
            logger.warning("processor: enrich sub-step failed: %s", exc)
            had_failure = True
            result["enrich"] = {"status": "error", "error": str(exc)}

        # 4. Stats.
        self.stats.total_processed += 1
        if had_failure:
            self.stats.failed_processed += 1
        else:
            self.stats.successful_processed += 1
        if had_fallback:
            self.stats.fallback_responses += 1

        result["had_fallback"] = had_fallback
        result["had_failure"] = had_failure
        return result

    async def process_batch(self, count: int) -> dict:
        """Generate ``count`` synthetic logs and process each. Returns aggregate dict."""
        start = time.time()
        results: list[dict] = []
        for i in range(count):
            entry = self._make_synthetic_log(i)
            results.append(await self.process_log(entry))
        return {
            "processed": len(results),
            "successful": sum(1 for r in results if not r.get("had_failure")),
            "fallback_responses": sum(1 for r in results if r.get("had_fallback")),
            "duration_ms": (time.time() - start) * 1000.0,
        }

    def _make_synthetic_log(self, seq: int) -> dict:
        return {
            "id": uuid.uuid4().hex,
            "timestamp": time.time(),
            "level": random.choice(("INFO", "WARN", "ERROR")),
            "message": f"synthetic log {seq}",
            "service": "demo-app",
            "user_id": f"user_{seq % 10}",
        }

    def get_processing_stats(self) -> dict:
        return self.stats.to_dict()

    def get_circuit_metrics(self) -> dict:
        return {
            "primary_db": self.primary_db.breaker.to_dict(),
            "backup_db": self.backup_db.breaker.to_dict(),
            "queue": self.queue.breaker.to_dict(),
            "ext_api": self.ext_api.breaker.to_dict(),
        }
