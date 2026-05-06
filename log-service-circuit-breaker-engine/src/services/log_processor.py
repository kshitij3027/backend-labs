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

    async def process_log(self, log_entry: dict) -> dict:
        """Run a single log through the pipeline. Never raises."""
        result: dict = {"log": log_entry}
        had_failure = False
        had_fallback = False

        # 1. Database — failover from primary to backup if primary breaker is OPEN.
        try:
            if self.primary_db.breaker.state == CircuitState.OPEN:
                result["db"] = await self.backup_db.insert_log(log_entry)
                result["used_backup"] = True
            else:
                result["db"] = await self.primary_db.insert_log(log_entry)
                result["used_backup"] = False
            if result["db"].get("status") == "fallback":
                had_fallback = True
        except Exception as exc:
            logger.warning("processor: db sub-step failed: %s", exc)
            had_failure = True
            result["db"] = {"status": "error", "error": str(exc)}

        # 2. Queue.
        try:
            result["queue"] = await self.queue.publish(log_entry)
            if result["queue"].get("status") == "fallback":
                had_fallback = True
        except Exception as exc:
            logger.warning("processor: queue sub-step failed: %s", exc)
            had_failure = True
            result["queue"] = {"status": "error", "error": str(exc)}

        # 3. External API enrichment.
        try:
            result["enrich"] = await self.ext_api.enrich(log_entry)
            if result["enrich"].get("status") == "fallback":
                had_fallback = True
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
