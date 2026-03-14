"""Statistics tracking for the Dead Letter Queue Log Processor."""

import json
import time

from src.config import Settings
from src.models import FailureType
from src.redis_client import RedisClient


class StatsTracker:
    """Tracks processing statistics using Redis counters and failure history."""

    def __init__(self, redis_client: RedisClient, settings: Settings):
        self.redis = redis_client
        self.settings = settings

    async def increment(self, field: str, amount: int = 1):
        """Increment a stat counter (e.g., 'processed', 'failed', 'retried', 'dlq_added')."""
        await self.redis.increment_stat(field, amount)

    async def get_stats(self) -> dict:
        """Get all current stats as a dict.

        Returns dict with keys like 'processed', 'failed', 'retried', 'dlq_added'
        with integer values. Convert string values from Redis to int.
        """
        raw = await self.redis.get_all_stats()
        return {k: int(v) for k, v in raw.items()}

    async def record_failure(self, failure_type: FailureType, source: str, error: str):
        """Record a failure event to the failure history list.

        Pushes a JSON object: {timestamp, failure_type, source, error}
        Also increments the 'failed' counter.
        """
        entry = json.dumps({
            "timestamp": time.time(),
            "failure_type": failure_type.value,
            "source": source,
            "error": error,
        })
        await self.redis.push_failure_history(entry)
        await self.increment("failed")

    async def get_failure_trends(self, window_seconds: float = 300.0) -> dict:
        """Analyze failure trends over a rolling window (default 5 minutes).

        Returns:
        {
            "window_seconds": 300.0,
            "total_failures": int,
            "by_type": {"PARSING": count, "NETWORK": count, ...},
            "by_source": {"api-gateway": count, ...},
            "top_errors": [{"error": str, "count": int}, ...],  # top 5 errors
        }

        Implementation: get failure_history entries, filter by timestamp within window,
        aggregate counts.
        """
        history = await self.redis.get_failure_history(count=self.settings.failure_history_max)
        now = time.time()
        cutoff = now - window_seconds

        by_type: dict[str, int] = {}
        by_source: dict[str, int] = {}
        error_counts: dict[str, int] = {}
        total = 0

        for raw in history:
            try:
                entry = json.loads(raw)
                if entry.get("timestamp", 0) < cutoff:
                    continue
                total += 1
                ft = entry.get("failure_type", "UNKNOWN")
                by_type[ft] = by_type.get(ft, 0) + 1
                src = entry.get("source", "unknown")
                by_source[src] = by_source.get(src, 0) + 1
                err = entry.get("error", "unknown")
                error_counts[err] = error_counts.get(err, 0) + 1
            except (json.JSONDecodeError, KeyError):
                continue

        top_errors = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        return {
            "window_seconds": window_seconds,
            "total_failures": total,
            "by_type": by_type,
            "by_source": by_source,
            "top_errors": [{"error": e, "count": c} for e, c in top_errors],
        }

    async def get_dlq_growth_rate(self, window_seconds: float = 60.0) -> float:
        """Calculate DLQ growth rate (messages added per second over the window).

        Count failure_history entries within the time window and return count / window.
        """
        history = await self.redis.get_failure_history(count=self.settings.failure_history_max)
        now = time.time()
        cutoff = now - window_seconds
        count = 0
        for raw in history:
            try:
                entry = json.loads(raw)
                if entry.get("timestamp", 0) >= cutoff:
                    count += 1
            except (json.JSONDecodeError, KeyError):
                continue
        return count / window_seconds if window_seconds > 0 else 0.0

    async def check_alerts(self) -> list[dict]:
        """Check for alert conditions.

        Returns list of alert dicts: [{type, message, severity, timestamp}]

        Alert conditions:
        1. DLQ size exceeds dlq_alert_threshold -> severity "high"
        2. DLQ growth rate > 1 msg/sec -> severity "medium"
        3. Any failure type > 80% of recent failures -> severity "high" with insight
        """
        alerts = []

        # Check DLQ size
        dlq_size = await self.redis.get_queue_length(self.settings.dlq_queue)
        if dlq_size >= self.settings.dlq_alert_threshold:
            alerts.append({
                "type": "dlq_size",
                "message": f"DLQ size ({dlq_size}) exceeds threshold ({self.settings.dlq_alert_threshold})",
                "severity": "high",
                "timestamp": time.time(),
            })

        # Check growth rate
        growth_rate = await self.get_dlq_growth_rate()
        if growth_rate > 1.0:
            alerts.append({
                "type": "dlq_growth",
                "message": f"DLQ growth rate is {growth_rate:.2f} msg/sec",
                "severity": "medium",
                "timestamp": time.time(),
            })

        # Check dominant failure type
        trends = await self.get_failure_trends(window_seconds=300.0)
        if trends["total_failures"] > 0:
            for ft, count in trends["by_type"].items():
                pct = count / trends["total_failures"]
                if pct > 0.8:
                    top_source = max(trends["by_source"].items(), key=lambda x: x[1])[0] if trends["by_source"] else "unknown"
                    alerts.append({
                        "type": "dominant_failure",
                        "message": f"{pct*100:.0f}% of failures are {ft} (top source: {top_source})",
                        "severity": "high",
                        "timestamp": time.time(),
                    })

        return alerts
