"""Health monitoring for consumer workers."""

from __future__ import annotations

from datetime import datetime, timezone

from src.models import ConsumerStats


class HealthMonitor:
    """Monitors consumer health based on activity and error rates."""

    STALE_THRESHOLD_SEC = 30
    ERROR_RATE_DEGRADED = 0.1
    ERROR_RATE_UNHEALTHY = 0.5

    def __init__(self, get_consumer_stats):
        """get_consumer_stats is a callable returning list[ConsumerStats]."""
        self._get_consumer_stats = get_consumer_stats

    def status(self) -> dict:
        """Return health status: healthy / degraded / unhealthy."""
        stats = self._get_consumer_stats()
        if not stats:
            return {"status": "unhealthy", "reason": "no consumers running", "consumers": []}

        now = datetime.now(timezone.utc)
        stale_count = 0
        high_error_count = 0
        consumer_details = []

        for s in stats:
            detail = {
                "consumer_id": s.consumer_id,
                "processed": s.processed_count,
                "errors": s.error_count,
                "success_rate": s.success_rate,
                "status": "active",
            }

            # Check if consumer is stale (no activity in STALE_THRESHOLD_SEC)
            if s.last_active is not None:
                idle_sec = (now - s.last_active).total_seconds()
                if idle_sec > self.STALE_THRESHOLD_SEC:
                    detail["status"] = "stale"
                    stale_count += 1
            elif s.processed_count == 0:
                # Never processed anything — might be starting up
                detail["status"] = "starting"

            # Check error rate
            error_rate = 1.0 - s.success_rate
            if error_rate >= self.ERROR_RATE_UNHEALTHY:
                detail["status"] = "unhealthy"
                high_error_count += 1
            elif error_rate >= self.ERROR_RATE_DEGRADED:
                detail["status"] = "degraded"
                high_error_count += 1

            consumer_details.append(detail)

        total = len(stats)
        if high_error_count > total / 2 or stale_count == total:
            overall = "unhealthy"
            reason = "majority of consumers have issues"
        elif high_error_count > 0 or stale_count > 0:
            overall = "degraded"
            reason = f"{high_error_count} error issues, {stale_count} stale"
        else:
            overall = "healthy"
            reason = "all consumers operating normally"

        return {
            "status": overall,
            "reason": reason,
            "consumers": consumer_details,
        }
