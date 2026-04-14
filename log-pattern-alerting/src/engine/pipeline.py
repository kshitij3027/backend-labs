"""Alert processing pipeline.

Orchestrates the full log-to-alert flow:
  1. Pattern matching against incoming log messages
  2. Rate limiting to prevent alert storms
  3. Correlation to deduplicate/update existing alerts
  4. Real-time WebSocket broadcast of alert updates
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from src.models import Alert, LogEntry

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from src.engine.correlation import AlertCorrelator
    from src.engine.pattern_matcher import PatternMatcher
    from src.engine.rate_limiter import RateLimiter
    from src.websocket import ConnectionManager

logger = structlog.get_logger(__name__)


class AlertPipeline:
    """End-to-end pipeline: log entry -> pattern match -> rate limit -> correlate -> broadcast."""

    def __init__(
        self,
        pattern_matcher: PatternMatcher,
        correlator: AlertCorrelator,
        rate_limiter: RateLimiter,
        connection_manager: ConnectionManager,
        session_factory: async_sessionmaker,
    ) -> None:
        self._matcher = pattern_matcher
        self._correlator = correlator
        self._rate_limiter = rate_limiter
        self._ws = connection_manager
        self._session_factory = session_factory

    async def process(
        self,
        log_entry: LogEntry,
        db_session: AsyncSession,
    ) -> list[Alert]:
        """Process a single log entry through the full pipeline.

        Returns a list of alerts that were created or updated.
        """
        # Step 1: Match log message against loaded patterns
        matched_rules = self._matcher.match(log_entry.message)

        if not matched_rules:
            logger.debug("no_pattern_match", message=log_entry.message[:120])

        alerts: list[Alert] = []

        # Step 2: For each matched pattern, rate-limit then correlate
        for rule in matched_rules:
            # Check rate limiter
            allowed = await self._rate_limiter.is_allowed(rule.name)
            if not allowed:
                logger.warning(
                    "alert_rate_limited",
                    pattern=rule.name,
                    message=log_entry.message[:120],
                )
                continue

            # Correlate into existing or new alert
            alert = await self._correlator.correlate(
                rule, log_entry.message, db_session
            )
            alerts.append(alert)

            # Broadcast alert update via WebSocket
            alert_data = {
                "type": "alert_update",
                "alert": {
                    "id": alert.id,
                    "pattern_name": alert.pattern_name,
                    "severity": alert.severity,
                    "message": alert.message,
                    "count": alert.count,
                    "state": alert.state,
                    "first_occurrence": (
                        alert.first_occurrence.isoformat()
                        if alert.first_occurrence
                        else None
                    ),
                    "last_occurrence": (
                        alert.last_occurrence.isoformat()
                        if alert.last_occurrence
                        else None
                    ),
                },
            }
            await self._ws.broadcast_json(alert_data)

        # Step 3: Mark log entry as processed
        log_entry.processed = True
        await db_session.commit()

        logger.info(
            "log_processed",
            log_id=log_entry.id,
            patterns_matched=len(matched_rules),
            alerts_generated=len(alerts),
        )
        return alerts

    async def initialize(self, db_session: AsyncSession) -> None:
        """Load alert-rule patterns from the database into the matcher."""
        await self._matcher.load_patterns(db_session)
        logger.info("pipeline_initialized")
