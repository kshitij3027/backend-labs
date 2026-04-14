"""Alert correlation engine.

Groups related log pattern matches into a single alert within a
configurable time window, preventing alert storms.  Existing active
alerts are updated in place (count incremented, last_occurrence
refreshed) rather than creating duplicates.  Alerts are auto-escalated
when the match count reaches 2x the rule threshold.
"""

from datetime import datetime, timedelta

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models import Alert, AlertRule, AlertState

logger = structlog.get_logger(__name__)


class AlertCorrelator:
    """Correlates incoming pattern matches into deduplicated alerts."""

    def __init__(self, correlation_window: int = 300) -> None:
        self._window = correlation_window  # seconds

    async def correlate(
        self,
        pattern_rule: AlertRule,
        log_message: str,
        db_session: AsyncSession,
    ) -> Alert:
        """Correlate a pattern match into an existing or new alert.

        If an active alert for the same pattern exists within the
        correlation window, its count is incremented and timestamps
        updated.  Otherwise a brand-new alert is created.

        Returns the created or updated :class:`Alert`.
        """
        now = datetime.utcnow()
        cutoff = now - timedelta(seconds=self._window)

        # Look for an existing active alert within the window
        stmt = (
            select(Alert)
            .where(
                Alert.pattern_name == pattern_rule.name,
                Alert.state.in_([
                    AlertState.NEW.value,
                    AlertState.ACKNOWLEDGED.value,
                    AlertState.ESCALATED.value,
                ]),
                Alert.last_occurrence >= cutoff,
            )
            .order_by(Alert.last_occurrence.desc())
            .limit(1)
        )
        result = await db_session.execute(stmt)
        existing: Alert | None = result.scalar_one_or_none()

        if existing is not None:
            existing.count += 1
            existing.last_occurrence = now

            # Auto-escalate when count hits 2x threshold and still NEW
            if (
                existing.count >= 2 * pattern_rule.threshold
                and existing.state == AlertState.NEW.value
            ):
                existing.state = AlertState.ESCALATED.value
                logger.warning(
                    "alert_auto_escalated",
                    alert_id=existing.id,
                    pattern=pattern_rule.name,
                    count=existing.count,
                )

            await db_session.commit()
            logger.info(
                "alert_correlated",
                alert_id=existing.id,
                pattern=pattern_rule.name,
                count=existing.count,
            )
            return existing

        # No existing alert -- create a new one
        alert = Alert(
            pattern_name=pattern_rule.name,
            severity=pattern_rule.severity,
            message=f"Pattern '{pattern_rule.name}' detected in logs",
            count=1,
            first_occurrence=now,
            last_occurrence=now,
            state=AlertState.NEW.value,
        )
        db_session.add(alert)
        await db_session.commit()
        await db_session.refresh(alert)

        logger.info(
            "alert_created",
            alert_id=alert.id,
            pattern=pattern_rule.name,
            severity=pattern_rule.severity,
        )
        return alert
