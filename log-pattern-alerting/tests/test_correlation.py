"""Tests for the alert correlation engine."""

import asyncio

import pytest
from sqlalchemy import select

from src.engine.correlation import AlertCorrelator
from src.models import Alert, AlertRule, AlertState


def _make_rule(name: str = "high_error_rate", threshold: int = 5) -> AlertRule:
    """Build an in-memory AlertRule (not persisted -- only used as param)."""
    return AlertRule(
        name=name,
        pattern=r"ERROR",
        threshold=threshold,
        window_seconds=300,
        severity="critical",
        enabled=True,
    )


class TestAlertCorrelator:
    """Tests for AlertCorrelator.correlate()."""

    async def test_new_alert_creation(self, db_session):
        """Correlating with no existing alert creates a NEW alert with count=1."""
        correlator = AlertCorrelator(correlation_window=300)
        rule = _make_rule()

        alert = await correlator.correlate(rule, "ERROR something broke", db_session)

        assert alert.id is not None
        assert alert.state == AlertState.NEW.value
        assert alert.count == 1
        assert alert.pattern_name == "high_error_rate"
        assert alert.severity == "critical"

    async def test_dedup_within_window(self, db_session):
        """Second correlate for same pattern increments count, same alert ID."""
        correlator = AlertCorrelator(correlation_window=300)
        rule = _make_rule()

        alert1 = await correlator.correlate(rule, "ERROR first", db_session)
        alert2 = await correlator.correlate(rule, "ERROR second", db_session)

        assert alert2.id == alert1.id
        assert alert2.count == 2

    async def test_auto_escalation(self, db_session):
        """Alert auto-escalates when count reaches 2x threshold."""
        correlator = AlertCorrelator(correlation_window=300)
        rule = _make_rule(threshold=2)  # escalation at 2 * 2 = 4

        for _ in range(3):
            alert = await correlator.correlate(rule, "ERROR", db_session)

        # count=3, threshold*2=4, should still be NEW
        assert alert.state == AlertState.NEW.value
        assert alert.count == 3

        # Fourth call hits the 2x threshold
        alert = await correlator.correlate(rule, "ERROR", db_session)
        assert alert.count == 4
        assert alert.state == AlertState.ESCALATED.value

    async def test_no_escalation_when_acknowledged(self, db_session):
        """ACKNOWLEDGED alerts stay ACKNOWLEDGED even past 2x threshold."""
        correlator = AlertCorrelator(correlation_window=300)
        rule = _make_rule(threshold=2)

        # Create alert and manually acknowledge it
        alert = await correlator.correlate(rule, "ERROR", db_session)
        alert.state = AlertState.ACKNOWLEDGED.value
        await db_session.commit()

        # Keep correlating past 2x threshold
        for _ in range(4):
            alert = await correlator.correlate(rule, "ERROR", db_session)

        # count is now 5 (1 original + 4 more), but state stays ACKNOWLEDGED
        assert alert.count == 5
        assert alert.state == AlertState.ACKNOWLEDGED.value

    async def test_new_alert_after_window(self, db_session):
        """After the correlation window expires, a new alert is created."""
        correlator = AlertCorrelator(correlation_window=1)  # 1-second window
        rule = _make_rule()

        alert1 = await correlator.correlate(rule, "ERROR first", db_session)
        first_id = alert1.id

        # Wait for the window to expire
        await asyncio.sleep(2)

        alert2 = await correlator.correlate(rule, "ERROR second", db_session)

        assert alert2.id != first_id
        assert alert2.count == 1
        assert alert2.state == AlertState.NEW.value
