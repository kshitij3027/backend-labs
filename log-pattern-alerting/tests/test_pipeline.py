"""Pipeline integration tests.

Tests the full AlertPipeline flow using real Postgres + Redis:
  - Pattern matching -> rate limiting -> correlation -> broadcast
  - Deduplication, escalation, rate limiting, multi-pattern matching
"""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.engine.correlation import AlertCorrelator
from src.engine.pattern_matcher import PatternMatcher
from src.engine.pipeline import AlertPipeline
from src.engine.rate_limiter import RateLimiter
from src.models import AlertRule, AlertState, LogEntry
from src.websocket import ConnectionManager


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
async def session_factory(async_engine):
    """Create an async_sessionmaker bound to the test engine."""
    return async_sessionmaker(
        async_engine, class_=AsyncSession, expire_on_commit=False,
    )


@pytest.fixture
async def pipeline_session(session_factory):
    """Provide a session for pipeline tests and clean up afterwards."""
    async with session_factory() as session:
        yield session


@pytest.fixture
async def _seed_rules(async_engine):
    """Seed AlertRule rows that the pipeline tests rely on."""
    factory = async_sessionmaker(
        async_engine, class_=AsyncSession, expire_on_commit=False,
    )
    async with factory() as session:
        # Clean first
        await session.execute(text("DELETE FROM alerts"))
        await session.execute(text("DELETE FROM alert_rules"))
        await session.execute(text("DELETE FROM log_entries"))
        await session.commit()

        rules = [
            AlertRule(
                name="auth_failure",
                pattern=r"authentication\s+failed|login\s+failed|auth\s+error",
                threshold=5,
                window_seconds=60,
                severity="high",
                enabled=True,
            ),
            AlertRule(
                name="db_error",
                pattern=r"database\s+error|connection\s+timeout",
                threshold=3,
                window_seconds=120,
                severity="critical",
                enabled=True,
            ),
            AlertRule(
                name="low_threshold",
                pattern=r"low_threshold_test_pattern",
                threshold=2,
                window_seconds=300,
                severity="medium",
                enabled=True,
            ),
        ]
        for r in rules:
            session.add(r)
        await session.commit()


@pytest.fixture
async def pipeline(
    _seed_rules,
    session_factory,
    redis_client,
):
    """Build and initialize an AlertPipeline with real components."""
    matcher = PatternMatcher()
    correlator = AlertCorrelator(correlation_window=600)
    rate_limiter = RateLimiter(redis_client, max_per_minute=10)
    ws_manager = ConnectionManager()

    pipe = AlertPipeline(
        pattern_matcher=matcher,
        correlator=correlator,
        rate_limiter=rate_limiter,
        connection_manager=ws_manager,
        session_factory=session_factory,
    )

    # Load patterns from the seeded DB
    async with session_factory() as init_session:
        await pipe.initialize(init_session)

    return pipe


@pytest.fixture
async def rate_limited_pipeline(
    _seed_rules,
    session_factory,
    redis_client,
):
    """Pipeline with a very low rate limit (max 2 per minute)."""
    matcher = PatternMatcher()
    correlator = AlertCorrelator(correlation_window=600)
    rate_limiter = RateLimiter(redis_client, max_per_minute=2)
    ws_manager = ConnectionManager()

    pipe = AlertPipeline(
        pattern_matcher=matcher,
        correlator=correlator,
        rate_limiter=rate_limiter,
        connection_manager=ws_manager,
        session_factory=session_factory,
    )

    async with session_factory() as init_session:
        await pipe.initialize(init_session)

    return pipe


@pytest.fixture
async def _cleanup(async_engine):
    """Clean all test data after the test."""
    yield
    async with async_engine.begin() as conn:
        await conn.execute(text("DELETE FROM alerts"))
        await conn.execute(text("DELETE FROM alert_rules"))
        await conn.execute(text("DELETE FROM log_entries"))


def _make_log(message: str, level: str = "ERROR") -> LogEntry:
    """Create a LogEntry model instance (not yet persisted)."""
    return LogEntry(message=message, level=level, source="test")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures("_cleanup")
class TestPipelineAuthFailure:
    """Auth failure log message creates an alert."""

    async def test_auth_failure_creates_alert(
        self, pipeline, session_factory,
    ):
        async with session_factory() as session:
            log = _make_log("Authentication failed for user admin")
            session.add(log)
            await session.commit()
            await session.refresh(log)

            alerts = await pipeline.process(log, session)

        assert len(alerts) == 1
        alert = alerts[0]
        assert alert.state == AlertState.NEW.value
        assert alert.pattern_name == "auth_failure"
        assert alert.count == 1
        assert alert.severity == "high"


@pytest.mark.usefixtures("_cleanup")
class TestPipelineDedup:
    """Injecting the same pattern twice deduplicates into one alert."""

    async def test_dedup_increments_count(
        self, pipeline, session_factory,
    ):
        async with session_factory() as session:
            log1 = _make_log("Authentication failed for user admin")
            session.add(log1)
            await session.commit()
            await session.refresh(log1)
            alerts1 = await pipeline.process(log1, session)

        assert len(alerts1) == 1
        first_alert_id = alerts1[0].id

        async with session_factory() as session:
            log2 = _make_log("Authentication failed for user bob")
            session.add(log2)
            await session.commit()
            await session.refresh(log2)
            alerts2 = await pipeline.process(log2, session)

        assert len(alerts2) == 1
        assert alerts2[0].id == first_alert_id
        assert alerts2[0].count == 2


@pytest.mark.usefixtures("_cleanup")
class TestPipelineEscalation:
    """Alert auto-escalates at 2x threshold."""

    async def test_escalation_at_2x_threshold(
        self, pipeline, session_factory,
    ):
        # low_threshold rule has threshold=2, so 2*2=4 hits triggers escalation
        for i in range(4):
            async with session_factory() as session:
                log = _make_log("low_threshold_test_pattern hit")
                session.add(log)
                await session.commit()
                await session.refresh(log)
                alerts = await pipeline.process(log, session)

        assert len(alerts) == 1
        assert alerts[0].state == AlertState.ESCALATED.value
        assert alerts[0].count == 4


@pytest.mark.usefixtures("_cleanup")
class TestPipelineRateLimiting:
    """Rate limiter blocks alerts above the configured max."""

    async def test_rate_limiting_blocks(
        self, rate_limited_pipeline, session_factory,
    ):
        results = []
        for i in range(3):
            async with session_factory() as session:
                log = _make_log("Authentication failed for user test")
                session.add(log)
                await session.commit()
                await session.refresh(log)
                alerts = await rate_limited_pipeline.process(log, session)
                results.append(alerts)

        # First 2 should produce alerts, third should be rate-limited
        assert len(results[0]) == 1
        assert len(results[1]) == 1
        assert len(results[2]) == 0


@pytest.mark.usefixtures("_cleanup")
class TestPipelineNonMatching:
    """A non-matching log produces no alerts."""

    async def test_non_matching_log(
        self, pipeline, session_factory,
    ):
        async with session_factory() as session:
            log = _make_log("System started successfully")
            session.add(log)
            await session.commit()
            await session.refresh(log)
            alerts = await pipeline.process(log, session)

        assert len(alerts) == 0


@pytest.mark.usefixtures("_cleanup")
class TestPipelineMultiPattern:
    """A log matching multiple patterns returns multiple alerts."""

    async def test_multi_pattern_match(
        self, pipeline, session_factory,
    ):
        # This message matches both auth_failure (via "auth error")
        # and db_error (via "connection timeout")
        msg = "auth error during connection timeout"
        async with session_factory() as session:
            log = _make_log(msg)
            session.add(log)
            await session.commit()
            await session.refresh(log)
            alerts = await pipeline.process(log, session)

        assert len(alerts) == 2
        pattern_names = {a.pattern_name for a in alerts}
        assert "auth_failure" in pattern_names
        assert "db_error" in pattern_names
