"""Integration tests for RedisStore — requires a live Redis connection."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from src.config import Config
from src.models import Event, Session, SessionState
from src.redis_store import RedisStore


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
async def redis_store():
    """Create a RedisStore connected to the test Redis, flush after each test."""
    config = Config(
        redis_url=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        session_timeout_seconds=60.0,
    )
    store = RedisStore(config)
    await store.connect()
    yield store
    # Cleanup: flush test data
    await store.redis.flushdb()
    await store.close()


def _make_session(
    user_id: str = "user_001",
    session_id: str | None = None,
    state: SessionState = SessionState.ACTIVE,
    events: list[Event] | None = None,
    pages: list[str] | None = None,
    event_types: list[str] | None = None,
) -> Session:
    """Helper to build a Session with sensible defaults."""
    now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    s = Session(
        user_id=user_id,
        state=state,
        start_time=now,
        last_event_time=now + timedelta(minutes=5),
        event_count=3,
        device_type="desktop",
        pages_visited=pages or ["/home", "/product"],
        event_types=event_types or ["page_view", "click"],
        quality_score=42.5,
        engagement="moderate",
        events=events or [],
    )
    if session_id:
        s.session_id = session_id
    return s


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_save_and_load_session(redis_store: RedisStore):
    """Save a Session, load it back, verify all fields match."""
    session = _make_session(user_id="u1")
    await redis_store.save_session(session)

    loaded = await redis_store.load_session(session.session_id)

    assert loaded is not None
    assert loaded.session_id == session.session_id
    assert loaded.user_id == session.user_id
    assert loaded.state == session.state
    assert loaded.start_time == session.start_time
    assert loaded.last_event_time == session.last_event_time
    assert loaded.event_count == session.event_count
    assert loaded.device_type == session.device_type
    assert loaded.pages_visited == session.pages_visited
    assert loaded.event_types == session.event_types
    assert loaded.quality_score == session.quality_score
    assert loaded.engagement == session.engagement


@pytest.mark.asyncio
async def test_ttl_is_set(redis_store: RedisStore):
    """Save session, check TTL exists on the key (> 0)."""
    session = _make_session(user_id="u2")
    await redis_store.save_session(session)

    ttl = await redis_store.redis.ttl(f"session:{session.session_id}")
    assert ttl > 0


@pytest.mark.asyncio
async def test_user_session_index(redis_store: RedisStore):
    """Save session, verify user_sessions SET contains session_id."""
    session = _make_session(user_id="u3")
    await redis_store.save_session(session)

    members = await redis_store.redis.smembers(f"user_sessions:u3")
    assert session.session_id in members


@pytest.mark.asyncio
async def test_delete_session(redis_store: RedisStore):
    """Save then delete, verify load returns None and index is empty."""
    session = _make_session(user_id="u4")
    await redis_store.save_session(session)

    # Verify it exists first
    loaded = await redis_store.load_session(session.session_id)
    assert loaded is not None

    await redis_store.delete_session(session.session_id, session.user_id)

    # Verify deletion
    loaded = await redis_store.load_session(session.session_id)
    assert loaded is None

    members = await redis_store.redis.smembers(f"user_sessions:u4")
    assert session.session_id not in members


@pytest.mark.asyncio
async def test_load_missing_session(redis_store: RedisStore):
    """Load non-existent session_id returns None."""
    loaded = await redis_store.load_session("nonexistent-session-id-12345")
    assert loaded is None


@pytest.mark.asyncio
async def test_get_user_sessions(redis_store: RedisStore):
    """Save 2 sessions for same user, get_user_sessions returns both."""
    s1 = _make_session(user_id="u5")
    s2 = _make_session(user_id="u5")
    # Ensure distinct session_ids (default factory generates unique UUIDs)
    assert s1.session_id != s2.session_id

    await redis_store.save_session(s1)
    await redis_store.save_session(s2)

    sessions = await redis_store.get_user_sessions("u5")
    assert len(sessions) == 2

    loaded_ids = {s.session_id for s in sessions}
    assert s1.session_id in loaded_ids
    assert s2.session_id in loaded_ids


@pytest.mark.asyncio
async def test_save_session_with_events(redis_store: RedisStore):
    """Save session containing events, verify events are preserved on load."""
    now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        Event(
            user_id="u6",
            event_type="page_view",
            timestamp=now,
            device_type="desktop",
            page_url="/home",
            metadata={"ref": "google"},
        ),
        Event(
            user_id="u6",
            event_type="click",
            timestamp=now + timedelta(seconds=30),
            device_type="desktop",
            page_url="/product",
            metadata={},
        ),
    ]
    session = _make_session(user_id="u6", events=events)
    await redis_store.save_session(session)

    loaded = await redis_store.load_session(session.session_id)
    assert loaded is not None
    assert len(loaded.events) == 2
    assert loaded.events[0].event_type == "page_view"
    assert loaded.events[0].page_url == "/home"
    assert loaded.events[0].metadata == {"ref": "google"}
    assert loaded.events[1].event_type == "click"
    assert loaded.events[1].page_url == "/product"
