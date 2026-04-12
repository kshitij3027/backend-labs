"""Comprehensive tests for the SessionEngine — boundary detection, quality scoring, and lifecycle."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.config import Config
from src.models import Event, SessionState
from src.session_engine import SessionEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BASE_TIME = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _config(timeout: float = 60.0, max_dur: float = 14400.0) -> Config:
    """Return a Config with a short timeout but standard max duration."""
    return Config(
        session_timeout_seconds=timeout,
        session_max_duration_seconds=max_dur,
    )


# ---------------------------------------------------------------------------
# Session creation & reuse
# ---------------------------------------------------------------------------


def test_new_session_on_first_event(make_event):
    """First event for a user creates a brand-new session."""
    engine = SessionEngine(_config())
    event = make_event(user_id="u1", timestamp=BASE_TIME)
    session, analysis = engine.process_event(event)

    assert session.user_id == "u1"
    assert session.event_count == 1
    assert session.session_id == analysis.session_id
    assert "u1" in engine.active_sessions


def test_session_reuse_within_timeout(make_event):
    """Subsequent event within timeout reuses the same session."""
    engine = SessionEngine(_config(timeout=60.0))
    e1 = make_event(user_id="u1", timestamp=BASE_TIME)
    e2 = make_event(user_id="u1", timestamp=BASE_TIME + timedelta(seconds=30))

    s1, _ = engine.process_event(e1)
    s2, _ = engine.process_event(e2)

    assert s1.session_id == s2.session_id
    assert s2.event_count == 2


def test_time_gap_creates_new_session(make_event):
    """Event arriving after the timeout window creates a new session."""
    engine = SessionEngine(_config(timeout=60.0))
    e1 = make_event(user_id="u1", timestamp=BASE_TIME)
    e2 = make_event(user_id="u1", timestamp=BASE_TIME + timedelta(seconds=120))

    s1, _ = engine.process_event(e1)
    s2, _ = engine.process_event(e2)

    assert s1.session_id != s2.session_id
    assert s1.state == SessionState.EXPIRED
    assert s2.state == SessionState.ACTIVE


# ---------------------------------------------------------------------------
# Force-boundary events
# ---------------------------------------------------------------------------


def test_force_end_logout_closes_session(make_event):
    """A logout event closes the current session and starts a fresh one."""
    engine = SessionEngine(_config())
    e1 = make_event(user_id="u1", event_type="page_view", timestamp=BASE_TIME)
    e2 = make_event(user_id="u1", event_type="logout", timestamp=BASE_TIME + timedelta(seconds=10))

    s1, _ = engine.process_event(e1)
    s2, _ = engine.process_event(e2)

    assert s1.session_id != s2.session_id
    assert s1.state == SessionState.EXPIRED


def test_force_end_purchase_closes_session(make_event):
    """A purchase event closes the current session."""
    engine = SessionEngine(_config())
    e1 = make_event(user_id="u1", event_type="page_view", timestamp=BASE_TIME)
    e2 = make_event(user_id="u1", event_type="purchase", timestamp=BASE_TIME + timedelta(seconds=5))

    s1, _ = engine.process_event(e1)
    s2, _ = engine.process_event(e2)

    assert s1.session_id != s2.session_id
    assert s1.state == SessionState.EXPIRED


def test_force_new_login_starts_session(make_event):
    """A login event always starts a new session regardless of timeout."""
    engine = SessionEngine(_config())
    e1 = make_event(user_id="u1", event_type="page_view", timestamp=BASE_TIME)
    e2 = make_event(user_id="u1", event_type="login", timestamp=BASE_TIME + timedelta(seconds=5))

    s1, _ = engine.process_event(e1)
    s2, _ = engine.process_event(e2)

    assert s1.session_id != s2.session_id
    assert s1.state == SessionState.EXPIRED
    assert s2.state == SessionState.ACTIVE


# ---------------------------------------------------------------------------
# Max duration cap
# ---------------------------------------------------------------------------


def test_max_duration_cap(make_event):
    """Session exceeding max duration is closed even without a timeout gap."""
    engine = SessionEngine(_config(timeout=6000.0, max_dur=120.0))
    e1 = make_event(user_id="u1", timestamp=BASE_TIME)
    # 130 seconds later — within timeout (6000s) but past max duration (120s)
    e2 = make_event(user_id="u1", timestamp=BASE_TIME + timedelta(seconds=130))

    s1, _ = engine.process_event(e1)
    s2, _ = engine.process_event(e2)

    assert s1.session_id != s2.session_id
    assert s1.state == SessionState.EXPIRED


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------


def test_state_transitions_created_to_active(make_event):
    """A newly created session transitions to ACTIVE immediately."""
    engine = SessionEngine(_config())
    event = make_event(user_id="u1", timestamp=BASE_TIME)
    session, _ = engine.process_event(event)

    assert session.state == SessionState.ACTIVE


def test_state_transition_idle(make_event):
    """An ACTIVE session can transition to IDLE via the engine's idle helper."""
    engine = SessionEngine(_config())
    event = make_event(user_id="u1", timestamp=BASE_TIME)
    session, _ = engine.process_event(event)

    assert session.state == SessionState.ACTIVE
    engine._transition_to_idle(session)
    assert session.state == SessionState.IDLE


@pytest.mark.asyncio
async def test_state_transition_expired(make_event):
    """An IDLE session transitions to EXPIRED during cleanup."""
    engine = SessionEngine(_config(timeout=1.0))
    # Create a session with a timestamp far in the past
    past = datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    event = make_event(user_id="u1", timestamp=past)
    session, _ = engine.process_event(event)

    # Force IDLE state
    engine._transition_to_idle(session)
    assert session.state == SessionState.IDLE

    # cleanup_idle_sessions uses datetime.now(utc), so the gap is huge
    expired = await engine.cleanup_idle_sessions()

    assert len(expired) == 1
    assert expired[0].state == SessionState.EXPIRED
    assert "u1" not in engine.active_sessions


# ---------------------------------------------------------------------------
# Quality scoring
# ---------------------------------------------------------------------------


def test_quality_score_single_event(make_event):
    """A single page_view with zero duration yields a low quality score."""
    engine = SessionEngine(_config())
    event = make_event(user_id="u1", event_type="page_view", timestamp=BASE_TIME)
    session, analysis = engine.process_event(event)

    assert analysis.quality_score < 20
    assert analysis.quality_score >= 0


def test_quality_score_many_diverse_events(make_event):
    """Many diverse events over a reasonable duration yield a high score.

    Note: 'purchase' is a force-end event so we use 'add_to_cart' as the
    deepest funnel stage that does not break the session.
    """
    engine = SessionEngine(_config(timeout=3600.0))
    types = ["page_view", "click", "search", "add_to_cart"]
    for i, et in enumerate(types):
        ev = make_event(
            user_id="u1",
            event_type=et,
            timestamp=BASE_TIME + timedelta(minutes=i * 3),
            page_url=f"/page_{i}",
        )
        engine.process_event(ev)
    # Add more events over time to boost count and duration
    for j in range(10):
        ev = make_event(
            user_id="u1",
            event_type="click",
            timestamp=BASE_TIME + timedelta(minutes=12 + j),
            page_url=f"/extra_{j}",
        )
        session, analysis = engine.process_event(ev)

    assert analysis.quality_score > 50


# ---------------------------------------------------------------------------
# Engagement classification
# ---------------------------------------------------------------------------


def test_engagement_bounce(make_event):
    """Score 0-15 classifies as 'bounce'."""
    engine = SessionEngine(_config())
    event = make_event(user_id="u1", event_type="page_view", timestamp=BASE_TIME)
    _, analysis = engine.process_event(event)

    # Single page_view with zero duration should be bounce
    assert analysis.engagement == "bounce"


def test_engagement_low(make_event):
    """Score 16-40 classifies as 'low'."""
    engine = SessionEngine(_config(timeout=3600.0))
    # Several page_views over a few minutes — enough to cross bounce threshold
    for i in range(5):
        ev = make_event(
            user_id="u1",
            event_type="page_view",
            timestamp=BASE_TIME + timedelta(minutes=i),
            page_url=f"/page_{i}",
        )
        _, analysis = engine.process_event(ev)

    assert analysis.engagement == "low"


def test_engagement_moderate(make_event):
    """Score 41-70 classifies as 'moderate'."""
    engine = SessionEngine(_config(timeout=3600.0))
    types = ["page_view", "click", "search"]
    for i, et in enumerate(types):
        ev = make_event(
            user_id="u1",
            event_type=et,
            timestamp=BASE_TIME + timedelta(minutes=i * 5),
            page_url=f"/page_{i}",
        )
        engine.process_event(ev)
    # Add more events spread over time to build count and duration
    for j in range(8):
        ev = make_event(
            user_id="u1",
            event_type="click",
            timestamp=BASE_TIME + timedelta(minutes=15 + j * 2),
            page_url=f"/extra_{j}",
        )
        _, analysis = engine.process_event(ev)

    assert analysis.engagement == "moderate"


def test_engagement_high(make_event):
    """Score 71+ classifies as 'high'.

    Uses add_to_cart (not purchase) as deepest funnel stage to avoid
    triggering force-end boundary detection.
    """
    engine = SessionEngine(_config(timeout=7200.0))
    types = ["page_view", "click", "search", "add_to_cart"]
    for i, et in enumerate(types):
        ev = make_event(
            user_id="u1",
            event_type=et,
            timestamp=BASE_TIME + timedelta(minutes=i * 3),
            page_url=f"/page_{i}",
        )
        engine.process_event(ev)
    # Pad with more events over time
    for j in range(10):
        ev = make_event(
            user_id="u1",
            event_type="click",
            timestamp=BASE_TIME + timedelta(minutes=12 + j),
            page_url=f"/deep_{j}",
        )
        _, analysis = engine.process_event(ev)

    assert analysis.engagement == "high"


# ---------------------------------------------------------------------------
# Metadata tracking
# ---------------------------------------------------------------------------


def test_metadata_pages_visited(make_event):
    """pages_visited tracks unique pages only."""
    engine = SessionEngine(_config())
    urls = ["/home", "/product", "/home", "/cart", "/product"]
    for i, url in enumerate(urls):
        ev = make_event(
            user_id="u1",
            timestamp=BASE_TIME + timedelta(seconds=i),
            page_url=url,
        )
        session, _ = engine.process_event(ev)

    assert session.pages_visited == ["/home", "/product", "/cart"]


def test_metadata_event_types(make_event):
    """event_types tracks unique event types only."""
    engine = SessionEngine(_config())
    types = ["page_view", "click", "page_view", "search", "click"]
    for i, et in enumerate(types):
        ev = make_event(
            user_id="u1",
            event_type=et,
            timestamp=BASE_TIME + timedelta(seconds=i),
        )
        session, _ = engine.process_event(ev)

    assert session.event_types == ["page_view", "click", "search"]


def test_metadata_device_type(make_event):
    """device_type on the session comes from the first event."""
    engine = SessionEngine(_config())
    ev = make_event(user_id="u1", device_type="mobile", timestamp=BASE_TIME)
    session, _ = engine.process_event(ev)

    assert session.device_type == "mobile"


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_cleanup_idle_sessions(make_event):
    """cleanup_idle_sessions expires IDLE sessions past timeout and idles long-inactive ACTIVE ones."""
    engine = SessionEngine(_config(timeout=1.0))
    past = datetime(2023, 6, 1, 0, 0, 0, tzinfo=timezone.utc)

    # Create two sessions with old timestamps
    e1 = make_event(user_id="u1", timestamp=past)
    e2 = make_event(user_id="u2", timestamp=past)
    s1, _ = engine.process_event(e1)
    s2, _ = engine.process_event(e2)

    # Move one to IDLE (eligible for expiration), leave other ACTIVE (eligible for idle)
    engine._transition_to_idle(s1)

    expired = await engine.cleanup_idle_sessions()

    # s1 was IDLE + past timeout => EXPIRED and removed
    assert len(expired) == 1
    assert expired[0].user_id == "u1"
    assert "u1" not in engine.active_sessions

    # s2 was ACTIVE + past half-timeout => now IDLE but still in active_sessions
    assert s2.state == SessionState.IDLE
    assert "u2" in engine.active_sessions
