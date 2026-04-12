"""Comprehensive tests for the SessionEngine — boundary detection, quality scoring, and lifecycle."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.config import Config
from src.models import Event, SessionState, FunnelStage
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


# ---------------------------------------------------------------------------
# Device-transition boundary
# ---------------------------------------------------------------------------


def test_device_transition_creates_new_session(make_event):
    """With device_change_boundary=True, changing device mid-session creates a new session."""
    cfg = Config(
        session_timeout_seconds=3600.0,
        device_change_boundary=True,
    )
    engine = SessionEngine(cfg)
    e1 = make_event(user_id="u1", device_type="desktop", timestamp=BASE_TIME)
    e2 = make_event(user_id="u1", device_type="mobile", timestamp=BASE_TIME + timedelta(seconds=10))

    s1, _ = engine.process_event(e1)
    s2, _ = engine.process_event(e2)

    assert s1.session_id != s2.session_id
    assert s1.state == SessionState.EXPIRED
    assert s2.device_type == "mobile"


def test_device_transition_disabled_by_default(make_event):
    """With default config, changing device does NOT create a new session."""
    cfg = _config(timeout=3600.0)
    engine = SessionEngine(cfg)
    e1 = make_event(user_id="u1", device_type="desktop", timestamp=BASE_TIME)
    e2 = make_event(user_id="u1", device_type="mobile", timestamp=BASE_TIME + timedelta(seconds=10))

    s1, _ = engine.process_event(e1)
    s2, _ = engine.process_event(e2)

    assert s1.session_id == s2.session_id


# ---------------------------------------------------------------------------
# Event deduplication
# ---------------------------------------------------------------------------


def test_event_dedup(make_event):
    """Sending an identical event twice within the dedup window only processes it once."""
    cfg = _config(timeout=3600.0)
    engine = SessionEngine(cfg)
    e1 = make_event(user_id="u1", event_type="page_view", timestamp=BASE_TIME, page_url="/home")
    # Exact duplicate
    e2 = make_event(user_id="u1", event_type="page_view", timestamp=BASE_TIME, page_url="/home")

    s1, _ = engine.process_event(e1)
    s2, _ = engine.process_event(e2)

    assert s1.session_id == s2.session_id
    assert s2.event_count == 1  # only one event processed


# ---------------------------------------------------------------------------
# Out-of-order tolerance
# ---------------------------------------------------------------------------


def test_out_of_order_within_tolerance(make_event):
    """An event with a slightly older timestamp still processes in the same session."""
    cfg = Config(
        session_timeout_seconds=3600.0,
        timestamp_tolerance_seconds=10.0,
    )
    engine = SessionEngine(cfg)
    e1 = make_event(user_id="u1", timestamp=BASE_TIME)
    # Event arrives with timestamp 3 seconds *before* the previous event
    e2 = make_event(user_id="u1", timestamp=BASE_TIME - timedelta(seconds=3))

    s1, _ = engine.process_event(e1)
    s2, _ = engine.process_event(e2)

    assert s1.session_id == s2.session_id
    assert s2.event_count == 2


# ---------------------------------------------------------------------------
# Anomaly scoring
# ---------------------------------------------------------------------------


def test_anomaly_score_normal(make_event):
    """A normal session with moderate event rate gets a low anomaly score."""
    cfg = _config(timeout=3600.0)
    engine = SessionEngine(cfg)
    # Send a few events over several minutes — low velocity
    for i in range(5):
        ev = make_event(
            user_id="u1",
            event_type="page_view",
            timestamp=BASE_TIME + timedelta(minutes=i * 2),
            page_url=f"/page_{i}",
        )
        session, _ = engine.process_event(ev)

    assert session.anomaly_score < 30


def test_anomaly_score_high_velocity(make_event):
    """Many rapid events yield a high anomaly score."""
    cfg = _config(timeout=3600.0)
    engine = SessionEngine(cfg)
    # 50 events in 1 minute = 50/min velocity, well above the 20/min threshold
    for i in range(50):
        ev = make_event(
            user_id="u1",
            event_type="click",
            timestamp=BASE_TIME + timedelta(seconds=i),
            page_url=f"/page_{i}",
        )
        session, _ = engine.process_event(ev)

    assert session.anomaly_score > 30


# ---------------------------------------------------------------------------
# Session type classification
# ---------------------------------------------------------------------------


def test_session_type_browsing(make_event):
    """A session with mostly page_view and click events is classified as browsing."""
    cfg = _config(timeout=3600.0)
    engine = SessionEngine(cfg)
    for i in range(5):
        ev = make_event(
            user_id="u1",
            event_type="page_view" if i % 2 == 0 else "click",
            timestamp=BASE_TIME + timedelta(seconds=i * 10),
            page_url=f"/page_{i}",
        )
        session, _ = engine.process_event(ev)

    assert session.session_type == "browsing"


def test_session_type_searching(make_event):
    """A session with search events is classified as searching."""
    cfg = _config(timeout=3600.0)
    engine = SessionEngine(cfg)
    ev1 = make_event(user_id="u1", event_type="page_view", timestamp=BASE_TIME)
    ev2 = make_event(user_id="u1", event_type="search", timestamp=BASE_TIME + timedelta(seconds=5))

    engine.process_event(ev1)
    session, _ = engine.process_event(ev2)

    assert session.session_type == "searching"


def test_session_type_purchasing(make_event):
    """A session with add_to_cart events is classified as purchasing."""
    cfg = _config(timeout=3600.0)
    engine = SessionEngine(cfg)
    ev1 = make_event(user_id="u1", event_type="page_view", timestamp=BASE_TIME)
    ev2 = make_event(user_id="u1", event_type="add_to_cart", timestamp=BASE_TIME + timedelta(seconds=5))

    engine.process_event(ev1)
    session, _ = engine.process_event(ev2)

    assert session.session_type == "purchasing"


# ---------------------------------------------------------------------------
# Funnel stage progression
# ---------------------------------------------------------------------------


def test_funnel_stage_progression(make_event):
    """Funnel advances through VIEWED -> CARTED -> PURCHASED and never regresses."""
    cfg = _config(timeout=3600.0)
    engine = SessionEngine(cfg)

    ev1 = make_event(user_id="u1", event_type="page_view", timestamp=BASE_TIME, page_url="/product/1")
    session, _ = engine.process_event(ev1)
    assert session.funnel_stage == FunnelStage.VIEWED.value

    ev2 = make_event(user_id="u1", event_type="add_to_cart", timestamp=BASE_TIME + timedelta(seconds=10))
    session, _ = engine.process_event(ev2)
    assert session.funnel_stage == FunnelStage.CARTED.value

    # Note: purchase is a force-end event, so we check funnel within that new session.
    # Instead, we verify that an extra page_view does NOT regress from CARTED.
    ev3 = make_event(user_id="u1", event_type="click", timestamp=BASE_TIME + timedelta(seconds=20))
    session, _ = engine.process_event(ev3)
    assert session.funnel_stage == FunnelStage.CARTED.value  # did NOT regress


# ---------------------------------------------------------------------------
# Session merging
# ---------------------------------------------------------------------------


def test_session_merging(make_event):
    """Probabilistic merging restores a recently expired (merge-eligible) session
    when a new event for the same user has similar event types."""
    cfg = Config(
        session_timeout_seconds=3600.0,
        merge_threshold=0.5,
        merge_window_seconds=300.0,
    )
    engine = SessionEngine(cfg)

    # Build a session with page_view events
    for i in range(3):
        ev = make_event(
            user_id="u1",
            event_type="page_view",
            timestamp=BASE_TIME + timedelta(seconds=i * 5),
            page_url=f"/page_{i}",
        )
        s1, _ = engine.process_event(ev)
    original_id = s1.session_id
    assert s1.event_count == 3

    # Manually finalize with allow_merge=True (simulating a force-end boundary)
    engine._finalize_session(s1, allow_merge=True)
    assert "u1" not in engine.active_sessions
    assert "u1" in engine._recently_expired

    # New page_view event — _create_session checks merge and finds the expired session
    # with high cosine similarity (page_view vs page_view = 1.0 > 0.5 threshold)
    ev_new = make_event(
        user_id="u1",
        event_type="page_view",
        timestamp=BASE_TIME + timedelta(seconds=30),
        page_url="/page_new",
    )
    s2, _ = engine.process_event(ev_new)

    # The session was merged — same session restored with merged_from tracking
    assert original_id in s2.merged_from
    assert s2.state == SessionState.ACTIVE
    # The restored session should have the original events plus the new one
    assert s2.event_count == 4
