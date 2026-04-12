"""Core sessionization engine with hybrid boundary detection and quality scoring."""
from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timezone, timedelta

from src.config import Config
from src.models import Event, Session, SessionState, SessionAnalysis

logger = logging.getLogger(__name__)

# Boundary event sets
FORCE_END_EVENTS = {"logout", "purchase"}
FORCE_NEW_EVENTS = {"login"}

# Valid state transitions
VALID_TRANSITIONS = {
    SessionState.CREATED: {SessionState.ACTIVE},
    SessionState.ACTIVE: {SessionState.IDLE, SessionState.EXPIRED},
    SessionState.IDLE: {SessionState.ACTIVE, SessionState.EXPIRED},
    SessionState.EXPIRED: set(),  # terminal state
}

# Quality scoring constants
MAX_EVENT_TYPES = 6  # page_view, click, search, add_to_cart, purchase, logout
FUNNEL_SCORES = {
    "page_view": 0,
    "click": 1,
    "search": 3,
    "add_to_cart": 10,
    "purchase": 15,
}


class SessionEngine:
    """Manages session lifecycle with hybrid boundary detection."""

    def __init__(self, config: Config, redis_store=None):
        self._config = config
        # In-memory active sessions: user_id -> Session
        self._active_sessions: dict[str, Session] = {}
        self._total_events: int = 0
        # Hash-partitioned queues for concurrent event processing
        self._queues: list[asyncio.Queue] = [
            asyncio.Queue(maxsize=1000) for _ in range(config.num_partitions)
        ]
        self._workers: list[asyncio.Task] = []
        self._redis_store = redis_store

    @property
    def active_sessions(self) -> dict[str, Session]:
        return self._active_sessions

    @property
    def total_events(self) -> int:
        return self._total_events

    def _partition_for(self, user_id: str) -> int:
        """Determine which partition queue handles a given user."""
        return hash(user_id) % self._config.num_partitions

    async def enqueue_event(self, event: Event) -> tuple[Session, SessionAnalysis]:
        """Put event into the correct partition queue and await the result."""
        partition = self._partition_for(event.user_id)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[tuple[Session, SessionAnalysis]] = loop.create_future()
        await self._queues[partition].put((event, future))
        return await future

    async def start_workers(self) -> None:
        """Create asyncio tasks for each partition worker."""
        for i in range(self._config.num_partitions):
            task = asyncio.create_task(
                self._run_partition_worker(i), name=f"partition-worker-{i}"
            )
            self._workers.append(task)
        logger.info("Started %d partition workers", len(self._workers))

    async def stop_workers(self) -> None:
        """Cancel all partition worker tasks."""
        for task in self._workers:
            task.cancel()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        logger.info("All partition workers stopped")

    async def _run_partition_worker(self, partition_id: int) -> None:
        """Drain queue for a partition, process events, and set results."""
        queue = self._queues[partition_id]
        logger.info("Partition worker %d started", partition_id)
        try:
            while True:
                event, future = await queue.get()
                try:
                    result = self.process_event(event)
                    if not future.cancelled():
                        future.set_result(result)
                except Exception as exc:
                    if not future.cancelled():
                        future.set_exception(exc)
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            logger.info("Partition worker %d cancelled", partition_id)

    async def flush_to_redis(self) -> None:
        """Persist all active sessions to Redis (for graceful shutdown)."""
        if not self._redis_store:
            return
        for session in self._active_sessions.values():
            try:
                await self._redis_store.save_session(session)
            except Exception:
                logger.exception(
                    "Failed to flush session %s to Redis", session.session_id
                )
        logger.info("Flushed %d sessions to Redis", len(self._active_sessions))

    async def get_user_sessions(self, user_id: str) -> list[Session]:
        """Check in-memory first, then fall back to Redis store."""
        sessions: list[Session] = []
        # Check in-memory active sessions
        active = self._active_sessions.get(user_id)
        if active:
            sessions.append(active)
        # Fall back to Redis for historical sessions
        if self._redis_store:
            redis_sessions = await self._redis_store.get_user_sessions(user_id)
            # Deduplicate: skip any already found in-memory
            active_ids = {s.session_id for s in sessions}
            for rs in redis_sessions:
                if rs.session_id not in active_ids:
                    sessions.append(rs)
        return sessions

    def process_event(self, event: Event) -> tuple[Session, SessionAnalysis]:
        """Process a single event and return (session, analysis)."""
        self._total_events += 1
        session = self._find_or_create_session(event)
        self._update_session(session, event)
        score = self._compute_quality_score(session)
        session.quality_score = score
        session.engagement = self._classify_engagement(score)
        analysis = SessionAnalysis(
            quality_score=score,
            engagement=session.engagement,
            session_id=session.session_id,
        )
        return session, analysis

    def _find_or_create_session(self, event: Event) -> Session:
        """Find existing session or create new one using hybrid boundary detection."""
        current = self._active_sessions.get(event.user_id)

        if current is None:
            # No active session — create new
            return self._create_session(event)

        # Priority 1: Force-end event closes current session
        if event.event_type in FORCE_END_EVENTS:
            self._finalize_session(current)
            return self._create_session(event)

        # Priority 2: Force-new event starts new session
        if event.event_type in FORCE_NEW_EVENTS:
            self._finalize_session(current)
            return self._create_session(event)

        # Priority 3: Time-gap check
        gap = (event.timestamp - current.last_event_time).total_seconds()
        if gap > self._config.session_timeout_seconds:
            self._finalize_session(current)
            return self._create_session(event)

        # Priority 4: Max duration cap
        duration = (event.timestamp - current.start_time).total_seconds()
        if duration > self._config.session_max_duration_seconds:
            self._finalize_session(current)
            return self._create_session(event)

        # No boundary — continue current session
        return current

    def _create_session(self, event: Event) -> Session:
        """Create a new session for the event."""
        session = Session(
            user_id=event.user_id,
            start_time=event.timestamp,
            last_event_time=event.timestamp,
            device_type=event.device_type,
        )
        # Transition to ACTIVE immediately
        session.state = SessionState.ACTIVE
        self._active_sessions[event.user_id] = session
        return session

    def _update_session(self, session: Session, event: Event) -> None:
        """Update session with new event data."""
        session.last_event_time = event.timestamp
        session.event_count += 1
        session.events.append(event)
        # Ensure ACTIVE state
        if session.state in (SessionState.CREATED, SessionState.IDLE):
            session.state = SessionState.ACTIVE
        # Track pages
        page = event.page_url or event.metadata.get("url", "")
        if page and page not in session.pages_visited:
            session.pages_visited.append(page)
        # Track event types
        if event.event_type not in session.event_types:
            session.event_types.append(event.event_type)

    def _finalize_session(self, session: Session) -> None:
        """Mark session as expired and remove from active sessions."""
        session.state = SessionState.EXPIRED
        self._active_sessions.pop(session.user_id, None)
        # Persist to Redis if available
        if self._redis_store:
            asyncio.create_task(self._redis_store.save_session(session))

    def _transition_to_idle(self, session: Session) -> None:
        """Transition session to IDLE state."""
        if SessionState.IDLE in VALID_TRANSITIONS.get(session.state, set()):
            session.state = SessionState.IDLE

    async def cleanup_idle_sessions(self) -> list[Session]:
        """Scan active sessions, idle those past timeout, expire those already idle."""
        now = datetime.now(timezone.utc)
        expired = []
        to_remove = []

        for user_id, session in self._active_sessions.items():
            gap = (now - session.last_event_time).total_seconds()
            if session.state == SessionState.IDLE and gap > self._config.session_timeout_seconds:
                session.state = SessionState.EXPIRED
                to_remove.append(user_id)
                expired.append(session)
            elif session.state == SessionState.ACTIVE and gap > self._config.session_timeout_seconds / 2:
                self._transition_to_idle(session)

        for uid in to_remove:
            self._active_sessions.pop(uid, None)

        # Persist expired sessions to Redis
        if self._redis_store:
            for session in expired:
                try:
                    await self._redis_store.save_session(session)
                except Exception:
                    logger.exception(
                        "Failed to persist expired session %s", session.session_id
                    )

        return expired

    @staticmethod
    def _compute_quality_score(session: Session) -> float:
        """Compute session quality score (0-100)."""
        # Event count score (0-30): log-scaled with diminishing returns
        count = session.event_count
        event_score = 30.0 * math.log(1 + count) / math.log(51) if count > 0 else 0.0

        # Duration score (0-30): sweet-spot curve
        duration = (session.last_event_time - session.start_time).total_seconds()
        if duration < 30:
            duration_score = 30.0 * (duration / 30.0) * 0.3  # penalize bounces
        elif duration <= 900:  # 15 minutes sweet spot
            duration_score = 30.0 * (duration / 900.0)
        else:
            # Diminishing returns after 15 min
            duration_score = 30.0 * (1.0 - 0.2 * min(1.0, (duration - 900) / 3600))

        # Event diversity score (0-25)
        unique_types = len(session.event_types)
        diversity_score = 25.0 * min(1.0, unique_types / MAX_EVENT_TYPES)

        # Conversion proximity score (0-15): deepest funnel stage
        max_funnel = 0
        for et in session.event_types:
            max_funnel = max(max_funnel, FUNNEL_SCORES.get(et, 0))
        proximity_score = float(max_funnel)

        total = event_score + duration_score + diversity_score + proximity_score
        return round(min(100.0, max(0.0, total)), 1)

    @staticmethod
    def _classify_engagement(score: float) -> str:
        """Classify session engagement based on quality score."""
        if score <= 15:
            return "bounce"
        elif score <= 40:
            return "low"
        elif score <= 70:
            return "moderate"
        else:
            return "high"
