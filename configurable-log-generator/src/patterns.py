"""State machine log patterns and session tracking."""

import time
import random
import uuid
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class PatternStep:
    level: str
    message: str
    delay_range: tuple  # (min_seconds, max_seconds) before next step


# ── User Session: login → browse → purchase → logout ──

USER_SESSION_PATTERN = [
    PatternStep("INFO", "User login successful", (0.5, 2.0)),
    PatternStep("DEBUG", "Loading user preferences", (0.2, 1.0)),
    PatternStep("INFO", "User browsing product catalog", (1.0, 5.0)),
    PatternStep("INFO", "Item added to cart", (0.5, 3.0)),
    PatternStep("INFO", "Purchase initiated", (0.3, 1.5)),
    PatternStep("INFO", "Payment processed successfully", (0.5, 2.0)),
    PatternStep("INFO", "Order confirmation email sent", (0.2, 1.0)),
    PatternStep("INFO", "User logged out", (0.0, 0.5)),
]

# ── API Request: request → processing → response ──

API_REQUEST_PATTERN = [
    PatternStep("INFO", "Incoming API request: GET /api/data", (0.1, 0.5)),
    PatternStep("DEBUG", "Authenticating request token", (0.1, 0.3)),
    PatternStep("DEBUG", "Processing request payload", (0.2, 1.0)),
    PatternStep("INFO", "Database query executed", (0.1, 0.5)),
    PatternStep("INFO", "API response sent: 200 OK", (0.0, 0.2)),
]

# ── Error Recovery: warning → error → recovery ──

ERROR_RECOVERY_PATTERN = [
    PatternStep("WARNING", "Elevated error rate detected", (0.5, 2.0)),
    PatternStep("WARNING", "Service response time degraded", (0.3, 1.0)),
    PatternStep("ERROR", "Service dependency unavailable", (0.5, 2.0)),
    PatternStep("ERROR", "Circuit breaker activated", (1.0, 3.0)),
    PatternStep("WARNING", "Attempting automatic recovery", (0.5, 1.5)),
    PatternStep("INFO", "Service recovered, circuit closed", (0.0, 0.5)),
]

ALL_PATTERNS = [USER_SESSION_PATTERN, API_REQUEST_PATTERN, ERROR_RECOVERY_PATTERN]


class ActiveSession:
    """A single in-progress pattern instance."""

    def __init__(self, pattern_steps: list, service: str, user_id: str, request_id: str):
        self.steps = pattern_steps
        self.current_step = 0
        self.service = service
        self.user_id = user_id
        self.request_id = request_id
        self.next_step_time = time.time()

    def is_ready(self) -> bool:
        return time.time() >= self.next_step_time

    def is_complete(self) -> bool:
        return self.current_step >= len(self.steps)

    def advance(self) -> PatternStep:
        """Return current step and schedule the next one."""
        step = self.steps[self.current_step]
        self.current_step += 1
        if not self.is_complete():
            lo, hi = step.delay_range
            self.next_step_time = time.time() + random.uniform(lo, hi)
        return step


class PatternManager:
    """Manages spawning and advancing pattern sessions."""

    def __init__(self, services: list, enabled: bool):
        self._services = services
        self._enabled = enabled
        self._active_sessions: list[ActiveSession] = []
        self._last_spawn_time = time.time()
        self._spawn_interval = 2.0

    def tick(self) -> list:
        """Called each iteration. Returns list of (level, message, service, user_id, request_id)."""
        if not self._enabled:
            return []

        results = []

        # Maybe spawn a new session
        now = time.time()
        if now - self._last_spawn_time >= self._spawn_interval:
            self._last_spawn_time = now
            if random.random() < 0.3:
                pattern = random.choice(ALL_PATTERNS)
                session = ActiveSession(
                    pattern_steps=pattern,
                    service=random.choice(self._services),
                    user_id=f"user-{random.randint(10000, 99999)}",
                    request_id=f"req-{uuid.uuid4().hex[:6]}",
                )
                self._active_sessions.append(session)
                logger.debug("Spawned new pattern session on %s", session.service)

        # Advance all ready sessions
        for session in self._active_sessions:
            if session.is_ready() and not session.is_complete():
                step = session.advance()
                results.append((
                    step.level,
                    step.message,
                    session.service,
                    session.user_id,
                    session.request_id,
                ))

        # Clean up completed sessions
        self._active_sessions = [s for s in self._active_sessions if not s.is_complete()]

        return results
