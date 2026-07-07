"""Domain models shared across the correlation-analysis pipeline.

This module is the single source of truth for the pipeline vocabulary: log
sources, correlation families, incident scenarios, the standardized
:class:`LogEvent`, detection outputs (:class:`Correlation`, :class:`Alert`),
generator ground truth (:class:`JourneyRecord`) and the error-code taxonomy.
Every later stage (generators, parsers, collector, engine, alerts, API) imports
from here and never redefines these shapes.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class SourceType(str, Enum):
    """The five simulated e-commerce log sources."""

    WEB = "web"
    DATABASE = "database"
    API_SERVICE = "api_service"
    PAYMENT = "payment"
    INVENTORY = "inventory"


class CorrelationType(str, Enum):
    """The five correlation families the engine can detect."""

    TEMPORAL = "temporal"
    SESSION = "session_based"
    USER = "user_based"
    CASCADE = "error_cascade"
    METRIC = "metric_based"


class ScenarioKind(str, Enum):
    """Injected incident scenarios rotated by the log generator."""

    DB_POOL_SATURATION = "db_pool_saturation"
    PAYMENT_SLOWDOWN = "payment_slowdown"
    INVENTORY_TIMEOUTS = "inventory_timeouts"


# --- Error-code taxonomy -----------------------------------------------------
# Plain string constants (not an Enum) so parsers can also derive codes
# dynamically (e.g. f"HTTP_{status}") while remaining comparable to these.
HTTP_500 = "HTTP_500"
HTTP_502 = "HTTP_502"
HTTP_503 = "HTTP_503"
DB_POOL_EXHAUSTED = "DB_POOL_EXHAUSTED"
DB_QUERY_ERROR = "DB_QUERY_ERROR"
PAYMENT_TIMEOUT = "PAYMENT_TIMEOUT"
PAYMENT_DECLINED = "PAYMENT_DECLINED"
INVENTORY_TIMEOUT = "INVENTORY_TIMEOUT"
CHECKOUT_FAILED = "CHECKOUT_FAILED"
CART_ABANDONED = "CART_ABANDONED"


class LogEvent(BaseModel):
    """A raw log line standardized into the pipeline's common shape."""

    id: str
    #: Event time (epoch seconds) extracted from the line itself — NOT ingestion time.
    timestamp: float
    source: SourceType
    #: Emitting service: nginx / postgresql / api-service / payment-service /
    #: inventory-service.
    service: str
    #: "INFO" | "WARN" | "ERROR"
    level: str
    #: Concise human-readable summary derived from the line.
    message: str
    #: Journey/session id shared by every hop of one checkout flow (None for
    #: background noise, which is exactly what makes noise non-correlatable by id).
    correlation_id: str | None = None
    user_id: str | None = None
    #: One of the module-level error-code constants (or f"HTTP_{status}") when the
    #: line represents a failure; None otherwise.
    error_code: str | None = None
    #: Numeric measurements extracted from the line — latency_ms, status,
    #: pool_in_use, pool_size, amount, bytes — whichever apply to the source.
    metrics: dict[str, float] = Field(default_factory=dict)
    #: The original unparsed line, kept for display and debugging.
    raw: str = ""


class EventRef(BaseModel):
    """Lightweight reference to a :class:`LogEvent`, embedded in a Correlation."""

    id: str
    source: SourceType
    service: str
    message: str
    timestamp: float
    correlation_id: str | None = None

    @classmethod
    def from_event(cls, ev: LogEvent) -> EventRef:
        """Project a full LogEvent down to the reference fields."""
        return cls(
            id=ev.id,
            source=ev.source,
            service=ev.service,
            message=ev.message,
            timestamp=ev.timestamp,
            correlation_id=ev.correlation_id,
        )


class Correlation(BaseModel):
    """A detected relationship between two events, with strength + confidence."""

    id: str
    #: Epoch seconds when the engine emitted this correlation.
    detected_at: float
    correlation_type: CorrelationType
    event_a: EventRef
    event_b: EventRef
    #: How strong the relationship is, clamped to [0, 1].
    strength: float
    #: How sure the engine is that the relationship is real, clamped to [0, 1].
    confidence: float
    #: Detector-specific extras (e.g. metric pair, r, p_adj, lag_seconds).
    details: dict[str, Any] = Field(default_factory=dict)


class Alert(BaseModel):
    """An operator-facing notification derived from detected correlations."""

    id: str
    created_at: float
    #: "info" | "warning" | "critical"
    severity: str
    title: str
    message: str
    correlation_type: CorrelationType
    strength: float
    confidence: float


class JourneyRecord(BaseModel):
    """Generator ground truth for one synthetic checkout journey.

    The E2E verifier compares detected session correlations against these
    records, so the generator appends one per spawned journey (bounded deque).
    """

    correlation_id: str
    user_id: str
    #: SourceType values of the journey's emitted lines, in hop order.
    sources: list[str] = Field(default_factory=list)
    started_at: float
    completed_at: float | None = None
    #: True when an injected scenario made the user abandon the cart.
    abandoned: bool = False
