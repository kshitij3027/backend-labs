"""Domain models shared across the RCA Analysis Engine.

This module is the single source of truth for the engine's vocabulary: the log
severity / event-classification / hypothesis-state enums, the incoming
:class:`LogEvent`, and the pieces of an :class:`IncidentReport`
(:class:`TimelineEntry`, :class:`RootCause`, :class:`Hypothesis`,
:class:`ImpactAnalysis`). Every later stage (timeline reconstructor, causal graph
builder, root-cause identifier, impact analyzer, multi-hypothesis tracker, the API
layer) imports these shapes from here and never redefines them.

Many fields carry sensible defaults on purpose: early commits assemble only a
partial :class:`IncidentReport` (e.g. a timeline with no root causes yet), and the
defaults let those partial reports validate and serialize cleanly.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class LogLevel(str, Enum):
    """Severity of a log event (ordered INFO < WARNING < ERROR < CRITICAL)."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class EventClass(str, Enum):
    """Post-mortem classification of an event's causal role (populated in C9)."""

    PRIMARY_TRIGGER = "primary_trigger"
    PROPAGATION_PATH = "propagation_path"
    CONTRIBUTING_FACTOR = "contributing_factor"


class HypothesisState(str, Enum):
    """Lifecycle state of a root-cause hypothesis (used by the tracker in C7)."""

    TENTATIVE = "tentative"
    CONFIRMED = "confirmed"
    PRUNED = "pruned"


class LogEvent(BaseModel):
    """A single incoming log event, as posted to ``/api/analyze-incident``.

    The timestamp is kept as the raw incoming string here — tolerant parsing into
    an absolute time happens in the timeline module (C2), which is also where the
    deterministic ``event_id`` is assigned when a client did not supply one.
    """

    timestamp: str
    service: str
    level: LogLevel
    message: str
    #: Optional client-supplied id; the timeline reconstructor derives a stable one
    #: when this is absent.
    event_id: str | None = None


class TimelineEntry(BaseModel):
    """One chronologically-ordered entry in a reconstructed incident timeline.

    Fully populated by the timeline reconstructor in C2; defined now so the shared
    :class:`IncidentReport` shape is stable from the start.
    """

    sequence_id: int
    timestamp: str
    #: Offset from the incident start, formatted ``T+M:SS`` (e.g. ``T+2:05``).
    relative_time: str
    service: str
    level: LogLevel
    message: str
    event_id: str
    #: Surrounding-event context (neighbouring services/levels); filled in C2.
    context: dict = Field(default_factory=dict)


class RootCause(BaseModel):
    """A ranked root-cause candidate with a calibrated confidence.

    Emitted by the root-cause identifier + confidence scorer in C4; defined now for
    the shared report shape.
    """

    event_id: str
    service: str
    level: LogLevel
    message: str
    #: Confidence in [0, 1] that this event is the (a) root cause.
    confidence: float = 0.0
    timestamp: str


class Hypothesis(BaseModel):
    """One concurrent root-cause hypothesis with an independent confidence.

    Produced by the multi-hypothesis tracker in C7; defined now for the shared
    report shape.
    """

    hypothesis_id: str
    root_cause_event_id: str
    #: Independent (not normalized-to-1 across hypotheses) confidence in [0, 1].
    confidence: float = 0.0
    state: HypothesisState = HypothesisState.TENTATIVE


class ImpactAnalysis(BaseModel):
    """Blast-radius / impact section of an incident report.

    Populated by the impact analyzer in C5 (descendants -> blast radius, distinct
    affected services, weighted reachability details); zeroed by default.
    """

    blast_radius: int = 0
    affected_services: list[str] = Field(default_factory=list)
    total_events: int = 0
    details: dict = Field(default_factory=dict)


class IncidentReport(BaseModel):
    """The full analysis result for one incident.

    Assembled by ``RCAAnalyzer.analyze`` from C5 onward and returned by
    ``POST /api/analyze-incident`` / stored in the in-memory history. Every
    collection defaults empty so partially-populated reports from earlier commits
    still validate.
    """

    incident_id: str
    timestamp: str
    events: list[LogEvent] = Field(default_factory=list)
    timeline: list[TimelineEntry] = Field(default_factory=list)
    root_causes: list[RootCause] = Field(default_factory=list)
    impact_analysis: ImpactAnalysis = Field(default_factory=ImpactAnalysis)
    #: Concurrent alternative explanations (multi-hypothesis tracking, C7).
    hypotheses: list[Hypothesis] = Field(default_factory=list)
    #: Per-event base-rate anomaly scores in [0, 1] (``event_id -> score``), produced by
    #: the anomaly amplifier in C7 and used to seed the multi-hypothesis PageRank.
    #: Defaults empty so earlier/partial reports still validate and serialize cleanly.
    anomaly_scores: dict = Field(default_factory=dict)
    #: Serialized causal DiGraph — ``{"nodes": [...], "edges": [...]}`` — built in C3
    #: and consumed by the C12 dashboard. Defaults empty so earlier/partial reports
    #: (assembled before the graph stage) still validate and serialize cleanly.
    causal_graph: dict = Field(default_factory=dict)
