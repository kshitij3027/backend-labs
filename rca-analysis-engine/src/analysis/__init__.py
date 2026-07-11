"""The ``analysis`` strategy subpackage — home of :class:`RCAAnalyzer`.

:class:`RCAAnalyzer` is the orchestrator that turns a batch of :class:`LogEvent`
into an :class:`IncidentReport`. It owns the bounded in-memory incident history and
composes the per-stage collaborators (each a small module in this package).

C2 wired the first stage — the :class:`~src.analysis.timeline.TimelineReconstructor`.
C3 adds the second — the :class:`~src.analysis.causal_graph.CausalGraphBuilder` —
so ``analyze`` now also populates the report's serialized ``causal_graph``. Root
causes, impact and hypotheses remain at their empty defaults until later commits fill
them in at the marked seams; those stages consume the in-memory ``graph`` object
built here. The remaining stages are folded into ``analyze`` at their marked seams:

* C3 — causal-graph builder (``networkx.DiGraph``),
* C4 — root-cause identifier + confidence scorer,
* C5 — impact analyzer + full report assembly,
* C7 — anomaly amplification + multi-hypothesis tracking,
* C8 — clock-skew correction + incremental streaming,
* C9 — confidence calibration + post-mortem reporting.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import uuid4

from src.analysis.causal_graph import CausalGraphBuilder
from src.analysis.timeline import TimelineReconstructor
from src.config import Settings
from src.models import ImpactAnalysis, IncidentReport, LogEvent
from src.service_map import ServiceDependencyMap

logger = logging.getLogger(__name__)

__all__ = ["RCAAnalyzer"]


class RCAAnalyzer:
    """Orchestrates causal analysis and owns the bounded incident history."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        #: Newest incident appended last; bounded to ``settings.max_incident_history``.
        self.incident_history: list[IncidentReport] = []
        self.timeline = TimelineReconstructor(settings)
        #: Directed upstream -> downstream topology; gates causal-edge direction.
        self.service_map = ServiceDependencyMap.from_settings(settings)
        self.graph_builder = CausalGraphBuilder(settings, self.service_map)
        # TODO(C4): self.root_cause = RootCauseIdentifier(settings)
        # TODO(C5): self.impact = ImpactAnalyzer(settings)
        # TODO(C7): self.anomaly = AnomalyAmplifier(settings)
        # TODO(C7): self.hypotheses = MultiHypothesisTracker(settings)
        # TODO(C8): self.clock = ClockSkewCorrector(settings)
        # TODO(C8): self.incremental = IncrementalAnalyzer(settings)
        # TODO(C9): self.calibration = ConfidenceCalibrator(settings)
        # TODO(C9): self.reporter = PostMortemReporter(settings)

    def analyze(self, events: list[LogEvent]) -> IncidentReport:
        """Analyze a batch of events into an :class:`IncidentReport`.

        C3 scope: reconstruct the timeline (which back-fills each event's
        ``event_id``), then build the causal :class:`networkx.DiGraph` and attach its
        serialized form as ``report.causal_graph``. Root causes, impact and hypotheses
        stay empty until later commits. The report is appended to the bounded
        in-memory history and returned; the live ``graph`` object is available to the
        C4/C5 stages at the TODO seams below.
        """
        # TODO(C8): clock-skew correction (reorder near-simultaneous / inverted events)
        # before timeline reconstruction.
        timeline = self.timeline.reconstruct(events)

        # Incident start = earliest event's timestamp (timeline is chronological), or
        # now (ISO-8601, UTC) when there were no events to anchor to.
        if timeline:
            incident_timestamp = timeline[0].timestamp
        else:
            incident_timestamp = datetime.now(timezone.utc).isoformat()

        # C3: build the causal DiGraph from the (now id-back-filled) events. Kept as a
        # local ``graph`` so the C4 root-cause and C5 impact stages can consume it in
        # place; the report only carries the JSON-serialized form.
        graph = self.graph_builder.build(events)
        # TODO(C7): anomaly seeds = self.anomaly.score(events, self.incident_history)
        # TODO(C4): root_causes = self.root_cause.identify(graph, timeline)
        # TODO(C7): hypotheses = self.hypotheses.track(graph, anomaly_seeds)
        # TODO(C5): impact = self.impact.analyze(graph, root_causes)
        # TODO(C9): root_causes = self.calibration.apply(root_causes)
        report = IncidentReport(
            incident_id="inc-" + uuid4().hex[:12],
            timestamp=incident_timestamp,
            events=events,
            timeline=timeline,
            root_causes=[],
            impact_analysis=ImpactAnalysis(),
            hypotheses=[],
            causal_graph=self.graph_builder.to_serializable(graph),
        )

        self._remember(report)
        return report

    def _remember(self, report: IncidentReport) -> None:
        """Append ``report`` to history, trimming the oldest past the bound."""
        self.incident_history.append(report)
        limit = self.settings.max_incident_history
        if limit and len(self.incident_history) > limit:
            # Drop the oldest overflow so the newest ``limit`` reports remain.
            del self.incident_history[: len(self.incident_history) - limit]
