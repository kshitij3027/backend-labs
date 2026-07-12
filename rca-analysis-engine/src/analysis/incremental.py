"""Incremental streaming analysis for the RCA Analysis Engine (C8, feature area B).

Where :class:`~src.analysis.RCAAnalyzer` analyzes a *batch* posted to
``/api/analyze-incident``, the :class:`IncrementalAnalyzer` maintains a **rolling
window** of the most recent events and re-ranks *as events stream in* — the engine that
backs the background live-stream loop. It reuses the batch stages verbatim (no duplicated
admissibility / strength / PageRank math): every re-rank rebuilds the *small, bounded*
windowed causal graph with :class:`~src.analysis.causal_graph.CausalGraphBuilder` and
ranks it with :class:`~src.analysis.hypotheses.MultiHypothesisTracker`.

**Bounded window.** :meth:`add_event` / :meth:`add_events` append to the window and then
**evict** anything that has aged out — older than ``temporal_window`` seconds behind the
newest event, or beyond the newest ``incremental_max_events`` — so the graph stays small
and analysis stays cheap no matter how long the stream runs. Each event is assigned a
stable id on arrival (client-supplied, else a deterministic derived id keyed on a
monotonic sequence counter) so ids never churn as the window slides.

**Warm-started re-rank.** :meth:`rerank` recomputes the ranking on the current window but
**warm-starts** the personalized-PageRank power iteration from the *previous* re-rank's
``pi`` instead of the uniform/restart cold start. When the graph changed only slightly
between ticks the walk begins near its new stationary point and converges in far fewer
iterations — the classic dynamic-PageRank optimization — while landing on the identical
fixed point, so the ranking itself is unchanged. The last run's step count is exposed as
:attr:`last_iterations` so a test can assert *warm < cold*.

:meth:`snapshot` assembles a full :class:`~src.models.IncidentReport` of the current live
state (timeline, causal graph, ranked causes, impact, hypotheses, anomaly seeds) for the
loop to broadcast. Pure and deterministic given its accumulated window.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from src.analysis.anomaly import AnomalyAmplifier
from src.analysis.causal_graph import CausalGraphBuilder
from src.analysis.hypotheses import MultiHypothesisTracker
from src.analysis.impact import ImpactAnalyzer
from src.analysis.root_cause import ConfidenceScorer
from src.analysis.timeline import TimelineReconstructor, _derive_event_id, _parse_timestamp
from src.config import Settings
from src.models import Hypothesis, IncidentReport, LogEvent
from src.service_map import ServiceDependencyMap

__all__ = ["IncrementalAnalyzer"]


class IncrementalAnalyzer:
    """Maintain a rolling window + incremental causal graph with warm-started re-rank."""

    def __init__(
        self, settings: Settings, service_map: ServiceDependencyMap | None = None
    ) -> None:
        self.settings = settings
        self.service_map = service_map or ServiceDependencyMap.from_settings(settings)
        self.graph_builder = CausalGraphBuilder(settings, self.service_map)
        self.hypothesis_tracker = MultiHypothesisTracker(settings)
        #: Seeds the PageRank restart vector; scored in empty-history fallback mode (no
        #: observe()) so it is a pure, deterministic function of the current window.
        self.anomaly_amplifier = AnomalyAmplifier(settings)
        self.confidence_scorer = ConfidenceScorer(settings)
        self.impact_analyzer = ImpactAnalyzer(settings)
        self.timeline = TimelineReconstructor(settings)

        #: The rolling window as ``(parsed_dt, event)`` records, kept sorted ascending by
        #: time so eviction and graph construction see a chronological window.
        self._records: list[tuple[datetime, LogEvent]] = []
        #: Monotonic counter for deriving stable ids for events that arrive without one.
        self._seq: int = 0
        #: Previous re-rank's pi (``node -> mass``); the warm-start seed for the next one.
        self._prev_pi: dict[str, float] = {}
        #: Power-iteration steps taken by the most recent :meth:`rerank` (for assertions).
        self.last_iterations: int = 0

    # --- Window maintenance ------------------------------------------------------

    def add_event(self, event: LogEvent) -> None:
        """Append one event to the window and evict anything that has aged out."""
        self.add_events([event])

    def add_events(self, events: list[LogEvent]) -> None:
        """Append a batch to the window (stable-id each), then re-sort and evict.

        Inputs are never mutated — an event lacking an ``event_id`` is copied with a
        derived stable id rather than back-filled in place. An unparseable timestamp raises
        :class:`ValueError` (consistent with the rest of the pipeline); the live-stream loop
        guards each tick so one bad event never kills the stream.
        """
        if not events:
            return
        for event in events:
            dt = _parse_timestamp(event.timestamp)
            if event.event_id is None:
                event = event.model_copy(
                    update={"event_id": _derive_event_id(self._seq, event)}
                )
            self._seq += 1
            self._records.append((dt, event))
        self._records.sort(key=lambda record: record[0])
        self._evict()

    def _evict(self) -> None:
        """Drop events older than ``temporal_window`` behind the newest, then cap the count."""
        if not self._records:
            return
        newest = self._records[-1][0]  # sorted ascending -> last is newest
        window = self.settings.temporal_window
        if window:
            cutoff = newest - timedelta(seconds=window)
            self._records = [rec for rec in self._records if rec[0] >= cutoff]
        max_events = self.settings.incremental_max_events
        if max_events and len(self._records) > max_events:
            # Keep the newest ``max_events`` (records are sorted ascending by time).
            self._records = self._records[-max_events:]

    def window_size(self) -> int:
        """Number of events currently retained in the rolling window."""
        return len(self._records)

    def _window_events(self) -> list[LogEvent]:
        """The current window's events in chronological order."""
        return [event for _dt, event in self._records]

    # --- Ranking -----------------------------------------------------------------

    def rerank(self, warm: bool = True) -> list[Hypothesis]:
        """Re-rank the current window's hypotheses, warm-starting the PageRank walk.

        Rebuilds the windowed causal graph, scores anomaly seeds, and runs the reversed-graph
        personalized PageRank — warm-started from the previous re-rank's ``pi`` when
        ``warm`` is set and a prior ``pi`` exists (the first re-rank is always a cold start).
        Records :attr:`last_iterations` and caches the new ``pi`` for the next warm start,
        then returns the surviving hypotheses. An empty window yields ``[]``.
        """
        _events, _graph, _anomaly, hypotheses = self._compute(warm)
        return hypotheses

    def _compute(
        self, warm: bool
    ) -> tuple[list[LogEvent], object, dict[str, float], list[Hypothesis]]:
        """Shared core of :meth:`rerank` and :meth:`snapshot`.

        Builds the windowed graph once, runs the (optionally warm-started) walk, updates the
        warm-start cache + iteration count, and returns everything both callers need.
        """
        events = self._window_events()
        graph = self.graph_builder.build(events)
        anomaly = self.anomaly_amplifier.score(events)
        initial = self._prev_pi if (warm and self._prev_pi) else None
        nodes, pi, iterations = self.hypothesis_tracker.random_walk_with_restart(
            graph, anomaly, initial=initial
        )
        self.last_iterations = iterations
        self._prev_pi = {node: float(pi[i]) for i, node in enumerate(nodes)}
        hypotheses = self.hypothesis_tracker.hypotheses_from_pi(nodes, pi, anomaly)
        return events, graph, anomaly, hypotheses

    def snapshot(self, warm: bool = True) -> IncidentReport:
        """Assemble a full :class:`IncidentReport` of the current live window state.

        Reuses the batch stages (timeline, causal graph, confidence scorer, impact analyzer,
        anomaly amplifier, warm-started multi-hypothesis tracker) so a live snapshot is
        shape-identical to a POSTed report. An empty window yields a valid, empty report
        (timestamped now). The report id is prefixed ``live-`` to distinguish live snapshots
        from POSTed ``inc-`` incidents in the shared history.
        """
        events, graph, anomaly, hypotheses = self._compute(warm)
        timeline = self.timeline.reconstruct(events)
        root_causes = self.confidence_scorer.rank(events, graph)
        impact = self.impact_analyzer.analyze(events, graph, root_causes)
        incident_timestamp = (
            timeline[0].timestamp if timeline else datetime.now(timezone.utc).isoformat()
        )
        return IncidentReport(
            incident_id="live-" + uuid4().hex[:12],
            timestamp=incident_timestamp,
            events=events,
            timeline=timeline,
            root_causes=root_causes,
            impact_analysis=impact,
            hypotheses=hypotheses,
            anomaly_scores=anomaly,
            causal_graph=self.graph_builder.to_serializable(graph),
        )
