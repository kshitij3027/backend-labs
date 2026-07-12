"""The ``analysis`` strategy subpackage — home of :class:`RCAAnalyzer`.

:class:`RCAAnalyzer` is the orchestrator that turns a batch of :class:`LogEvent`
into an :class:`IncidentReport`. It owns the bounded in-memory incident history and
composes the per-stage collaborators (each a small module in this package).

C2 wired the first stage — the :class:`~src.analysis.timeline.TimelineReconstructor`.
C3 added the second — the :class:`~src.analysis.causal_graph.CausalGraphBuilder` —
populating the report's serialized ``causal_graph``. C4 added the third — the
:class:`~src.analysis.root_cause.RootCauseIdentifier` +
:class:`~src.analysis.root_cause.ConfidenceScorer` — populating the report's ranked
``root_causes``. C5 adds the fourth — the
:class:`~src.analysis.impact.ImpactAnalyzer` — and completes report assembly:
``analyze`` now also populates ``impact_analysis`` (blast radius, affected services,
weighted reachability) off that same graph and ranked causes. C7 adds two fidelity
stages — the :class:`~src.analysis.anomaly.AnomalyAmplifier` (base-rate anomaly scoring)
and the :class:`~src.analysis.hypotheses.MultiHypothesisTracker` (top-k concurrent
root-cause hypotheses via anomaly-seeded reversed-graph PageRank) — so ``analyze`` now
also fills ``report.anomaly_scores`` and ``report.hypotheses``. Each stage is folded into
``analyze`` at its marked seam:

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

from src.analysis.anomaly import AnomalyAmplifier
from src.analysis.calibration import ConfidenceCalibrator
from src.analysis.causal_graph import CausalGraphBuilder
from src.analysis.clock import ClockSkewCorrector
from src.analysis.hypotheses import MultiHypothesisTracker
from src.analysis.impact import ImpactAnalyzer
from src.analysis.report import PostMortemReporter
from src.analysis.root_cause import ConfidenceScorer, RootCauseIdentifier
from src.analysis.timeline import TimelineReconstructor
from src.config import Settings
from src.models import IncidentReport, LogEvent
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
        #: Selects candidate root causes off the causal graph (sources / severe events).
        self.root_cause_identifier = RootCauseIdentifier(settings)
        #: Scores + descending-ranks those candidates into RootCause objects.
        self.confidence_scorer = ConfidenceScorer(settings)
        #: Blast radius / affected services / weighted reachability off the graph.
        self.impact_analyzer = ImpactAnalyzer(settings)
        #: Base-rate anomaly scoring; learns a running per-type baseline via observe().
        self.anomaly_amplifier = AnomalyAmplifier(settings)
        #: Top-k concurrent root-cause hypotheses via anomaly-seeded reversed-graph PPR.
        self.hypothesis_tracker = MultiHypothesisTracker(settings)
        #: Clock-skew correction (C8): re-orders near-simultaneous / skew-inverted events
        #: into a causally consistent order BEFORE the timeline stage; shares the service
        #: map so its dependency happens-before edges match the causal graph's direction.
        self.clock_corrector = ClockSkewCorrector(settings, self.service_map)
        #: Confidence calibration (C9): learns raw-confidence -> empirical-root-cause
        #: probability from resolved incidents (via record_outcome). Identity until fitted,
        #: so analyze() is unaffected until enough outcomes have been fed back.
        self.calibrator = ConfidenceCalibrator(settings)
        #: Post-mortem generator (C9): recovery points, event classification, markdown.
        self.reporter = PostMortemReporter(settings)

    def analyze(self, events: list[LogEvent]) -> IncidentReport:
        """Analyze a batch of events into an :class:`IncidentReport`.

        Apply clock-skew correction FIRST (C8) so the whole pipeline keys off one
        causally-consistent ordering, then reconstruct the timeline (which back-fills each
        event's ``event_id``), build the causal :class:`networkx.DiGraph`, score per-event
        base-rate ``anomaly_scores``,
        rank candidate ``root_causes`` (C9 then stashes each raw score on
        ``raw_confidence`` and overwrites ``confidence`` with the calibrated probability —
        identity until the calibrator learns, so the ranking is unchanged), track the top-k
        concurrent ``hypotheses`` via anomaly-seeded reversed-graph PageRank, compute the
        blast-radius / weighted-reachability ``impact_analysis`` off that same graph, and
        derive the C9 post-mortem ``recovery_points`` + per-event ``event_classifications``;
        the serialized graph is attached as ``report.causal_graph``. The fully-assembled
        report is appended to the bounded in-memory history and returned.
        """
        # C8: clock-skew correction runs FIRST — before timeline reconstruction — so every
        # downstream stage keys off a single causally-consistent ordering. It returns a new
        # list of new events (inputs untouched); sub-ε timestamp noise is neutralized and a
        # dependency happens-before restores an upstream cause ahead of a skew-early effect.
        events = self.clock_corrector.correct(events)
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
        # C7: base-rate anomaly amplification. Score BEFORE observing so this incident is
        # graded against PRIOR history only (it must never trivially explain itself); then
        # fold it into the running baseline so future incidents learn from it. This
        # score-then-observe ordering is load-bearing — do not reorder.
        anomaly_scores = self.anomaly_amplifier.score(events)
        self.anomaly_amplifier.observe(events)
        # C4: rank candidate root causes (severity + temporal position + normalized
        # out-degree centrality) off the causal graph; empty when there were no events.
        root_causes = self.confidence_scorer.rank(events, graph)
        # C9: apply confidence calibration as a *display* value while preserving the raw
        # ranking. ``root_causes`` is already sorted descending by the scorer's raw
        # confidence; we stash that on ``raw_confidence`` and overwrite ``confidence`` with
        # the calibrated probability IN PLACE (no re-sort). The calibrator is the identity
        # until enough outcomes are fed back — and isotonic/Platt are monotonic — so the
        # #1 ground-truth cause never changes rank; only the number shown is recalibrated.
        for rc in root_causes:
            rc.raw_confidence = rc.confidence
            rc.confidence = self.calibrator.transform(rc.raw_confidence)
        # C7: top-k concurrent root-cause hypotheses with independent confidences, ranked
        # by personalized PageRank / RWR on the reversed causal graph seeded by the
        # anomaly scores (mass flows from symptoms back toward the causal sources).
        hypotheses = self.hypothesis_tracker.rank(events, graph, anomaly_scores)
        # C5: blast-radius / affected-services / weighted-reachability impact, computed
        # off the same graph + ranked causes (never recomputed differently downstream).
        impact = self.impact_analyzer.analyze(events, graph, root_causes)
        # C9: post-mortem artifacts off the same graph + ranked causes — recovery points
        # (interior choke points that gate the largest downstream subtree) and a
        # one-class-per-event causal-role classification.
        recovery_points = self.reporter.recovery_points(graph, root_causes)
        event_classifications = self.reporter.classify_events(events, graph, root_causes)
        report = IncidentReport(
            incident_id="inc-" + uuid4().hex[:12],
            timestamp=incident_timestamp,
            events=events,
            timeline=timeline,
            root_causes=root_causes,
            impact_analysis=impact,
            hypotheses=hypotheses,
            anomaly_scores=anomaly_scores,
            causal_graph=self.graph_builder.to_serializable(graph),
            recovery_points=recovery_points,
            event_classifications=event_classifications,
        )

        self._remember(report)
        return report

    def record_outcome(self, incident_id: str, true_root_cause_event_id: str) -> dict:
        """Learn from a resolved incident and return the updated calibration stats (C9).

        Looks up ``incident_id`` in the bounded history, feeds each of that report's ranked
        candidates to the calibrator as a ``(raw_confidence, was_root_cause)`` sample (the
        candidate matching ``true_root_cause_event_id`` is the positive), refits the
        calibrator (a no-op until it has ``calibration_min_samples`` samples across both
        classes), and returns :meth:`ConfidenceCalibrator.stats`. Raises :class:`KeyError`
        when no such incident is retained — the API layer maps that to a 404.
        """
        report = self._find(incident_id)
        if report is None:
            raise KeyError(incident_id)
        self.calibrator.record_outcome(report.root_causes, true_root_cause_event_id)
        self.calibrator.fit()
        return self.calibrator.stats()

    def _find(self, incident_id: str) -> IncidentReport | None:
        """Return the retained report with ``incident_id``, or ``None`` if trimmed/unknown."""
        for report in self.incident_history:
            if report.incident_id == incident_id:
                return report
        return None

    def remember(self, report: IncidentReport) -> None:
        """Append an externally-produced report to the bounded history.

        Public entry point for the C8 live-stream loop, whose incremental snapshots are
        assembled outside :meth:`analyze` but must still land in the same bounded, newest-
        last history the REST surface serves. Delegates to :meth:`_remember` so the trim
        policy lives in exactly one place.
        """
        self._remember(report)

    def _remember(self, report: IncidentReport) -> None:
        """Append ``report`` to history, trimming the oldest past the bound."""
        self.incident_history.append(report)
        limit = self.settings.max_incident_history
        if limit and len(self.incident_history) > limit:
            # Drop the oldest overflow so the newest ``limit`` reports remain.
            del self.incident_history[: len(self.incident_history) - limit]
