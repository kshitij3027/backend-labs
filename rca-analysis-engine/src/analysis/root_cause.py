"""Root-cause identification and confidence scoring for the RCA Analysis Engine (C4).

This module is the ranking stage of ``RCAAnalyzer.analyze``. It consumes the causal
:class:`networkx.DiGraph` built in C3 and turns it into a descending-ranked list of
:class:`~src.models.RootCause` candidates, each carrying a calibrated confidence in
``[0, 1]``.

Two small, pure collaborators:

* :class:`RootCauseIdentifier` picks the **candidate** events worth scoring. An event
  is a candidate iff it is a *source of causal influence* (out-degree ``> 0`` in the
  causal graph) **or** it is intrinsically severe (level ``ERROR`` / ``CRITICAL``).
  The two sets are unioned and de-duplicated, so a severe leaf with no outgoing edges
  is still considered, and a benign ``WARNING`` that nonetheless propagates is too.

* :class:`ConfidenceScorer` scores each candidate with the spec's hand-crafted,
  fully config-tunable formula (all weights live on :class:`~src.config.Settings`)::

      confidence = clamp(
          severity_score
          + TEMPORAL_SCORE_WEIGHT   * temporal_pos
          + CENTRALITY_SCORE_WEIGHT * centrality,
          0.0, 1.0,
      )

  where

  * ``severity_score`` maps the node's level to ``{CRITICAL: score_critical,
    ERROR: score_error, WARNING: score_warning, INFO: 0.0}``;
  * ``temporal_pos = 1 - (t_e - t_start) / (t_end - t_start)`` rewards *earlier*
    events (a cause precedes its effects); it is defined as ``1.0`` for a single-event
    / zero-span incident and is clamped to ``[0, 1]``;
  * ``centrality = out_degree(e) / max_out_degree`` is the node's **unweighted**
    out-degree centrality, normalized against the busiest source (``0`` when the graph
    has no edges).

Candidates are ranked by confidence **descending**, with deterministic tie-breaks —
earlier timestamp first, then event id — so the output ordering is stable for tests
and the dashboard. The ground-truth property the C4 requirement cares about: for a
generated cascading incident, the injected root (the earliest event, the sole
``CRITICAL``, and the highest-out-degree source) scores the maximal ``1.0`` (severity
``0.6`` + temporal ``0.3`` + centrality ``0.2`` = ``1.1``, clamped) and lands at rank
#1.

The collaborators are **pure**: no network, no globals, no wall-clock reads —
determinism comes from the graph and the injected settings.
"""

from __future__ import annotations

from datetime import datetime

import networkx as nx

from src.analysis.timeline import _derive_event_id
from src.config import Settings
from src.models import LogEvent, LogLevel, RootCause

__all__ = ["RootCauseIdentifier", "ConfidenceScorer"]

#: Levels that make an event a candidate on severity alone (regardless of out-degree).
_HIGH_SEVERITY_LEVELS: frozenset[LogLevel] = frozenset(
    {LogLevel.ERROR, LogLevel.CRITICAL}
)


def _clamp(value: float, low: float, high: float) -> float:
    """Clamp ``value`` into the inclusive ``[low, high]`` range."""
    return max(low, min(high, value))


class RootCauseIdentifier:
    """Select the candidate events worth scoring as potential root causes."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def identify(self, events: list[LogEvent], graph: nx.DiGraph) -> list[str]:
        """Return the de-duplicated ``event_id``s of candidate root causes.

        The candidate set is the **union** of two rules:

        * **causal sources** — any node with out-degree ``> 0`` (it plausibly caused
          something downstream), read directly off the graph, and
        * **severe events** — any event whose level is ``ERROR`` or ``CRITICAL``,
          whether or not it has outgoing edges (a severe leaf is still a suspect).

        Ordering here is not significant (:meth:`ConfidenceScorer.rank` re-sorts) but is
        kept deterministic: graph sources in chronological node order first, then any
        remaining severe events in input order. Each event's id is resolved exactly as
        the causal-graph builder resolved it (client-supplied id, else the timeline
        fallback) so the returned ids are always keys of ``graph``.
        """
        candidates: list[str] = []
        seen: set[str] = set()

        # Rule 1 — sources of causal influence (out-degree > 0). Node iteration order is
        # chronological because the builder inserts nodes sorted by time.
        for node_id, out_degree in graph.out_degree():
            if out_degree > 0 and node_id not in seen:
                seen.add(node_id)
                candidates.append(node_id)

        # Rule 2 — intrinsically severe events, even with no outgoing edge. Resolve each
        # id the same way the builder did and only keep ids that are actually nodes.
        for index, event in enumerate(events):
            if event.level not in _HIGH_SEVERITY_LEVELS:
                continue
            event_id = event.event_id or _derive_event_id(index, event)
            if event_id in graph and event_id not in seen:
                seen.add(event_id)
                candidates.append(event_id)

        return candidates


class ConfidenceScorer:
    """Score and descending-rank candidate root causes with the spec's formula."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        #: Owned so :meth:`rank` is self-contained given only ``(events, graph)``.
        self.identifier = RootCauseIdentifier(settings)

    def _severity_score(self, level: str) -> float:
        """Map a node's level (the enum's string value) to its severity component."""
        s = self.settings
        return {
            LogLevel.CRITICAL.value: s.score_critical,
            LogLevel.ERROR.value: s.score_error,
            LogLevel.WARNING.value: s.score_warning,
            LogLevel.INFO.value: 0.0,
        }.get(level, 0.0)

    def score(
        self,
        event_id: str,
        graph: nx.DiGraph,
        incident_start_dt: datetime,
        incident_end_dt: datetime,
        max_out_degree: int,
    ) -> float:
        """Confidence in ``[0, 1]`` that node ``event_id`` is the root cause.

        Sums three config-weighted terms — severity, temporal position (earlier is more
        causal), and normalized out-degree centrality — then clamps to ``[0, 1]``.
        ``incident_start_dt`` / ``incident_end_dt`` are the min / max node datetimes and
        ``max_out_degree`` the busiest source's out-degree; :meth:`rank` computes all
        three once and passes them in so scoring a whole batch stays cheap.
        """
        data = graph.nodes[event_id]
        severity_score = self._severity_score(data["level"])

        # Temporal position: 1 at the incident start, 0 at the end (earlier => higher).
        # A zero-span incident (single event, or all-identical timestamps) is defined as
        # 1.0 — it avoids a divide-by-zero and treats the lone event as maximally causal.
        span = (incident_end_dt - incident_start_dt).total_seconds()
        if span <= 0.0:
            temporal_pos = 1.0
        else:
            elapsed = (data["dt"] - incident_start_dt).total_seconds()
            temporal_pos = _clamp(1.0 - elapsed / span, 0.0, 1.0)

        # Unweighted out-degree centrality, normalized against the busiest source.
        if max_out_degree > 0:
            centrality = graph.out_degree(event_id) / max_out_degree
        else:
            centrality = 0.0

        confidence = (
            severity_score
            + self.settings.temporal_score_weight * temporal_pos
            + self.settings.centrality_score_weight * centrality
        )
        return _clamp(confidence, 0.0, 1.0)

    def rank(self, events: list[LogEvent], graph: nx.DiGraph) -> list[RootCause]:
        """Return candidate root causes as :class:`RootCause`, ranked by confidence.

        Computes the incident window (min / max node datetime) and the max out-degree
        once, scores every candidate from :class:`RootCauseIdentifier`, and returns the
        resulting :class:`RootCause` objects sorted by confidence **descending**. Ties
        break deterministically by earlier timestamp, then event id, so the ordering is
        stable across runs. An empty graph yields an empty list.
        """
        if graph.number_of_nodes() == 0:
            return []

        dts = [data["dt"] for _node_id, data in graph.nodes(data=True)]
        incident_start_dt = min(dts)
        incident_end_dt = max(dts)
        max_out_degree = max((deg for _node_id, deg in graph.out_degree()), default=0)

        candidate_ids = self.identifier.identify(events, graph)

        # (confidence, dt, event_id, RootCause) so the sort key needs no re-lookups.
        scored: list[tuple[float, datetime, str, RootCause]] = []
        for event_id in candidate_ids:
            data = graph.nodes[event_id]
            confidence = self.score(
                event_id, graph, incident_start_dt, incident_end_dt, max_out_degree
            )
            root_cause = RootCause(
                event_id=event_id,
                service=data["service"],
                level=LogLevel(data["level"]),
                message=data["message"],
                confidence=confidence,
                timestamp=data["timestamp"],
            )
            scored.append((confidence, data["dt"], event_id, root_cause))

        # Descending confidence; tie-break by earlier datetime then event id (both
        # ascending) for a total, deterministic order.
        scored.sort(key=lambda item: (-item[0], item[1], item[2]))
        return [item[3] for item in scored]
