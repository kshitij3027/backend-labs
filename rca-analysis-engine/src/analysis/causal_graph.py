"""Causal graph construction for the RCA Analysis Engine (C3).

The :class:`CausalGraphBuilder` is the causal-inference stage of
``RCAAnalyzer.analyze``: it turns the (already timestamp-parsed, id-back-filled)
batch of :class:`LogEvent` into a directed causal graph
(:class:`networkx.DiGraph`) whose nodes are events and whose edges encode "u
plausibly caused / preceded v". Downstream stages consume it — the root-cause
identifier (C4) ranks by out-degree centrality, and the impact analyzer (C5) walks
``nx.descendants`` for blast radius.

An edge ``u -> v`` is drawn only when it is **admissible** on all three axes:

* **temporal** — ``0 <= t(v) - t(u) <= TEMPORAL_WINDOW`` (v is after u, within window);
* **service dependency** — ``service(u)`` is upstream-of-or-equal ``service(v)``:
  either a declared **direct one-hop** dependency in the service map, or the same
  service (self-propagation);
* **severity** — **both** endpoints are at least ``WARNING`` (an ``INFO`` endpoint
  never participates in a causal edge).

Its strength is the spec's additive form, clamped to ``[MIN, MAX]``::

    strength = BASE
             + SERVICE_DEPENDENCY_BONUS   (if u -> v is a declared dependency)
             + ERROR_PROPAGATION_BONUS    (if both endpoints are ERROR)
             - TEMPORAL_GAP_PENALTY       (if t(v) - t(u) > TEMPORAL_GAP_THRESHOLD)

Edges are built with a **sorted temporal sweep** (two-pointer sliding window), not an
O(n^2) double loop: events are sorted once by time, and a left pointer tracks the
earliest event still inside ``[t(v) - TEMPORAL_WINDOW, t(v)]`` so each event only
considers the (usually small) active window of predecessors. The left pointer only
ever advances, so the window bookkeeping is amortized O(n); this keeps the build
near-linear in practice and lets the engine sustain 1000+ events/s.

The builder is **pure**: no network, no globals, no wall-clock reads; determinism
comes from the sorted input and the injected service map.
"""

from __future__ import annotations

from datetime import datetime

import networkx as nx

from src.analysis.timeline import _derive_event_id, _parse_timestamp
from src.config import Settings
from src.models import LogEvent, LogLevel
from src.service_map import ServiceDependencyMap

#: Levels that may participate in a causal edge; an INFO endpoint is never causal.
_CAUSAL_LEVELS: frozenset[LogLevel] = frozenset(
    {LogLevel.WARNING, LogLevel.ERROR, LogLevel.CRITICAL}
)


class CausalGraphBuilder:
    """Build a directed causal graph (``networkx.DiGraph``) from a batch of events."""

    def __init__(self, settings: Settings, service_map: ServiceDependencyMap) -> None:
        self.settings = settings
        self.service_map = service_map

    def build(self, events: list[LogEvent]) -> nx.DiGraph:
        """Construct the causal :class:`networkx.DiGraph` for ``events``.

        One node per event, keyed by ``event.event_id`` (the id the timeline stage
        back-fills; a deterministic fallback is derived if an event somehow lacks
        one). Nodes are inserted chronologically and carry ``service``, ``level``
        (the enum's string value), ``message``, the original ``timestamp`` string and
        an internal parsed ``dt``. Edges are added by a sorted two-pointer temporal
        sweep — see the module docstring for the admissibility rule and the strength
        formula. Pure and deterministic given the input order.
        """
        graph = nx.DiGraph()
        if not events:
            return graph

        # Parse once and resolve ids. Ids normally arrive back-filled by the timeline
        # stage; the ``or`` fallback keeps build() usable standalone (e.g. in unit
        # tests) using the exact same deterministic scheme the timeline uses, keyed on
        # the event's original input position.
        records: list[tuple[datetime, str, LogEvent]] = []
        for index, event in enumerate(events):
            dt = _parse_timestamp(event.timestamp)
            event_id = event.event_id or _derive_event_id(index, event)
            records.append((dt, event_id, event))

        # Sort chronologically (stable: equal timestamps keep input order) so the
        # sweep's active window is a contiguous index range and node insertion order
        # is chronological.
        records.sort(key=lambda record: record[0])

        for dt, event_id, event in records:
            graph.add_node(
                event_id,
                service=event.service,
                level=event.level.value,
                message=event.message,
                timestamp=event.timestamp,
                dt=dt,  # internal only; excluded from to_serializable()
            )

        self._add_edges(graph, records)
        return graph

    def _add_edges(
        self, graph: nx.DiGraph, records: list[tuple[datetime, str, LogEvent]]
    ) -> None:
        """Add admissible causal edges via a two-pointer temporal sweep.

        ``records`` must already be sorted ascending by datetime. ``left`` marks the
        earliest predecessor still within ``TEMPORAL_WINDOW`` of the current event and
        only ever advances, so the window bookkeeping is amortized O(n); each event
        then considers just the predecessors in its active window.
        """
        window = self.settings.temporal_window
        left = 0
        for j in range(len(records)):
            dt_v, id_v, event_v = records[j]
            # Slide the window's left edge forward past anything older than the window.
            # ``left`` is monotonic across all j -> total advancement is O(n).
            while (dt_v - records[left][0]).total_seconds() > window:
                left += 1
            # An INFO sink can have no admissible incoming edge -> skip its window.
            if event_v.level not in _CAUSAL_LEVELS:
                continue
            for i in range(left, j):
                dt_u, id_u, event_u = records[i]
                # INFO source can have no admissible outgoing edge.
                if event_u.level not in _CAUSAL_LEVELS:
                    continue
                # Direction: u upstream-of-or-equal v (declared one-hop dep or same
                # service); anything else is not an admissible causal direction.
                dep = self.service_map.is_dependency(event_u.service, event_v.service)
                if not dep and event_u.service != event_v.service:
                    continue
                delta = (dt_v - dt_u).total_seconds()
                strength = self._edge_strength(
                    dep=dep, level_u=event_u.level, level_v=event_v.level, delta=delta
                )
                # ``weight`` and ``strength`` carry the same value: ``weight`` feeds the
                # C5 impact Dijkstra (-log w best-path product), ``strength`` feeds the
                # serialized dashboard payload.
                graph.add_edge(id_u, id_v, weight=strength, strength=strength)

    def _edge_strength(
        self, *, dep: bool, level_u: LogLevel, level_v: LogLevel, delta: float
    ) -> float:
        """Compute the clamped causal strength for an admissible edge ``u -> v``."""
        s = self.settings
        strength = s.base_causal_strength
        if dep:
            strength += s.service_dependency_bonus
        if level_u == LogLevel.ERROR and level_v == LogLevel.ERROR:
            strength += s.error_propagation_bonus
        if delta > s.temporal_gap_threshold:
            strength -= s.temporal_gap_penalty
        # Clamp into the configured range.
        return max(s.causal_strength_min, min(s.causal_strength_max, strength))

    def to_serializable(self, graph: nx.DiGraph) -> dict:
        """Return a JSON-safe ``{"nodes": [...], "edges": [...]}`` view of ``graph``.

        Feeds the C12 dashboard Plotly network plot. The internal ``dt`` datetime node
        attribute is deliberately dropped (only the original ``timestamp`` string is
        emitted) so the payload is trivially JSON-serializable.
        """
        nodes = [
            {
                "id": node_id,
                "service": data.get("service"),
                "level": data.get("level"),
                "message": data.get("message"),
                "timestamp": data.get("timestamp"),
            }
            for node_id, data in graph.nodes(data=True)
        ]
        edges = [
            {"source": source, "target": target, "strength": data.get("strength")}
            for source, target, data in graph.edges(data=True)
        ]
        return {"nodes": nodes, "edges": edges}
