"""Impact / blast-radius analysis for the RCA Analysis Engine (C5).

The :class:`ImpactAnalyzer` is the impact stage of ``RCAAnalyzer.analyze``: given the
causal :class:`networkx.DiGraph` built in C3 and the descending-ranked
:class:`~src.models.RootCause` list from C4, it quantifies *how far* an incident's top
root cause propagated. It produces an :class:`~src.models.ImpactAnalysis`:

* **blast radius** — ``len(networkx.descendants(graph, root))``: the number of events
  causally reachable from the top root cause (the size of its downstream cone);
* **affected services** — the sorted, distinct ``service`` attributes of that
  downstream cone, plus the root's own originating service (it is impacted too);
* **weighted reachability** — a single ``weighted_impact`` scalar that weights each
  reachable event by *both* its severity *and* how strongly it is reachable:

      weighted_impact = sum over descendants d of
                        severity_weight(level_d) * best_path_product(root -> d)

  where ``best_path_product`` is the product of edge ``weight``s along the **strongest**
  (highest-product) causal path from the root to ``d``. Products are found without
  enumerating paths: minimizing the additive transform ``-log(weight)`` maximizes the
  multiplicative product, so a single :func:`networkx.single_source_dijkstra_path_length`
  over ``-log(weight)`` yields, for every reachable node, ``dist = -log(product)`` and
  hence ``product = exp(-dist)``. Edge weights are guarded into ``(0, 1]`` (the builder
  already clamps them to ``[0.1, 1.0]``) so every transformed weight is non-negative and
  Dijkstra is well-defined. The sweep is O(reachable) per root — cheap at incident sizes.

The same three quantities are also computed for each of the **top-K** (K=3) ranked root
causes and surfaced in ``details.per_root_cause`` so the dashboard can compare rival
explanations. An empty graph or an empty root-cause list yields a zeroed
:class:`~src.models.ImpactAnalysis` (carrying only ``total_events``) rather than raising.

The analyzer is **pure**: no network, no globals, no wall-clock reads — determinism
comes from the graph, the ranked causes and the injected settings. It reads only the
severity weights already on :class:`~src.config.Settings` (the same ``score_*`` knobs the
confidence scorer uses), so impact stays config-tunable alongside the rest of the engine.
"""

from __future__ import annotations

import math

import networkx as nx

from src.config import Settings
from src.models import ImpactAnalysis, LogEvent, LogLevel, RootCause

__all__ = ["ImpactAnalyzer"]

#: How many top-ranked root causes get an entry in ``details.per_root_cause``.
_TOP_K: int = 3

#: Floor applied to an edge weight before ``-log`` so a pathological ``weight <= 0``
#: cannot raise a math-domain error; it becomes a tiny positive (huge but finite
#: ``-log``, i.e. an effectively zero best-path product). Real weights are >= 0.1.
_MIN_WEIGHT: float = 1e-12


def _zero_details() -> dict:
    """The ``details`` payload for a zero-impact report (stable key set for the UI)."""
    return {
        "primary_root_cause_event_id": None,
        "weighted_impact": 0.0,
        "reachable_event_ids": [],
        "per_root_cause": [],
        "affected_service_count": 0,
    }


class ImpactAnalyzer:
    """Compute blast radius, affected services and weighted reachability for an incident."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def analyze(
        self,
        events: list[LogEvent],
        graph: nx.DiGraph,
        root_causes: list[RootCause],
    ) -> ImpactAnalysis:
        """Return the :class:`~src.models.ImpactAnalysis` for this incident.

        ``events`` is used only for ``total_events`` (the incident's event count). The
        blast radius / affected services / weighted impact are all derived from the top
        root cause's downstream cone in ``graph``; the top-K causes are additionally
        summarized in ``details.per_root_cause``. With no root causes or an empty graph,
        a zeroed report (carrying only ``total_events``) is returned — never a crash.
        """
        total_events = len(events)
        if not root_causes or graph.number_of_nodes() == 0:
            return ImpactAnalysis(total_events=total_events, details=_zero_details())

        # Summarize the top-K ranked causes; remember the primary's descendant cone so
        # the report's reachable_event_ids reflects the #1 root cause specifically.
        per_root_cause: list[dict] = []
        primary_descendants: set[str] = set()
        for rank, root_cause in enumerate(root_causes[:_TOP_K]):
            entry, descendants = self._analyze_one(root_cause.event_id, graph)
            per_root_cause.append(entry)
            if rank == 0:
                primary_descendants = descendants

        primary = per_root_cause[0]
        details = {
            "primary_root_cause_event_id": primary["event_id"],
            "weighted_impact": primary["weighted_impact"],
            "reachable_event_ids": sorted(primary_descendants),
            "per_root_cause": per_root_cause,
            "affected_service_count": len(primary["affected_services"]),
        }
        return ImpactAnalysis(
            blast_radius=primary["blast_radius"],
            affected_services=primary["affected_services"],
            total_events=total_events,
            details=details,
        )

    def _analyze_one(
        self, root_id: str, graph: nx.DiGraph
    ) -> tuple[dict, set[str]]:
        """Impact summary for a single root ``root_id`` plus its descendant set.

        Returns ``({event_id, blast_radius, affected_services, weighted_impact}, desc)``.
        A root that is not a node in ``graph`` (defensive — ranked causes are always
        graph nodes) yields a zeroed entry and an empty descendant set.
        """
        if root_id not in graph:
            return (
                {
                    "event_id": root_id,
                    "blast_radius": 0,
                    "affected_services": [],
                    "weighted_impact": 0.0,
                },
                set(),
            )

        descendants = nx.descendants(graph, root_id)
        entry = {
            "event_id": root_id,
            "blast_radius": len(descendants),
            "affected_services": self._affected_services(graph, root_id, descendants),
            "weighted_impact": self._weighted_impact(graph, root_id, descendants),
        }
        return entry, descendants

    def _affected_services(
        self, graph: nx.DiGraph, root_id: str, descendants: set[str]
    ) -> list[str]:
        """Sorted, distinct services across the descendant cone plus the root's own."""
        services = {graph.nodes[root_id].get("service")}
        for node_id in descendants:
            services.add(graph.nodes[node_id].get("service"))
        services.discard(None)  # drop any node missing the attribute defensively
        return sorted(services)

    def _weighted_impact(
        self, graph: nx.DiGraph, root_id: str, descendants: set[str]
    ) -> float:
        """Severity-weighted, strongest-path-product-weighted reach from ``root_id``.

        One :func:`networkx.single_source_dijkstra_path_length` over the ``-log(weight)``
        transform gives ``dist = -log(strongest-path product)`` for every reachable node;
        ``exp(-dist)`` recovers that product. Each descendant contributes
        ``severity_weight(level) * product``. Returns ``0.0`` for an isolated root.
        """
        if not descendants:
            return 0.0

        distances = nx.single_source_dijkstra_path_length(
            graph, root_id, weight=self._neg_log_weight
        )
        total = 0.0
        for node_id in descendants:
            distance = distances.get(node_id)
            if distance is None:
                continue  # unreachable (a descendant always is reachable; guard anyway)
            product = math.exp(-distance)
            total += self._severity_weight(graph.nodes[node_id].get("level")) * product
        return total

    def _neg_log_weight(self, _u: str, _v: str, data: dict) -> float:
        """Dijkstra edge cost ``-log(weight)``; summing it minimizes to the max product.

        The edge ``weight`` (falling back to ``strength``, else ``1.0``) is guarded into
        ``(0, 1]`` first, so the returned cost is always a finite non-negative number and
        Dijkstra stays valid.
        """
        weight = data.get("weight")
        if weight is None:
            weight = data.get("strength", 1.0)
        weight = min(1.0, max(_MIN_WEIGHT, float(weight)))
        return -math.log(weight)

    def _severity_weight(self, level: str | None) -> float:
        """Map a node's level (the enum's string value) to its severity weight.

        Reuses the confidence scorer's ``score_*`` knobs so impact and confidence share
        one severity scale; an ``INFO`` (or unknown) level contributes zero weight.
        """
        s = self.settings
        return {
            LogLevel.CRITICAL.value: s.score_critical,
            LogLevel.ERROR.value: s.score_error,
            LogLevel.WARNING.value: s.score_warning,
            LogLevel.INFO.value: 0.0,
        }.get(level, 0.0)
