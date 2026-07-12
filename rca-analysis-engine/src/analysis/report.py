"""Post-mortem incident reporting for the RCA Analysis Engine (C9, feature area F).

The :class:`PostMortemReporter` turns a finished :class:`~src.models.IncidentReport` into
an **exportable post-mortem** — a human-readable markdown document plus two structured
artifacts that a reviewer (or the dashboard) can act on:

* **Recovery points** (:meth:`recovery_points`) — the interior nodes on the top root
  cause's propagation path where an intervention (a circuit-breaker, a failover, a manual
  hold) would have *truncated the largest downstream subtree*. For every node reachable
  from the top root cause we measure ``gated = len(networkx.descendants(node))`` — how many
  events sit downstream of it — drop the leaves (``gated == 0``: nothing left to prevent),
  and rank the rest descending. The #1 recovery point is the earliest, highest-leverage
  choke point: stopping the cascade there averts the most escalation. The root cause itself
  is reported separately (it is the *trigger*, not a recovery point).

* **Event classification** (:meth:`classify_events`) — every event is placed into exactly
  one :class:`~src.models.EventClass` from its position in the causal graph:

  * **PRIMARY_TRIGGER** — a causal *source*: ``in_degree == 0`` and ``out_degree > 0``
    (nothing caused it, but it caused others). The top root cause is guaranteed to land
    here.
  * **PROPAGATION_PATH** — an *interior* node: ``in_degree > 0`` **and** ``out_degree > 0``
    (it was both an effect and a cause — the cascade flowed through it).
  * **CONTRIBUTING_FACTOR** — everything else: a leaf/sink (``out_degree == 0``), an
    isolated node (both degrees ``0`` — e.g. a benign ``INFO`` that never joined an edge),
    or any event missing from the graph.

* **Markdown export** (:meth:`to_markdown`) — a well-formed post-mortem: title + incident
  id/time, a one-line summary, the ranked root causes with (raw and calibrated)
  confidence, the impact / blast-radius and affected services, the recovery points, an
  event-classification table, and the multi-hypothesis shortlist.

:meth:`build` is the single entry point the API uses: given a stored report (and,
optionally, its live in-memory graph) it returns ``{"markdown", "recovery_points",
"classifications"}``. When no graph is supplied it rebuilds a lightweight
:class:`networkx.DiGraph` from the report's serialized ``causal_graph`` so a post-mortem
can be produced from history alone.

Pure and deterministic: no network, no globals, no wall-clock reads — everything derives
from the report, the graph and the injected settings.
"""

from __future__ import annotations

import networkx as nx

from src.config import Settings
from src.models import EventClass, IncidentReport

__all__ = ["PostMortemReporter"]

#: How many recovery points to surface (highest-leverage choke points first).
_TOP_K_RECOVERY: int = 5


class PostMortemReporter:
    """Generate an exportable post-mortem: recovery points, classification, markdown."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    # --- Recovery points ---------------------------------------------------------

    def recovery_points(self, graph: nx.DiGraph, root_causes) -> list[dict]:
        """Rank interior propagation-path nodes by the downstream subtree they gate.

        Considers the nodes **reachable from** the top-ranked root cause (its propagation
        path). For each, ``gated_subtree_size = len(networkx.descendants(node))``; leaves
        (``gated == 0``) are excluded because there is nothing downstream left to prevent.
        The survivors are returned as dicts sorted by ``gated_subtree_size`` descending
        (ties broken by ``event_id`` for determinism), capped at the top ``_TOP_K_RECOVERY``.
        An empty graph or empty ``root_causes`` yields ``[]``.
        """
        if not root_causes or graph.number_of_nodes() == 0:
            return []
        top_root = root_causes[0].event_id
        if top_root not in graph:
            return []

        scored: list[tuple[int, str, str]] = []
        for node in nx.descendants(graph, top_root):
            gated = len(nx.descendants(graph, node))
            if gated <= 0:
                continue  # a leaf on the path — no downstream escalation left to truncate
            service = graph.nodes[node].get("service")
            scored.append((gated, node, service))

        # Largest gated subtree first; deterministic tie-break on the (stable) event id.
        scored.sort(key=lambda item: (-item[0], item[1]))
        return [
            {
                "event_id": node,
                "service": service,
                "gated_subtree_size": gated,
                "rationale": (
                    f"Intervening at {service} ({node}) truncates a downstream subtree of "
                    f"{gated} event(s) on the propagation path, preventing further escalation."
                ),
            }
            for gated, node, service in scored[:_TOP_K_RECOVERY]
        ]

    # --- Event classification ----------------------------------------------------

    def classify_events(self, events, graph: nx.DiGraph, root_causes) -> dict[str, str]:
        """Classify every event into exactly one :class:`~src.models.EventClass` value.

        The rule is purely structural (see the module docstring): sources are
        ``PRIMARY_TRIGGER``, interior nodes ``PROPAGATION_PATH``, and everything else
        ``CONTRIBUTING_FACTOR``. The top root cause is additionally *forced* to
        ``PRIMARY_TRIGGER`` when it has any outgoing influence, so the reported trigger and
        the ranked #1 cause never disagree. Returns ``{event_id: EventClass value}`` — one
        entry per event, using the same id resolution the rest of the pipeline uses (each
        event has already been id-back-filled by the timeline stage before this runs).
        """
        classifications: dict[str, str] = {}
        for event in events:
            event_id = event.event_id
            if event_id is None or event_id not in graph:
                # Defensive: an event with no resolvable node contributed no causal edge.
                if event_id is not None:
                    classifications[event_id] = EventClass.CONTRIBUTING_FACTOR.value
                continue
            in_degree = graph.in_degree(event_id)
            out_degree = graph.out_degree(event_id)
            if in_degree == 0 and out_degree > 0:
                classifications[event_id] = EventClass.PRIMARY_TRIGGER.value
            elif in_degree > 0 and out_degree > 0:
                classifications[event_id] = EventClass.PROPAGATION_PATH.value
            else:
                classifications[event_id] = EventClass.CONTRIBUTING_FACTOR.value

        # Reconcile the reported trigger with the ranked #1 cause: a top root cause that
        # actually propagated is the primary trigger even if a same-timestamp back-edge
        # gave it an in-degree. (No-op for the common in_degree==0 source.)
        if root_causes:
            top = root_causes[0].event_id
            if top in graph and graph.out_degree(top) > 0:
                classifications[top] = EventClass.PRIMARY_TRIGGER.value
        return classifications

    # --- Assembly ----------------------------------------------------------------

    def build(self, report: IncidentReport, graph: nx.DiGraph | None = None) -> dict:
        """Return ``{"markdown", "recovery_points", "classifications"}`` for a report.

        The single API-facing entry point. When ``graph`` is ``None`` (the usual case —
        the report came from history) a lightweight :class:`networkx.DiGraph` is rebuilt
        from ``report.causal_graph`` so the post-mortem works from a stored report alone.
        Recovery points and classifications are computed here and reflected in the markdown
        (the report is not mutated — a copy carries the freshly computed fields).
        """
        if graph is None:
            graph = self._graph_from_serialized(report.causal_graph)

        recovery = self.recovery_points(graph, report.root_causes)
        classifications = self.classify_events(report.events, graph, report.root_causes)
        # Reflect the computed artifacts in the rendered markdown without mutating the
        # stored history object.
        enriched = report.model_copy(
            update={"recovery_points": recovery, "event_classifications": classifications}
        )
        return {
            "markdown": self.to_markdown(enriched),
            "recovery_points": recovery,
            "classifications": classifications,
        }

    def _graph_from_serialized(self, serialized: dict) -> nx.DiGraph:
        """Rebuild a causal :class:`networkx.DiGraph` from ``report.causal_graph``.

        Mirrors :meth:`~src.analysis.causal_graph.CausalGraphBuilder.to_serializable`:
        nodes carry ``service`` / ``level`` / ``message`` / ``timestamp`` and each edge's
        ``strength`` is stored as both ``weight`` and ``strength`` (matching the live
        graph) so degree- and descendant-based analysis behaves identically.
        """
        graph = nx.DiGraph()
        for node in serialized.get("nodes", []) if serialized else []:
            graph.add_node(
                node.get("id"),
                service=node.get("service"),
                level=node.get("level"),
                message=node.get("message"),
                timestamp=node.get("timestamp"),
            )
        for edge in serialized.get("edges", []) if serialized else []:
            strength = edge.get("strength")
            graph.add_edge(
                edge.get("source"), edge.get("target"), weight=strength, strength=strength
            )
        return graph

    # --- Markdown rendering ------------------------------------------------------

    def to_markdown(self, report: IncidentReport) -> str:
        """Render a finished :class:`IncidentReport` as a readable post-mortem document.

        Non-empty and well-formed for any report: sections whose data is missing degrade to
        an explicit "none" line rather than being omitted, so the structure is stable.
        Reads ``recovery_points`` / ``event_classifications`` off the report (populated by
        ``analyze`` or by :meth:`build`).
        """
        impact = report.impact_analysis
        lines: list[str] = []

        # --- Header -------------------------------------------------------------
        lines.append(f"# Post-Mortem: Incident {report.incident_id}")
        lines.append("")
        lines.append(f"- **Incident ID:** {report.incident_id}")
        lines.append(f"- **Started:** {report.timestamp}")
        lines.append(f"- **Total events:** {impact.total_events or len(report.events)}")
        lines.append(f"- **Blast radius:** {impact.blast_radius} downstream event(s)")
        lines.append("")

        # --- Summary ------------------------------------------------------------
        lines.append("## Summary")
        lines.append("")
        lines.append(self._summary_sentence(report))
        lines.append("")

        # --- Root causes --------------------------------------------------------
        lines.append("## Root Causes")
        lines.append("")
        if report.root_causes:
            lines.append("| Rank | Service | Level | Confidence | Raw | Event | Message |")
            lines.append("|------|---------|-------|-----------|-----|-------|---------|")
            for rank, rc in enumerate(report.root_causes, start=1):
                raw = "-" if rc.raw_confidence is None else f"{rc.raw_confidence:.3f}"
                lines.append(
                    f"| {rank} | {rc.service} | {rc.level.value} | {rc.confidence:.3f} "
                    f"| {raw} | `{rc.event_id}` | {rc.message} |"
                )
        else:
            lines.append("_No root cause could be identified for this incident._")
        lines.append("")

        # --- Impact -------------------------------------------------------------
        lines.append("## Impact / Blast Radius")
        lines.append("")
        services = ", ".join(impact.affected_services) if impact.affected_services else "none"
        weighted = impact.details.get("weighted_impact") if impact.details else None
        lines.append(f"- **Blast radius:** {impact.blast_radius} downstream event(s)")
        lines.append(f"- **Affected services:** {services}")
        if weighted is not None:
            lines.append(f"- **Weighted impact:** {weighted:.3f}")
        lines.append("")

        # --- Recovery points ----------------------------------------------------
        lines.append("## Recovery Points")
        lines.append("")
        lines.append(
            "Interior choke points where an intervention would have truncated the largest "
            "downstream subtree, ranked by leverage:"
        )
        lines.append("")
        if report.recovery_points:
            lines.append("| Rank | Service | Event | Gated subtree | Rationale |")
            lines.append("|------|---------|-------|---------------|-----------|")
            for rank, point in enumerate(report.recovery_points, start=1):
                lines.append(
                    f"| {rank} | {point.get('service')} | `{point.get('event_id')}` "
                    f"| {point.get('gated_subtree_size')} | {point.get('rationale')} |"
                )
        else:
            lines.append(
                "_No interior recovery point was identified — the trigger propagated "
                "directly to leaf effects with no intermediate choke point._"
            )
        lines.append("")

        # --- Event classification ----------------------------------------------
        lines.append("## Event Classification")
        lines.append("")
        lines.extend(self._classification_section(report))
        lines.append("")

        # --- Alternative hypotheses --------------------------------------------
        lines.append("## Alternative Hypotheses")
        lines.append("")
        if report.hypotheses:
            lines.append("| Hypothesis | Root cause event | Confidence | State |")
            lines.append("|------------|------------------|-----------|-------|")
            for hyp in report.hypotheses:
                lines.append(
                    f"| `{hyp.hypothesis_id}` | `{hyp.root_cause_event_id}` "
                    f"| {hyp.confidence:.3f} | {hyp.state.value} |"
                )
        else:
            lines.append("_Only a single explanation was retained for this incident._")
        lines.append("")

        return "\n".join(lines)

    def _summary_sentence(self, report: IncidentReport) -> str:
        """A one-line natural-language summary of the incident's headline findings."""
        impact = report.impact_analysis
        if not report.root_causes:
            return (
                f"Incident {report.incident_id} produced no rankable root cause across "
                f"{impact.total_events or len(report.events)} event(s)."
            )
        top = report.root_causes[0]
        services = len(impact.affected_services)
        return (
            f"The most likely root cause is a {top.level.value} in `{top.service}` "
            f"(confidence {top.confidence:.0%}), which propagated to {impact.blast_radius} "
            f"downstream event(s) across {services} affected service(s)."
        )

    def _classification_section(self, report: IncidentReport) -> list[str]:
        """Render the event-classification summary (counts + a per-class listing)."""
        classifications = report.event_classifications
        if not classifications:
            return ["_No events were classified for this incident._"]

        # Group event ids by class, preserving a stable class order.
        buckets: dict[str, list[str]] = {cls.value: [] for cls in EventClass}
        for event_id, cls_value in classifications.items():
            buckets.setdefault(cls_value, []).append(event_id)

        lines = ["| Class | Count | Events |", "|-------|-------|--------|"]
        for cls in EventClass:
            ids = sorted(buckets.get(cls.value, []))
            shown = ", ".join(f"`{eid}`" for eid in ids) if ids else "—"
            lines.append(f"| {cls.value} | {len(ids)} | {shown} |")
        return lines
