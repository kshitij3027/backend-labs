"""Clock-skew correction for the RCA Analysis Engine (C8, feature area E).

In a distributed system the per-host clocks are never perfectly aligned, so a naive
"sort by timestamp" can invert causality: a downstream effect stamped by a slightly
fast clock can appear *before* the upstream cause that produced it. The
:class:`ClockSkewCorrector` is the **first** stage of ``RCAAnalyzer.analyze`` — it runs
*before* the timeline reconstructor — and re-derives a *causally consistent* ordering
that every downstream stage (timeline, causal graph, ranking, impact) then keys off.

It combines three ideas from the distributed-clocks literature (Lamport / hybrid
logical clocks), all bounded so a correction never moves an event by more than ``ε``:

* **ε-tolerance band.** Two events whose ``|Δt| < clock_skew_epsilon`` are "concurrent":
  their sub-ε timestamp difference is treated as clock noise, not signal, so it must
  **not** force an order between them. Events are greedily bucketed into ε-clusters
  (each member within ``ε`` of the cluster's earliest member); across clusters the time
  gap is ``>= ε`` and therefore *real*, so raw chronological order is trusted untouched.

* **Dependency-informed happens-before.** A declared service dependency is a hard
  happens-before edge: if ``service(A)`` is an upstream dependency of ``service(B)``
  (``service_map.is_dependency(A, B)``) then A must precede B — even when B's raw
  timestamp is slightly *before* A's within the band (the classic skew inversion). Within
  each ε-cluster the members are **topologically ordered** by these dependency edges, with
  a stable tie-break on original input order for genuinely-concurrent (unconstrained)
  events, so their input order is preserved rather than reshuffled by sub-ε noise.

* **Correlation-id happens-before (best-effort).** If a request / trace / correlation id
  is parseable from two events' messages and they share it, that corroborates the
  dependency-derived direction (upstream precedes). Purely defensive: no parseable id
  simply means the extra constraint is skipped.

The **corrected sort key** is an *effective time* per event, written back onto the event
as a canonical timestamp: each ε-cluster collapses to its anchor time and its members are
spread by a microscopic ``_ORDER_STEP`` in topological order, so (a) the causal order is
encoded in the timestamp itself — surviving the independent re-sorts that timeline and the
graph builder each perform — while (b) the nudge stays far inside the ε band. A far-apart
(single-member-cluster) event keeps its exact original *instant* (re-emitted in canonical
ISO-8601 form). An optional per-service ``offsets`` map (default empty → a no-op)
pre-shifts each service's clock before clustering, and — because the shift is baked into
the emitted timestamp — that alignment likewise survives downstream.

``correct`` returns a **new** list of **new** :class:`~src.models.LogEvent` objects (the
inputs are never mutated) ordered by the corrected key. It is pure and deterministic. An
unparseable timestamp is *not* raised here — the batch is returned untouched so the
timeline stage raises the single canonical :class:`ValueError` (mapped to HTTP 422),
keeping error handling in one place.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta

from src.analysis.timeline import _parse_timestamp
from src.config import Settings
from src.models import LogEvent
from src.service_map import ServiceDependencyMap

__all__ = ["ClockSkewCorrector"]

#: Microscopic effective-time increment used to spread an ε-cluster's members in their
#: corrected (topological / input) order. Far smaller than any realistic ε and than the
#: ``>= ε`` inter-cluster gap, so it encodes order without ever reordering across clusters
#: (a cluster would need > ε / _ORDER_STEP members to overrun the next cluster's anchor).
_ORDER_STEP: float = 1e-6

#: Best-effort correlation / request / trace id extraction from a log message. Matches the
#: common ``key=value`` / ``key: value`` shapes; the captured token is the id. Optional by
#: design — a message with no such token yields ``None`` and the id constraint is skipped.
_ID_PATTERN = re.compile(
    r"(?:request[_-]?id|correlation[_-]?id|trace[_-]?id|trace|req[_-]?id|txn[_-]?id|rid)"
    r"\s*[=:]\s*([A-Za-z0-9._\-]+)",
    re.IGNORECASE,
)


def _parse_correlation_id(message: str) -> str | None:
    """Return a correlation / request / trace id parsed from ``message``, or ``None``.

    Best-effort and defensive: the first matching ``key=value`` / ``key: value`` token is
    returned (lower-cased so ids compare case-insensitively); anything unparseable yields
    ``None`` so the caller simply skips the optional id-based happens-before constraint.
    """
    match = _ID_PATTERN.search(message or "")
    return match.group(1).lower() if match else None


class ClockSkewCorrector:
    """Reconstruct a causally-consistent event ordering under misaligned clocks."""

    def __init__(
        self,
        settings: Settings,
        service_map: ServiceDependencyMap,
        offsets: dict[str, float] | None = None,
    ) -> None:
        self.settings = settings
        self.service_map = service_map
        #: Optional per-service clock offset in seconds (a service's clock running this
        #: many seconds fast). Subtracted before clustering to align clocks. Default empty
        #: → a no-op; wire in real offsets (e.g. from NTP telemetry) to enable.
        self.offsets: dict[str, float] = dict(offsets or {})

    def correct(self, events: list[LogEvent]) -> list[LogEvent]:
        """Return a new, causally-ordered copy of ``events`` (inputs never mutated).

        Parses timestamps, greedily buckets events into ε-clusters, topologically orders
        each cluster by dependency / correlation-id happens-before constraints (stable on
        input order for concurrent events), and writes a corrected effective time back onto
        each event so the ordering survives the downstream re-sorts. A far-apart event's
        effective time equals its original instant (re-emitted canonically). An unparseable
        timestamp defers to the timeline stage: the batch is returned as-is (untouched
        copies) so timeline raises the single canonical ``ValueError``.
        """
        if not events:
            return []

        # Parse every timestamp up front; if ANY is unparseable, don't raise here — return
        # untouched copies and let the timeline stage raise the one canonical ValueError.
        adjusted: list[datetime] = []
        try:
            for event in events:
                dt = _parse_timestamp(event.timestamp)
                offset = self.offsets.get(event.service, 0.0)
                adjusted.append(dt - timedelta(seconds=offset) if offset else dt)
        except ValueError:
            return [event.model_copy() for event in events]

        n = len(events)
        # Baseline order: ascending adjusted time, ties broken by original input index so
        # the sweep is deterministic and stable.
        order = sorted(range(n), key=lambda i: (adjusted[i], i))

        t0 = adjusted[order[0]]
        rel = [(adjusted[i] - t0).total_seconds() for i in range(n)]
        epsilon = self.settings.clock_skew_epsilon
        ids = [_parse_correlation_id(event.message) for event in events]

        # Greedy ε-clustering over the time-sorted order: a new cluster starts once an
        # event is >= ε from the current cluster's anchor (its earliest member).
        clusters: list[list[int]] = []
        current: list[int] = [order[0]]
        anchor_rel = rel[order[0]]
        for idx in order[1:]:
            if rel[idx] - anchor_rel < epsilon:
                current.append(idx)
            else:
                clusters.append(current)
                current = [idx]
                anchor_rel = rel[idx]
        clusters.append(current)

        # Assign each event a corrected effective time: cluster anchor + topological rank
        # * a microscopic step. Genuinely-concurrent members keep input order; a dependency
        # (or shared-id) happens-before overrides the sub-ε clock noise.
        eff_rel: dict[int, float] = {}
        for cluster in clusters:
            base_rel = min(rel[i] for i in cluster)
            ordered = self._order_cluster(cluster, events, ids)
            for rank, i in enumerate(ordered):
                eff_rel[i] = base_rel + rank * _ORDER_STEP

        # Emit new events (inputs never mutated), each carrying its corrected effective time
        # as a canonical timestamp so the ordering — and any per-service offset — survives
        # the re-sorts timeline and the graph builder each perform. A far-apart event's
        # effective time equals its original instant, so its timestamp is unchanged (bar
        # canonical re-formatting); only concurrency-band members are actually nudged.
        corrected: list[tuple[float, LogEvent]] = []
        for i, event in enumerate(events):
            corrected_dt = t0 + timedelta(seconds=eff_rel[i])
            corrected.append(
                (eff_rel[i], event.model_copy(update={"timestamp": corrected_dt.isoformat()}))
            )
        corrected.sort(key=lambda item: item[0])
        return [event for _eff, event in corrected]

    def _order_cluster(
        self, cluster: list[int], events: list[LogEvent], ids: list[str | None]
    ) -> list[int]:
        """Topologically order one ε-cluster's members by happens-before, stable on input.

        Builds dependency (and corroborating correlation-id) happens-before edges among the
        members and runs Kahn's algorithm, always choosing the available node with the
        smallest original input index. Unconstrained (concurrent) members therefore keep
        their input order, while an upstream cause is pulled ahead of a downstream effect
        even when clock skew stamped the effect first. Any residual cycle (only possible
        with a pathological non-DAG service map) degrades to input order.
        """
        if len(cluster) <= 1:
            return list(cluster)

        succ: dict[int, set[int]] = {i: set() for i in cluster}
        indeg: dict[int, int] = {i: 0 for i in cluster}
        for a in cluster:
            for b in cluster:
                if a == b:
                    continue
                if self._happens_before(events[a], events[b], ids[a], ids[b]):
                    if b not in succ[a]:
                        succ[a].add(b)
                        indeg[b] += 1

        # Kahn with a smallest-input-index tie-break (the members ARE their input indices).
        available = sorted(i for i in cluster if indeg[i] == 0)
        order: list[int] = []
        seen: set[int] = set()
        while available:
            node = available.pop(0)
            if node in seen:
                continue
            seen.add(node)
            order.append(node)
            newly: list[int] = []
            for s in sorted(succ[node]):
                indeg[s] -= 1
                if indeg[s] == 0:
                    newly.append(s)
            if newly:
                available = sorted(set(available) | set(newly))

        # Defensive: append any node left out by a cycle, in input order.
        if len(order) < len(cluster):
            order.extend(sorted(i for i in cluster if i not in seen))
        return order

    def _happens_before(
        self, a: LogEvent, b: LogEvent, id_a: str | None, id_b: str | None
    ) -> bool:
        """True iff event ``a`` must precede ``b`` under the happens-before constraints.

        The primary signal is the service dependency map — ``a``'s service being a declared
        upstream dependency of ``b``'s is a hard happens-before edge. A shared correlation
        id corroborates that same direction; on its own an id cannot orient causality (the
        map still supplies the direction), so it only reinforces an existing dependency.
        """
        if self.service_map.is_dependency(a.service, b.service):
            return True
        if (
            id_a is not None
            and id_a == id_b
            and self.service_map.is_dependency(a.service, b.service)
        ):
            return True
        return False
