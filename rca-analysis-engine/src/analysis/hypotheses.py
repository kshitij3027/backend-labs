"""Multi-hypothesis root-cause tracking for the RCA Analysis Engine (C7, feature area A).

The :class:`MultiHypothesisTracker` is the multi-hypothesis stage of
``RCAAnalyzer.analyze``. Rather than commit to a single root cause, it keeps the top-``k``
concurrent :class:`~src.models.Hypothesis` explanations with **independent** confidences
and a ``tentative -> confirmed -> pruned`` lifecycle, ranked by **personalized PageRank /
random-walk-with-restart on the reversed causal graph, seeded by the anomaly scores**.

Why the *reversed* graph. The causal :class:`networkx.DiGraph` has edges ``cause ->
effect``; anomalies light up on the *symptoms* (the effects). To rank *causes*, the walk
must flow the other way — from symptoms back toward their sources. So the transition
matrix is built on the reversed adjacency ``A.T``: a walker standing on a symptom steps to
one of its causes, and mass accumulates on the upstream sources that explain many
anomalous downstream events.

The walk (vectorized power iteration over ``scipy.sparse`` matrices):

* Index the ``n`` nodes ``0..n-1`` and build the weighted adjacency ``A`` (edge
  ``strength``). Reverse it (``A.T``) and row-normalize to a transition matrix ``P``.
  A **dangling** row — an original source with no causes of its own — has no outgoing
  reversed edge; its mass is redistributed via the restart vector (standard PPR handling),
  which keeps ``P`` effectively column-stochastic and the iterate a proper distribution.
* The **restart / personalization** vector ``v`` is the L1-normalized anomaly-score
  vector (uniform if every score is zero), so the walk restarts on the most anomalous
  events.
* Iterate ``pi_next = alpha * (P.T @ pi) + (alpha * dangling_mass + (1 - alpha)) * v``
  until the L1 change drops below ``pagerank_tol`` (or ``pagerank_max_iter`` is hit). The
  restart term makes this a contraction with factor ``alpha``, so it converges
  geometrically — a trivial graph settles in a handful of iterations, well short of the
  cap.

Each of the top-``max_hypotheses`` nodes by ``pi`` becomes a hypothesis whose
**independent** confidence blends its *relative* PageRank mass (``pi_i / max(pi)``, a
max-normalization — deliberately **not** a sum-to-1 normalization) with its own anomaly
score, so several rival hypotheses can each be highly confident at once. A Bayesian-ish
lifecycle rule then labels each: ``CONFIRMED`` at/above ``hypothesis_confirm_threshold``,
``PRUNED`` (and dropped from the result) below ``hypothesis_prune_threshold``, else
``TENTATIVE``. Survivors are returned sorted by confidence descending.

Pure and deterministic: no network, no globals, no wall-clock reads. Node indexing follows
the graph's (chronological) insertion order and ties are broken by index, so the ranking
is stable across runs.
"""

from __future__ import annotations

import hashlib

import networkx as nx
import numpy as np
from scipy import sparse

from src.config import Settings
from src.models import Hypothesis, HypothesisState, LogEvent

__all__ = ["MultiHypothesisTracker"]

#: Floor of the anomaly multiplier applied to a hypothesis's relative PageRank mass: the
#: confidence is ``rel_mass * (FLOOR + (1 - FLOOR) * anomaly)``, so a node's causal
#: centrality sets the ceiling and its anomaly modulates within ``[FLOOR, 1]`` of it.
_ANOMALY_FLOOR: float = 0.5


def _clamp(value: float, low: float, high: float) -> float:
    """Clamp ``value`` into the inclusive ``[low, high]`` range."""
    return max(low, min(high, value))


def _short_id(event_id: str) -> str:
    """A short, stable, collision-resistant suffix for a hypothesis id."""
    return hashlib.sha1(event_id.encode("utf-8")).hexdigest()[:8]


class MultiHypothesisTracker:
    """Rank concurrent root-cause hypotheses via anomaly-seeded reversed-graph PPR."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def random_walk_with_restart(
        self,
        graph: nx.DiGraph,
        anomaly_scores: dict[str, float],
        initial: dict[str, float] | None = None,
    ) -> tuple[list[str], np.ndarray, int]:
        """Run anomaly-seeded RWR on the reversed causal graph.

        Returns ``(nodes, pi, iterations)`` where ``nodes`` is the index order, ``pi`` the
        stationary distribution (sums to 1 for a non-empty graph), and ``iterations`` the
        number of power-iteration steps taken (``< pagerank_max_iter`` once converged). An
        empty graph yields ``([], empty, 0)``.

        ``initial`` is an optional **warm start** — a ``node -> mass`` map (typically a
        previous run's ``pi``) used as the iteration's starting vector instead of the
        uniform/restart cold start. The fixed point is unchanged (so the ranking is
        identical), but when the graph changed only slightly the walk begins near its new
        stationary point and converges in fewer iterations. Missing nodes seed to 0 and the
        vector is L1-normalized; an all-zero / absent ``initial`` falls back to a cold
        start, so existing callers that omit it are completely unaffected.
        """
        nodes = list(graph.nodes())
        n = len(nodes)
        if n == 0:
            return [], np.zeros(0, dtype=float), 0
        index = {node: i for i, node in enumerate(nodes)}

        # Weighted adjacency A[i, j] = strength of the causal edge i -> j.
        if graph.number_of_edges() == 0:
            adjacency = sparse.csr_matrix((n, n), dtype=float)
        else:
            rows, cols, data = [], [], []
            for source, target, edge in graph.edges(data=True):
                strength = edge.get("strength")
                if strength is None:
                    strength = edge.get("weight", 1.0)
                rows.append(index[source])
                cols.append(index[target])
                data.append(float(strength))
            adjacency = sparse.csr_matrix(
                (np.asarray(data, dtype=float), (np.asarray(rows), np.asarray(cols))),
                shape=(n, n),
            )

        # Reverse (symptom -> cause) and row-normalize to a transition matrix P.
        reversed_adj = adjacency.transpose().tocsr()
        row_sums = np.asarray(reversed_adj.sum(axis=1)).ravel()
        dangling = row_sums <= 0.0
        inv = np.zeros(n, dtype=float)
        inv[~dangling] = 1.0 / row_sums[~dangling]
        transition = (sparse.diags(inv) @ reversed_adj).tocsr()
        transition_t = transition.transpose().tocsr()

        # Restart vector = L1-normalized anomaly scores (uniform if all zero).
        restart = np.array(
            [max(0.0, float(anomaly_scores.get(node, 0.0))) for node in nodes],
            dtype=float,
        )
        mass = restart.sum()
        restart = restart / mass if mass > 0.0 else np.full(n, 1.0 / n, dtype=float)

        alpha = self.settings.pagerank_alpha
        tol = self.settings.pagerank_tol
        max_iter = self.settings.pagerank_max_iter

        # Warm start from ``initial`` (a prior pi) when supplied and non-degenerate; else
        # cold-start from the restart vector (the original, unchanged behaviour).
        if initial is not None:
            seed = np.array(
                [max(0.0, float(initial.get(node, 0.0))) for node in nodes], dtype=float
            )
            seed_mass = seed.sum()
            pi = seed / seed_mass if seed_mass > 0.0 else restart.copy()
        else:
            pi = restart.copy()
        iterations = 0
        for step in range(1, max_iter + 1):
            iterations = step
            dangling_mass = float(pi[dangling].sum())
            pi_next = alpha * (transition_t @ pi) + (
                alpha * dangling_mass + (1.0 - alpha)
            ) * restart
            if np.abs(pi_next - pi).sum() < tol:
                pi = pi_next
                break
            pi = pi_next
        return nodes, pi, iterations

    def rank(
        self,
        events: list[LogEvent],
        graph: nx.DiGraph,
        anomaly_scores: dict[str, float],
        initial: dict[str, float] | None = None,
    ) -> list[Hypothesis]:
        """Return the surviving top-k root-cause hypotheses, sorted by confidence desc.

        The causal ``graph`` (built in C3) and the per-event ``anomaly_scores`` (from the
        C7 amplifier) drive the ranking; ``events`` is accepted for interface symmetry with
        the other analyze stages. Nodes are ranked by reversed-graph personalized PageRank,
        the top ``max_hypotheses`` get an independent confidence blended from their relative
        PageRank mass and anomaly score, and each is labelled CONFIRMED / TENTATIVE or
        dropped when PRUNED. An empty graph yields an empty list; a single node yields one
        hypothesis.

        ``initial`` is an optional warm-start ``node -> mass`` vector forwarded to the power
        iteration (see :meth:`random_walk_with_restart`); it changes only how fast the walk
        converges, not the resulting ranking. Defaults to ``None`` (cold start), so the C7
        analyze call site is unchanged.
        """
        nodes, pi, _iterations = self.random_walk_with_restart(
            graph, anomaly_scores, initial=initial
        )
        return self.hypotheses_from_pi(nodes, pi, anomaly_scores)

    def hypotheses_from_pi(
        self,
        nodes: list[str],
        pi: np.ndarray,
        anomaly_scores: dict[str, float],
    ) -> list[Hypothesis]:
        """Build the surviving top-k hypotheses from a precomputed PageRank vector.

        Extracted so a caller that already ran the walk (e.g. the incremental analyzer,
        which needs the iteration count and the raw ``pi`` for its warm start) can reuse the
        exact independent-confidence + lifecycle logic without recomputing the walk. Given
        the index order ``nodes`` and its stationary distribution ``pi``, returns the
        surviving hypotheses sorted by confidence descending; an empty ``nodes`` yields
        ``[]``.
        """
        n = len(nodes)
        if n == 0:
            return []

        pi_max = float(pi.max())
        confirm = self.settings.hypothesis_confirm_threshold
        prune = self.settings.hypothesis_prune_threshold

        # Consider the top-k nodes by PageRank mass (ties broken by index for determinism).
        order = sorted(range(n), key=lambda i: (-pi[i], i))
        top_k = order[: max(1, self.settings.max_hypotheses)]

        scored: list[tuple[float, int, Hypothesis]] = []
        for i in top_k:
            node = nodes[i]
            relative_mass = (pi[i] / pi_max) if pi_max > 0.0 else 0.0
            anomaly = _clamp(float(anomaly_scores.get(node, 0.0)), 0.0, 1.0)
            # Independent confidence: relative PageRank mass sets the ceiling, the node's
            # own anomaly modulates within [FLOOR, 1] of it. NOT normalized across
            # hypotheses, so rivals can each be independently confident.
            confidence = float(
                _clamp(
                    relative_mass * (_ANOMALY_FLOOR + (1.0 - _ANOMALY_FLOOR) * anomaly),
                    0.0,
                    1.0,
                )
            )
            if confidence < prune:
                continue  # PRUNED -> dropped from the returned set.
            state = (
                HypothesisState.CONFIRMED
                if confidence >= confirm
                else HypothesisState.TENTATIVE
            )
            scored.append(
                (
                    confidence,
                    i,
                    Hypothesis(
                        hypothesis_id="hyp-" + _short_id(node),
                        root_cause_event_id=node,
                        confidence=confidence,
                        state=state,
                    ),
                )
            )

        # Confidence descending; tie-break by PageRank order (node index) for stability.
        scored.sort(key=lambda item: (-item[0], item[1]))
        return [item[2] for item in scored]
