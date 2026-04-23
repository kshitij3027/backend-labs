"""Multi-factor reranker that composes the commit-07 primitives.

Retrieval is handled by :class:`InvertedIndex.retrieve_candidates`;
the reranker takes that bounded candidate set and produces a top-``limit``
list ordered by a weighted sum of per-factor scores. The scoring pass
is CPU-bound pure-python, so it runs inside ``asyncio.to_thread`` —
every caller stays on the event loop without blocking concurrent
requests.
"""

from __future__ import annotations

import asyncio
import heapq
from dataclasses import dataclass, field

from src.config import Settings
from src.index.inverted_index import InvertedIndex
from src.query.parser import ParsedQuery
from src.ranking.context import effective_weights, context_bonus
from src.ranking.service_authority import ServiceAuthorityScorer
from src.ranking.severity import SeverityScorer
from src.ranking.temporal import TemporalScorer
from src.ranking.tfidf import TfIdfScorer


@dataclass
class ScoredDoc:
    """A single ranked candidate with per-factor score breakdown.

    Stored as a dataclass (not a pydantic model) because it lives
    entirely inside the ranking layer — the service boundary converts
    it into a :class:`~src.models.SearchResult` via
    :func:`src.ranking.explain.build_explanation`. Keeping this shape
    light means the scoring loop allocates cheaply per candidate.

    Attributes:
        doc_id: The ``InvertedIndex`` doc id this score refers to.
        total: Weighted sum of the per-factor scores.
        breakdown: Raw per-factor scores (pre-weight). Keys are
            ``tfidf``, ``temporal``, ``severity``, ``service``,
            ``context``.
        reasons: Short human-readable strings explaining which
            boosts/penalties fired (e.g. ``"incident_mode_boost"``,
            ``"high_severity_ERROR"``, ``"recent"``).
    """

    doc_id: int
    total: float
    breakdown: dict[str, float] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)


class MultiFactorReranker:
    """Compose the commit-07 primitives into a weighted-sum reranker.

    Ownership model: the reranker holds **references** to the index and
    the four per-factor scorers — it does not construct them. Commit
    09's service layer wires the whole graph on app startup so all
    endpoints share one instance per scorer (the tfidf scorer's
    ``idf_cache`` in particular must be shared, not duplicated).

    The async :meth:`rerank` is the public entry point; the sync
    :meth:`_score_candidates` method is a deliberate split — it's the
    CPU-bound loop that runs under :func:`asyncio.to_thread`. Keeping
    it a regular method (not an awaitable) means the ``to_thread``
    call stays trivial.
    """

    def __init__(
        self,
        index: InvertedIndex,
        tfidf: TfIdfScorer,
        temporal: TemporalScorer,
        severity: SeverityScorer,
        service: ServiceAuthorityScorer,
        settings: Settings,
    ) -> None:
        self._index = index
        self._tfidf = tfidf
        self._temporal = temporal
        self._severity = severity
        self._service = service
        self._settings = settings

    async def rerank(
        self,
        parsed: ParsedQuery,
        candidates: list[int],
        limit: int,
        context: dict | None,
        now: float,
    ) -> list[ScoredDoc]:
        """Return the top-``limit`` scored docs from ``candidates``.

        The scoring loop is offloaded with :func:`asyncio.to_thread` so
        the event loop keeps accepting concurrent requests while a
        worker chews through 200 candidates.
        """
        if not candidates:
            return []
        mode = (context or {}).get("mode")
        weights = effective_weights(mode, self._settings)
        # Refresh idf cache if needed — cheap when no rebuild is due.
        self._tfidf.maybe_rebuild()
        # Precompute the token list the scorer iterates per doc; passing
        # expanded_tokens lets synonyms reach their matching posting
        # lists during tfidf scoring too.
        tokens = parsed.expanded_tokens or parsed.tokens
        scored = await asyncio.to_thread(
            self._score_candidates,
            candidates=candidates,
            tokens=tokens,
            weights=weights,
            mode=mode,
            now=now,
        )
        if limit >= len(scored):
            return sorted(scored, key=_sort_key, reverse=True)
        # heapq.nlargest is cheaper than a full sort for small limit / large N.
        return heapq.nlargest(limit, scored, key=_sort_key)

    def _score_candidates(
        self,
        *,
        candidates: list[int],
        tokens: list[str],
        weights: dict,
        mode: str | None,
        now: float,
    ) -> list[ScoredDoc]:
        """Score every candidate and return the un-sorted list.

        Runs under :func:`asyncio.to_thread`. No ``await`` calls here —
        every dependency is sync by design. The per-factor scorer
        handles its own defensive defaults so a missing service /
        level / timestamp never raises.
        """
        w_tfidf = weights["tfidf"]
        w_temp = weights["temporal"]
        w_sev = weights["severity"]
        w_svc = weights["service"]
        w_ctx = weights["context"]
        half_life = weights["half_life_s"]
        out: list[ScoredDoc] = []
        for doc_id in candidates:
            entry = self._index.doc(doc_id)
            if entry is None:
                # The retriever may have surfaced a doc_id that was
                # evicted or never materialised — skip rather than
                # crash, the caller is allowed to pass a superset.
                continue
            s_tfidf = self._tfidf.score(doc_id, tokens)
            s_temp = self._temporal.score(entry.timestamp, now, half_life)
            s_sev = self._severity.score(entry.level)
            s_svc = self._service.score(entry.service)
            s_ctx = context_bonus(mode, entry.level)
            total = (
                w_tfidf * s_tfidf
                + w_temp * s_temp
                + w_sev * s_sev
                + w_svc * s_svc
                + w_ctx * s_ctx
            )
            reasons: list[str] = []
            if mode and s_ctx > 0:
                # The bonus only fires on a mode+level match, so the
                # label includes the active mode for traceability.
                reasons.append(f"{mode}_mode_boost")
            if s_sev >= 0.9:
                reasons.append(f"high_severity_{entry.level}")
            if s_temp >= 0.9:
                reasons.append("recent")
            out.append(
                ScoredDoc(
                    doc_id=doc_id,
                    total=total,
                    breakdown={
                        "tfidf": s_tfidf,
                        "temporal": s_temp,
                        "severity": s_sev,
                        "service": s_svc,
                        "context": s_ctx,
                    },
                    reasons=reasons,
                )
            )
        return out


def _sort_key(sd: ScoredDoc) -> tuple[float, float, int]:
    """Primary: total score. Tiebreakers: newer timestamp, higher doc_id.

    The tiebreakers lean on the score breakdown indirectly (temporal
    already encodes recency) but we want absolute recency as the final
    tie-breaker and doc_id as a deterministic fallback so equal docs
    resolve the same way across runs.
    """
    return (sd.total, sd.breakdown.get("temporal", 0.0), sd.doc_id)
