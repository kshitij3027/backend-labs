"""Build a :class:`RankingExplanation` from a :class:`ScoredDoc`."""

from __future__ import annotations

from src.models import RankingExplanation
from src.ranking.reranker import ScoredDoc


def build_explanation(
    scored: ScoredDoc,
    weights: dict,
    mode: str | None,
) -> RankingExplanation:
    """Translate a :class:`ScoredDoc` into the public explanation shape.

    The per-factor values in ``scored.breakdown`` are the raw scorer
    outputs (pre-weight); the ``RankingExplanation`` payload keeps them
    verbatim so clients can reason about both "what did the ranker
    think of each signal" and "how much did the ranker weight each
    signal". Weights live in separate fields on the response envelope
    (commit 09 plumbs them through the stats endpoint).

    ``reasons`` carries the mode-match / high-severity / recency flags
    the reranker appended during scoring, prefixed with the active mode
    string when a mode is set.
    """
    reasons = list(scored.reasons)
    if mode and not any(r.endswith("_mode_boost") for r in reasons):
        # Still record the mode even when no bonus fired, so clients can
        # see the request was ranked under that context.
        reasons.append(f"{mode}_mode")
    return RankingExplanation(
        tfidf=scored.breakdown.get("tfidf", 0.0),
        temporal=scored.breakdown.get("temporal", 0.0),
        severity=scored.breakdown.get("severity", 0.0),
        service=scored.breakdown.get("service", 0.0),
        context=scored.breakdown.get("context", 0.0),
        reasons=reasons,
    )


__all__ = ("build_explanation",)
