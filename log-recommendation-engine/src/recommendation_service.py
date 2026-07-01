"""Recommendation orchestration — the full ``POST /recommend`` pipeline (C9).

This is the composition root that wires every prior stage into one call:

    embed query  ->  pgvector retrieve candidates  ->  contextual score
                 ->  weighted-blend rank            ->  top-K suggestions

Each suggestion carries the matched historical incident's **resolution** (the fix)
plus a per-signal score breakdown, so the response is directly actionable and
self-explaining. The served result is persisted (a :class:`Recommendation` row, so
C10 feedback can reference it) and cached in Redis (so an identical repeated query
is answered without recomputing).

Design notes
------------
* **Reuse, don't reimplement.** Embedding (:mod:`src.embeddings`), retrieval
  (:mod:`src.retrieval`), contextual scoring (:mod:`src.contextual`) and ranking
  (:mod:`src.ranker`) are all consumed as-is. This module is *only* orchestration
  + persistence + cache.
* **Soft-by-default hybrid.** By default retrieval is pure semantic and the
  contextual signals act as soft preferences (same service / close severity /
  shared tags are *rewarded*, not *required*). A caller opts into hard constraints
  with ``restrict_service`` / ``restrict_severity``; if a hard filter is too narrow
  to fill ``top_k`` we widen by re-retrieving without it (logged).
* **Feedback is still 0.** The feedback blend term is a stub until C11; nothing
  here reads feedback state.
* **Fault tolerance.** The Redis cache is best-effort on both read and write — if
  Redis is down we simply recompute and return ``cached=False``. An empty corpus /
  no candidates returns an empty (``count=0``) response, never a 500.
"""

from __future__ import annotations

import hashlib
import json

from sqlalchemy.orm import Session

from src import embeddings, observability
from src.clients import redis as redis_client
from src.config import get_settings
from src.db import repository
from src.ranker import QueryContext, RankedSuggestion, rank_candidates
from src.retrieval import Candidate, retrieve_candidates
from src.schemas import (
    QueryEcho,
    RecommendRequest,
    RecommendResponse,
    Suggestion,
)

logger = observability.get_logger(__name__)


# --------------------------------------------------------------------------- #
# Query hashing (stable cache / dedup key)
# --------------------------------------------------------------------------- #
def _effective_top_k(req: RecommendRequest) -> int:
    """Resolve the effective ``top_k`` for this request (override or config default)."""
    return req.top_k if req.top_k is not None else get_settings().top_k


def compute_query_hash(req: RecommendRequest) -> str:
    """Return a stable SHA-256 hash of the *normalised* query.

    The hash is what the Redis recommendation cache and the persisted
    ``recommendations`` row are keyed by, so two requests that mean the same thing
    map to the same entry. Normalisation:

    * ``title`` / ``description`` — stripped, lower-cased.
    * ``service`` / ``severity`` — stripped, lower-cased, ``None`` when blank.
    * ``tags`` — stripped, lower-cased, de-duplicated, **sorted** (order-independent).
    * ``top_k`` — the *effective* value (so an explicit ``top_k`` equal to the
      config default hashes the same as omitting it).
    * ``restrict_service`` / ``restrict_severity`` — the boolean flags (they change
      the result set, so they must change the key).

    The normalised mapping is serialised with sorted keys + a compact separator so
    the digest is deterministic across processes and Python runs.
    """
    title = (req.title or "").strip().lower()
    description = (req.description or "").strip().lower()
    service = req.service.strip().lower() if req.service else None
    severity = req.severity.strip().lower() if req.severity else None
    tags = sorted({t.strip().lower() for t in (req.tags or []) if t and t.strip()})

    normalised = {
        "title": title,
        "description": description,
        "service": service,
        "severity": severity,
        "tags": tags,
        "top_k": _effective_top_k(req),
        "restrict_service": bool(req.restrict_service),
        "restrict_severity": bool(req.restrict_severity),
    }
    blob = json.dumps(normalised, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- #
# Retrieval with soft-vs-hard filtering + widening
# --------------------------------------------------------------------------- #
def _retrieve_with_widening(
    session: Session,
    query_vec,
    req: RecommendRequest,
    *,
    candidate_k: int,
    top_k: int,
) -> list[Candidate]:
    """Retrieve candidates, honouring hard filters and widening when too narrow.

    Hard filters are applied **only** for the ``restrict_*`` flags that are set
    (service -> exact ``service`` pre-filter; severity -> ``severity IN [req.severity]``).
    With no restrict flag the query is pure semantic and the caller relies on the
    contextual signals to softly rank same-service / same-severity matches higher.

    If a hard filter yields fewer than ``top_k`` candidates, the filter is too
    narrow to produce a full page, so we re-retrieve **without** the hard filters
    (logging it) and fall back to the wider result. This keeps hard filters a
    *preference for constraint* without ever silently returning an empty page when
    the corpus does have semantically-relevant incidents.
    """
    service_filter = req.service if req.restrict_service and req.service else None
    severity_filter = (
        [req.severity] if req.restrict_severity and req.severity else None
    )

    candidates = retrieve_candidates(
        session,
        query_vec,
        k=candidate_k,
        service=service_filter,
        severities=severity_filter,
    )

    hard_filtered = service_filter is not None or severity_filter is not None
    if hard_filtered and len(candidates) < top_k:
        logger.info(
            "hard filter too narrow; widening retrieval",
            restrict_service=req.restrict_service,
            restrict_severity=req.restrict_severity,
            got=len(candidates),
            top_k=top_k,
        )
        candidates = retrieve_candidates(
            session,
            query_vec,
            k=candidate_k,
            service=None,
            severities=None,
        )

    return candidates


# --------------------------------------------------------------------------- #
# Serialisation helpers (RankedSuggestion <-> Suggestion <-> cached JSON)
# --------------------------------------------------------------------------- #
def _to_suggestion(ranked: RankedSuggestion) -> Suggestion:
    """Map a :class:`~src.ranker.RankedSuggestion` to the wire :class:`Suggestion`.

    Carries the matched incident's ``resolution`` and the full per-signal
    ``breakdown`` through unchanged — the resolution is the actionable payload and
    the breakdown makes the ranking explainable.
    """
    return Suggestion(
        incident_id=ranked.incident_id,
        title=ranked.title,
        service=ranked.service,
        severity=ranked.severity,
        tags=list(ranked.tags or []),
        resolution=ranked.resolution,
        score=ranked.score,
        semantic=ranked.semantic,
        contextual=ranked.contextual,
        feedback=ranked.feedback,
        breakdown=ranked.breakdown,
    )


def _query_echo(req: RecommendRequest) -> QueryEcho:
    """Echo the (normalised) query facets back on the response."""
    return QueryEcho(
        title=req.title,
        description=req.description,
        service=req.service,
        severity=req.severity,
        tags=list(req.tags or []),
    )


# --------------------------------------------------------------------------- #
# Orchestrator
# --------------------------------------------------------------------------- #
def recommend(session: Session, req: RecommendRequest) -> RecommendResponse:
    """Run the full recommendation pipeline for ``req`` and return ranked suggestions.

    Pipeline
    --------
    1. **Cache check** — build a stable ``query_hash`` and look up ``rec:<hash>`` in
       Redis. On a hit, return the cached response with ``cached=True`` (fault
       tolerant: Redis down simply skips the cache).
    2. **Embed** the query (via the Redis-backed embedding cache read-through).
    3. **Retrieve** ``candidate_k`` candidates by pgvector cosine K-NN — hard-filtered
       only for the ``restrict_*`` flags, widening if a hard filter is too narrow.
    4. **Rank** the candidates by the blended semantic + contextual (+ 0 feedback)
       score, truncated to the effective ``top_k``.
    5. **Persist** a :class:`~src.db.models.Recommendation` row whose ``query_json``
       holds the request facets plus the returned suggestion ``incident_id``s (so
       C10 feedback can validate a suggestion belongs to a real prior result).
    6. **Cache** the built response under ``rec:<hash>`` and return it.

    An empty corpus / no candidates yields an empty (``count=0``) response — still
    persisted and cached — rather than an error.
    """
    settings = get_settings()
    query_hash = compute_query_hash(req)

    # 1. Cache check (best-effort; a down cache just falls through to recompute).
    cached = redis_client.cache_get_recommendation(query_hash)
    if cached is not None:
        response = _response_from_cache(cached, req)
        if response is not None:
            logger.debug("recommendation cache hit", query_hash=query_hash)
            return response
        # A malformed/incompatible cache entry falls through to a fresh compute.

    # 2. Embed the query (read-through embedding cache). build_incident_text is the
    #    same canonical doc text used for corpus incidents, so vectors are comparable.
    query_text = embeddings.build_incident_text(
        req.title, req.description, req.tags
    )
    query_vec = embeddings.embed_text_cached(query_text)

    top_k = _effective_top_k(req)

    # 3. Retrieve candidates (soft-by-default; hard filter + widen only on restrict).
    candidates = _retrieve_with_widening(
        session,
        query_vec,
        req,
        candidate_k=settings.candidate_k,
        top_k=top_k,
    )

    # 4. Re-rank the candidate pool down to top_k (feedback term is 0 until C11).
    ranked = rank_candidates(
        candidates,
        QueryContext(
            service=req.service,
            severity=req.severity,
            tags=list(req.tags or []),
        ),
        top_k=top_k,
    )

    suggestions = [_to_suggestion(r) for r in ranked]

    # 5. Persist the served recommendation. query_json holds the query facets plus
    #    the returned suggestion ids so C10 feedback can confirm a suggestion was
    #    actually served for this recommendation.
    suggestion_ids = [s.incident_id for s in suggestions]
    recommendation = repository.create_recommendation(
        session,
        query_hash=query_hash,
        query_json={
            "title": req.title,
            "description": req.description,
            "service": req.service,
            "severity": req.severity,
            "tags": list(req.tags or []),
            "top_k": top_k,
            "restrict_service": bool(req.restrict_service),
            "restrict_severity": bool(req.restrict_severity),
            "suggestion_ids": suggestion_ids,
        },
        commit=True,
    )

    response = RecommendResponse(
        recommendation_id=recommendation.id,
        query=_query_echo(req),
        suggestions=suggestions,
        count=len(suggestions),
        cached=False,
    )

    logger.info(
        "recommendation served",
        recommendation_id=recommendation.id,
        query_hash=query_hash,
        candidates=len(candidates),
        suggestions=len(suggestions),
        top_semantic=suggestions[0].semantic if suggestions else None,
    )

    # 6. Cache the response (best-effort). Store the full payload incl. the
    #    recommendation_id so a cache hit reconstructs an identical response.
    redis_client.cache_set_recommendation(
        query_hash,
        _response_to_cache(response),
        ttl=settings.recommendation_cache_ttl_sec,
    )

    return response


# --------------------------------------------------------------------------- #
# Cache (de)serialisation — the cached payload is a plain JSON dict
# --------------------------------------------------------------------------- #
def _response_to_cache(response: RecommendResponse) -> dict:
    """Serialise a :class:`RecommendResponse` to a JSON-safe dict for Redis.

    Stores everything needed to reconstruct an identical response on a later hit,
    including ``recommendation_id`` (so the served id is stable across the cache
    window and C10 feedback can reference it).
    """
    return response.model_dump(mode="json")


def _response_from_cache(
    data: dict, req: RecommendRequest
) -> RecommendResponse | None:
    """Rebuild a :class:`RecommendResponse` from a cached dict, marking it cached.

    Returns ``None`` if the cached payload cannot be validated into the current
    response shape (e.g. a schema change since it was written), so the caller
    transparently recomputes instead of surfacing a stale/broken entry. The echoed
    ``query`` is refreshed from the live request so it always mirrors what was asked.
    """
    try:
        response = RecommendResponse.model_validate(data)
    except Exception as exc:  # noqa: BLE001 - stale/incompatible entry -> recompute
        logger.warning("could not rebuild cached recommendation: %s", exc)
        return None
    response.cached = True
    response.query = _query_echo(req)
    return response


__all__ = ["recommend", "compute_query_hash"]
