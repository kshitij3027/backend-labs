"""Search router — exposes the full query surface.

Three endpoints live here, all mounted under the ``/api/search``
prefix by :func:`src.main.build_app`:

* ``POST /api/search`` — full-text search with multi-factor ranking.
* ``GET  /api/search/suggestions`` — prefix-based autocomplete.
* ``GET  /api/search/stats`` — service diagnostics (corpus size,
  cache hit ratio, p95 latency).

All three delegate to :class:`~src.service.SearchService` so the
router stays thin and the service facade is the single place that
knows the actual pipeline.

Lazy-rebuild pattern
--------------------

The trie is not updated on every ingest — the service's
:meth:`SearchService.suggest` compares the trie's cached
``_indexed_version`` marker against the current ``index.version`` and
rebuilds from scratch when they diverge. The rebuild is cheap for the
corpus sizes in scope (tens of thousands of unique tokens) and the
version-compare short-circuit means steady-state suggest calls never
touch the inverted index at all.
"""

from fastapi import APIRouter, HTTPException, Query, Request

from src.models import SearchRequest, SearchResponse, StatsResponse, SuggestionsResponse


router = APIRouter(prefix="/api/search", tags=["search"])


@router.post("", response_model=SearchResponse)
async def search(req: SearchRequest, request: Request) -> SearchResponse:
    """Run a ranked full-text search.

    Pydantic validates ``req`` before this handler runs — an empty
    query or an out-of-range ``limit`` short-circuits to a 422 at
    the framework layer, so the body here can trust its inputs.

    Any exception raised by the service layer is converted to an
    HTTP 500 so the client sees a well-formed JSON error instead of
    an opaque FastAPI default.
    """
    service = request.app.state.search_service
    try:
        return await service.search(req)
    except Exception as exc:  # noqa: BLE001 — route boundary
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/suggestions", response_model=SuggestionsResponse)
async def suggestions(
    request: Request,
    q: str = Query(default="", max_length=200),
    limit: int = Query(default=10, ge=1, le=100),
) -> SuggestionsResponse:
    """Return up to ``limit`` tokens that start with ``q``.

    The returned list is ordered by document-frequency descending
    (most-popular prefix completions first) with alphabetical ties.
    Empty ``q`` returns an empty list per the spec — we do not dump
    the whole vocabulary to a curious client.

    Casing is normalised inside :meth:`SearchService.suggest`; the
    tokenizer stores lowercased tokens, so lowercasing the user input
    before trie lookup makes the API behaviour case-insensitive
    without storing duplicate capitalisations.
    """
    service = request.app.state.search_service
    return service.suggest(q, limit=limit)


@router.get("/stats", response_model=StatsResponse)
async def stats(request: Request) -> StatsResponse:
    """Return the combined index + cache + latency snapshot.

    Safe to poll — the underlying calls are pure dict lookups plus
    a bounded sort for the latency percentile. Intended for the
    dashboard and ``make demo``.
    """
    service = request.app.state.search_service
    return service.stats()
