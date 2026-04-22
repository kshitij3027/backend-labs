"""Search router — ``GET /api/search/suggestions`` (commit 05).

Only the autocomplete surface lands in this file for now. Commit 09
extends this router with the actual ``POST /api/search`` endpoint
and the ``/stats`` sibling, but until then the router exists solely
to serve prefix suggestions off the :class:`~src.index.trie.PrefixTrie`
that :func:`src.main.build_app` stashes on ``app.state``.

Lazy-rebuild pattern
--------------------

The trie is not updated on every ingest — doing so would require a
writer-side hook threaded through the index, which we want to keep
append-only. Instead, this route compares the trie's cached
``_indexed_version`` marker against the current ``index.version`` and
rebuilds from scratch when they diverge. The rebuild is cheap for the
corpus sizes in scope (tens of thousands of unique tokens) and the
version-compare short-circuit means steady-state suggest calls never
touch the inverted index at all.

Casing
------

The ``q`` query parameter is lowercased before being passed to the
trie. The tokenizer emits lowercased tokens, so the trie stores only
lowercase keys — lowercasing the user input here makes the API
behaviour case-insensitive without storing duplicate capitalisations.
This keeps the suggestion contract predictable: ``q=AUTH``, ``q=auth``,
and ``q=Auth`` all return the same suggestions.
"""

from fastapi import APIRouter, Query, Request

from src.models import SuggestionsResponse


router = APIRouter(prefix="/api/search", tags=["search"])


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
    """
    trie = request.app.state.trie
    index = request.app.state.index

    # Lazy refresh: if the index has advanced since the last rebuild,
    # clear and re-populate the trie from scratch. Using a wholesale
    # rebuild (rather than an incremental diff) means the trie's
    # terminal frequencies always reflect the current doc-frequency
    # — if a previously-rare token became popular, we see that the
    # next time the version advances.
    current_version = index.version
    if getattr(trie, "_indexed_version", -1) != current_version:
        trie.clear()
        for token in index.unique_tokens():
            trie.insert(token, freq=index.doc_frequency(token))
        # Stash the version on the trie instance so we can skip the
        # rebuild on subsequent calls until the index moves again.
        trie._indexed_version = current_version

    # Normalise casing so ``q=AUTH`` matches the lowercased tokens
    # the tokenizer emits.
    normalised = q.lower()
    return SuggestionsResponse(suggestions=trie.suggest(normalised, limit=limit))
