"""FastAPI application entry point.

The module exports a :func:`build_app` factory and a module-level
``app = build_app()`` so ``uvicorn src.main:app`` still resolves
directly. Commit 04 extends the factory to construct the tokenizer
and inverted index up front (so every request sees the same
long-lived instance) and mount the health + ingest routers.

Settings, tokenizer, and index are stashed on ``app.state`` so
downstream handlers can reach them without re-reading env or
importing module-level singletons. Tests that need a fresh index can
overwrite ``app.state.index`` between cases — that is the pattern
``tests/test_api_logs.py`` uses for function-scoped isolation.
"""

from fastapi import FastAPI

from src.api.routes_health import router as health_router
from src.api.routes_logs import router as logs_router
from src.api.routes_search import router as search_router
from src.config import Settings, get_settings
from src.index.inverted_index import InvertedIndex
from src.index.tokenizer import LogTokenizer
from src.index.trie import PrefixTrie
from src.logging_setup import configure_logging


def build_app(settings: Settings | None = None) -> FastAPI:
    """Construct a fresh FastAPI app, optionally with custom settings.

    ``build_app()`` with no argument behaves identically to the
    module-level ``app = build_app()``. Passing a :class:`Settings`
    instance lets tests isolate configuration without mutating the
    cached singleton or the process environment.

    Logging is configured here — callers that construct multiple apps
    in-process (tests) pay a cheap handler swap each time, which is
    the intended behaviour of :func:`configure_logging`.

    The tokenizer and index are constructed once per app so their
    per-instance caches (lemma cache, postings, docs, version
    counter) are shared across every request. Fresh apps start with
    empty state — by design, since the service is single-node in-
    memory.
    """
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    # Build the core search components up front. The tokenizer and
    # index are intentionally constructed here (not lazily on first
    # request) so a health probe immediately after startup reflects
    # a fully-ready app. The index owns a reference to the tokenizer
    # so ingest and search always agree on tokenisation rules.
    tokenizer = LogTokenizer(settings)
    index = InvertedIndex(settings=settings, tokenizer=tokenizer)
    trie = PrefixTrie()

    app = FastAPI(title="log-fulltext-search-rerank")
    # Stash on state so later-commit routes/middleware can reach the
    # same instances via ``request.app.state.*`` without importing
    # the cached accessor or the module-level ``app``.
    app.state.settings = settings
    app.state.tokenizer = tokenizer
    app.state.index = index
    app.state.trie = trie

    app.include_router(health_router)
    app.include_router(logs_router)
    app.include_router(search_router)

    return app


# Module-level app instance for ``uvicorn src.main:app``. The Dockerfile
# CMD and ``start.sh`` both import this symbol directly.
app = build_app()
