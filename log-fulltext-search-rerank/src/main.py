"""FastAPI application entry point.

The module exports a :func:`build_app` factory and a module-level
``app = build_app()`` so ``uvicorn src.main:app`` still resolves
directly. The factory wires every piece of the search pipeline on
startup so individual request handlers only need to reach for
``app.state.search_service`` — the facade that knows how to run the
full parse -> retrieve -> rerank -> explain pipeline.

Settings, tokenizer, and index are stashed on ``app.state`` so
downstream handlers can reach them without re-reading env or
importing module-level singletons. Tests that need a fresh state use
:func:`reset_app_state` which rebuilds every component so nothing
leaks between cases.
"""

from fastapi import FastAPI

from src.api.routes_health import router as health_router
from src.api.routes_logs import router as logs_router
from src.api.routes_search import router as search_router
from src.cache.query_cache import QueryCache
from src.config import Settings, get_settings
from src.index.inverted_index import InvertedIndex
from src.index.tokenizer import LogTokenizer
from src.index.trie import PrefixTrie
from src.logging_setup import configure_logging
from src.query.intent import IntentDetector
from src.query.parser import QueryParser
from src.query.synonyms import SynonymExpander
from src.ranking.reranker import MultiFactorReranker
from src.ranking.service_authority import ServiceAuthorityScorer
from src.ranking.severity import SeverityScorer
from src.ranking.temporal import TemporalScorer
from src.ranking.tfidf import TfIdfScorer
from src.service import SearchService


def build_app(settings: Settings | None = None) -> FastAPI:
    """Construct a fresh FastAPI app, optionally with custom settings.

    ``build_app()`` with no argument behaves identically to the
    module-level ``app = build_app()``. Passing a :class:`Settings`
    instance lets tests isolate configuration without mutating the
    cached singleton or the process environment.

    Every component — tokenizer, index, parser, reranker, cache,
    trie, service facade — is constructed once per app so per-
    instance caches (the tfidf ``idf_cache``, the LRU query cache,
    the parser's synonyms dict) are shared across every request.
    Fresh apps start with empty state — by design, since the service
    is single-node in-memory.
    """
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    # ----- Core index + tokenizer ----------------------------------
    # Constructed up front (not lazily on first request) so a health
    # probe immediately after startup reflects a fully-ready app. The
    # index owns a reference to the tokenizer so ingest and search
    # always agree on tokenisation rules.
    tokenizer = LogTokenizer(settings)
    index = InvertedIndex(settings=settings, tokenizer=tokenizer)

    # ----- Query pipeline ------------------------------------------
    # The intent detector compiles its regex buckets at import time,
    # so constructing it here is just a thin __init__ call. The
    # synonym expander loads its default JSON file eagerly so the
    # first request does not pay the disk read.
    synonym_expander = SynonymExpander(
        path=settings.synonyms_path,
        # WordNet is optional — we lean on the curated JSON by default
        # to keep expansion tight (see plan.md §4).
        use_wordnet=False,
    )
    intent_detector = IntentDetector()
    parser = QueryParser(tokenizer, intent_detector, synonym_expander)

    # ----- Ranking primitives --------------------------------------
    # The tfidf scorer owns the lazy ``idf_cache`` and its
    # ``idf_version``, both of which feed the cache-key shape and
    # the stats endpoint. It must be shared across every request.
    tfidf = TfIdfScorer(index, settings)
    temporal = TemporalScorer()
    severity = SeverityScorer(settings)
    service_auth = ServiceAuthorityScorer(settings)
    reranker = MultiFactorReranker(
        index=index,
        tfidf=tfidf,
        temporal=temporal,
        severity=severity,
        service=service_auth,
        settings=settings,
    )

    # ----- Cache + trie + facade -----------------------------------
    cache = QueryCache(max_size=settings.query_cache_size)
    trie = PrefixTrie()
    search_service = SearchService(
        index=index,
        parser=parser,
        reranker=reranker,
        tfidf=tfidf,
        cache=cache,
        trie=trie,
        settings=settings,
    )

    app = FastAPI(title="log-fulltext-search-rerank")
    # Stash on state so later-commit routes/middleware can reach the
    # same instances via ``request.app.state.*`` without importing
    # the cached accessor or the module-level ``app``.
    app.state.settings = settings
    app.state.tokenizer = tokenizer
    app.state.index = index
    app.state.trie = trie
    app.state.search_service = search_service

    app.include_router(health_router)
    app.include_router(logs_router)
    app.include_router(search_router)

    return app


def reset_app_state(app: FastAPI, settings: Settings | None = None) -> None:
    """Rebuild index/trie/cache/service for tests that need a blank slate.

    Tests share the module-level ``app`` instance, so without a
    wholesale rebuild the index, trie, and cache would leak state
    between cases. Building a fresh app and copying its attribute
    graph onto the existing ``app.state`` is simpler than manually
    wiring every dependency.
    """
    new_app = build_app(settings)
    for key in ("settings", "tokenizer", "index", "trie", "search_service"):
        setattr(app.state, key, getattr(new_app.state, key))


# Module-level app instance for ``uvicorn src.main:app``. The Dockerfile
# CMD and ``start.sh`` both import this symbol directly.
app = build_app()
