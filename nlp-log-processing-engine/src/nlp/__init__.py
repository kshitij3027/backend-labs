"""The NLP layer of the log-processing engine — one analyzer per capability, one engine.

Each capability lives in its own module so it can be built, unit-tested and reasoned
about in isolation:

* :mod:`src.nlp.entity`    — spaCy NER + deterministic log-entity rules (C3).
* :mod:`src.nlp.intent`    — TF-IDF + LogisticRegression intent classifier (C4).
* :mod:`src.nlp.sentiment` — VADER sentiment/severity with an ops lexicon (C5).
* :mod:`src.nlp.keyword`   — YAKE keyword extraction + trending aggregation (C6).

**C7 — :class:`NLPEngine`, the single orchestrator.** This package now exposes one engine
that owns exactly one instance of each analyzer and fans a message out across all four.
Crucially there is **one spaCy model in the whole process**: :class:`EntityAnalyzer` loads
and configures it in its constructor and exposes it as ``.nlp``; the engine holds that one
``EntityAnalyzer`` and never re-loads spaCy anywhere else.

The heavy work — loading spaCy, loading (or training) the intent pipeline, constructing the
VADER analyzer with its augmented lexicon, and building the YAKE extractor — happens **once**
in :meth:`NLPEngine.load`, called at application startup (see ``src.main.Runtime.build_loaded``
and the FastAPI lifespan). After ``load()`` the engine sets :attr:`NLPEngine.ready` so
``GET /api/health`` can report real readiness. Per-message :meth:`NLPEngine.analyze` and the
throughput-oriented :meth:`NLPEngine.analyze_batch` are then cheap and construct nothing.

Everything downstream is deterministic: each analyzer is itself deterministic (fixed model
weights + rules, a seeded/​baked intent pipeline, fixed lexicons, a fixed statistical keyword
algorithm), so a given message always yields the same analysis — no wall clock, no global RNG.
"""

from __future__ import annotations

from src.nlp.entity import EntityAnalyzer
from src.nlp.intent import IntentAnalyzer
from src.nlp.keyword import KeywordAnalyzer, TrendingKeywords
from src.nlp.sentiment import SentimentAnalyzer

#: Precision the reported ``intent.confidence`` / ``sentiment.score`` are rounded to. Four
#: decimals is ample for a UI and keeps JSON payloads tidy without lying about the value.
_ROUND_NDIGITS: int = 4

__all__ = [
    "EntityAnalyzer",
    "IntentAnalyzer",
    "KeywordAnalyzer",
    "NLPEngine",
    "SentimentAnalyzer",
    "TrendingKeywords",
]


class NLPEngine:
    """Single orchestrator over the four analyzers — load once, analyze cheaply.

    Owns one :class:`EntityAnalyzer` (which holds the process's only spaCy model), one
    :class:`IntentAnalyzer`, one :class:`SentimentAnalyzer` and one :class:`KeywordAnalyzer`.
    Construct it, call :meth:`load` at startup (the expensive step — models are built exactly
    once), then :meth:`analyze` / :meth:`analyze_batch` per message.

    :attr:`ready` is ``False`` until :meth:`load` completes; the API reads it for
    ``GET /api/health`` and refuses ``/api/analyze`` with a 503 until it is ``True``.
    """

    def __init__(self) -> None:
        """Create an *unloaded* engine — no models are built until :meth:`load` is called.

        Keeping construction free of I/O means simply importing / instantiating the engine
        (e.g. for typing, or the cheap unit-test Runtime) never loads spaCy or trains a model.
        """
        self.ready: bool = False
        self._entity: EntityAnalyzer | None = None
        self._intent: IntentAnalyzer | None = None
        self._sentiment: SentimentAnalyzer | None = None
        self._keyword: KeywordAnalyzer | None = None

    # -- lifecycle ---------------------------------------------------------------------
    def load(self) -> "NLPEngine":
        """Construct all four analyzers **once** and mark the engine ready; returns ``self``.

        Builds, in order: the :class:`EntityAnalyzer` (loads + configures the single spaCy
        pipeline), the :class:`IntentAnalyzer` via :meth:`IntentAnalyzer.load_or_train` (loads
        the baked artifact if present, else trains on the seeded corpus), the
        :class:`SentimentAnalyzer` (VADER + ops lexicon) and the :class:`KeywordAnalyzer`
        (YAKE). This is the heavy, blocking startup step.

        Idempotent: calling it again on an already-loaded engine is a no-op and does **not**
        re-load or re-train anything. The analyzers are only bound to ``self`` once all four
        have been constructed, so a failure part-way through leaves the engine cleanly
        not-ready rather than half-wired.
        """
        if self.ready:
            return self
        entity = EntityAnalyzer()
        intent = IntentAnalyzer.load_or_train()
        sentiment = SentimentAnalyzer()
        keyword = KeywordAnalyzer()
        # Bind everything only after all four succeed — an exception above leaves ready=False
        # and the attributes untouched (still None), so the engine never ends up half-loaded.
        self._entity = entity
        self._intent = intent
        self._sentiment = sentiment
        self._keyword = keyword
        self.ready = True
        return self

    # -- inference ---------------------------------------------------------------------
    def analyze(self, message: str) -> dict:
        """Analyze one log line into the full response schema.

        Returns ``{message, entities:[{text,label,start,end}], intent:{label,confidence},
        sentiment:{label,score}, keywords:[...]}`` — the shape the ``/api/analyze`` route
        serves. ``confidence`` (``[0, 1]``) and ``score`` (``[-1, 1]``) are rounded to
        :data:`_ROUND_NDIGITS`. Empty / whitespace input is handled by each analyzer's own
        empty-safe contract (no entities/keywords, intent ``"other"`` @ 0.0, sentiment
        ``"neutral"`` @ 0.0), so it yields a valid, fully-populated response and never raises.

        Raises:
            RuntimeError: If :meth:`load` has not been called (the engine is not ready).
        """
        self._require_ready()
        entities = self._entity.analyze(message)  # type: ignore[union-attr]
        intent = self._intent.predict(message)  # type: ignore[union-attr]
        sentiment = self._sentiment.analyze(message)  # type: ignore[union-attr]
        keywords = self._keyword.extract(message)  # type: ignore[union-attr]
        return self._compose(message, entities, intent, sentiment, keywords)

    def analyze_batch(self, messages: list[str]) -> list[dict]:
        """Analyze many log lines efficiently; one result dict per input, order preserved.

        Entities use :meth:`EntityAnalyzer.analyze_batch` (a single ``nlp.pipe`` pass) and
        intents use :meth:`IntentAnalyzer.predict_batch` (one ``predict_proba`` call over the
        batch) — the two per-call-expensive paths are amortised across the whole batch.
        Sentiment and keywords are cheap per-line lexical/statistical passes, so they run
        item-by-item. Each element is identical to what :meth:`analyze` returns for that line.
        An empty list returns ``[]``; a batch containing empty strings is handled per-item.

        Raises:
            RuntimeError: If :meth:`load` has not been called (the engine is not ready).
        """
        self._require_ready()
        entities_batch = self._entity.analyze_batch(messages)  # type: ignore[union-attr]
        intents_batch = self._intent.predict_batch(messages)  # type: ignore[union-attr]
        results: list[dict] = []
        for message, entities, intent in zip(messages, entities_batch, intents_batch):
            sentiment = self._sentiment.analyze(message)  # type: ignore[union-attr]
            keywords = self._keyword.extract(message)  # type: ignore[union-attr]
            results.append(self._compose(message, entities, intent, sentiment, keywords))
        return results

    # -- internals ---------------------------------------------------------------------
    def _require_ready(self) -> None:
        """Raise a clear error if analysis is attempted before :meth:`load`."""
        if not self.ready:
            raise RuntimeError(
                "NLPEngine is not loaded; call load() before analyze()/analyze_batch()."
            )

    @staticmethod
    def _compose(
        message: str,
        entities: list[dict],
        intent: tuple[str, float],
        sentiment: tuple[str, float],
        keywords: list[str],
    ) -> dict:
        """Assemble the per-message response dict from the four analyzers' raw outputs."""
        intent_label, intent_confidence = intent
        sentiment_label, sentiment_score = sentiment
        return {
            "message": message,
            "entities": entities,
            "intent": {
                "label": intent_label,
                "confidence": round(float(intent_confidence), _ROUND_NDIGITS),
            },
            "sentiment": {
                "label": sentiment_label,
                "score": round(float(sentiment_score), _ROUND_NDIGITS),
            },
            "keywords": list(keywords),
        }
