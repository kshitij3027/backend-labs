"""Top-level query parser.

Composes the tokenizer, intent detector, and synonym expander into a
single ``parse(raw) -> ParsedQuery`` step that downstream layers
(retriever, reranker, service facade) can consume without knowing the
internals.

Intent detection deliberately runs on the **raw** query string rather
than the tokenized form — lemmatization and stopword filtering can hide
intent keywords (``errors`` -> ``error`` is fine, but ``no errors`` ->
``error`` loses the negation context, and ``the payment failed`` ->
``payment failed`` trims the articles we might one day want). Keeping
the intent pass on raw text keeps that signal intact. Token extraction
uses :meth:`LogTokenizer.tokenize_query` (not :meth:`tokenize`) so
short intent-relevant words like ``api``, ``go``, ``500`` survive.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from src.index.tokenizer import LogTokenizer
from src.query.intent import IntentDetector
from src.query.synonyms import SynonymExpander


@dataclass
class ParsedQuery:
    """Structured representation of a user query.

    Attributes:
        raw: The original query string, untouched.
        tokens: Tokens produced by :meth:`LogTokenizer.tokenize_query`.
        expanded_tokens: ``tokens`` plus synonym expansions, deduped.
        intent: The detected intent bucket (one of the five in
            ``project_requirements.md`` §2).
        metadata: Free-form room for future signals (e.g. detected
            entities, negations, context hints) without churning the
            dataclass shape.
    """

    raw: str
    tokens: list[str]
    expanded_tokens: list[str]
    intent: str
    metadata: dict = field(default_factory=dict)


class QueryParser:
    """Stitch the tokenizer, intent detector, and expander together.

    The parser owns nothing — all three dependencies are injected so
    tests can swap in fakes and so production wiring can share a
    single :class:`LogTokenizer` between the index writer and the
    parser (they both rely on the same baked NLTK corpus).
    """

    def __init__(
        self,
        tokenizer: LogTokenizer,
        intent: IntentDetector,
        synonyms: SynonymExpander,
    ) -> None:
        self._tokenizer = tokenizer
        self._intent = intent
        self._synonyms = synonyms

    def parse(self, raw: str) -> ParsedQuery:
        """Return a :class:`ParsedQuery` for ``raw``.

        The pipeline:

        1. Detect intent on the **raw** string (lemmas can hide it).
        2. Tokenize via ``tokenize_query`` so short intent words
           survive.
        3. Expand the tokens through the synonym dict.

        An empty ``raw`` string yields an empty token list, an empty
        expansion list, and the ``general_search`` fallback intent.
        """
        intent_label = self._intent.detect(raw)
        tokens = self._tokenizer.tokenize_query(raw)
        expanded = self._synonyms.expand(tokens)
        return ParsedQuery(
            raw=raw,
            tokens=tokens,
            expanded_tokens=expanded,
            intent=intent_label,
        )


__all__ = ("ParsedQuery", "QueryParser")
