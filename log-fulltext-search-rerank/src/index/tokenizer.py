"""Compound-preserving tokenizer for log text.

Log lines mix structured fragments (IP addresses, UUIDs, URLs, dotted
package identifiers, ISO-8601 timestamps, HTTP status codes) with
free-form English prose. A naive ``text.split()`` or purely word-level
tokenizer shreds those structured fragments into meaningless
sub-tokens — splitting ``10.0.0.1`` into ``10 0 0 1`` destroys the
signal that "that IP showed up on twelve error lines".

:class:`LogTokenizer` performs a single regex pass that matches the
structured patterns in priority order, then treats the remainder as
plain words (lowercased, stopwords removed, lemmatized through
WordNet). The regex alternation is ordered — earlier groups win on
overlap — so the dotted-identifier pattern cannot accidentally swallow
a URL's ``example.com``.

The tokenizer exposes two methods so corpus indexing and query-time
handling can diverge: :meth:`tokenize` drops short tokens (they bloat
the index without adding signal), while :meth:`tokenize_query` keeps
them so short intent words like ``log`` / ``api`` / ``500`` survive
into the intent detector.

NLTK corpora are baked into the runtime image at ``/usr/share/nltk_data``
(see ``Dockerfile``) and the ``NLTK_DATA`` env var points there. This
module **never** calls ``nltk.download()`` — doing so at request time
would be both slow and offline-hostile.
"""

from __future__ import annotations

import re
from typing import Iterable, Iterator

from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

from src.config import Settings


# Compound patterns in priority order. Each group name labels the
# semantic category so callers can later branch on it — the commit-06
# query parser, for example, may want to skip lemmatization on dotted
# identifiers it already knows are package paths.
#
# Ordering rationale:
# - ``ipv4`` before ``dotted`` so ``10.0.0.1`` is matched as an IP, not
#   a four-segment dotted identifier (which it isn't — the leading
#   segment doesn't start with a letter anyway, but the priority is
#   belt-and-braces).
# - ``iso_ts`` before ``num`` so a timestamp's year segment isn't
#   harvested as a standalone number.
# - ``uuid`` before ``dotted`` / ``num`` so hex segments stay fused.
# - ``url`` before ``email`` / ``dotted`` so ``https://example.com/x``
#   does not get sliced up around its dots and slashes.
# - ``email`` before ``dotted`` so ``user@example.com`` stays whole.
# - ``http_code`` before ``num`` so ``500`` / ``404`` become first-class
#   tokens rather than being lumped with arbitrary numbers.
# - ``word`` before ``num`` — they are disjoint on first character, so
#   the order is cosmetic here.
_COMPOUND_PATTERN = re.compile(
    r"""
      (?P<ipv4>\b\d{1,3}(?:\.\d{1,3}){3}\b)
    | (?P<iso_ts>\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?\b)
    | (?P<uuid>\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b)
    | (?P<url>https?://[^\s<>"']+)
    | (?P<email>\b[\w.+-]+@[\w-]+(?:\.[\w-]+)+\b)
    | (?P<dotted>\b[A-Za-z_][\w]*(?:\.[A-Za-z_][\w]*)+\b)
    | (?P<http_code>\b[1-5]\d{2}\b)
    | (?P<word>[A-Za-z][A-Za-z']+)
    | (?P<num>\b\d+\b)
    """,
    re.VERBOSE,
)


# Strip leading / trailing punctuation off plain-word matches while
# preserving interior apostrophes (e.g. ``don't``). The character
# classes use ``\w`` to include underscores, which are unusual but
# harmless inside identifiers.
_STRIP_EDGE_PUNCT = re.compile(r"^[^\w]+|[^\w']+$")


# Compound group names that must be emitted verbatim (lowercased only).
# Factored out as a module-level frozenset so the hot path does a
# single set-membership lookup instead of building a tuple per
# iteration.
_COMPOUND_KINDS: frozenset[str] = frozenset(
    {"ipv4", "iso_ts", "uuid", "url", "email", "dotted", "http_code"}
)


# Cap for the per-instance lemma cache. 50k tokens comfortably covers
# production-size English vocabularies (Google's one-million-word
# corpus has a long tail, but in practice log vocabularies top out in
# the low tens of thousands). The cap exists to prevent adversarial
# inputs from making the cache grow without bound.
_LEMMA_CACHE_CAP = 50_000


class LogTokenizer:
    """Compound-preserving tokenizer for log text.

    One regex pass extracts structured tokens (IPs, UUIDs, URLs,
    dotted identifiers, ISO timestamps, HTTP status codes, emails)
    verbatim, and plain words flow through a stopword filter plus
    WordNet lemmatization. Per-instance lemma caching keeps the hot
    path cheap on repeated tokens.

    The tokenizer is stateful (stopword set, lemmatizer, cache) but
    not synchronized — treat an instance as owned by whoever created
    it. The inverted index, for example, keeps exactly one tokenizer
    on its ``self.``.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # Freeze the stopword list into an immutable set so the lookups
        # on the hot path are O(1) and the allocation happens once.
        self._stopwords: frozenset[str] = frozenset(
            stopwords.words(settings.stop_words_language)
        )
        self._lemmatizer = WordNetLemmatizer()
        # Plain ``dict`` rather than ``functools.lru_cache``: a method
        # lru_cache keeps ``self`` alive through the cache, which leaks
        # the tokenizer and its stopword set on instance churn. A
        # manually-bounded dict avoids that and keeps eviction trivial.
        self._lemma_cache: dict[str, str] = {}

    def tokenize(self, text: str) -> list[str]:
        """Return corpus tokens for ``text``.

        Applies the full pipeline: compound preservation, lowercasing,
        stopword removal, lemmatization, and a strict minimum-length
        filter (tokens shorter than :attr:`Settings.min_token_length`
        are dropped). Used for indexing.
        """

        return list(self._iter_tokens(text, min_length=self._settings.min_token_length))

    def tokenize_query(self, text: str) -> list[str]:
        """Return query tokens for ``text``.

        Same pipeline as :meth:`tokenize` but with the minimum-length
        filter relaxed to 1 so short intent words like ``log``,
        ``api``, ``500``, or ``go`` survive into the intent detector.
        The intent classifier pattern-matches on these short tokens
        so dropping them at ingest-time would silently break it.
        """

        return list(self._iter_tokens(text, min_length=1))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _iter_tokens(self, text: str, *, min_length: int) -> Iterator[str]:
        """Yield tokens from ``text`` respecting ``min_length``.

        Single ``finditer`` pass over the compiled alternation — the
        regex engine itself handles the priority ordering, so we never
        need to re-scan the same span with a different pattern.
        """

        if not text:
            return
        for match in _COMPOUND_PATTERN.finditer(text):
            kind = match.lastgroup
            raw = match.group(0)
            if kind in _COMPOUND_KINDS:
                # Emit compounds verbatim (lowercased for case
                # stability — tests and downstream consumers expect a
                # deterministic case). We deliberately skip the
                # stopword filter: an identifier like ``a.b.c`` is not
                # a stopword candidate even if its segments are.
                yield raw.lower()
                continue
            if kind == "num":
                # Numbers that are not HTTP codes still have signal
                # (error codes, PIDs, counts) but we do respect the
                # minimum-length filter so single-digit noise doesn't
                # bloat the index.
                if len(raw) >= min_length:
                    yield raw
                continue
            # ``kind == "word"`` from here on: strip edge punctuation,
            # lowercase, drop stopwords, lemmatize, length-filter.
            word = _STRIP_EDGE_PUNCT.sub("", raw).lower()
            if not word:
                continue
            if word in self._stopwords:
                continue
            lemma = self._lemma(word)
            if len(lemma) < min_length:
                continue
            yield lemma

    def _lemma(self, token: str) -> str:
        """Return the WordNet lemma for ``token`` with per-instance memoization.

        Uses default POS (noun), which is the simplest setting and
        matches what the rest of the reranker assumes. Cache size is
        bounded at :data:`_LEMMA_CACHE_CAP`; eviction is FIFO (oldest
        insertion first, leveraging CPython's insertion-ordered
        ``dict``).
        """

        cached = self._lemma_cache.get(token)
        if cached is not None:
            return cached
        lemma = self._lemmatizer.lemmatize(token)
        if len(self._lemma_cache) >= _LEMMA_CACHE_CAP:
            # Drop the oldest entry. ``next(iter(...))`` fetches the
            # first inserted key — Python 3.7+ guarantees insertion
            # order on plain dicts, so this is FIFO without needing
            # ``collections.OrderedDict``.
            self._lemma_cache.pop(next(iter(self._lemma_cache)))
        self._lemma_cache[token] = lemma
        return lemma


# Export surface — keep it small so import-cycle debugging stays easy.
__all__: Iterable[str] = ("LogTokenizer",)
