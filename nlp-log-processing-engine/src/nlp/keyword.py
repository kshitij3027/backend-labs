"""Keyword / keyphrase extractor — YAKE, unsupervised and fully self-contained (C6).

A log line's **keywords** are the handful of salient surface phrases that summarise "which
words matter here" — ``invalid token``, ``connection pool``, ``disk full`` — the axis the
dashboard's search, per-line tag chips and *trending* panel are built on. Unlike intent (a
trained classifier) or sentiment (a lexicon), keywords are extracted **per line, with no
training and no persisted model artifact** using **YAKE** (Yet Another Keyword Extractor,
:class:`yake.KeywordExtractor`).

**Why YAKE — and specifically not RAKE / rake-nltk.** YAKE is *unsupervised*,
*single-document* and *statistical*: it scores candidate n-grams from one text using term
casing, position, frequency, in-context relatedness and sentence dispersion — no corpus, no
training, no external model. Crucially it is **self-contained**: it ships its own stop-word
lists *inside the wheel* (``yake/StopwordsList/``) and reads **no NLTK data**. ``rake-nltk``,
by contrast, needs ``nltk.download(...)`` corpora at runtime — an extra image layer, a
network fetch, and one more thing that fails in an offline build. Choosing YAKE keeps the
image smaller and removes a whole runtime failure mode. (The keyword unit tests pass inside
the deliberately nltk-free Docker image, which *is* the end-to-end proof that no NLTK data
is required.)

**Lower score is better.** YAKE's score is a *penalty*: the smaller the number, the more
relevant the keyphrase. :meth:`KeywordExtractor.extract_keywords` already returns candidates
best-first (ascending score); :meth:`KeywordAnalyzer.extract_scored` re-sorts defensively so
the "best-first, non-decreasing score" contract holds regardless of YAKE internals, and
de-duplicates case-insensitively while preserving that order.

**Robustness — :meth:`KeywordAnalyzer.extract` never raises.** Empty / whitespace input
short-circuits to ``[]``, and YAKE's failure modes on *degenerate* input (a lone token, a
punctuation-only string — where its internal statistics reduce over an empty term/candidate
set) are caught and turned into ``[]`` rather than propagated. Output is stripped, empties
dropped, de-duplicated (case-insensitive) and capped at ``top_k``.

**:class:`TrendingKeywords`** is a tiny rolling :class:`collections.Counter` of
globally-seen keywords that C8's ``/api/stats`` endpoint reads to render the dashboard's
*trending* panel. It normalises case on the way in and breaks count ties **alphabetically**
so ``top(k)`` is deterministic regardless of the order keywords arrived in.

Everything here is deterministic: YAKE is a fixed statistical algorithm (no RNG, no wall
clock, no network) and — like the rest of the NLP layer — this production module never
imports the (test-only) corpus generator.
"""

from __future__ import annotations

from collections import Counter

import yake

#: Default maximum number of words in an extracted keyphrase (YAKE's ``n``). ``2`` keeps the
#: useful bigrams ("invalid token", "connection pool") while avoiding long, noisy spans.
DEFAULT_MAX_NGRAM: int = 2

#: Default number of keyphrases returned per line (YAKE's ``top``). Five is ample for one log
#: line and for the dashboard's per-line tag chips.
DEFAULT_TOP_K: int = 5


class KeywordAnalyzer:
    """Extract the salient keyphrases from a single log line with YAKE.

    Builds one :class:`yake.KeywordExtractor` in :meth:`__init__` (constructing it loads the
    stop-word list and compiles the feature config — non-trivial, so it is done once) and
    reuses it for every call. The instance is stateless across calls and deterministic:
    :meth:`extract` returns up to ``top_k`` de-duplicated keyphrases best-first, and never
    raises on any input.
    """

    def __init__(
        self,
        max_ngram: int = DEFAULT_MAX_NGRAM,
        top_k: int = DEFAULT_TOP_K,
        lan: str = "en",
    ) -> None:
        """Construct the YAKE extractor once with the given phrase length and result cap.

        Args:
            max_ngram: Maximum words per keyphrase (YAKE's ``n``).
            top_k: Maximum number of keyphrases returned per line (YAKE's ``top``).
            lan: Language whose bundled stop-word list YAKE loads (``"en"``). YAKE reads this
                list from inside its own wheel — never from NLTK.
        """
        self.max_ngram = max_ngram
        self.top_k = top_k
        self.lan = lan
        # One extractor, built once and reused — constructing it loads YAKE's stop-word list
        # and compiles its feature configuration, which we do not want to repeat per line.
        self._extractor = yake.KeywordExtractor(lan=lan, n=max_ngram, top=top_k)

    def extract(self, text: str) -> list[str]:
        """Return up to ``top_k`` keyphrases for one log line, best-first and de-duplicated.

        De-duplication is case-insensitive (surface forms are preserved, best-first order is
        kept). Empty / whitespace / degenerate input returns ``[]``; this method never raises.
        """
        return [phrase for phrase, _score in self.extract_scored(text)]

    def extract_scored(self, text: str) -> list[tuple[str, float]]:
        """Like :meth:`extract` but each phrase is paired with its YAKE score (lower = better).

        Handy for debugging and for the dashboard (it can show relevance, not just the
        phrase). Same guarantees as :meth:`extract`: best-first (non-decreasing score),
        de-duplicated case-insensitively, capped at ``top_k``, and never raises.
        """
        if not text or not text.strip():
            return []
        try:
            scored = self._extractor.extract_keywords(text)
        except Exception:
            # YAKE can raise on degenerate input (a single token, punctuation-only text) when
            # its internal statistics reduce over an empty term/candidate set. The public
            # contract is "never raises", so any such failure becomes an empty result. The
            # broad except is deliberate — we do not want any YAKE edge case to escape.
            return []
        # YAKE already returns candidates best-first (ascending score = more relevant), but
        # re-sort defensively so the best-first / non-decreasing-score contract holds across
        # YAKE versions. sorted() is stable, so score ties keep YAKE's original order.
        scored = sorted(scored, key=lambda kw_score: kw_score[1])
        return self._dedupe(scored)[: self.top_k]

    @staticmethod
    def _dedupe(scored: list[tuple[str, float]]) -> list[tuple[str, float]]:
        """Strip, drop empties, and remove case-insensitive duplicates, preserving order."""
        seen: set[str] = set()
        deduped: list[tuple[str, float]] = []
        for phrase, score in scored:
            cleaned = phrase.strip()
            if not cleaned:
                continue
            key = cleaned.casefold()
            if key in seen:
                continue
            seen.add(key)
            deduped.append((cleaned, float(score)))
        return deduped


class TrendingKeywords:
    """Rolling global keyword frequency for the dashboard's *trending* panel (feeds C8).

    A thin wrapper over :class:`collections.Counter`: :meth:`add` folds each line's keywords
    into a running tally (case-normalised), :meth:`top` returns the most frequent keywords
    with counts, and :meth:`reset` clears the window. Count ties are broken alphabetically so
    :meth:`top` is deterministic no matter what order keywords were added in.
    """

    def __init__(self) -> None:
        """Start with an empty rolling counter of globally-seen keywords."""
        self._counts: Counter[str] = Counter()

    def add(self, keywords: list[str]) -> None:
        """Increment the running count for each keyword in ``keywords`` (case-normalised).

        Keywords are lower-cased (``str.casefold``) so ``"Timeout"`` and ``"timeout"`` share a
        tally; blank / whitespace-only entries are ignored.
        """
        for keyword in keywords:
            normalized = keyword.strip().casefold()
            if normalized:
                self._counts[normalized] += 1

    def top(self, k: int = 10) -> list[tuple[str, int]]:
        """Return the ``k`` most frequent ``(keyword, count)`` pairs, most-frequent first.

        Ordered by descending count, then ascending keyword so ties break alphabetically —
        deterministic regardless of insertion order. ``k <= 0`` returns ``[]``; fewer than
        ``k`` distinct keywords returns all of them.
        """
        if k <= 0:
            return []
        ordered = sorted(self._counts.items(), key=lambda item: (-item[1], item[0]))
        return ordered[:k]

    def reset(self) -> None:
        """Clear all accumulated counts (e.g. to start a fresh trending window)."""
        self._counts.clear()
