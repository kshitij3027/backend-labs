"""Synonym expansion for query tokens.

Two sources of synonyms live here:

1. A JSON dictionary shipped alongside this module
   (``synonyms.default.json``) that maps each head word to a small list
   of alternates. This is the primary source and is deliberately small
   — tuned to the five intent buckets listed in
   ``project_requirements.md`` §2.
2. An optional WordNet fallback (gated by ``use_wordnet=True``) that
   runs only for head words absent from the dict. Pulling from WordNet
   can balloon query size quickly, so the fallback is capped per-token
   and overall.

Both sources respect the same caps:

- ``_MAX_EXPANSIONS_PER_TOKEN``: at most this many synonyms per input token.
- ``_MAX_TOTAL_EXPANSIONS``: at most this many total expansion terms
  added across the entire query.

Capping matters because the retriever fans out across the posting
lists of every token in the expanded set; an unbounded expansion would
make a single-word query sweep half the index.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable


DEFAULT_SYNONYMS_PATH = Path(__file__).parent / "synonyms.default.json"
# Cap per token and total to avoid query explosion.
_MAX_EXPANSIONS_PER_TOKEN = 3
_MAX_TOTAL_EXPANSIONS = 12


class SynonymExpander:
    """Expand query tokens with hand-curated synonyms.

    Loads a JSON dict of ``{token: [synonym, ...]}`` from the configured
    path (defaulting to ``synonyms.default.json`` shipped with this
    module). Optional ``use_wordnet`` pulls a small list of WordNet
    synsets for tokens absent from the dict — still capped at
    ``_MAX_EXPANSIONS_PER_TOKEN`` to keep queries bounded.

    The constructor is defensive: a missing or malformed JSON file
    silently falls back to an empty dict so a bad config never wedges
    the service. Expansion itself never raises — failures in the
    WordNet path return an empty list.
    """

    def __init__(self, path: str | None = None, use_wordnet: bool = False) -> None:
        self._use_wordnet = use_wordnet
        p = Path(path) if path else DEFAULT_SYNONYMS_PATH
        raw: dict[str, list[str]] = {}
        if p.exists():
            try:
                loaded = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    raw = loaded
            except json.JSONDecodeError:
                # Garbled config shouldn't wedge the service — fall back
                # to an empty dict and keep serving queries.
                raw = {}
        self._dict: dict[str, list[str]] = {
            k.lower(): [s.lower() for s in v]
            for k, v in raw.items()
            if isinstance(v, list)
        }

    def expand(self, tokens: Iterable[str]) -> list[str]:
        """Return the input tokens plus dedup'd synonyms, capped.

        Ordering: each input token appears first, immediately followed
        by its own synonyms (in dict order), before moving on to the
        next input token. Deduping is case-insensitive (everything is
        lowercased on the way in).

        The caps are applied greedily: once ``_MAX_TOTAL_EXPANSIONS``
        is hit, no further synonyms are added but already-seen input
        tokens continue to be emitted (callers always get back at
        least every unique input token).
        """
        out: list[str] = []
        seen: set[str] = set()
        total_added = 0
        for tok in tokens:
            tl = tok.lower()
            if tl not in seen:
                out.append(tl)
                seen.add(tl)
            added_this_tok = 0
            extras = self._dict.get(tl, [])
            if self._use_wordnet and not extras:
                extras = self._wordnet_synonyms(tl)
            for s in extras:
                if added_this_tok >= _MAX_EXPANSIONS_PER_TOKEN:
                    break
                if total_added >= _MAX_TOTAL_EXPANSIONS:
                    break
                if s in seen:
                    continue
                out.append(s)
                seen.add(s)
                added_this_tok += 1
                total_added += 1
        return out

    @staticmethod
    def _wordnet_synonyms(token: str) -> list[str]:
        """Return up to ``_MAX_EXPANSIONS_PER_TOKEN`` WordNet synonyms.

        A soft import of ``nltk.corpus.wordnet`` means tests running
        without the corpus baked in (or with NLTK uninstalled) still
        succeed — they just get an empty list.
        """
        try:
            from nltk.corpus import wordnet
        except Exception:  # noqa: BLE001 — any import error is a fallback signal
            return []
        found: list[str] = []
        try:
            for syn in wordnet.synsets(token):
                for lemma in syn.lemmas():
                    name = lemma.name().replace("_", " ").lower()
                    if name != token and name not in found:
                        found.append(name)
                        if len(found) >= _MAX_EXPANSIONS_PER_TOKEN:
                            return found
        except Exception:  # noqa: BLE001 — corpus not loaded etc.
            return []
        return found


__all__ = ("SynonymExpander",)
