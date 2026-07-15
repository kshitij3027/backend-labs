"""Sentiment / severity analyzer — VADER, augmented for ops & log text.

A log line's **severity** (``positive`` / ``neutral`` / ``negative`` / ``critical``) is the
"how alarming is this line" axis the dashboard colours and alerts on. It is a property of
the *phrasing*, not the coarse intent: an ``authentication`` line can be a cheerful "login
succeeded" (positive) or a "brute-force attack detected" (critical). This analyzer scores
that axis with **VADER** (:class:`vaderSentiment.vaderSentiment.SentimentIntensityAnalyzer`),
a lexicon+rule sentiment engine.

**Why VADER must be augmented.** VADER's lexicon is tuned for *social-media* language
(emoji, slang, "LOVE this!!!"). Ops log lines share almost none of that vocabulary, so
out-of-the-box VADER scores the overwhelming majority of log lines at compound ``0.0`` —
flat neutral — and is useless as a severity signal. The augmentation is therefore the whole
point: in :data:`OPS_LEXICON` we add the ops/SRE vocabulary (``failed``, ``timeout``,
``segfault``, ``oom``, ``healthy``, ``recovered`` ...) on VADER's native **-4 … +4 valence
scale**, and :meth:`SentimentAnalyzer.__init__` merges it into the analyzer's lexicon once at
construction (``sia.lexicon.update(OPS_LEXICON)``). Our values deliberately *override* any
base-VADER entry for the same word so the scale is ops-consistent. With the augmented lexicon
the same VADER compound-score machinery (booster words, ALL-CAPS emphasis, negation) now
lights up correctly on real log phrasings.

**The hard critical override.** A handful of tokens are, on their own, an unambiguous signal
of a severe event: ``fatal``, ``panic``, ``segfault``, ``outage``, ``oom``, ``data loss``,
``corrupt``, ``critical`` (:data:`CRITICAL_OVERRIDE_TERMS`). If any of these appears as a
whole word (case-insensitive; ``data loss`` as a phrase) the label is forced to
``"critical"`` regardless of the compound score. This is *more* faithful than trusting the
lexical sum, because a single "``FATAL``" is decisive even when surrounding positive words
("service recovered after fatal error") would otherwise dilute the compound above the
critical cutoff. The override is a whole-word regex so ``corrupt`` does not fire on an
unrelated substring and ``oom`` does not fire inside "room".

**Compound → label (when no override fires).** VADER's ``compound`` is a single number in
``[-1, 1]``. We threshold it:

* ``compound <= -0.60`` → ``critical``
* ``compound <= -0.05`` → ``negative``
* ``compound >= +0.05`` → ``positive``
* otherwise             → ``neutral``

**Contract & robustness.** :meth:`analyze` returns ``(label, compound)`` — ``label`` in
:data:`SENTIMENT_LABELS`, ``compound`` the raw VADER score in ``[-1, 1]``. Empty / whitespace
input short-circuits to ``("neutral", 0.0)`` and no input ever raises.

Everything here is **deterministic and self-contained**: VADER is a fixed lexicon + fixed
rules (no wall clock, no RNG, no network — the lexicon ships inside the ``vaderSentiment``
wheel), and production NLP code never imports the (test-only) corpus generator, so the label
set and lexicon are duplicated here on purpose.
"""

from __future__ import annotations

import re

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ---------------------------------------------------------------------------------------
# Label set. Own copy that mirrors ``generators.SENTIMENTS`` by value — production code
# stays self-contained and never imports the corpus generator. Order matches the generator.
# ---------------------------------------------------------------------------------------
SENTIMENT_LABELS: list[str] = ["positive", "neutral", "negative", "critical"]

# ---------------------------------------------------------------------------------------
# Hard severity-override tokens: an unambiguous critical signal on their own. Matched as
# whole words, case-insensitively (``data loss`` as a two-word phrase). Presence of any of
# these forces the ``"critical"`` label regardless of the compound score. Kept as a module
# constant so it is inspectable / testable.
# ---------------------------------------------------------------------------------------
CRITICAL_OVERRIDE_TERMS: frozenset[str] = frozenset(
    {"fatal", "panic", "segfault", "outage", "oom", "data loss", "corrupt", "critical"}
)

# ---------------------------------------------------------------------------------------
# Compound-score thresholds (see the module docstring). VADER ``compound`` is in [-1, 1].
# ---------------------------------------------------------------------------------------
CRITICAL_THRESHOLD: float = -0.60
NEGATIVE_THRESHOLD: float = -0.05
POSITIVE_THRESHOLD: float = 0.05

# ---------------------------------------------------------------------------------------
# The ops-vocabulary lexicon merged into VADER at construction. Values are on VADER's native
# valence scale (roughly -4 … +4). These OVERRIDE any base-VADER entry for the same word, so
# the severity scale is ops-consistent. Grouped for readability; every value stays in range.
#
# The set is deliberately a superset of the corpus vocabulary: it also carries common SRE
# terms the corpus does not use (``overloaded``, ``unresponsive`` ...) so the analyzer is
# faithful on *real* logs in the dashboard, not only on the synthetic corpus. Terms marked
# "(corpus)" were added specifically so a generator template scores in its ground-truth
# polarity — including token variants VADER looks up individually (``successfully`` is a
# different token from ``successful``; ``throttling`` from ``throttled``) and the synonym
# surfaces the generator swaps in (``errored``/``severed``/``malformed`` ...).
# ---------------------------------------------------------------------------------------
OPS_LEXICON: dict[str, float] = {
    # --- failure / error (negative) ---
    "failed": -3.0, "failure": -3.0, "fail": -2.5, "fails": -2.5,
    "errored": -3.0,                                   # (corpus) "failed"->"errored" synonym
    "error": -2.5, "errors": -2.4,
    "exception": -2.0, "exceptions": -2.0, "unhandled": -1.5,
    "invalid": -1.5, "malformed": -1.5,                # (corpus) invalid password/config/token
    "misconfigured": -1.8,
    # --- connectivity / availability (negative) ---
    "timeout": -2.5, "timed": -2.5,                    # (corpus) "timed out"
    "refused": -2.5, "rejected": -2.0, "denied": -2.5,
    "unavailable": -2.5, "unreachable": -2.5, "unresponsive": -2.5,
    "lost": -1.5, "loss": -1.5,                         # (corpus) connection lost / packet loss
    "dropped": -1.5, "severed": -2.0,                  # (corpus) "lost"->"dropped"/"severed"
    "partition": -2.0,                                 # (corpus) network partition (split-brain)
    "lag": -1.0, "lagging": -1.2, "stale": -1.2, "expired": -1.5, "expire": -1.2,
    # --- severe / crash (negative → usually critical) ---
    "critical": -3.5, "fatal": -3.8, "panic": -3.8, "segfault": -3.2,
    "crash": -3.0, "crashed": -3.0, "crashing": -3.0,
    "corrupt": -2.8, "corrupted": -2.8, "corruption": -2.8,
    "outage": -3.0, "oom": -3.0, "deadlock": -2.8, "deadlocked": -2.8,
    "breach": -3.0, "breached": -3.0, "attack": -2.8,  # (corpus) brute-force attack
    "killed": -2.9,                                    # (corpus) out of memory: ... killed
    # --- resource pressure (negative) ---
    "degraded": -2.0, "degradation": -2.0,
    "saturation": -1.5, "saturated": -1.5,
    "throttled": -1.5, "throttling": -1.5,             # (corpus) cpu throttling
    "overloaded": -2.0, "excessive": -1.5,             # (corpus) "high"->"excessive" synonym
    "slow": -1.5, "sluggish": -1.5,                    # (corpus) slow query
    "warning": -1.0,                                   # (corpus) disk usage warning
    # --- recovery / health (positive) ---
    "success": 2.5, "succeeded": 2.5, "successful": 2.5, "successfully": 2.5,  # (corpus)
    "healthy": 2.0, "recovered": 2.5, "recovery": 1.5, "restored": 2.5, "restore": 1.5,
    "operational": 1.5, "reachable": 1.2, "nominal": 1.2, "stable": 1.5,
    "online": 1.5, "ready": 1.2, "ok": 1.5, "okay": 1.5,
    "passed": 1.5, "pass": 1.0, "completed": 1.5, "complete": 1.2,
    "authenticated": 1.8, "heartbeat": 1.0, "green": 1.2,  # (corpus) auth ok / heartbeat / all green
}


def _build_override_pattern(terms: frozenset[str]) -> re.Pattern[str]:
    """Compile a whole-word, case-insensitive alternation of the override ``terms``.

    Longest-first so a multi-word phrase (``data loss``) is preferred, and ``\\b`` anchored so
    a term matches only as a whole word — ``corrupt`` never fires on ``corrupted`` and ``oom``
    never fires inside ``room``.
    """
    ordered = sorted(terms, key=len, reverse=True)
    alternation = "|".join(re.escape(term) for term in ordered)
    return re.compile(r"\b(?:" + alternation + r")\b", re.IGNORECASE)


#: Precompiled override matcher (built once from :data:`CRITICAL_OVERRIDE_TERMS`).
_OVERRIDE_RE: re.Pattern[str] = _build_override_pattern(CRITICAL_OVERRIDE_TERMS)

#: A neutral, zero-valued scores dict returned for empty input by :meth:`SentimentAnalyzer.scores`.
_EMPTY_SCORES: dict[str, float] = {"neg": 0.0, "neu": 0.0, "pos": 0.0, "compound": 0.0}


def _has_critical_override(text: str) -> bool:
    """True if ``text`` contains any :data:`CRITICAL_OVERRIDE_TERMS` token as a whole word."""
    return _OVERRIDE_RE.search(text) is not None


def _label_for(text: str, compound: float) -> str:
    """Map a message + its compound score to one of :data:`SENTIMENT_LABELS`.

    The hard override wins first; otherwise the compound thresholds decide.
    """
    if _has_critical_override(text):
        return "critical"
    if compound <= CRITICAL_THRESHOLD:
        return "critical"
    if compound <= NEGATIVE_THRESHOLD:
        return "negative"
    if compound >= POSITIVE_THRESHOLD:
        return "positive"
    return "neutral"


class SentimentAnalyzer:
    """Score a log line's severity/sentiment with ops-augmented VADER.

    Builds one :class:`SentimentIntensityAnalyzer` in :meth:`__init__` and merges
    :data:`OPS_LEXICON` into its lexicon (the expensive-ish step, done once); :meth:`analyze`
    is then a cheap per-line call. The instance is stateless across calls and deterministic.
    """

    def __init__(self) -> None:
        """Construct the VADER analyzer once and augment its lexicon with the ops vocabulary.

        ``sia.lexicon`` is a plain ``{word: valence}`` dict; :meth:`dict.update` merges our
        ops terms in, overriding any base-VADER entry for the same word so the valence scale
        is ops-consistent.
        """
        self._sia = SentimentIntensityAnalyzer()
        self._sia.lexicon.update(OPS_LEXICON)

    def scores(self, text: str) -> dict[str, float]:
        """Return the full VADER score dict ``{"neg", "neu", "pos", "compound"}``.

        Handy for the dashboard (it can show the pos/neg breakdown, not just the label).
        Empty / whitespace-only input returns an all-zero dict; never raises.
        """
        if not text or not text.strip():
            return dict(_EMPTY_SCORES)
        return self._sia.polarity_scores(text)

    def analyze(self, text: str) -> tuple[str, float]:
        """Return ``(label, compound)`` for one log line.

        ``label`` is one of :data:`SENTIMENT_LABELS`; ``compound`` is VADER's raw compound
        score in ``[-1, 1]``. The hard critical override (see the module docstring) takes
        precedence over the compound thresholds. Empty / whitespace-only input returns
        ``("neutral", 0.0)`` and no input ever raises.
        """
        if not text or not text.strip():
            return ("neutral", 0.0)
        compound = float(self._sia.polarity_scores(text)["compound"])
        return (_label_for(text, compound), compound)
