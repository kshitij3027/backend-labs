"""Entity-recognition analyzer — spaCy statistical NER layered with log-specific rules.

A raw ops log line carries two very different kinds of named entity:

* **General language entities** (GPE, ORG, PERSON, DATE, TIME, ...) that spaCy's
  statistical ``ner`` model already recognises well.
* **Log-specific entities** (SERVICE, HOST, IP, USER_ID, ERROR_CODE, PATH, URL, PORT)
  that the statistical model has never seen — it typically mis-labels an IP as
  ``CARDINAL`` or a ``user 4821`` id as a plain number.

Rather than train a custom NER model, this analyzer keeps spaCy's general ``ner`` and
layers **deterministic rules** on top of it, in one pipeline loaded once:

    tok2vec → entity_ruler (before ner) → ner → log_entity_regex (last)

**1. EntityRuler (before ``ner``).** Token-shaped known entities that align cleanly to
spaCy tokens: a SERVICE *gazetteer* (matched as phrase patterns, so it is robust to
however spaCy tokenises a hyphenated name like ``payments-api``) plus a ``-svc``/``-api``
shape pattern, and ERROR_CODE token patterns (``E\\d{3,5}`` / ``ERR_[A-Z_]+``). Running
before ``ner`` means these win over the statistical model for their tokens.

**2. ``log_entity_regex`` component (last).** The entities whose surface spaCy's
tokenizer splits unpredictably — IP, URL, PATH, PORT, HOST and the *contextual* USER_ID
(bare digits after ``user``/``for``/``by``, plus the ``u_1234`` / ``user-12`` shapes) —
are matched with :mod:`re` against ``doc.text`` and projected back onto the document with
``doc.char_span(start, end, label=..., alignment_mode="expand")`` (a match that cannot be
aligned yields ``None`` and is skipped). This is tokenizer-independent: the char span
snaps to whatever token boundaries exist.

**Priority rule (log entities win on overlap).** The regex matches are resolved among
themselves in a fixed priority order (URL ▶ PATH ▶ IP ▶ ERROR_CODE ▶ USER_ID ▶ PORT ▶
HOST) by claiming token indices, so a longer container wins over a fragment it encloses
(e.g. the ``web-01`` inside ``http://web-01.internal/status`` stays part of the URL, and
the ``10`` in ``to 10.52.44.216`` is not stolen as a PORT). The surviving regex spans are
then unioned with the ruler/``ner`` entities that do **not** overlap any regex span, and
the whole set is passed through :func:`spacy.util.filter_spans`. Net effect: a log-label
regex span always beats a general spaCy span it overlaps — an IP is an IP, not a
``CARDINAL``; a ``user 4821`` is a USER_ID, not a number — while non-overlapping general
entities (a GPE, an ORG) are preserved.

The spaCy model is loaded once with the tagger/parser/lemmatizer/attribute_ruler disabled
(only ``tok2vec`` + ``ner`` are needed), and :attr:`EntityAnalyzer.nlp` is exposed so the
C7 engine can reuse the single configured instance. Everything here is deterministic: the
model weights and the rules are fixed, and nothing consults the wall clock or the global
RNG.
"""

from __future__ import annotations

import re

import spacy
from spacy.language import Language
from spacy.tokens import Doc, Span
from spacy.util import filter_spans

# ---------------------------------------------------------------------------------------
# The custom log-entity labels this analyzer targets. Mirrors ``generators.ENTITY_LABELS``
# by value — but production NLP code stays self-contained and never imports the (test-only)
# corpus generator, so the tuple is duplicated here on purpose.
# ---------------------------------------------------------------------------------------
LOG_LABELS: frozenset[str] = frozenset(
    {"SERVICE", "HOST", "IP", "USER_ID", "ERROR_CODE", "PATH", "URL", "PORT"}
)

# The spaCy components ``ner`` does not need. ``tok2vec`` feeds ``ner`` and is kept.
_DISABLED_PIPES: tuple[str, ...] = ("tagger", "parser", "lemmatizer", "attribute_ruler")

# ---------------------------------------------------------------------------------------
# EntityRuler patterns (matched BEFORE ``ner``).
#
# The SERVICE gazetteer is expressed as phrase (string) patterns so the EntityRuler
# tokenises each name the same way the document is tokenised — matching regardless of
# whether spaCy keeps ``payments-api`` as one token or splits it into ``payments`` ``-``
# ``api``. Values mirror ``generators._VOCAB["SERVICE"]``.
# ---------------------------------------------------------------------------------------
_SERVICE_GAZETTEER: tuple[str, ...] = (
    "auth-svc", "payments-api", "gateway", "inventory-svc", "notifications",
    "user-svc", "billing-svc", "search-svc", "cache-svc", "order-svc",
)

# ---------------------------------------------------------------------------------------
# Regex rules for the trailing ``log_entity_regex`` component. Each entry is
# ``(label, compiled_pattern, group_index)`` where ``group_index`` selects which capture
# group's char span becomes the entity (0 = whole match). ORDER IS PRIORITY: earlier rules
# claim their tokens first, so a container (URL/PATH/IP) beats a fragment it encloses.
#
# Notes on the shapes (tuned to ``generators._VOCAB`` / the template contexts):
#   * PORT and the contextual USER_ID trigger on connective words (``port``/``to`` /
#     ``user``/``for``/``by``) that the corpus' realism noise may upper/capitalise, so
#     those patterns are case-insensitive.
#   * HOST uses the known prefixes plus a general ``<letters>-<digits>`` fallback, with a
#     ``(?!user-)`` guard so a ``user-88`` USER_ID is never swallowed as a host.
#   * The bare ``:port`` shape is intentionally NOT matched: it would tag the seconds in a
#     ``12:00:00`` timestamp; the corpus only ever writes ``port N`` / ``changed to N``.
# ---------------------------------------------------------------------------------------
_RE_URL = re.compile(r"https?://[^\s]+")
_RE_PATH = re.compile(r"(?:/[\w.\-]+)+")
_RE_IP = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_RE_ERROR = re.compile(r"\bE\d{3,5}\b|\bERR_[A-Z_]+\b")
_RE_USER_SHAPE = re.compile(r"\b(?:u_\d+|user-\d+)\b", re.IGNORECASE)
_RE_USER_CTX = re.compile(r"\b(?:user|for|by)\s+(\d{2,6})\b", re.IGNORECASE)
_RE_PORT = re.compile(r"(?:\bport\s+|\bto\s+)(\d{2,5})\b", re.IGNORECASE)
_RE_HOST = re.compile(
    r"\b(?!user-)(?:web|db|cache|worker|app|host|node|svc|api|lb|queue|edge|gw|[a-z]{2,})-\d{1,3}\b",
    re.IGNORECASE,
)

_REGEX_RULES: tuple[tuple[str, re.Pattern[str], int], ...] = (
    ("URL", _RE_URL, 0),
    ("PATH", _RE_PATH, 0),
    ("IP", _RE_IP, 0),
    ("ERROR_CODE", _RE_ERROR, 0),
    ("USER_ID", _RE_USER_SHAPE, 0),
    ("USER_ID", _RE_USER_CTX, 1),
    ("PORT", _RE_PORT, 1),
    ("HOST", _RE_HOST, 0),
)

# Labels whose match may capture trailing sentence punctuation to trim before projection.
_TRIMMABLE: frozenset[str] = frozenset({"URL", "PATH"})
_TRAIL_PUNCT: str = ".,;:!?)]}>\"'"

# Name under which the trailing regex component is registered with spaCy.
_REGEX_COMPONENT: str = "log_entity_regex"


def _token_indices(span: Span) -> set[int]:
    """The set of token indices a span covers (used for overlap resolution)."""
    return set(range(span.start, span.end))


@Language.component(_REGEX_COMPONENT)
def log_entity_regex(doc: Doc) -> Doc:
    """Overlay deterministic log-entity spans, letting them win over general NER on overlap.

    Runs last in the pipeline. It:

    1. Scans ``doc.text`` with :data:`_REGEX_RULES` (priority order) and projects each match
       back with ``doc.char_span(..., alignment_mode="expand")``, skipping any that cannot
       be aligned (``None``).
    2. Resolves the regex spans among themselves by claiming token indices in priority
       order, so a higher-priority / enclosing span wins (URL over the HOST inside it, an
       IP over a PORT fragment, ...).
    3. Rebuilds ``doc.ents`` as ``filter_spans(regex_spans + existing_ents_not_overlapping
       a_regex_span)`` — the log labels replace any general entity they overlap, while
       non-overlapping ruler/``ner`` entities (SERVICE, GPE, ORG, ...) are preserved.

    Never raises on odd input: an empty document simply yields no regex spans.
    """
    text = doc.text
    if not text:
        return doc

    # (1) collect candidate spans in priority order (rule order, then left-to-right).
    candidates: list[Span] = []
    for label, pattern, group in _REGEX_RULES:
        for match in pattern.finditer(text):
            start, end = match.span(group)
            if start < 0:  # capture group did not participate
                continue
            if label in _TRIMMABLE:
                while end > start and text[end - 1] in _TRAIL_PUNCT:
                    end -= 1
            if end <= start:
                continue
            span = doc.char_span(start, end, label=label, alignment_mode="expand")
            if span is not None:
                candidates.append(span)

    # (2) resolve regex spans against each other by priority (claim tokens first-come).
    regex_spans: list[Span] = []
    claimed: set[int] = set()
    for span in candidates:
        tokens = _token_indices(span)
        if tokens & claimed:
            continue
        regex_spans.append(span)
        claimed |= tokens

    # (3) keep only the ruler/ner entities that don't collide with a regex span, then merge.
    kept = [ent for ent in doc.ents if not (_token_indices(ent) & claimed)]
    doc.ents = filter_spans(regex_spans + kept)
    return doc


class EntityAnalyzer:
    """Recognise general + log-specific named entities in a single log line.

    Loads and configures the spaCy pipeline **once** in :meth:`__init__` (expensive), then
    :meth:`analyze` / :meth:`analyze_batch` are cheap. The configured model is exposed as
    :attr:`nlp` so the C7 engine can share one instance across analyzers.
    """

    def __init__(self, nlp: Language | None = None, model: str = "en_core_web_sm") -> None:
        """Build (or adopt) and configure the pipeline once.

        Args:
            nlp: An already-loaded spaCy ``Language`` to configure in place (tests and the
                C7 engine inject one to avoid re-loading the model). When ``None`` the
                ``model`` is loaded with the unused components disabled.
            model: The spaCy model name to load when ``nlp`` is not supplied.
        """
        if nlp is None:
            self.nlp: Language = spacy.load(model, disable=list(_DISABLED_PIPES))
        else:
            self.nlp = nlp
        self._configure(self.nlp)

    @staticmethod
    def _ruler_patterns() -> list[dict]:
        """The EntityRuler pattern list: SERVICE gazetteer + shapes, and ERROR_CODE."""
        patterns: list[dict] = [
            {"label": "SERVICE", "pattern": name} for name in _SERVICE_GAZETTEER
        ]
        # SERVICE shape: ``<word> - svc|api`` — a multi-token pattern so it still matches
        # when spaCy splits the hyphen (``auth`` ``-`` ``svc``); generalises beyond the
        # gazetteer to unseen ``*-svc`` / ``*-api`` service names.
        patterns.append(
            {
                "label": "SERVICE",
                "pattern": [
                    {"LOWER": {"REGEX": r"^[a-z0-9]+$"}},
                    {"TEXT": "-"},
                    {"LOWER": {"IN": ["svc", "api"]}},
                ],
            }
        )
        # ERROR_CODE (belt-and-suspenders with the regex component): single-token shapes.
        patterns.append({"label": "ERROR_CODE", "pattern": [{"TEXT": {"REGEX": r"^E\d{3,5}$"}}]})
        patterns.append({"label": "ERROR_CODE", "pattern": [{"TEXT": {"REGEX": r"^ERR_[A-Z_]+$"}}]})
        # USER_ID token shapes (``u_1002`` / ``user-88``). These fire when spaCy keeps the id
        # as one token; the regex component (which is tokenizer-independent) is the primary
        # path and wins on overlap, so this is belt-and-suspenders — but it lets the ruler
        # own these ids before ``ner`` can mislabel them.
        patterns.append({"label": "USER_ID", "pattern": [{"TEXT": {"REGEX": r"^u_\d+$"}}]})
        patterns.append({"label": "USER_ID", "pattern": [{"TEXT": {"REGEX": r"^user-\d+$"}}]})
        return patterns

    def _configure(self, nlp: Language) -> None:
        """Add the EntityRuler (before ``ner``) and the regex component (last), idempotently.

        Guarded by ``pipe_names`` membership so re-configuring an already-wired model (an
        injected, previously-configured ``nlp``) is a no-op rather than a double-add error.
        """
        if "entity_ruler" not in nlp.pipe_names:
            before = "ner" if "ner" in nlp.pipe_names else None
            ruler = nlp.add_pipe(
                "entity_ruler",
                before=before,
                config={"phrase_matcher_attr": "LOWER"},
            )
            ruler.add_patterns(self._ruler_patterns())
        if _REGEX_COMPONENT not in nlp.pipe_names:
            nlp.add_pipe(_REGEX_COMPONENT, last=True)

    @staticmethod
    def _entities(doc: Doc) -> list[dict]:
        """Project ``doc.ents`` to the analyzer's dict shape (offsets are char offsets)."""
        return [
            {
                "text": ent.text,
                "label": ent.label_,
                "start": ent.start_char,
                "end": ent.end_char,
            }
            for ent in doc.ents
        ]

    def analyze(self, text: str) -> list[dict]:
        """Return the entities in ``text`` as ``[{text, label, start, end}, ...]``.

        For every returned entity ``text[start:end] == entity["text"]``. Log-specific
        labels win over general spaCy labels on overlap. Empty / whitespace-only input
        returns ``[]`` and no input ever raises.
        """
        if not text or not text.strip():
            return []
        return self._entities(self.nlp(text))

    def analyze_batch(self, texts: list[str]) -> list[list[dict]]:
        """Analyze many lines via ``nlp.pipe`` for throughput; one result list per input.

        Same per-item shape as :meth:`analyze` and order-preserving. Empty / whitespace
        items yield ``[]``.
        """
        return [
            [] if (not text or not text.strip()) else self._entities(doc)
            for text, doc in zip(texts, self.nlp.pipe(texts))
        ]
