"""Log-text preprocessing for the ML Log Classifier (Commit 3).

This module turns a single *raw* log line — the noisy, timestamped free text
emitted by :mod:`src.log_generator` — into clean, normalized text that is good
fodder for TF-IDF vectorization. The transformation strips high-cardinality
noise (timestamps, IPs, UUIDs, latencies, ports, opaque ``key=<id>`` tokens),
lowercases, normalizes whitespace, tokenizes with NLTK and drops English
stopwords.

Why strip the noise first
-------------------------
A vocabulary built from raw logs would be dominated by tokens that appear at
most once (a UUID, a millisecond count, an ephemeral request id). Those tokens
carry no class signal yet inflate the feature space and dilute the genuinely
discriminative words (``timeout``, ``deadlock``, ``handshake``, ...). Removing
them up front keeps TF-IDF focused on the *semantics* of the message.

What we deliberately keep
-------------------------
The explicit severity word inside the message (``error``, ``warn``, ...) is
*not* specially stripped — as plain text it is legitimate signal. The structured
log level is captured separately by the metadata extractor in a later commit;
here we only treat the text as text.

Public API
----------
* :func:`clean_text` — regex strip of noise + lowercase + whitespace normalize.
  Pure and deterministic; no tokenization.
* :func:`tokenize` — NLTK tokenize + stopword / noise-token removal. Accepts raw
  *or* already-cleaned text.
* :func:`preprocess` — the full pipeline (``clean_text`` → ``tokenize`` → join).
  This is the entry point used by feature extraction and is **idempotent** on
  already-clean text.

NLTK robustness
---------------
The Docker images download ``punkt`` + ``stopwords`` at build time (with
``NLTK_DATA=/usr/share/nltk_data``), so the resources exist at runtime. Loading
is still defensive: resources are lazily initialized and cached at module level,
and if they cannot be found we attempt a quiet on-demand download and, failing
that, fall back to a simple regex tokenizer plus a small built-in stopword set.
The module therefore never hard-crashes when NLTK data is absent, while using
real NLTK whenever it is present.

Input handling
--------------
All public functions coerce non-string input via ``str(...)`` rather than
raising. Empty / whitespace-only / ``None`` inputs yield empty results
(``preprocess(None) == ""``, ``preprocess("   ") == ""``).
"""

from __future__ import annotations

import os
import re
from typing import Optional

import nltk

# ---------------------------------------------------------------------------
# Compiled noise regexes (built once at import; exposed for tests to reference).
#
# Order matters in :func:`clean_text`: the more specific / structural patterns
# (timestamps, ``key=value`` ids, UUIDs, IPs, latency) run before the broad
# fallbacks (hex blobs, bare number runs) so we never half-eat a structured
# token. Every match is replaced with a single space, which the final
# whitespace pass collapses.
# ---------------------------------------------------------------------------

#: ISO-8601 timestamps with optional fractional seconds and ``Z`` / numeric
#: offset (``2026-06-21T15:32:10.123Z``, ``2026-06-21T15:32:10+05:30``) **and**
#: the common space-separated / syslog-ish form (``2026-06-21 15:32:10``).
TIMESTAMP_RE = re.compile(
    r"""
    \d{4}-\d{2}-\d{2}            # date: YYYY-MM-DD
    [T\ ]                        # 'T' or a space separator
    \d{2}:\d{2}:\d{2}           # time: HH:MM:SS
    (?:\.\d+)?                   # optional .fractional seconds
    (?:Z|[+-]\d{2}:?\d{2})?      # optional 'Z' or +/-HH:MM offset
    """,
    re.VERBOSE,
)

#: IPv4 dotted-quad addresses (``10.12.34.56``). Anchored on word boundaries so
#: we do not nibble digits out of unrelated dotted tokens.
IPV4_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")

#: IPv6 addresses — full 8-group form plus the ``::`` compressed forms. Kept
#: deliberately broad (it runs on machine-generated logs, not user free text).
IPV6_RE = re.compile(
    r"""
    \b(?:[0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4}\b   # full 8-group address
    |                                                # -- or --
    \b(?:[0-9A-Fa-f]{1,4}:)+:(?:[0-9A-Fa-f]{1,4}:)*[0-9A-Fa-f]{1,4}\b  # '::' compressed
    |
    ::(?:[0-9A-Fa-f]{1,4}:)*[0-9A-Fa-f]{1,4}\b      # leading '::'
    """,
    re.VERBOSE,
)

#: Canonical 8-4-4-4-12 UUIDs (``a1b2c3d4-e5f6-4789-a012-3456789abcde``).
UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)

#: Latency tokens — a digit run immediately followed by ``ms`` (``4523ms``).
#: Case-insensitive so ``4523MS`` is caught too.
LATENCY_RE = re.compile(r"\b\d+\s*ms\b", re.IGNORECASE)

#: ``key=value`` noise tokens whose value is an opaque id — ``req_id=<uuid>``,
#: ``conn_id=...``, ``txn=...``, ``session=...``, ``payload=...``. We drop the
#: *whole* token (key and value): the value is high-cardinality and the bare key
#: alone adds little signal. The value half matches a UUID, a hex/alnum blob, a
#: long number, or a dotted/colon-bearing identifier. Runs **before** the bare
#: UUID/number patterns so the ``key=`` prefix is consumed too.
KV_ID_RE = re.compile(
    r"""
    \b[A-Za-z_][A-Za-z0-9_]*    # a key: starts with a letter/underscore
    =                            # the '=' separator
    (?:                          # an id-shaped value:
        [0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}  # UUID
        | [0-9A-Za-z]*\d[0-9A-Za-z]*       # alnum blob containing >=1 digit
        | [0-9A-Za-z._:-]*[0-9A-Za-z]      # dotted/colon id (ips, hosts, paths)
    )
    """,
    re.VERBOSE,
)

#: Ports written as a leading colon then digits (``:443``, ``:5432``). Used to
#: scrub the port that trails an already-removed host. The leading boundary is a
#: non-digit (or start) so we don't clip the seconds of an un-stripped clock.
PORT_RE = re.compile(r"(?<!\d):\d{2,5}\b")

#: Long hexadecimal blobs (>=6 hex chars), e.g. a digest fragment or an id that
#: was *not* in canonical UUID form. Requires at least one digit *or* runs of
#: >=6 chars to avoid eating ordinary words like ``database`` (which is all
#: a-f/0-9 letters). We therefore only treat it as hex-noise when it is long and
#: contains a digit, OR is a pure run with a high length.
HEX_RE = re.compile(r"\b(?=[0-9a-fA-F]*\d)[0-9a-fA-F]{6,}\b")

#: Standalone numeric runs (``9999``, ``3``) and number-with-units that survive
#: the specific passes. Word-boundary anchored; floats included.
NUM_RE = re.compile(r"\b\d[\d.,]*\b")

#: Collapse any run of whitespace (spaces, tabs, newlines) to a single space.
_WHITESPACE_RE = re.compile(r"\s+")

#: Final-pass token used by the regex tokenizer fallback and the numeric/punct
#: filter in :func:`tokenize` — what we consider a "word" token.
_WORD_RE = re.compile(r"[a-z0-9]+")

# Order the cleaning passes are applied. Tuple of (compiled, replacement).
_CLEAN_PASSES: tuple[tuple[re.Pattern[str], str], ...] = (
    (TIMESTAMP_RE, " "),
    (KV_ID_RE, " "),     # consume 'key=<id>' before bare UUID/number passes
    (UUID_RE, " "),
    (IPV6_RE, " "),
    (IPV4_RE, " "),
    (LATENCY_RE, " "),
    (PORT_RE, " "),
    (HEX_RE, " "),
    (NUM_RE, " "),
)


# ---------------------------------------------------------------------------
# Lazily-initialized, module-cached NLTK resources.
#
# We avoid importing/loading at module import so a missing data dir doesn't make
# *importing* this module fail; the cost is paid (once) on first tokenize call.
# ---------------------------------------------------------------------------

# A minimal English stopword list used only when NLTK's ``stopwords`` corpus is
# unavailable. Not exhaustive — just the highest-frequency function words — so
# the regex fallback still produces reasonable tokens.
_FALLBACK_STOPWORDS: frozenset[str] = frozenset(
    {
        "a", "an", "the", "and", "or", "but", "if", "then", "else", "for",
        "of", "to", "in", "on", "at", "by", "with", "from", "as", "is",
        "are", "was", "were", "be", "been", "being", "this", "that", "these",
        "those", "it", "its", "we", "you", "they", "he", "she", "i", "me",
        "my", "our", "your", "their", "them", "us", "do", "does", "did",
        "has", "have", "had", "not", "no", "so", "up", "out", "off", "over",
        "after", "before", "while", "than", "too", "very", "can", "will",
        "just", "about", "into", "through", "during", "again", "further",
        "here", "there", "when", "where", "why", "how", "all", "any", "both",
        "each", "few", "more", "most", "other", "some", "such", "only", "own",
        "same",
    }
)

# Cached after first successful load. ``None`` means "not yet initialized".
_STOPWORDS_CACHE: Optional[frozenset[str]] = None
#: True once NLTK's punkt tokenizer is confirmed usable; False to use the regex
#: fallback. ``None`` means "not yet probed".
_USE_NLTK_TOKENIZER: Optional[bool] = None


def _nltk_data_dir() -> Optional[str]:
    """Return the ``NLTK_DATA`` directory if the env var is set, else ``None``."""
    path = os.environ.get("NLTK_DATA")
    return path or None


def _ensure_nltk_resource(resource_path: str, download_name: str) -> bool:
    """Make an NLTK resource available, attempting a quiet download on miss.

    Args:
        resource_path: The lookup path passed to :func:`nltk.data.find`
            (e.g. ``"tokenizers/punkt"``).
        download_name: The corpus/model id passed to :func:`nltk.download`
            (e.g. ``"punkt"``).

    Returns:
        ``True`` if the resource is present (already, or after a successful
        download); ``False`` if it remains unavailable.
    """
    try:
        nltk.data.find(resource_path)
        return True
    except LookupError:
        pass

    # Not found locally — try a quiet download (into NLTK_DATA when configured).
    try:
        download_dir = _nltk_data_dir()
        ok = nltk.download(download_name, quiet=True, download_dir=download_dir)
        if not ok:
            return False
        nltk.data.find(resource_path)
        return True
    except (LookupError, OSError, ValueError):
        return False


def _get_stopwords() -> frozenset[str]:
    """Return the English stopword set, loading and caching it on first use.

    Prefers NLTK's ``stopwords`` corpus; on any lookup failure (even after an
    attempted download) falls back to :data:`_FALLBACK_STOPWORDS`. The result is
    cached at module level so the work happens at most once.
    """
    global _STOPWORDS_CACHE
    if _STOPWORDS_CACHE is not None:
        return _STOPWORDS_CACHE

    if _ensure_nltk_resource("corpora/stopwords", "stopwords"):
        try:
            from nltk.corpus import stopwords

            _STOPWORDS_CACHE = frozenset(stopwords.words("english"))
            return _STOPWORDS_CACHE
        except (LookupError, OSError):
            pass

    _STOPWORDS_CACHE = _FALLBACK_STOPWORDS
    return _STOPWORDS_CACHE


def _should_use_nltk_tokenizer() -> bool:
    """Probe (once) whether NLTK's ``word_tokenize`` can be used.

    ``word_tokenize`` needs the ``punkt`` tokenizer data. Newer NLTK also looks
    for ``punkt_tab``; we try to make either available. If neither can be
    resolved we use the regex fallback. The decision is cached.
    """
    global _USE_NLTK_TOKENIZER
    if _USE_NLTK_TOKENIZER is not None:
        return _USE_NLTK_TOKENIZER

    available = _ensure_nltk_resource("tokenizers/punkt", "punkt")
    # NLTK >= 3.8.2 split the tables into ``punkt_tab``; make a best effort so
    # ``word_tokenize`` doesn't raise. Either resource being present is enough.
    _ensure_nltk_resource("tokenizers/punkt_tab", "punkt_tab")

    _USE_NLTK_TOKENIZER = available
    return _USE_NLTK_TOKENIZER


def _raw_tokens(text: str) -> list[str]:
    """Split ``text`` into raw tokens using NLTK if possible, else a regex.

    Returns lowercase-agnostic tokens (the caller has already lowercased). On any
    unexpected NLTK failure at call time we degrade to the regex tokenizer rather
    than propagate the error.
    """
    global _USE_NLTK_TOKENIZER

    if _should_use_nltk_tokenizer():
        try:
            from nltk.tokenize import word_tokenize

            return word_tokenize(text)
        except (LookupError, OSError):
            # Data vanished or is corrupt at call time — fall through to regex.
            _USE_NLTK_TOKENIZER = False

    return _WORD_RE.findall(text)


# ---------------------------------------------------------------------------
# Public API.
# ---------------------------------------------------------------------------


def _coerce(text: object) -> str:
    """Coerce arbitrary input to ``str``; ``None`` becomes the empty string.

    Non-string, non-``None`` values are stringified via ``str(...)`` so the
    pipeline never raises ``TypeError`` on, e.g., an ``int`` log line.
    """
    if text is None:
        return ""
    if isinstance(text, str):
        return text
    return str(text)


def clean_text(text: str) -> str:
    """Strip high-cardinality noise, lowercase, and normalize whitespace.

    This is the *deterministic, pure* first half of the pipeline (no
    tokenization, no NLTK). It applies the module-level regexes in
    :data:`_CLEAN_PASSES` in order, lowercases the result, then collapses
    whitespace runs to single spaces and trims the ends.

    Removed / normalized:

    * ISO-8601 and space-separated timestamps (:data:`TIMESTAMP_RE`)
    * ``key=<id>`` noise tokens such as ``req_id=...`` (:data:`KV_ID_RE`)
    * canonical UUIDs (:data:`UUID_RE`)
    * IPv4 and IPv6 addresses (:data:`IPV4_RE`, :data:`IPV6_RE`)
    * latency tokens like ``4523ms`` (:data:`LATENCY_RE`)
    * trailing ports like ``:443`` (:data:`PORT_RE`)
    * long hex blobs (:data:`HEX_RE`) and bare numeric runs (:data:`NUM_RE`)

    Args:
        text: The raw log text (any type; coerced via ``str``; ``None`` → ``""``).

    Returns:
        The cleaned, lowercased, whitespace-normalized string. May still contain
        punctuation (``[`` ``]`` ``/`` ``=``) — that is removed during
        :func:`tokenize`.
    """
    s = _coerce(text)
    if not s:
        return ""

    for pattern, replacement in _CLEAN_PASSES:
        s = pattern.sub(replacement, s)

    s = s.lower()
    s = _WHITESPACE_RE.sub(" ", s).strip()
    return s


def tokenize(text: str) -> list[str]:
    """Tokenize text and drop stopwords, punctuation, and pure-numeric tokens.

    Accepts either raw or already-:func:`clean_text`-ed input. The text is
    lowercased, run through NLTK's ``word_tokenize`` (or the regex fallback), and
    filtered to keep only alphanumeric word tokens that are **not** English
    stopwords and **not** purely numeric. Pure-punctuation tokens (``[``, ``=``,
    ``/`` ...) are dropped.

    Note: this does *not* itself strip timestamps/IPs/UUIDs — call
    :func:`clean_text` (or :func:`preprocess`) first for that. Tokenizing raw
    text directly still works but leaves noise that the numeric/stopword filter
    only partially removes.

    Args:
        text: Text to tokenize (any type; coerced via ``str``; ``None`` → ``[]``).

    Returns:
        A list of lowercase word tokens with stopwords and noise removed.
    """
    s = _coerce(text)
    if not s:
        return []

    s = s.lower()
    stop = _get_stopwords()

    tokens: list[str] = []
    for tok in _raw_tokens(s):
        # Keep only tokens that contain at least one alphanumeric char.
        if not any(ch.isalnum() for ch in tok):
            continue  # pure punctuation (e.g. '[', '=', '/')
        if tok.isdigit():
            continue  # pure-numeric (any survivors of NUM_RE / raw input)
        if tok in stop:
            continue  # English stopword
        tokens.append(tok)
    return tokens


def preprocess(text: str) -> str:
    """Run the full preprocessing pipeline and return a single clean string.

    Pipeline: :func:`clean_text` (regex strip + lowercase + whitespace) →
    :func:`tokenize` (NLTK tokenize + stopword/noise removal) → join the tokens
    with single spaces. This is the main entry point consumed by feature
    extraction.

    The function is **idempotent on already-clean text**: because the cleaning
    regexes and the tokenizer are stable and the output is itself a
    space-joined, lowercased, noise-free token string, ``preprocess(preprocess(x))
    == preprocess(x)``.

    Args:
        text: The raw log text (any type; coerced via ``str``; ``None`` → ``""``).

    Returns:
        A single space-separated string of clean tokens, or ``""`` for empty /
        whitespace-only / ``None`` input.
    """
    cleaned = clean_text(text)
    if not cleaned:
        return ""
    return " ".join(tokenize(cleaned))
