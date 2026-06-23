"""Log preprocessing: variable-token masking, field parsing, and tokenization.

This module is the text-normalization foundation for the clustering engine. Raw log
messages are noisy — they embed timestamps, IP addresses, UUIDs, request ids, numeric
durations, and file paths that differ on *every* line even when the underlying event is
identical. Feeding that raw text to TF-IDF (project_requirements §2, content features)
would scatter near-identical events across many clusters.

:func:`mask_log` collapses those variable tokens to stable placeholders (``<IP>``,
``<NUM>`` ...), so two logs describing the *same* event — differing only in the IP they
came from or the latency they recorded — normalize to the **same** masked string. That
collision is the property the clustering relies on. :func:`tokenize` then turns a masked
message into a lowercased, stopword-free token list (with placeholders kept intact), and
:func:`parse_log` normalizes structured :class:`~src.schemas.LogEntry` fields. The public
entrypoint :func:`preprocess` bundles all three.

Public API (stable — downstream commits import these names):

* Mask placeholder constants: ``MASK_TS`` ``MASK_URL`` ``MASK_EMAIL`` ``MASK_IP``
  ``MASK_UUID`` ``MASK_HEX`` ``MASK_PATH`` ``MASK_NUM``.
* :func:`mask_log` — idempotent variable-token masking.
* :func:`tokenize` — NLTK ``word_tokenize`` + stopword removal (regex fallback).
* :func:`parse_log` — normalize a ``LogEntry``/dict into a flat dict of typed fields.
* :func:`preprocess` — ``{"parsed", "masked_message", "tokens"}`` for one entry.

The module degrades gracefully if NLTK corpora are unavailable (a regex tokenizer and a
small built-in stopword set take over), so it imports and runs with no network access and
no external data beyond what ships in the container (``NLTK_DATA=/usr/share/nltk_data``).
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing-only import, avoids runtime coupling
    from src.schemas import LogEntry


# ---------------------------------------------------------------------------
# Mask placeholders
# ---------------------------------------------------------------------------
# Uppercase, angle-bracketed sentinels. They contain no characters that any of the
# variable-token regexes below can match (no digits, no IP/UUID shape, no '/'), which is
# what makes :func:`mask_log` idempotent: once a token is replaced, the placeholder is
# inert and a second pass leaves it untouched.
MASK_TS = "<TS>"
MASK_IP = "<IP>"
MASK_UUID = "<UUID>"
MASK_HEX = "<HEX>"
MASK_NUM = "<NUM>"
MASK_URL = "<URL>"
MASK_PATH = "<PATH>"
MASK_EMAIL = "<EMAIL>"


# ---------------------------------------------------------------------------
# Variable-token regexes (compiled once at import; applied in the order below)
# ---------------------------------------------------------------------------
# Ordering rationale — most-specific / longest patterns FIRST, the greedy numeric
# pattern LAST, so a timestamp like "2026-06-23" is masked as <TS> before the number
# regex could nibble its "2026". Each pattern is anchored on word/space boundaries so it
# bites whole tokens, never a fragment of an already-emitted placeholder.

# ISO-8601 ("2026-06-23T12:00:00Z", "...,123", "+05:30") and the common log variants
# "2026-06-23 12:00:00,123" and Apache CLF "23/Jun/2026:12:00:00".
_RE_TS_ISO = re.compile(
    r"\d{4}-\d{2}-\d{2}"  # date
    r"[T ]\d{2}:\d{2}:\d{2}"  # time (T- or space-separated)
    r"(?:[.,]\d+)?"  # optional fractional seconds
    r"(?:Z|[+-]\d{2}:?\d{2})?"  # optional timezone
)
_RE_TS_CLF = re.compile(
    r"\d{1,2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2}(?:\s*[+-]\d{4})?"
)
# A bare calendar date with no time component ("2026-06-23"); masked so it does not
# decompose into three <NUM> tokens.
_RE_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")

# URLs first, then emails (an email could otherwise be partly eaten by host matching).
_RE_URL = re.compile(r"\b(?:https?|ftp)://[^\s]+", re.IGNORECASE)
_RE_EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")

# IPv4 with an optional :port, then IPv6. The IPv4 alternative is listed first so a
# trailing ":port" is captured into the same <IP>.
_RE_IPV4 = re.compile(
    r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)"
    r"(?::\d{1,5})?\b"
)
_RE_IPV6 = re.compile(
    r"\b(?:[A-Fa-f0-9]{1,4}:){2,7}[A-Fa-f0-9]{1,4}\b"  # 3+ hextets => unambiguous IPv6
)

# UUID (canonical 8-4-4-4-12) before the generic hex rule, which would otherwise consume
# its first run of hex digits.
_RE_UUID = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)

# Hex / hashes: "0x..." literals and long bare hex runs (md5/sha-style). Length >= 7 so
# ordinary decimal numbers and short words are left for the number rule / stay as text.
_RE_HEX = re.compile(r"\b0[xX][0-9a-fA-F]+\b|\b[0-9a-fA-F]{7,}\b")

# Unix file paths: an absolute path with at least one more segment ("/var/log/app.log").
# Requires a leading slash + a path char then another slash so a lone "/" or a REST
# endpoint like "/users" is NOT masked (those stay meaningful for clustering).
_RE_PATH = re.compile(r"/(?:[\w.-]+/)+[\w.-]+")

# Standalone numbers last: signed ints/floats (incl. scientific). The leading lookbehind
# stops it from biting into the middle of an alphanumeric id ("v2", "abc123") or into a
# placeholder ("<NUM>"); we deliberately do NOT constrain the trailing side so a number
# glued to a unit ("12.5ms") still masks fully to "<NUM>ms" — which keeps the collision
# property (12.5ms and 99.9ms both -> "<NUM>ms"). Placeholders carry no digits, so this
# can never re-match its own output, preserving idempotency.
_RE_NUM = re.compile(r"(?<![\w<])[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?")

# Substitutions applied in sequence. Order is load-bearing (see notes above).
_MASK_PIPELINE: tuple[tuple[re.Pattern[str], str], ...] = (
    (_RE_TS_ISO, MASK_TS),
    (_RE_TS_CLF, MASK_TS),
    (_RE_DATE, MASK_TS),
    (_RE_URL, MASK_URL),
    (_RE_EMAIL, MASK_EMAIL),
    (_RE_IPV4, MASK_IP),
    (_RE_IPV6, MASK_IP),
    (_RE_UUID, MASK_UUID),
    (_RE_HEX, MASK_HEX),
    (_RE_PATH, MASK_PATH),
    (_RE_NUM, MASK_NUM),
)

# Collapse any run of whitespace to a single space.
_RE_WS = re.compile(r"\s+")

# Token that is purely punctuation/symbols (no letters, digits, or '_'); dropped by
# :func:`tokenize`.
_RE_PUNCT_ONLY = re.compile(r"^[^\w]+$")

# A mask placeholder token, e.g. "<IP>"; matched first by the fallback tokenizer so a
# placeholder survives as a single token.
_RE_MASK_TOKEN = re.compile(r"<[A-Z]+>")


def mask_log(text: str) -> str:
    """Replace variable tokens in a log line with stable placeholders.

    Applies the masking pipeline (timestamps, URLs, emails, IPs, UUIDs, hex/hashes,
    file paths, then bare numbers) in a fixed, specificity-ordered sequence and collapses
    repeated whitespace. The result is **idempotent** — ``mask_log(mask_log(s))`` equals
    ``mask_log(s)`` because every placeholder is inert to all the patterns — and
    **collision-inducing**: two messages that differ only in their IP, numeric values, or
    UUIDs mask to the identical string, which is the signal the clustering depends on.

    Args:
        text: A raw log message (any string). ``None``/non-str inputs are coerced to a
            string so callers need not pre-validate.

    Returns:
        The masked, whitespace-normalized message. Empty input yields ``""``.
    """
    if not text:
        return ""
    if not isinstance(text, str):  # defensive: accept stray non-str payloads
        text = str(text)

    for pattern, replacement in _MASK_PIPELINE:
        text = pattern.sub(replacement, text)

    return _RE_WS.sub(" ", text).strip()


def _load_stopwords() -> frozenset[str]:
    """Load the English stopword set once, falling back to a small built-in set.

    Tries NLTK's ``stopwords`` corpus (present in the container image). If the corpus is
    missing or NLTK is not importable, returns a compact hand-picked set so tokenization
    still removes the most common filler words. Called exactly once at import time.
    """
    try:
        from nltk.corpus import stopwords  # imported lazily so the module loads w/o NLTK

        return frozenset(stopwords.words("english"))
    except Exception:  # noqa: BLE001 - any failure (missing corpus, import error) -> fallback
        return _FALLBACK_STOPWORDS


# Minimal English stopword set used only when the NLTK corpus is unavailable. Kept short
# but covers the highest-frequency function words so clustering features stay meaningful.
_FALLBACK_STOPWORDS: frozenset[str] = frozenset(
    {
        "a", "an", "the", "and", "or", "but", "if", "then", "else", "for", "of", "on",
        "in", "to", "from", "by", "with", "at", "as", "is", "are", "was", "were", "be",
        "been", "being", "am", "do", "does", "did", "doing", "have", "has", "had",
        "having", "this", "that", "these", "those", "it", "its", "i", "you", "he", "she",
        "we", "they", "them", "his", "her", "their", "our", "your", "my", "me", "us",
        "not", "no", "nor", "so", "than", "too", "very", "can", "will", "just", "up",
        "down", "out", "over", "under", "again", "there", "here", "when", "where", "why",
        "how", "all", "any", "both", "each", "few", "more", "most", "some", "such", "own",
        "same", "what", "which", "who", "whom", "into", "about", "between", "through",
    }
)

# Module-level caches (loaded once at import; never per-call, per the C2 spec).
_STOPWORDS: frozenset[str] = _load_stopwords()


def _regex_tokenize(text: str) -> list[str]:
    """Fallback tokenizer: split lowercased text into placeholder/word tokens via regex.

    Mirrors the contract of the NLTK path closely enough to be a drop-in: mask
    placeholders are matched first and kept whole, then alphanumeric/underscore word
    tokens. Punctuation is implicitly dropped (it matches nothing). The input is lowercased
    *before* matching, so the placeholder alternative is matched case-insensitively and
    re-uppercased — otherwise ``<IP>`` (already lowercased to ``<ip>``) would not equal the
    public ``MASK_IP`` constant.
    """
    return [
        tok.upper() if tok.startswith("<") else tok
        for tok in re.findall(r"<[a-zA-Z]+>|[a-z0-9_]+", text.lower())
    ]


def tokenize(text: str) -> list[str]:
    """Tokenize a (typically masked) log message into clustering-ready tokens.

    Lowercases the input, tokenizes with NLTK's ``word_tokenize``, removes English
    stopwords and pure-punctuation tokens, and crucially keeps mask placeholders such as
    ``<IP>`` as single tokens (``word_tokenize`` would otherwise split them on ``<``/``>``).
    If NLTK (or its ``punkt`` data) is unavailable, a regex tokenizer + built-in stopword
    set produce an equivalent token list, so the function never fails for lack of corpora.

    Args:
        text: The message to tokenize. Pass the output of :func:`mask_log` to keep the
            placeholders intact and the vocabulary small.

    Returns:
        A list of lowercase tokens (mask placeholders preserved verbatim), in order, with
        stopwords and punctuation-only tokens removed. Empty input yields ``[]``.
    """
    if not text:
        return []

    lowered = text.lower()
    try:
        from nltk import word_tokenize  # lazy import; falls back if NLTK/data absent

        raw_tokens = word_tokenize(lowered)
        tokens = _recombine_mask_tokens(raw_tokens)
    except Exception:  # noqa: BLE001 - missing punkt/data/import -> regex fallback
        tokens = _regex_tokenize(lowered)

    return [
        tok
        for tok in tokens
        if tok
        and tok not in _STOPWORDS
        and not _RE_PUNCT_ONLY.match(tok)
    ]


def _recombine_mask_tokens(tokens: list[str]) -> list[str]:
    """Re-fuse a ``<``, ``WORD``, ``>`` triple that ``word_tokenize`` split apart.

    NLTK splits ``<IP>`` into ``['<', 'ip', '>']``. This walks the token stream and
    collapses any ``<`` … ``>`` wrapping a single bareword back into one ``<WORD>``
    placeholder (re-uppercased), leaving all other tokens untouched.
    """
    out: list[str] = []
    i = 0
    n = len(tokens)
    while i < n:
        if (
            tokens[i] == "<"
            and i + 2 < n
            and tokens[i + 2] == ">"
            and tokens[i + 1].isalpha()
        ):
            out.append(f"<{tokens[i + 1].upper()}>")
            i += 3
        else:
            out.append(tokens[i])
            i += 1
    return out


# Endpoint/component derivation (used when a LogEntry has no explicit ``endpoint``).
# 1) a REST-style path token ("/users", "/api/v1/login") — but not a bare "/".
_RE_ENDPOINT_PATH = re.compile(r"/[A-Za-z][\w/-]*")
# 2) a dotted "service.component" identifier ("auth.login", "db.pool.acquire").
_RE_DOTTED_COMPONENT = re.compile(r"\b[a-zA-Z][\w-]*(?:\.[a-zA-Z][\w-]*)+\b")


def _derive_endpoint(message: str) -> str | None:
    """Best-effort extraction of a coarse endpoint/component from a log message.

    Prefers a REST-style ``/path`` token; otherwise falls back to a dotted
    ``service.component`` identifier. Returns ``None`` when neither shape is present.
    Used only as a fallback when the structured ``endpoint`` field is absent.
    """
    if not message:
        return None
    if (m := _RE_ENDPOINT_PATH.search(message)) is not None:
        return m.group(0)
    if (m := _RE_DOTTED_COMPONENT.search(message)) is not None:
        return m.group(0)
    return None


def _get(entry: "LogEntry | dict[str, Any]", key: str) -> Any:
    """Read ``key`` from either a Pydantic model (attribute) or a mapping (item).

    Returns ``None`` for any missing key/attribute so :func:`parse_log` can stay
    defensive about partially-populated inputs.
    """
    if isinstance(entry, dict):
        return entry.get(key)
    return getattr(entry, key, None)


def _coerce_timestamp(value: Any) -> datetime | None:
    """Coerce assorted timestamp representations into a ``datetime`` (or ``None``).

    Accepts an existing ``datetime`` as-is, parses ISO-8601 strings (tolerating a
    trailing ``Z``), and treats int/float as a Unix epoch in seconds. Anything
    unparseable yields ``None`` rather than raising.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return datetime.fromtimestamp(value)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        # datetime.fromisoformat handles "...Z" only on 3.11+; normalize defensively.
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None
    return None


def _coerce_float(value: Any) -> float | None:
    """Coerce ``value`` to ``float``; return ``None`` if it is absent or non-numeric."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    """Coerce ``value`` to ``int``; return ``None`` if it is absent or non-numeric.

    Floats are truncated via ``int(float(...))`` so a JSON ``200.0`` status code still
    lands as ``200``.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def parse_log(entry: "LogEntry | dict[str, Any]") -> dict[str, Any]:
    """Normalize a :class:`~src.schemas.LogEntry` (or plain dict) into typed log fields.

    Accepts either a Pydantic ``LogEntry`` or a loosely-shaped mapping (e.g. decoded
    JSON) and returns a flat dict with consistent types and casing, defending against
    missing keys throughout. ``service`` is lowercased and ``level`` uppercased so they
    cluster/group consistently regardless of source formatting. When ``endpoint`` is
    absent, a coarse endpoint/component is derived from the message (a ``/path`` token or
    a ``service.component`` identifier) if one is present.

    Args:
        entry: A ``LogEntry`` instance or a dict with any subset of the log fields.

    Returns:
        A dict with keys ``timestamp`` (datetime|None), ``service`` (str, lowercased),
        ``level`` (str, UPPERCASED), ``message`` (str), ``source_ip`` (str|None),
        ``endpoint`` (str|None), ``response_time_ms`` (float|None), and ``status_code``
        (int|None).
    """
    timestamp = _coerce_timestamp(_get(entry, "timestamp"))

    raw_service = _get(entry, "service")
    service = str(raw_service).lower() if raw_service is not None else ""

    raw_level = _get(entry, "level")
    level = str(raw_level).upper() if raw_level is not None else ""

    raw_message = _get(entry, "message")
    message = str(raw_message) if raw_message is not None else ""

    raw_ip = _get(entry, "source_ip")
    source_ip = str(raw_ip) if raw_ip is not None else None

    raw_endpoint = _get(entry, "endpoint")
    if raw_endpoint is not None and str(raw_endpoint).strip():
        endpoint: str | None = str(raw_endpoint)
    else:
        endpoint = _derive_endpoint(message)

    return {
        "timestamp": timestamp,
        "service": service,
        "level": level,
        "message": message,
        "source_ip": source_ip,
        "endpoint": endpoint,
        "response_time_ms": _coerce_float(_get(entry, "response_time_ms")),
        "status_code": _coerce_int(_get(entry, "status_code")),
    }


def preprocess(entry: "LogEntry | dict[str, Any]") -> dict[str, Any]:
    """Run the full preprocessing pipeline for a single log entry.

    Combines :func:`parse_log`, :func:`mask_log`, and :func:`tokenize` into the bundle
    the feature pipeline (C3) consumes: the masked message normalizes the entry's text
    and the token list is ready for TF-IDF / bag-of-words vectorization.

    Args:
        entry: A ``LogEntry`` instance or a plain dict of log fields.

    Returns:
        A dict with keys ``parsed`` (the :func:`parse_log` result), ``masked_message``
        (the masked, whitespace-normalized message string), and ``tokens`` (the
        :func:`tokenize` output over the masked message).
    """
    parsed = parse_log(entry)
    masked_message = mask_log(parsed["message"])
    tokens = tokenize(masked_message)
    return {
        "parsed": parsed,
        "masked_message": masked_message,
        "tokens": tokens,
    }


__all__ = [
    "MASK_TS",
    "MASK_IP",
    "MASK_UUID",
    "MASK_HEX",
    "MASK_NUM",
    "MASK_URL",
    "MASK_PATH",
    "MASK_EMAIL",
    "mask_log",
    "tokenize",
    "parse_log",
    "preprocess",
]
