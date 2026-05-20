"""Compiled regex patterns + Luhn check for the detection layer.

The five regexes (SSN, credit card, email, US phone, MRN) are compiled ONCE
at import time and stored in the module-level ``PATTERNS`` mapping so the
hot path never pays compilation cost. The third-party ``regex`` library is
used instead of stdlib ``re`` because it accepts a ``timeout=`` keyword on
``finditer`` / ``findall``: catastrophic backtracking on adversarial input
is bounded by ``REGEX_TIMEOUT_SEC`` and falls back to "no hits for this
pattern" rather than blocking the request thread.

The credit-card regex is intentionally broad (``\\b(?:\\d[ -]*?){13,19}\\b``)
because Visa / MC / Amex / Discover all have different prefixes; instead of
maintaining a brand table we apply the Luhn checksum after the regex match
and drop any candidate that fails. That way a 16-digit order ID never gets
flagged as a credit card.

Pattern table
-------------
* ``ssn``         — full SSN with area-number validation (rejects ``000``,
  ``666``, ``9xx`` areas; rejects ``00`` group and ``0000`` serial).
* ``credit_card`` — 13-19 digit run allowing spaces/dashes; Luhn-filtered.
* ``email``       — standard local@domain.tld, TLD ``[A-Za-z]{2,}``.
* ``us_phone``    — optional ``+1`` / parens / dot/dash/space separators.
* ``mrn``         — ``MRN-`` literal prefix + exactly 6 digits.

Public surface:
* :class:`Detection` — frozen dataclass describing one hit.
* :func:`_luhn`      — mod-10 checksum (exposed for unit tests).
* :func:`match_all`  — run every pattern, return sorted hits.
* ``PATTERNS``       — module-level dict of compiled regexes.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Final

import regex

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Detection dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Detection:
    """One match emitted by the detection layer.

    Frozen so it can be safely shared across threads (the redaction layer
    in C3 will fan out per-detection work via a thread pool) and used as a
    hashable key when deduplicating overlapping spans.

    Attributes
    ----------
    pattern_name : str
        Canonical type, e.g. ``"ssn"``, ``"credit_card"``, ``"email"``,
        ``"us_phone"``, ``"mrn"`` for regex hits or ``"person"`` / ``"org"``
        for NER hits.
    value : str
        The exact matched substring from the source text.
    start : int
        Inclusive start offset into the source text.
    end : int
        Exclusive end offset into the source text (i.e. ``text[start:end]``
        equals ``value``).
    confidence : float
        ``1.0`` for regex hits (already passed structural validation +
        Luhn where applicable); ``0.85`` for NER hits.
    source : str
        ``"regex"`` or ``"ner"`` — identifies which subsystem produced the
        hit, used by the deduper to break confidence ties.
    """

    pattern_name: str
    value: str
    start: int
    end: int
    confidence: float = 1.0
    source: str = "regex"


# ---------------------------------------------------------------------------
# Compiled patterns (one-time cost at import)
# ---------------------------------------------------------------------------

# NOTE: keep these in sync with the project_requirements.md detection
# checklist. The SSN regex enforces area-number validity in the regex itself
# (negative lookaheads) so we don't need a Python-side post-filter for it.
PATTERNS: Final[dict[str, regex.Pattern]] = {
    "ssn": regex.compile(
        r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"
    ),
    # Broad numeric net; Luhn filter in match_all() drops false positives.
    "credit_card": regex.compile(r"\b(?:\d[ -]*?){13,19}\b"),
    "email": regex.compile(
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
    ),
    "us_phone": regex.compile(
        r"\b(?:\+?1[-.\s]?)?\(?[2-9]\d{2}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b"
    ),
    "mrn": regex.compile(r"\bMRN-\d{6}\b"),
}


# ---------------------------------------------------------------------------
# Luhn checksum (mod-10)
# ---------------------------------------------------------------------------

def _luhn(digits: str) -> bool:
    """Return ``True`` iff ``digits`` is Luhn-valid after stripping separators.

    The Luhn algorithm doubles every second digit from the right; if the
    double is >= 10 you sum the resulting digits (equivalent to subtracting
    9). A valid number has a total divisible by 10.

    >>> _luhn("4111 1111 1111 1111")
    True
    >>> _luhn("4111-1111-1111-1112")
    False
    >>> _luhn("123")  # too short after stripping
    False
    """
    only_digits = "".join(c for c in digits if c.isdigit())
    if len(only_digits) < 13 or len(only_digits) > 19:
        return False

    total = 0
    # i == 0 corresponds to the rightmost (least-significant) digit.
    for i, ch in enumerate(reversed(only_digits)):
        d = ord(ch) - 48  # ord() is faster than int() in tight loops
        if i % 2 == 1:  # every other digit from the right gets doubled
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


# ---------------------------------------------------------------------------
# match_all — run every compiled pattern against the text
# ---------------------------------------------------------------------------

def match_all(text: str, timeout: float = 0.05) -> list[Detection]:
    """Run every pattern in ``PATTERNS`` against ``text`` and return all hits.

    The per-pattern ``timeout`` (in seconds) bounds catastrophic-backtracking
    blowups on adversarial input — if a single pattern exceeds the budget we
    log a warning and skip THAT pattern only; the remaining patterns still
    run normally.

    Credit-card matches are Luhn-validated inline: a Luhn failure causes the
    detection to be dropped entirely (not emitted at confidence 0.0). That
    keeps the output list cleaner for downstream consumers — they never see
    something they have to filter again.

    Returns the list sorted by ``Detection.start`` so callers can scan
    left-to-right.
    """
    if not text:
        return []

    hits: list[Detection] = []
    for name, pattern in PATTERNS.items():
        try:
            for m in pattern.finditer(text, timeout=timeout):
                value = m.group()

                # Credit-card requires structural Luhn validation; the regex
                # only certifies "looks like 13-19 separated digits".
                if name == "credit_card" and not _luhn(value):
                    continue

                hits.append(
                    Detection(
                        pattern_name=name,
                        value=value,
                        start=m.start(),
                        end=m.end(),
                        confidence=1.0,
                        source="regex",
                    )
                )
        except TimeoutError:
            # Adversarial / pathological input. Log and skip; don't fail the
            # whole request just because one pattern misbehaved.
            logger.warning(
                "regex timeout (%.3fs) on pattern %r; skipping this pattern",
                timeout,
                name,
            )
            continue

    hits.sort(key=lambda d: d.start)
    return hits
