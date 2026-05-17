"""Regex-based value scanning for the PII detection layer.

Each `PatternMatcher` loads its configuration ONCE at construction:

* `re.compile(...)` runs at startup, never on the hot path.
* The YAML file is read from disk a single time.

Matching a value is therefore O(P) re scans where P is the number of patterns
(currently 7). On a typical short field this is ~5-30 microseconds total.

Public surface of this module:

* :class:`Detection`       — frozen dataclass describing one hit.
* :class:`PatternMatcher`  — runs every compiled regex against a string.
* :func:`default_pattern_path` — resolves ``<repo>/config/patterns.yaml``.

Credit-card matches additionally pass through a Luhn checksum (RFC 4226 has
nothing to do with this; Luhn is the standard ISO/IEC 7812-1 algorithm). If
the candidate digits fail Luhn, the detection is dropped (confidence 0.0).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Pattern

import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Detection result
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Detection:
    """A single PII finding produced by either matcher.

    Immutable so it can be safely passed across threads (C5's parallel
    encryption path) and trusted as a hashable record.

    Attributes
    ----------
    field_path : str
        Dotted JSON path to the leaf, e.g. ``user.contact.email``. Empty
        when emitted by an individual matcher; the ``Detector`` orchestrator
        fills this in.
    field_type : str
        Canonical PII type (``email``, ``ssn``, ``phone``, ``credit_card``,
        ``jwt``, ``ipv4``, ``ipv6``, or any of the field-name substrings).
    confidence : float
        In ``[0.0, 1.0]``. Higher = more likely true positive.
    reason : str
        Diagnostic string, e.g. ``"regex:email"`` or ``"field_name:password"``.
        Useful for audit logs and debugging.
    value_preview : str
        First 8 characters of the original value (already truncated by the
        caller). Stored so the audit trail (C6) can identify *which* value
        was flagged without persisting the full plaintext.
    """

    field_path: str
    field_type: str
    confidence: float
    reason: str
    value_preview: str = ""


# ---------------------------------------------------------------------------
# Luhn check (for credit-card validation)
# ---------------------------------------------------------------------------

def _luhn(digits: str) -> bool:
    """Return ``True`` iff ``digits`` (after stripping non-digits) is Luhn-valid.

    The Luhn algorithm doubles every second digit from the right, summing
    the digits of any double >= 10 (equivalent: subtract 9). A valid card
    number has a total sum divisible by 10.

    >>> _luhn("4111 1111 1111 1111")
    True
    >>> _luhn("4111-1111-1111-1112")
    False
    >>> _luhn("abc")  # too short after stripping
    False
    """
    only_digits = "".join(c for c in digits if c.isdigit())
    if len(only_digits) < 13 or len(only_digits) > 19:
        return False

    total = 0
    # Iterate right-to-left so the LAST digit is "position 0".
    for i, ch in enumerate(reversed(only_digits)):
        d = ord(ch) - 48  # 'ord' is faster than int() in tight loops
        if i % 2 == 1:  # every second digit from the right is doubled
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


# ---------------------------------------------------------------------------
# Compiled pattern struct (internal)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _CompiledPattern:
    """Internal representation of one YAML entry after compilation."""

    name: str
    field_type: str
    pattern: Pattern[str]
    confidence_base: float
    requires_luhn: bool


# ---------------------------------------------------------------------------
# Pattern matcher
# ---------------------------------------------------------------------------

class PatternMatcher:
    """Scans a string value against every compiled PII regex.

    Loaded once at startup. Thread-safe (matchers are stateless after init).

    Parameters
    ----------
    config_path : Path | str
        YAML file describing the patterns (see ``config/patterns.yaml``).
    """

    def __init__(self, config_path: Path | str) -> None:
        self._config_path = Path(config_path)
        self._patterns: list[_CompiledPattern] = self._load(self._config_path)
        logger.debug(
            "PatternMatcher loaded %d patterns from %s",
            len(self._patterns),
            self._config_path,
        )

    # -- public ----------------------------------------------------------

    def match(self, value: str) -> list[Detection]:
        """Return every regex hit for ``value``.

        ``field_path`` and ``value_preview`` are left empty; the orchestrator
        (``Detector``) populates them. We return ALL hits and let the
        orchestrator pick the highest-confidence one.

        Credit-card hits run an extra Luhn check on the matched substring.
        Failures are silently dropped — they're false positives.
        """
        if not isinstance(value, str) or not value:
            return []

        hits: list[Detection] = []
        for pat in self._patterns:
            m = pat.pattern.search(value)
            if not m:
                continue

            confidence = pat.confidence_base
            if pat.requires_luhn:
                # The pattern matched a digit run; run Luhn on those digits.
                if _luhn(m.group(0)):
                    # Promote score: a Luhn-valid CC is high-confidence.
                    confidence = 0.95
                else:
                    # False positive — drop entirely rather than emit at 0.0.
                    continue

            if pat.field_type == "ipv6":
                # Confirm structural validity via the stdlib parser; the
                # regex is intentionally permissive.
                if not _is_valid_ipv6(m.group(0)):
                    continue

            hits.append(
                Detection(
                    field_path="",
                    field_type=pat.field_type,
                    confidence=confidence,
                    reason=f"regex:{pat.name}",
                    value_preview="",
                )
            )
        return hits

    # -- internal --------------------------------------------------------

    @staticmethod
    def _load(path: Path) -> list[_CompiledPattern]:
        with path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
        if not isinstance(raw, list):
            raise ValueError(
                f"Pattern config at {path} must be a YAML list, got {type(raw).__name__}"
            )

        compiled: list[_CompiledPattern] = []
        for entry in raw:
            try:
                compiled.append(
                    _CompiledPattern(
                        name=entry["name"],
                        field_type=entry["field_type"],
                        pattern=re.compile(entry["regex"]),
                        confidence_base=float(entry["confidence_base"]),
                        requires_luhn=bool(entry.get("requires_luhn", False)),
                    )
                )
            except KeyError as exc:
                raise ValueError(
                    f"Pattern entry {entry!r} missing required key {exc.args[0]!r}"
                ) from exc
        return compiled


# ---------------------------------------------------------------------------
# IPv6 validator helper (uses stdlib `ipaddress`)
# ---------------------------------------------------------------------------

def _is_valid_ipv6(candidate: str) -> bool:
    """True iff ``candidate`` is parseable as an IPv6 address.

    The regex catches the broad shape; stdlib confirms validity (rejecting
    e.g. groups with > 4 hex digits, > 7 colons, etc.).
    """
    import ipaddress
    try:
        ipaddress.IPv6Address(candidate)
        return True
    except (ipaddress.AddressValueError, ValueError):
        return False


# ---------------------------------------------------------------------------
# Module-level default path resolver
# ---------------------------------------------------------------------------

def default_pattern_path() -> Path:
    """Resolve ``<repo-root>/config/patterns.yaml`` relative to this file.

    Layout: ``<repo>/src/detection/patterns.py`` → parents[2] is ``<repo>``.
    """
    return Path(__file__).resolve().parents[2] / "config" / "patterns.yaml"
