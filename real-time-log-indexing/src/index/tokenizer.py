"""Regex-based tokenizer for log messages.

The tokenizer turns a raw log message into a list of lowercased
terms suitable for an inverted index. Special structures that appear
in real logs — IPs (optionally with port), email addresses, URLs,
ISO-8601 timestamps, UUIDs, and dotted identifiers like
``service.user.controller`` — are preserved as compound tokens so
they are searchable by their full form. For IPs, emails, URLs, and
dotted identifiers we also emit the component parts so a query for
``example`` still finds ``user@example.com``.

Design notes
------------

* We lowercase upfront so every downstream comparison works on the
  same case.
* We do a **single left-to-right scan** that matches compound
  patterns with a fixed priority (URL > EMAIL > UUID > ISO_TS > IP >
  DOTTED_ID > NUMBER > WORD). The priority prevents e.g. an IP being
  re-matched as three dotted identifiers, or an email local-part
  being re-emitted as a bare word from the leftover text.
* Compound tokens emit their component parts immediately after the
  compound itself; UUIDs and ISO timestamps are kept atomic because
  their hex/digit chunks are noise.
* The minimum-length filter is measured on the token as-is so
  numeric tokens like ``500`` (length 3) still pass even though they
  contain only digits.
"""

from __future__ import annotations

import re
from typing import Iterable


class LogTokenizer:
    """Turns a log message string into lowercased terms for the inverted index.

    Preserves IPs, emails, URLs, dotted identifiers, and UUIDs as compound
    tokens so they are searchable by their full form as well as component
    parts.
    """

    # ------------------------------------------------------------------
    # Compound patterns (apply in priority order during the single scan).
    # Order matters: URL must win over EMAIL (URLs may contain ``@``),
    # EMAIL before IP (``foo@10.0.0.1`` would otherwise split weirdly),
    # UUID before DOTTED_ID (hex groups are not dotted), ISO_TS before
    # NUMBER (``2026-04-19...`` shouldn't decompose to digits), IP before
    # DOTTED_ID/NUMBER so ``192.168.1.1`` survives intact, DOTTED_ID
    # before WORD so we get the compound plus parts.
    # ------------------------------------------------------------------

    IP_PATTERN = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}(?::\d{1,5})?\b")
    EMAIL_PATTERN = re.compile(r"\b[\w\.\+\-]+@[\w\-]+\.[\w\-\.]+\b")
    URL_PATTERN = re.compile(r"\bhttps?://[^\s\"<>]+")
    UUID_PATTERN = re.compile(
        r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b",
        re.I,
    )
    ISO_TS_PATTERN = re.compile(
        r"\b\d{4}-\d{2}-\d{2}[Tt ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+\-]\d{2}:?\d{2})?\b"
    )
    # Dotted identifier: starts with an alpha/underscore, then at least one
    # ``.segment`` where each segment also starts alpha/underscore. This
    # avoids matching bare numeric decimals.
    DOTTED_ID_PATTERN = re.compile(r"\b[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)+\b")
    # Bare numbers — status codes, durations, counts.
    NUMBER_PATTERN = re.compile(r"\b\d+(?:\.\d+)?\b")
    # Fallback word: alpha-led identifier run.
    WORD_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_]*")

    # Pattern scan order. Names are only used for debuggability; the
    # tuple order is what matters for dispatch.
    _PATTERN_ORDER: tuple[tuple[str, re.Pattern[str]], ...] = (
        ("URL", URL_PATTERN),
        ("EMAIL", EMAIL_PATTERN),
        ("UUID", UUID_PATTERN),
        ("ISO_TS", ISO_TS_PATTERN),
        ("IP", IP_PATTERN),
        ("DOTTED_ID", DOTTED_ID_PATTERN),
        ("NUMBER", NUMBER_PATTERN),
        ("WORD", WORD_PATTERN),
    )

    # Kinds whose compound token should also emit component sub-tokens.
    # UUID and ISO_TS deliberately excluded.
    _DECOMPOSE_KINDS: frozenset[str] = frozenset({"URL", "EMAIL", "IP", "DOTTED_ID"})

    STOP_WORDS: frozenset[str] = frozenset(
        {
            "a", "an", "the", "is", "are", "was", "were", "be", "been",
            "to", "of", "in", "on", "at", "and", "or", "but", "for",
            "with", "as", "by", "from", "that", "this",
        }
    )

    MIN_TERM_LEN: int = 2

    def __init__(
        self,
        stop_words: Iterable[str] | None = None,
        min_term_len: int | None = None,
    ) -> None:
        """Build a tokenizer, optionally overriding stop-words / min length.

        Passing ``stop_words=set()`` disables stop-word filtering. Passing
        ``min_term_len=1`` keeps single-character tokens.
        """
        self.stop_words: frozenset[str] = (
            frozenset(s.lower() for s in stop_words)
            if stop_words is not None
            else self.STOP_WORDS
        )
        self.min_term_len: int = (
            min_term_len if min_term_len is not None else self.MIN_TERM_LEN
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _decompose(self, kind: str, token: str) -> list[str]:
        """Return the component sub-tokens for a compound match.

        Only called for kinds in ``_DECOMPOSE_KINDS``. Order matters —
        we preserve left-to-right appearance inside the compound so
        positional ordering stays intuitive.
        """
        if kind == "IP":
            # ``192.168.1.1`` or ``192.168.1.1:8080`` — when a port is
            # present, emit the bare IP (no port) first so searches for
            # ``192.168.1.100`` match even when the log had a port, then
            # each octet.
            host = token.split(":", 1)[0]
            parts: list[str] = []
            if host != token:
                parts.append(host)
            parts.extend(p for p in host.split(".") if p)
            return parts

        if kind == "EMAIL":
            # ``user.name+tag@host.sub.tld`` -> local-part pieces + domain
            # pieces. Split on ``.`` / ``+`` / ``-`` inside the local
            # part so ``user.name`` yields ``user`` and ``name`` too.
            try:
                local, domain = token.split("@", 1)
            except ValueError:
                return []
            parts: list[str] = []
            parts.extend(p for p in re.split(r"[\.\+\-]", local) if p)
            parts.extend(p for p in re.split(r"[\.\-]", domain) if p)
            return parts

        if kind == "DOTTED_ID":
            return [p for p in token.split(".") if p]

        if kind == "URL":
            # ``https://api.example.com/v1/users?x=1#frag``
            # Drop scheme, then yield (host-as-dotted-id, host parts,
            # each path/query segment).
            m = re.match(r"https?://([^/?#\s]+)(.*)", token)
            if not m:
                return []
            host, tail = m.group(1), m.group(2)
            parts: list[str] = []
            if host:
                # Strip any user-info or port fragments for the sub-token.
                host_clean = host.split("@", 1)[-1].split(":", 1)[0]
                if host_clean:
                    # Emit the whole host as a compound (so a search for
                    # ``api.example.com`` matches) plus each label.
                    if "." in host_clean:
                        parts.append(host_clean)
                    parts.extend(p for p in host_clean.split(".") if p)
            # Path/query — split on anything that isn't an identifier
            # character so ``/v1/users?x=1`` gives ``v1``, ``users``,
            # ``x``, ``1``.
            parts.extend(p for p in re.split(r"[^A-Za-z0-9_]+", tail) if p)
            return parts

        return []  # pragma: no cover - guarded by _DECOMPOSE_KINDS

    def _scan(self, text: str) -> list[str]:
        """Single left-to-right pass — return ordered raw tokens.

        Tokens are lowercased (``text`` arrives already lowercased) and
        compound matches are immediately followed by their component
        sub-tokens. No filtering is applied here; that happens in
        ``_filter`` so both public methods can share logic.
        """
        tokens: list[str] = []
        n = len(text)
        i = 0
        while i < n:
            # Fast-skip characters that can't start any pattern we care
            # about. Our scanners all anchor on alnum / ``h`` / digit, so
            # punctuation/whitespace can be skipped cheaply.
            ch = text[i]
            if not (ch.isalnum() or ch == "_"):
                i += 1
                continue

            matched = False
            for kind, pattern in self._PATTERN_ORDER:
                m = pattern.match(text, i)
                if m is None:
                    continue
                token = m.group(0)
                tokens.append(token)
                if kind in self._DECOMPOSE_KINDS:
                    tokens.extend(self._decompose(kind, token))
                i = m.end()
                matched = True
                break
            if not matched:
                # No pattern fired at this index — advance by one to
                # avoid an infinite loop. Should be rare because WORD
                # covers alpha-led runs and NUMBER covers digit runs;
                # lone underscores / digits after underscores fall
                # through here.
                i += 1
        return tokens

    def _keep(self, token: str) -> bool:
        """Return True if *token* survives the stop-word + length filter."""
        if token in self.stop_words:
            return False
        # Length check counts the token as-is so numeric tokens pass
        # when ``min_term_len <= len(digits)``.
        return len(token) >= self.min_term_len

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def tokenize(self, text: str) -> list[str]:
        """Return unique lowercased tokens in first-seen order."""
        if not text or not text.strip():
            return []
        lowered = text.lower()
        raw = self._scan(lowered)
        seen: set[str] = set()
        out: list[str] = []
        for tok in raw:
            if tok in seen:
                continue
            if not self._keep(tok):
                continue
            seen.add(tok)
            out.append(tok)
        return out

    def tokenize_with_positions(self, text: str) -> dict[str, list[int]]:
        """Return term -> positions (0-based, post-filter stream index).

        Positions are assigned **after** stop-word / length filtering, so
        they reflect the sequence of terms that actually make it into
        the index. Compound tokens and their sub-tokens occupy
        consecutive positions in the order the scan emitted them.
        """
        if not text or not text.strip():
            return {}
        lowered = text.lower()
        raw = self._scan(lowered)
        positions: dict[str, list[int]] = {}
        pos = 0
        for tok in raw:
            if not self._keep(tok):
                continue
            positions.setdefault(tok, []).append(pos)
            pos += 1
        return positions
