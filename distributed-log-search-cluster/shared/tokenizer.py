"""Multi-pass regex tokenizer for log messages."""

import re
from typing import Dict, List


class LogTokenizer:
    """Multi-pass regex tokenizer for log messages.

    Extracts structured tokens (IPs, emails, URLs, timestamps) first,
    then compound dotted terms, then splits the remainder into words.
    All tokens are lowercased; stop words and short tokens are removed.
    """

    STOP_WORDS = {
        "a", "an", "the", "is", "was", "were", "are", "at", "in", "on", "of",
        "to", "for", "and", "or", "but", "not", "with", "from", "by", "this",
        "that", "it", "be", "has", "had", "have", "do", "does", "did", "will",
        "would", "could", "should", "may", "might", "can", "shall", "its",
        "as", "if", "then", "than", "so", "no", "up", "out", "about",
    }

    # Compiled regex patterns (order matters for extraction priority)
    IP_PATTERN = re.compile(r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")
    EMAIL_PATTERN = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")
    URL_PATTERN = re.compile(r"https?://[^\s<>\"']+|www\.[^\s<>\"']+")
    TIMESTAMP_PATTERN = re.compile(
        r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?"
    )
    COMPOUND_PATTERN = re.compile(r"\b[a-zA-Z]\w*\.[a-zA-Z]\w*(?:\.[a-zA-Z]\w*)*\b")

    def _extract_pattern(
        self, pattern: re.Pattern, text: str, tokens: list[str]
    ) -> str:
        """Find all matches for *pattern* in *text*, append them to *tokens*,
        and return the text with matched spans replaced by spaces."""
        for match in pattern.finditer(text):
            tokens.append(match.group())
        return pattern.sub(" ", text)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def tokenize(self, text: str) -> list[str]:
        """Tokenize a log message into a deduplicated list of terms.

        Processing passes:
        1. Extract IPs, emails, URLs, timestamps (structured tokens).
        2. Extract compound dotted terms and their individual components.
        3. Split the remaining text on non-alphanumeric characters.
        4. Lowercase everything, filter stop words and short tokens.
        5. Deduplicate while preserving insertion order.
        """
        if not text or not text.strip():
            return []

        tokens: list[str] = []
        working = text

        # --- Pass 1: structured tokens (order matters) ---
        working = self._extract_pattern(self.URL_PATTERN, working, tokens)
        working = self._extract_pattern(self.EMAIL_PATTERN, working, tokens)
        working = self._extract_pattern(self.TIMESTAMP_PATTERN, working, tokens)
        working = self._extract_pattern(self.IP_PATTERN, working, tokens)

        # --- Pass 2: compound dotted terms ---
        compound_tokens: list[str] = []
        for match in self.COMPOUND_PATTERN.finditer(working):
            compound = match.group()
            compound_tokens.append(compound)
            # Also add each dot-separated component
            for part in compound.split("."):
                if part:
                    compound_tokens.append(part)
        working = self.COMPOUND_PATTERN.sub(" ", working)
        tokens.extend(compound_tokens)

        # --- Pass 3: split remainder on non-alphanumeric ---
        remainder = re.split(r"[^a-zA-Z0-9]+", working)
        tokens.extend(part for part in remainder if part)

        # --- Pass 4: lowercase, filter stop words + short tokens ---
        tokens = [t.lower() for t in tokens]
        tokens = [
            t for t in tokens if t not in self.STOP_WORDS and len(t) >= 2
        ]

        # --- Pass 5: deduplicate, preserve insertion order ---
        return list(dict.fromkeys(tokens))

    def tokenize_with_positions(self, text: str) -> Dict[str, List[int]]:
        """Tokenize and track the positional index of every token occurrence.

        Returns a dict mapping each term to a sorted list of positions
        (0-based sequential indices in the document).  Unlike ``tokenize``,
        duplicate occurrences are preserved in the position lists.
        """
        if not text or not text.strip():
            return {}

        tokens: list[str] = []
        working = text

        # --- Pass 1: structured tokens ---
        working = self._extract_pattern(self.URL_PATTERN, working, tokens)
        working = self._extract_pattern(self.EMAIL_PATTERN, working, tokens)
        working = self._extract_pattern(self.TIMESTAMP_PATTERN, working, tokens)
        working = self._extract_pattern(self.IP_PATTERN, working, tokens)

        # --- Pass 2: compound dotted terms ---
        compound_tokens: list[str] = []
        for match in self.COMPOUND_PATTERN.finditer(working):
            compound = match.group()
            compound_tokens.append(compound)
            for part in compound.split("."):
                if part:
                    compound_tokens.append(part)
        working = self.COMPOUND_PATTERN.sub(" ", working)
        tokens.extend(compound_tokens)

        # --- Pass 3: split remainder ---
        remainder = re.split(r"[^a-zA-Z0-9]+", working)
        tokens.extend(part for part in remainder if part)

        # --- Pass 4: lowercase + filter ---
        tokens = [t.lower() for t in tokens]
        tokens = [
            t for t in tokens if t not in self.STOP_WORDS and len(t) >= 2
        ]

        # --- Build position map (no deduplication — track every occurrence) ---
        positions: Dict[str, List[int]] = {}
        for idx, term in enumerate(tokens):
            positions.setdefault(term, []).append(idx)

        return positions
