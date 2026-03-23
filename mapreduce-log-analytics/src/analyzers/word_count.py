"""Word count analyzer: tokenizes log messages and counts word occurrences."""

import re

from src.analyzers.registry import register_map, register_reduce


@register_map("word_count")
def word_count_map(record: dict) -> list[tuple[str, int]]:
    """Tokenize the message field, filter words <= 2 chars, emit (word, 1)."""
    message = record.get("message", "")
    results = []
    # Tokenize: split on whitespace, strip punctuation
    for token in message.split():
        word = re.sub(r'[^\w]', '', token).lower()
        if len(word) > 2:
            results.append((word, 1))
    return results


@register_reduce("word_count")
def word_count_reduce(key: str, values: list) -> int:
    """Sum all counts for a word."""
    return sum(values)
