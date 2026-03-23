"""Word count map function."""

from src.mapfunctions.registry import register_map


@register_map("word_count")
def word_count_map(log_line: dict):
    """Emit (word, 1) for each word in the log message."""
    message = log_line.get("message", "")
    for word in message.lower().split():
        # Clean punctuation
        word = word.strip(".,!?;:\"'()[]{}").lower()
        if word:
            yield (word, 1)
