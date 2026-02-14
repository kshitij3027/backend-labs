"""Log level filtering — pure functions for level comparison."""

from src.config import LOG_LEVELS


def level_index(level: str) -> int:
    """Return the position of a log level in LOG_LEVELS, or -1 if unknown."""
    normalized = level.strip().upper()
    if normalized in LOG_LEVELS:
        return LOG_LEVELS.index(normalized)
    return -1


def should_accept(message_level: str, min_level: str) -> bool:
    """Return True if message_level >= min_level in severity."""
    msg_idx = level_index(message_level)
    min_idx = level_index(min_level)
    if msg_idx == -1 or min_idx == -1:
        return False
    return msg_idx >= min_idx
