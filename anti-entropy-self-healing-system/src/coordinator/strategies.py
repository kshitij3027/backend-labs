from enum import Enum


class Strategy(Enum):
    LATEST_WRITE_WINS = "latest_write_wins"
    HIGHEST_VERSION = "highest_version"


def resolve_conflict(entries: list[dict], strategy: Strategy = Strategy.LATEST_WRITE_WINS) -> dict:
    """Given a list of entry dicts (value, version, timestamp), return the winner."""
    if not entries:
        raise ValueError("No entries to resolve")
    if strategy == Strategy.LATEST_WRITE_WINS:
        return max(entries, key=lambda e: e.get("timestamp", 0))
    elif strategy == Strategy.HIGHEST_VERSION:
        return max(entries, key=lambda e: e.get("version", 0))
    return entries[0]
