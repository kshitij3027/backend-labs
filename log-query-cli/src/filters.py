"""Filter predicates for log entries — level, search, date, time-range."""

from datetime import datetime, time
from typing import Callable

from src.parser import LogEntry


def filter_by_level(entry: LogEntry, level: str) -> bool:
    """True if entry matches the given level (case-insensitive)."""
    return entry.level == level.upper()


def filter_by_search(entry: LogEntry, keyword: str) -> bool:
    """True if keyword appears in the message (case-insensitive)."""
    return keyword.lower() in entry.message.lower()


def filter_by_date(entry: LogEntry, date_str: str) -> bool:
    """True if entry's date matches YYYY-MM-DD string."""
    target = datetime.strptime(date_str, "%Y-%m-%d").date()
    return entry.timestamp.date() == target


def filter_by_time_range(entry: LogEntry, time_range_str: str) -> bool:
    """True if entry's time falls within HH:MM-HH:MM (inclusive).

    Handles cross-midnight ranges (e.g. 23:00-01:00).
    """
    start_str, end_str = time_range_str.split("-")
    start = time.fromisoformat(start_str.strip())
    end = time.fromisoformat(end_str.strip())
    entry_time = entry.timestamp.time()

    if start <= end:
        return start <= entry_time <= end
    else:
        # Cross-midnight: e.g. 23:00-01:00
        return entry_time >= start or entry_time <= end


def build_filter_chain(args) -> Callable[[LogEntry], bool]:
    """Combine all active filters from parsed args into a single callable.

    Returns a function that ANDs all active predicates together.
    """
    predicates = []

    if getattr(args, "level", None):
        level = args.level
        predicates.append(lambda entry, l=level: filter_by_level(entry, l))

    if getattr(args, "search", None):
        keyword = args.search
        predicates.append(lambda entry, k=keyword: filter_by_search(entry, k))

    if getattr(args, "date", None):
        date_str = args.date
        predicates.append(lambda entry, d=date_str: filter_by_date(entry, d))

    if getattr(args, "time_range", None):
        time_range_str = args.time_range
        predicates.append(lambda entry, t=time_range_str: filter_by_time_range(entry, t))

    if not predicates:
        return lambda entry: True

    def combined(entry: LogEntry) -> bool:
        return all(p(entry) for p in predicates)

    return combined
