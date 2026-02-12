"""Statistics — level counts, entries per hour, common errors."""

import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Iterable

from src.parser import LogEntry


@dataclass
class LogStats:
    total_entries: int = 0
    level_counts: dict[str, int] = field(default_factory=dict)
    entries_per_hour: dict[str, int] = field(default_factory=dict)
    error_messages: list[str] = field(default_factory=list)


def compute_stats(entries: Iterable[LogEntry]) -> LogStats:
    """Consume an entry stream and produce aggregated statistics."""
    level_counter = Counter()
    hour_counter = Counter()
    error_msgs = []
    total = 0

    for entry in entries:
        total += 1
        level_counter[entry.level] += 1
        hour_key = entry.timestamp.strftime("%Y-%m-%d %H:00")
        hour_counter[hour_key] += 1
        if entry.level == "ERROR":
            error_msgs.append(entry.message)

    return LogStats(
        total_entries=total,
        level_counts=dict(level_counter.most_common()),
        entries_per_hour=dict(sorted(hour_counter.items())),
        error_messages=error_msgs,
    )


def format_stats_text(stats: LogStats) -> str:
    """Human-readable stats summary."""
    lines = []
    lines.append(f"Total entries: {stats.total_entries}")
    lines.append("")

    lines.append("Level counts:")
    for level, count in stats.level_counts.items():
        lines.append(f"  {level:8s} {count}")
    lines.append("")

    lines.append("Entries per hour:")
    for hour, count in stats.entries_per_hour.items():
        lines.append(f"  {hour}  {count}")
    lines.append("")

    if stats.error_messages:
        lines.append(f"Error messages ({len(stats.error_messages)}):")
        for msg in stats.error_messages:
            lines.append(f"  - {msg}")
    else:
        lines.append("No error messages.")

    return "\n".join(lines)


def format_stats_json(stats: LogStats) -> str:
    """JSON stats output."""
    return json.dumps({
        "total_entries": stats.total_entries,
        "level_counts": stats.level_counts,
        "entries_per_hour": stats.entries_per_hour,
        "error_messages": stats.error_messages,
    }, indent=2)
