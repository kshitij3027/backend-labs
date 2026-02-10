"""Statistics collector — per-file tracking with atomic persistence."""

import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone

from src.models import ParsedLogEntry


@dataclass
class FileStats:
    total_lines: int = 0
    success_count: int = 0
    failure_count: int = 0
    format_counts: dict[str, int] = field(default_factory=dict)
    status_code_distribution: dict[str, int] = field(default_factory=dict)
    ip_distribution: dict[str, int] = field(default_factory=dict)
    level_distribution: dict[str, int] = field(default_factory=dict)


class StatsCollector:
    """Tracks parsing statistics per file. Re-processing replaces (not appends)."""

    def __init__(self, output_dir: str):
        self._output_dir = output_dir
        self._file_stats: dict[str, FileStats] = {}

    def record_file(self, filepath: str, entries: list[ParsedLogEntry]):
        """Record stats for all entries from a single file, replacing any previous data."""
        stats = FileStats()
        for entry in entries:
            stats.total_lines += 1
            if entry.parsed:
                stats.success_count += 1
                fmt = entry.source_format
                stats.format_counts[fmt] = stats.format_counts.get(fmt, 0) + 1
                if entry.status_code is not None:
                    key = str(entry.status_code)
                    stats.status_code_distribution[key] = stats.status_code_distribution.get(key, 0) + 1
                if entry.remote_host:
                    stats.ip_distribution[entry.remote_host] = stats.ip_distribution.get(entry.remote_host, 0) + 1
                if entry.level:
                    stats.level_distribution[entry.level] = stats.level_distribution.get(entry.level, 0) + 1
            else:
                stats.failure_count += 1
        self._file_stats[filepath] = stats

    def save(self):
        """Aggregate across all files and write parsing_stats.json atomically."""
        total_lines = 0
        success = 0
        failure = 0
        format_counts: dict[str, int] = {}
        status_codes: dict[str, int] = {}
        ips: dict[str, int] = {}
        levels: dict[str, int] = {}

        for fs in self._file_stats.values():
            total_lines += fs.total_lines
            success += fs.success_count
            failure += fs.failure_count
            for k, v in fs.format_counts.items():
                format_counts[k] = format_counts.get(k, 0) + v
            for k, v in fs.status_code_distribution.items():
                status_codes[k] = status_codes.get(k, 0) + v
            for k, v in fs.ip_distribution.items():
                ips[k] = ips.get(k, 0) + v
            for k, v in fs.level_distribution.items():
                levels[k] = levels.get(k, 0) + v

        aggregate = {
            "total_lines": total_lines,
            "success_count": success,
            "failure_count": failure,
            "format_counts": format_counts,
            "status_code_distribution": status_codes,
            "ip_distribution": ips,
            "level_distribution": levels,
            "files_processed": len(self._file_stats),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }

        os.makedirs(self._output_dir, exist_ok=True)
        target = os.path.join(self._output_dir, "parsing_stats.json")
        tmp_fd, tmp_path = tempfile.mkstemp(dir=self._output_dir, suffix=".tmp")
        try:
            with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
                json.dump(aggregate, f, indent=2)
                f.write("\n")
            os.replace(tmp_path, target)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
