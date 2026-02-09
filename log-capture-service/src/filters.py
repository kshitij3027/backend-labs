"""EntryProcessor: regex-based filtering and tagging of log entries."""

import re
import logging

from src.models import LogEntry
from src.config import FilterRule, TagRule

logger = logging.getLogger(__name__)


class EntryProcessor:
    def __init__(self, filter_rules: list[FilterRule], tag_rules: list[TagRule]):
        self._include_patterns = []
        self._exclude_patterns = []
        self._tag_rules: list[tuple[str, re.Pattern, str]] = []

        for rule in filter_rules:
            compiled = re.compile(rule.pattern)
            if rule.action == "exclude":
                self._exclude_patterns.append(compiled)
            elif rule.action == "include":
                self._include_patterns.append(compiled)

        for rule in tag_rules:
            self._tag_rules.append((rule.name, re.compile(rule.pattern), rule.field))

    def should_include(self, entry: LogEntry) -> bool:
        """Check if entry passes filter rules.

        Exclude rules: if any match the raw line, reject.
        Include rules: if any exist, at least one must match.
        """
        raw = entry.raw

        for pattern in self._exclude_patterns:
            if pattern.search(raw):
                return False

        if self._include_patterns:
            return any(p.search(raw) for p in self._include_patterns)

        return True

    def apply_tags(self, entry: LogEntry) -> LogEntry:
        """Apply tag rules and always add severity:<LEVEL>."""
        tags = [f"severity:{entry.level}"]

        for name, pattern, field in self._tag_rules:
            value = getattr(entry, field, entry.raw)
            if pattern.search(value):
                tags.append(name)

        entry.tags = tags
        return entry

    def process(self, entry: LogEntry) -> LogEntry | None:
        """Filter then tag. Returns None if excluded."""
        if not self.should_include(entry):
            return None
        return self.apply_tags(entry)
