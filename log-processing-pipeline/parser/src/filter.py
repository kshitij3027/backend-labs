"""Parsed entry filter for the parser.

Each rule has ``field``, ``pattern``, and ``action``.
Checks the specified field of the parsed dict against the pattern.
"""

import re


class EntryFilter:
    def __init__(self, rules: list[dict]):
        self._rules = [
            (r["field"], re.compile(str(r["pattern"])), r["action"])
            for r in rules
        ]

    def should_keep(self, entry: dict) -> bool:
        for field, pattern, action in self._rules:
            value = str(entry.get(field, ""))
            if pattern.search(value):
                return action == "include"
        return True
