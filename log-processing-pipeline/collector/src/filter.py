"""Raw line filter for the collector.

Each rule has a ``pattern`` (regex) and ``action`` ("include" or "exclude").
Rules are evaluated in order. An "exclude" match drops the line, an "include"
match keeps it. If no rule matches, the line is kept by default.
"""

import re


class RawLineFilter:
    def __init__(self, rules: list[dict]):
        self._rules = [(re.compile(r["pattern"]), r["action"]) for r in rules]

    def should_keep(self, line: str) -> bool:
        for pattern, action in self._rules:
            if pattern.search(line):
                return action == "include"
        return True
