"""Rule engine for matching log messages to metadata collectors."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class Rule:
    """A single enrichment rule that matches log messages to collectors."""

    name: str
    keywords: List[str]
    match_all: bool = False
    collectors: List[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._lower_keywords: List[str] = [k.lower() for k in self.keywords]

    def matches(self, log_message: str) -> bool:
        """Check whether this rule matches the given log message.

        If match_all is True, always returns True.
        Otherwise, returns True if any keyword is found as a substring
        in the lowercased log message.
        """
        if self.match_all:
            return True
        msg_lower = log_message.lower()
        return any(kw in msg_lower for kw in self._lower_keywords)


class RuleEngine:
    """Evaluates a sequence of rules against log messages."""

    def __init__(self, rules: List[Rule]) -> None:
        self._rules = rules

    def evaluate(self, log_message: str) -> List[str]:
        """Return deduplicated collectors for all rules matching *log_message*.

        Rules are evaluated in order.  Collector names are deduplicated while
        preserving first-seen order.
        """
        seen: set[str] = set()
        result: List[str] = []
        for rule in self._rules:
            if rule.matches(log_message):
                for collector in rule.collectors:
                    if collector not in seen:
                        seen.add(collector)
                        result.append(collector)
        return result

    @classmethod
    def from_yaml(cls, data: dict) -> RuleEngine:
        """Build a RuleEngine from the parsed YAML dict returned by load_rules().

        Falls back to default() when *data* is empty or missing the 'rules' key.
        """
        raw_rules = data.get("rules", []) if data else []
        if not raw_rules:
            return cls.default()
        rules = [
            Rule(
                name=r["name"],
                keywords=r.get("keywords", []),
                match_all=r.get("match_all", False),
                collectors=r.get("collectors", []),
            )
            for r in raw_rules
        ]
        return cls(rules)

    @classmethod
    def default(cls) -> RuleEngine:
        """Return a RuleEngine with the three standard hardcoded rules."""
        return cls(
            [
                Rule(
                    name="critical_errors",
                    keywords=["error", "critical", "fatal", "exception", "traceback"],
                    match_all=False,
                    collectors=["system_info", "environment", "performance"],
                ),
                Rule(
                    name="warnings",
                    keywords=["warning", "warn", "deprecated"],
                    match_all=False,
                    collectors=["system_info", "environment", "performance"],
                ),
                Rule(
                    name="default",
                    keywords=[],
                    match_all=True,
                    collectors=["system_info", "environment"],
                ),
            ]
        )
