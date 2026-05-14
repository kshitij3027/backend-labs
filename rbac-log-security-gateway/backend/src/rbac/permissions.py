"""Permission DSL: parsing, matching, and decisions.

Permission strings:  logs:<action>:<resource>
  - <action> ∈ {read, export, audit, admin}
  - <resource> is a dotted name like "application.auth"
  - "*" wildcards via fnmatch
  - Deny rules start with "!" and override allows

Examples:
  "logs:read:application.*"      — allow reading application logs
  "!logs:export:business.*"      — deny exporting any business log
  "logs:admin:*"                 — allow all admin actions
"""
from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from typing import FrozenSet, Iterable


@dataclass(frozen=True)
class Permission:
    """Parsed permission. `pattern` is the rule body without the leading `!`."""
    raw: str
    pattern: str
    is_deny: bool
    tags: FrozenSet[str] = field(default_factory=frozenset)

    def matches(self, requested: str) -> bool:
        return fnmatch.fnmatchcase(requested, self.pattern)


@dataclass(frozen=True)
class Decision:
    """Result of an RBAC check. `rule` is the raw matched permission string (with `!` prefix for denies)."""
    allow: bool
    rule: str | None
    reason: str
    tags: FrozenSet[str] = field(default_factory=frozenset)


def parse(raw: str, tags: Iterable[str] = ()) -> Permission:
    """Parse a permission string into a Permission. `tags` are attached at config time."""
    if not raw or ":" not in raw:
        raise ValueError(f"invalid permission string: {raw!r}")
    is_deny = raw.startswith("!")
    pattern = raw[1:] if is_deny else raw
    if not pattern or pattern.count(":") < 2:
        raise ValueError(f"permission must be 'domain:action:resource', got {raw!r}")
    if any(not seg for seg in pattern.split(":", 2)):
        raise ValueError(f"permission segments must be non-empty: {raw!r}")
    return Permission(raw=raw, pattern=pattern, is_deny=is_deny, tags=frozenset(tags))


def match(perms: Iterable[Permission], requested: str) -> Decision:
    """Evaluate a permission list against a requested action. Denies always win."""
    perm_list = list(perms)

    # 1. Denies first — first match short-circuits.
    for p in perm_list:
        if p.is_deny and p.matches(requested):
            return Decision(
                allow=False,
                rule=p.raw,
                reason="explicit deny",
                tags=frozenset(),
            )

    # 2. Allows next — first match wins; carry its tags.
    for p in perm_list:
        if not p.is_deny and p.matches(requested):
            return Decision(
                allow=True,
                rule=p.raw,
                reason="allow match",
                tags=p.tags,
            )

    # 3. No match → deny.
    return Decision(
        allow=False,
        rule=None,
        reason="no matching rule",
        tags=frozenset(),
    )
