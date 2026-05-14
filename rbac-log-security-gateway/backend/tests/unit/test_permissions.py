"""Unit tests for the permission DSL parser and matcher."""
import pytest

from src.rbac.permissions import Decision, Permission, match, parse


def test_parse_extracts_pattern_and_is_deny() -> None:
    p_allow = parse("logs:read:application.auth")
    assert p_allow.pattern == "logs:read:application.auth"
    assert p_allow.is_deny is False
    assert p_allow.raw == "logs:read:application.auth"

    p_deny = parse("!logs:export:business.financial")
    assert p_deny.pattern == "logs:export:business.financial"
    assert p_deny.is_deny is True
    assert p_deny.raw == "!logs:export:business.financial"


def test_parse_attaches_tags() -> None:
    p = parse("logs:read:business.*", tags=["aggregated_only"])
    assert p.tags == frozenset({"aggregated_only"})


@pytest.mark.parametrize("raw", ["", ":", "logs", "logs:read", "!logs:read", "::"])
def test_parse_rejects_malformed(raw: str) -> None:
    with pytest.raises(ValueError):
        parse(raw)


def test_plain_allow_match() -> None:
    perms = [parse("logs:read:application.auth")]
    d = match(perms, "logs:read:application.auth")
    assert d.allow is True
    assert d.rule == "logs:read:application.auth"
    assert d.reason == "allow match"


def test_no_match_returns_deny_with_no_rule() -> None:
    perms = [parse("logs:read:application.auth")]
    d = match(perms, "logs:read:business.metrics")
    assert d.allow is False
    assert d.rule is None
    assert d.reason == "no matching rule"


def test_wildcard_matches_subresource() -> None:
    perms = [parse("logs:read:application.*")]
    assert match(perms, "logs:read:application.auth").allow is True
    assert match(perms, "logs:read:application.worker").allow is True
    assert match(perms, "logs:read:business.metrics").allow is False  # different prefix


def test_deny_over_allow_precedence() -> None:
    """Deny rules always evaluated first, even when allow appears first in the list."""
    perms = [
        parse("logs:read:business.*"),         # allow business.*
        parse("!logs:read:business.customer"), # but deny business.customer
    ]
    d = match(perms, "logs:read:business.customer")
    assert d.allow is False
    assert d.reason == "explicit deny"
    assert d.rule == "!logs:read:business.customer"


def test_deny_on_wildcard_blocks_specific_resource() -> None:
    """!logs:export:*.financial must block logs:export:business.financial."""
    perms = [
        parse("logs:export:*"),
        parse("!logs:export:*.financial"),
    ]
    d = match(perms, "logs:export:business.financial")
    assert d.allow is False
    assert d.reason == "explicit deny"


def test_allow_unaffected_by_unrelated_deny() -> None:
    perms = [
        parse("logs:read:application.*"),
        parse("!logs:read:business.customer"),
    ]
    d = match(perms, "logs:read:application.auth")
    assert d.allow is True
    assert d.rule == "logs:read:application.*"


def test_tags_propagate_from_matched_allow() -> None:
    perms = [parse("logs:read:business.*", tags=["aggregated_only"])]
    d = match(perms, "logs:read:business.metrics")
    assert d.allow is True
    assert d.tags == frozenset({"aggregated_only"})


def test_tags_empty_for_no_match() -> None:
    perms = [parse("logs:read:business.*", tags=["aggregated_only"])]
    d = match(perms, "logs:read:application.api")
    assert d.allow is False
    assert d.tags == frozenset()


def test_tags_empty_for_deny() -> None:
    """Even if a deny rule had tags, the deny Decision must not carry tags."""
    perms = [
        parse("logs:read:business.*", tags=["aggregated_only"]),
        parse("!logs:read:business.customer", tags=["should_not_propagate"]),
    ]
    d = match(perms, "logs:read:business.customer")
    assert d.allow is False
    assert d.tags == frozenset()


def test_empty_permission_list_denies_everything() -> None:
    d = match([], "logs:read:application.auth")
    assert d.allow is False
    assert d.reason == "no matching rule"


def test_wildcard_action_and_resource() -> None:
    """A super-permission like logs:*:* matches anything."""
    perms = [parse("logs:*:*")]
    assert match(perms, "logs:read:application.auth").allow is True
    assert match(perms, "logs:export:business.metrics").allow is True
    assert match(perms, "logs:admin:system.kernel").allow is True


def test_decision_is_dataclass_frozen() -> None:
    d = Decision(allow=True, rule="logs:read:*", reason="allow match", tags=frozenset({"x"}))
    with pytest.raises(Exception):
        d.allow = False  # type: ignore[misc]


def test_permission_dataclass_is_frozen() -> None:
    p = parse("logs:read:application.auth")
    with pytest.raises(Exception):
        p.pattern = "something else"  # type: ignore[misc]
