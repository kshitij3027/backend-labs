"""Unit tests for the C2 PII detection layer.

Coverage targets:

* :class:`TestLuhn`             — Luhn checksum helper.
* :class:`TestPatternMatcher`   — every regex pattern, positive + negative.
* :class:`TestFieldNameMatcher` — substring matching + case insensitivity.
* :class:`TestDetector`         — full orchestration on real-shaped logs.

The default YAML configs at ``config/patterns.yaml`` and
``config/field_names.yaml`` are used unmodified — these are the same files
that ship in the runtime container.
"""
from __future__ import annotations

import pytest

from src.detection import (
    Detector,
    FieldNameMatcher,
    PatternMatcher,
    default_field_names_path,
    default_pattern_path,
)
from src.detection.patterns import _luhn


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def pattern_matcher() -> PatternMatcher:
    """Real production PatternMatcher built from the shipped YAML."""
    return PatternMatcher(default_pattern_path())


@pytest.fixture(scope="module")
def name_matcher() -> FieldNameMatcher:
    """Real production FieldNameMatcher built from the shipped YAML."""
    return FieldNameMatcher(default_field_names_path())


@pytest.fixture()
def detector(pattern_matcher: PatternMatcher, name_matcher: FieldNameMatcher) -> Detector:
    return Detector(patterns=pattern_matcher, names=name_matcher)


# ---------------------------------------------------------------------------
# TestLuhn — checksum helper
# ---------------------------------------------------------------------------

class TestLuhn:
    """Luhn algorithm validation for credit-card-number candidates."""

    def test_valid_visa_passes(self) -> None:
        # Well-known Visa test number (Luhn-valid).
        assert _luhn("4111 1111 1111 1111") is True

    def test_invalid_cc_fails(self) -> None:
        # Same prefix, last digit bumped by 1 — Luhn must reject.
        assert _luhn("4111 1111 1111 1112") is False

    def test_strips_non_digits(self) -> None:
        # Dashes, spaces, and stray letters should be stripped before summing.
        assert _luhn("4111-1111-1111-1111") is True
        assert _luhn("4111 1111 abc 1111 1111") is True

    def test_too_short_fails(self) -> None:
        # Fewer than 13 digits is structurally invalid even if sum % 10 == 0.
        assert _luhn("0") is False
        assert _luhn("123456789012") is False  # 12 digits

    def test_too_long_fails(self) -> None:
        # > 19 digits is also structurally invalid.
        assert _luhn("1" * 20) is False


# ---------------------------------------------------------------------------
# TestPatternMatcher — one positive + one negative per pattern type
# ---------------------------------------------------------------------------

class TestPatternMatcher:
    """Verifies each YAML pattern entry fires on a positive example and
    rejects a clear negative example."""

    # ---- email ----
    def test_email_positive(self, pattern_matcher: PatternMatcher) -> None:
        hits = pattern_matcher.match("contact me at alice@example.com please")
        assert any(h.field_type == "email" for h in hits)

    def test_email_negative(self, pattern_matcher: PatternMatcher) -> None:
        hits = pattern_matcher.match("no at-sign here just text")
        assert all(h.field_type != "email" for h in hits)

    # ---- ssn ----
    def test_ssn_positive(self, pattern_matcher: PatternMatcher) -> None:
        hits = pattern_matcher.match("SSN 123-45-6789 on file")
        assert any(h.field_type == "ssn" for h in hits)

    def test_ssn_negative_reserved_area(self, pattern_matcher: PatternMatcher) -> None:
        # Area number 000 is reserved → regex's negative lookahead rejects it.
        hits = pattern_matcher.match("000-12-3456")
        assert all(h.field_type != "ssn" for h in hits)

    # ---- phone ----
    def test_phone_positive(self, pattern_matcher: PatternMatcher) -> None:
        hits = pattern_matcher.match("call me at (415) 555-1234 anytime")
        assert any(h.field_type == "phone" for h in hits)

    def test_phone_negative(self, pattern_matcher: PatternMatcher) -> None:
        hits = pattern_matcher.match("just a short number 12345")
        assert all(h.field_type != "phone" for h in hits)

    # ---- credit_card ----
    def test_credit_card_positive_luhn_valid(self, pattern_matcher: PatternMatcher) -> None:
        # Visa test number — passes Luhn.
        hits = pattern_matcher.match("4111 1111 1111 1111")
        cc = [h for h in hits if h.field_type == "credit_card"]
        assert cc, "Luhn-valid CC should be detected"
        assert cc[0].confidence == 0.95  # promoted after Luhn

    def test_credit_card_negative_luhn_invalid(self, pattern_matcher: PatternMatcher) -> None:
        # Same digit shape but Luhn-invalid → must be dropped, not emitted at 0.0.
        hits = pattern_matcher.match("1234567890123456")
        assert all(h.field_type != "credit_card" for h in hits)

    def test_credit_card_order_id_not_flagged(self, pattern_matcher: PatternMatcher) -> None:
        # A 13-digit order ID that isn't Luhn-valid must NOT be flagged as CC.
        # 1234567890123 is famously Luhn-invalid.
        hits = pattern_matcher.match("1234567890123")
        assert all(h.field_type != "credit_card" for h in hits)

    # ---- jwt ----
    def test_jwt_positive(self, pattern_matcher: PatternMatcher) -> None:
        # Real-shape JWT (header.payload.signature); not a valid token,
        # but structurally matches the regex.
        jwt = (
            "eyJhbGciOiJIUzI1NiJ9"
            ".eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIn0"
            ".SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        )
        hits = pattern_matcher.match(f"Authorization Bearer {jwt}")
        assert any(h.field_type == "jwt" for h in hits)

    def test_jwt_negative(self, pattern_matcher: PatternMatcher) -> None:
        # No "eyJ" prefix → not a JWT.
        hits = pattern_matcher.match("aaaaa.bbbbb.ccccc")
        assert all(h.field_type != "jwt" for h in hits)

    # ---- ipv4 ----
    def test_ipv4_positive(self, pattern_matcher: PatternMatcher) -> None:
        hits = pattern_matcher.match("request from 192.168.1.42 received")
        assert any(h.field_type == "ipv4" for h in hits)

    def test_ipv4_negative_out_of_range(self, pattern_matcher: PatternMatcher) -> None:
        # 999 is out of the 0-255 octet range — regex rejects it.
        hits = pattern_matcher.match("999.999.999.999 not a real ip")
        assert all(h.field_type != "ipv4" for h in hits)

    # ---- ipv6 ----
    def test_ipv6_positive(self, pattern_matcher: PatternMatcher) -> None:
        hits = pattern_matcher.match("client 2001:0db8:85a3:0000:0000:8a2e:0370:7334 connected")
        assert any(h.field_type == "ipv6" for h in hits)

    def test_ipv6_negative_too_few_groups(self, pattern_matcher: PatternMatcher) -> None:
        # 4 groups satisfies the permissive regex but is not a valid IPv6
        # address (needs 8 groups, or `::` shorthand). The stdlib
        # `ipaddress.IPv6Address` check in PatternMatcher must reject it.
        hits = pattern_matcher.match("12:34:56:78 not a real address")
        assert all(h.field_type != "ipv6" for h in hits)

    # ---- generic ----
    def test_empty_value_returns_empty(self, pattern_matcher: PatternMatcher) -> None:
        assert pattern_matcher.match("") == []

    def test_non_string_returns_empty(self, pattern_matcher: PatternMatcher) -> None:
        # The matcher guards against being called with a non-string.
        # In practice the orchestrator stringifies first; this is defense in depth.
        assert pattern_matcher.match(None) == []  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# TestFieldNameMatcher — substring + case + miss
# ---------------------------------------------------------------------------

class TestFieldNameMatcher:
    """Verifies the field-name substring matcher."""

    def test_exact_match_email(self, name_matcher: FieldNameMatcher) -> None:
        hit = name_matcher.match("email")
        assert hit is not None
        assert hit.field_type == "email"
        assert hit.confidence == 0.95
        assert hit.reason == "field_name:email"

    def test_substring_match_customer_email(self, name_matcher: FieldNameMatcher) -> None:
        # "customer_email" should match because "email" is a substring.
        hit = name_matcher.match("customer_email")
        assert hit is not None
        assert hit.field_type == "email"

    def test_case_insensitive(self, name_matcher: FieldNameMatcher) -> None:
        # Upper- and mixed-case names should match identically.
        for name in ("EMAIL", "Email", "EMaIl", "CUSTOMER_EMAIL"):
            hit = name_matcher.match(name)
            assert hit is not None, f"expected hit for {name!r}"
            assert hit.field_type == "email"

    def test_safe_key_does_not_match(self, name_matcher: FieldNameMatcher) -> None:
        # "timestamp" is intentionally not in the sensitive-names list and
        # contains no sensitive substring.
        assert name_matcher.match("timestamp") is None

    def test_empty_name_returns_none(self, name_matcher: FieldNameMatcher) -> None:
        assert name_matcher.match("") is None

    def test_password_match(self, name_matcher: FieldNameMatcher) -> None:
        # Sanity: the canonical credential field always matches.
        hit = name_matcher.match("user_password")
        assert hit is not None
        assert hit.field_type == "password"


# ---------------------------------------------------------------------------
# TestDetector — full orchestrator over realistic dicts
# ---------------------------------------------------------------------------

class TestDetector:
    """End-to-end detection over realistic log shapes."""

    def test_empty_dict_returns_no_detections(self, detector: Detector) -> None:
        assert detector.detect({}) == []

    def test_field_name_beats_value_regex(self, detector: Detector) -> None:
        # `user_email` matches `email` by name; value just happens to be an
        # email too. Either way exactly one detection at path=user_email.
        log = {"user_email": "x@y.com", "order_id": "1234"}
        detections = detector.detect(log)
        assert len(detections) == 1
        assert detections[0].field_path == "user_email"
        # reason should reflect the WINNING matcher (field-name).
        assert detections[0].reason.startswith("field_name:")

    def test_value_regex_when_key_does_not_trigger(self, detector: Detector) -> None:
        # Key "note" doesn't contain any sensitive substring; value contains
        # an email → exactly one regex detection on `note`.
        log = {"note": "contact me at x@y.com"}
        detections = detector.detect(log)
        assert len(detections) == 1
        assert detections[0].field_path == "note"
        assert detections[0].reason == "regex:email"
        assert detections[0].field_type == "email"

    def test_nested_dict_path(self, detector: Detector) -> None:
        log = {"user": {"email": "x@y.com"}}
        detections = detector.detect(log)
        assert len(detections) == 1
        assert detections[0].field_path == "user.email"

    def test_operational_only_no_detections(self, detector: Detector) -> None:
        # `order_id` value is alpha so credit-card regex won't fire either.
        log = {"timestamp": "2025-01-01T00:00:00Z", "order_id": "abc", "amount": 42}
        assert detector.detect(log) == []

    def test_confidence_sort_descending(self, detector: Detector) -> None:
        # Two fields with different VALUE-regex confidences: ssn (0.95) > phone (0.80).
        # Keys are chosen so neither triggers a field-name hit (which would
        # equalise confidences at 0.95 and defeat the test). Verify the
        # higher-confidence detection comes first in the result list.
        log = {
            "free_text_a": "call 415-555-1234",     # phone, 0.80
            "free_text_b": "id 123-45-6789",        # ssn,   0.95
        }
        detections = detector.detect(log)
        assert len(detections) == 2
        assert detections[0].confidence >= detections[1].confidence
        # Highest confidence first → SSN.
        assert detections[0].field_type == "ssn"
        assert detections[1].field_type == "phone"

    def test_field_name_match_on_innocuous_value(self, detector: Detector) -> None:
        # "hunter2" doesn't match any regex, but the field name does → detect.
        log = {"password": "hunter2"}
        detections = detector.detect(log)
        assert len(detections) == 1
        assert detections[0].field_path == "password"
        assert detections[0].reason == "field_name:password"

    def test_list_values_skipped(self, detector: Detector) -> None:
        # v1 treats lists as opaque scalars — no detection on values inside.
        log = {"tags": ["foo@bar.com", "baz@qux.com"]}
        assert detector.detect(log) == []

    def test_value_preview_truncated(self, detector: Detector) -> None:
        # Preview must be at most 8 characters even when value is long.
        log = {"password": "supersecretmegatoken1234567890"}
        detections = detector.detect(log)
        assert len(detections) == 1
        assert len(detections[0].value_preview) <= 8
        # And it must NOT be the full plaintext.
        assert detections[0].value_preview != log["password"]

    def test_nested_two_levels_deep(self, detector: Detector) -> None:
        # Sanity: the walker keeps recursing.
        log = {"a": {"b": {"c": {"email": "x@y.com"}}}}
        detections = detector.detect(log)
        assert len(detections) == 1
        assert detections[0].field_path == "a.b.c.email"

    def test_multiple_field_name_hits(self, detector: Detector) -> None:
        # Each key triggers its own detection.
        log = {"password": "x", "api_key": "y", "ssn": "z"}
        detections = detector.detect(log)
        assert len(detections) == 3
        paths = {d.field_path for d in detections}
        assert paths == {"password", "api_key", "ssn"}

    def test_non_string_value_stringified(self, detector: Detector) -> None:
        # IP value stored as bare string (no quotes) — typical of JSON ints
        # appearing in a log dict. The detector must stringify before regex.
        log = {"client": "192.168.1.1"}  # neither name nor regex says it's PII
        # `client` isn't a sensitive name; but the value matches ipv4 regex.
        detections = detector.detect(log)
        assert len(detections) == 1
        assert detections[0].field_type == "ipv4"
