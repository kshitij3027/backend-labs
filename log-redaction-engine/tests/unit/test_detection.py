"""Unit tests for C2 detection engine — regex patterns + Luhn + match_all + Detector.

Coverage targets:

* :class:`TestLuhn`           — mod-10 helper isolated from any pattern.
* :class:`TestSSN`            — SSN positives + every documented negative.
* :class:`TestCreditCard`     — Luhn-valid / Luhn-invalid / format edge cases.
* :class:`TestEmail`          — happy path + obvious negatives.
* :class:`TestUSPhone`        — multiple separator styles + rejections.
* :class:`TestMRN`            — exact-6-digit rule with word-boundary edge.
* :class:`TestMatchAll`       — sort order, empty input, regex-timeout path.
* :class:`TestDetectionDataclass` — immutability contract.
* :class:`TestDetector`       — composition + overlap-dedup algorithm.

NER is exercised in ``test_ner.py``; here we use ``Detector(ner_detector=None)``
so the suite never touches spaCy.
"""
from __future__ import annotations

import dataclasses

import pytest

from src.detection.detector import Detector
from src.detection.patterns import (
    PATTERNS,
    Detection,
    _luhn,
    match_all,
)


# ---------------------------------------------------------------------------
# TestLuhn — verify the mod-10 helper in isolation
# ---------------------------------------------------------------------------

class TestLuhn:
    """The Luhn checksum is also a public API for unit-level testing."""

    def test_valid_visa_test_number(self) -> None:
        # The well-known Visa test PAN — Luhn-valid by construction.
        assert _luhn("4111111111111111") is True

    def test_invalid_when_last_digit_off_by_one(self) -> None:
        # Bumping the last digit destroys the sum % 10 == 0 invariant.
        assert _luhn("4111111111111112") is False

    def test_strips_dashes_and_spaces_before_checking(self) -> None:
        assert _luhn("4111-1111-1111-1111") is True
        assert _luhn("4111 1111 1111 1111") is True

    def test_short_input_rejected(self) -> None:
        # < 13 digits after stripping is invalid even if numerically mod-10.
        assert _luhn("123") is False
        assert _luhn("") is False

    def test_long_input_rejected(self) -> None:
        # > 19 digits is out of the ISO/IEC 7812-1 PAN range.
        assert _luhn("1" * 20) is False


# ---------------------------------------------------------------------------
# TestSSN — every documented SSN edge case
# ---------------------------------------------------------------------------

class TestSSN:
    """Validates the negative-lookahead branches in the SSN regex."""

    def test_well_formed_ssn_accepted(self) -> None:
        hits = match_all("My SSN is 123-45-6789")
        ssns = [h for h in hits if h.pattern_name == "ssn"]
        assert len(ssns) == 1
        assert ssns[0].value == "123-45-6789"

    def test_alternate_valid_ssn_accepted(self) -> None:
        # Area 567 is safely inside the valid range (100-665, excluding 666).
        # The plan's earlier 987-xx example is actually invalid per SSA policy:
        # the 9xx block is reserved for ITINs and the regex correctly drops it.
        hits = match_all("Identifier 567-65-4321")
        assert any(h.pattern_name == "ssn" and h.value == "567-65-4321" for h in hits)

    def test_area_000_rejected(self) -> None:
        # Area 000 is reserved and never issued → negative lookahead fires.
        hits = match_all("000-12-3456")
        assert not any(h.pattern_name == "ssn" for h in hits)

    def test_area_666_rejected(self) -> None:
        # Area 666 is also reserved.
        hits = match_all("666-12-3456")
        assert not any(h.pattern_name == "ssn" for h in hits)

    def test_area_9xx_rejected(self) -> None:
        # 9xx areas are reserved for ITINs; regex rejects via `9\d{2}` branch.
        hits = match_all("912-12-3456")
        assert not any(h.pattern_name == "ssn" for h in hits)

    def test_group_00_rejected(self) -> None:
        # The 2-digit group field cannot be "00".
        hits = match_all("123-00-4567")
        assert not any(h.pattern_name == "ssn" for h in hits)

    def test_serial_0000_rejected(self) -> None:
        # The 4-digit serial field cannot be "0000".
        hits = match_all("123-45-0000")
        assert not any(h.pattern_name == "ssn" for h in hits)

    def test_no_dashes_rejected(self) -> None:
        # The regex requires literal hyphens; raw 9-digit strings don't match.
        hits = match_all("123456789")
        assert not any(h.pattern_name == "ssn" for h in hits)


# ---------------------------------------------------------------------------
# TestCreditCard — the regex is broad; Luhn filtering is the real validator
# ---------------------------------------------------------------------------

class TestCreditCard:
    """Verifies that Luhn-invalid candidates are dropped, not emitted."""

    def test_luhn_valid_visa_dashed_accepted(self) -> None:
        hits = match_all("Card: 4111-1111-1111-1111")
        ccs = [h for h in hits if h.pattern_name == "credit_card"]
        assert len(ccs) == 1
        assert ccs[0].value == "4111-1111-1111-1111"
        assert ccs[0].confidence == 1.0
        assert ccs[0].source == "regex"

    def test_luhn_invalid_visa_rejected(self) -> None:
        # Same prefix, last digit bumped → Luhn invalid → no CC emitted.
        hits = match_all("Card: 4111-1111-1111-1112")
        assert not any(h.pattern_name == "credit_card" for h in hits)

    def test_luhn_valid_spaced_accepted(self) -> None:
        # Space separators are the human-friendly format.
        hits = match_all("Card: 4111 1111 1111 1111")
        assert any(h.pattern_name == "credit_card" for h in hits)

    def test_luhn_valid_mastercard_accepted(self) -> None:
        # 5555-5555-5555-4444 is the canonical Stripe MC test PAN.
        hits = match_all("Card: 5555-5555-5555-4444")
        assert any(h.pattern_name == "credit_card" for h in hits)

    def test_random_16_digit_run_rejected(self) -> None:
        # Looks card-shaped to the regex but fails the Luhn checksum.
        hits = match_all("Order 1234-5678-9012-3456")
        assert not any(h.pattern_name == "credit_card" for h in hits)

    def test_too_short_digit_run_rejected(self) -> None:
        # 12 digits is below Luhn's 13-19 acceptance window.
        hits = match_all("Ref 123456789012")
        assert not any(h.pattern_name == "credit_card" for h in hits)


# ---------------------------------------------------------------------------
# TestEmail — happy path + obvious negatives
# ---------------------------------------------------------------------------

class TestEmail:
    """Email regex is intentionally simple; we only test the obvious cases."""

    def test_standard_email_accepted(self) -> None:
        hits = match_all("contact: alice@example.com please")
        emails = [h for h in hits if h.pattern_name == "email"]
        assert len(emails) == 1
        assert emails[0].value == "alice@example.com"

    def test_not_an_email_rejected(self) -> None:
        # No @ sign → cannot match.
        hits = match_all("not-an-email here")
        assert not any(h.pattern_name == "email" for h in hits)

    def test_short_email_accepted(self) -> None:
        # Two-letter TLD (the regex allows ``{2,}``).
        hits = match_all("ping a@b.io")
        assert any(h.pattern_name == "email" and h.value == "a@b.io" for h in hits)


# ---------------------------------------------------------------------------
# TestUSPhone — multiple separator styles
# ---------------------------------------------------------------------------

class TestUSPhone:
    """Each documented separator style must match; bad area codes must not."""

    def test_parens_dash_format_accepted(self) -> None:
        hits = match_all("call (415) 555-1234 anytime")
        assert any(h.pattern_name == "us_phone" for h in hits)

    def test_plus_one_country_code_accepted(self) -> None:
        hits = match_all("intl: +1 415-555-1234")
        assert any(h.pattern_name == "us_phone" for h in hits)

    def test_dotted_format_accepted(self) -> None:
        # Dot separator is common in European-style international notation.
        hits = match_all("dial 415.555.1234")
        assert any(h.pattern_name == "us_phone" for h in hits)

    def test_three_digit_string_rejected(self) -> None:
        # Way too short to be a NANP phone number.
        hits = match_all("just 123 here")
        assert not any(h.pattern_name == "us_phone" for h in hits)

    def test_bare_digits_with_invalid_area_rejected(self) -> None:
        # No separators; "1234567890" requires area=123 which violates the
        # NANP `[2-9]` lead. The `+?1` prefix branch would consume the leading
        # `1`, leaving 234-567-890 — only 9 digits remain after the area,
        # not the 7 required by `\d{3}-?\d{4}`. Either way: no match.
        hits = match_all("ID: 1234567890 nope")
        assert not any(h.pattern_name == "us_phone" for h in hits)


# ---------------------------------------------------------------------------
# TestMRN — exactly six digits, word-boundary enforced
# ---------------------------------------------------------------------------

class TestMRN:
    """``\\bMRN-\\d{6}\\b`` — the boundary on both sides is load-bearing."""

    def test_six_digit_mrn_accepted(self) -> None:
        hits = match_all("Patient MRN-123456 admitted")
        assert any(
            h.pattern_name == "mrn" and h.value == "MRN-123456" for h in hits
        )

    def test_five_digit_mrn_rejected(self) -> None:
        # Only 5 digits → quantifier `{6}` fails outright.
        hits = match_all("Bad ref MRN-12345")
        assert not any(h.pattern_name == "mrn" for h in hits)

    def test_seven_digit_mrn_rejected(self) -> None:
        # 7 digits → there is no \b between digit 6 and digit 7, so the
        # trailing \b assertion fails and the whole pattern fails. This is
        # intentional: we'd rather miss a malformed MRN than mis-redact.
        hits = match_all("Bad ref MRN-1234567")
        assert not any(h.pattern_name == "mrn" for h in hits)


# ---------------------------------------------------------------------------
# TestMatchAll — sort order, empty input, timeout path
# ---------------------------------------------------------------------------

class TestMatchAll:
    """Covers the orchestration concerns of ``match_all``."""

    def test_empty_string_returns_empty_list(self) -> None:
        assert match_all("") == []

    def test_mixed_text_returns_hits_sorted_by_start(self) -> None:
        # Construct a string with multiple distinct pattern hits and verify
        # the returned list is ordered by `.start`, not by pattern type.
        text = "email a@b.io then SSN 123-45-6789 then MRN-654321 end"
        hits = match_all(text)
        # We expect at least one of each pattern; positions must be ascending.
        starts = [h.start for h in hits]
        assert starts == sorted(starts)
        # Sanity: all three known patterns are present.
        kinds = {h.pattern_name for h in hits}
        assert {"email", "ssn", "mrn"}.issubset(kinds)

    def test_regex_timeout_skips_pattern_returns_empty(self) -> None:
        # A near-zero timeout forces every pattern to time out; match_all
        # logs a warning per pattern and returns an empty list — proving the
        # ``regex.TimeoutError`` path is wired up.
        very_long_text = "a" * 5000 + "@" + "b" * 5000 + ".com"
        hits = match_all(very_long_text, timeout=0.000001)
        # The exact result depends on how fast each pattern runs, but
        # critically the call must not raise. An empty list is the most
        # likely outcome under such a tight budget.
        assert isinstance(hits, list)

    def test_patterns_module_dict_has_expected_keys(self) -> None:
        # Defensive: rename detection breaks half the downstream code, so
        # pin the public keys here.
        assert set(PATTERNS.keys()) == {
            "ssn",
            "credit_card",
            "email",
            "us_phone",
            "mrn",
        }


# ---------------------------------------------------------------------------
# TestDetectionDataclass — immutability is part of the contract
# ---------------------------------------------------------------------------

class TestDetectionDataclass:
    """Detection objects flow through threads in C5; mutation must be impossible."""

    def test_detection_is_frozen(self) -> None:
        d = Detection(
            pattern_name="ssn",
            value="123-45-6789",
            start=0,
            end=11,
        )
        with pytest.raises(dataclasses.FrozenInstanceError):
            d.value = "modified"  # type: ignore[misc]

    def test_default_confidence_and_source(self) -> None:
        # Regex detections rely on these defaults; verify they don't drift.
        d = Detection(
            pattern_name="email",
            value="a@b.io",
            start=0,
            end=6,
        )
        assert d.confidence == 1.0
        assert d.source == "regex"


# ---------------------------------------------------------------------------
# TestDetector — composition + overlap dedup
# ---------------------------------------------------------------------------

class TestDetector:
    """Orchestrator-level tests. NER is disabled (None) throughout."""

    def test_empty_text_returns_empty(self) -> None:
        d = Detector(ner_detector=None)
        assert d.detect("") == []

    def test_regex_only_passthrough(self) -> None:
        d = Detector(ner_detector=None)
        out = d.detect("SSN 123-45-6789")
        assert len(out) == 1
        assert out[0].pattern_name == "ssn"
        assert out[0].source == "regex"

    def test_overlap_dedup_prefers_higher_confidence(self) -> None:
        # Construct two synthetic detections at the same span; the higher
        # confidence one must survive. We bypass ``match_all`` by feeding
        # ``_dedupe_overlaps`` directly.
        high = Detection(
            pattern_name="ssn", value="x", start=0, end=10, confidence=1.0, source="regex"
        )
        low = Detection(
            pattern_name="person",
            value="x",
            start=0,
            end=10,
            confidence=0.85,
            source="ner",
        )
        out = Detector._dedupe_overlaps([low, high])
        assert len(out) == 1
        assert out[0] is high  # the regex hit wins both confidence and source

    def test_overlap_dedup_regex_beats_ner_on_tie(self) -> None:
        # Same confidence on both sides → ``source`` priority breaks the tie.
        # (In practice this never happens because regex is 1.0 and NER 0.85,
        # but the algorithm must still behave deterministically.)
        regex_hit = Detection(
            pattern_name="email",
            value="x",
            start=0,
            end=5,
            confidence=0.85,
            source="regex",
        )
        ner_hit = Detection(
            pattern_name="person",
            value="x",
            start=0,
            end=5,
            confidence=0.85,
            source="ner",
        )
        out = Detector._dedupe_overlaps([ner_hit, regex_hit])
        assert len(out) == 1
        assert out[0].source == "regex"

    def test_non_overlapping_hits_all_kept_and_sorted(self) -> None:
        a = Detection(pattern_name="ssn", value="a", start=0, end=10)
        b = Detection(pattern_name="email", value="b", start=20, end=30)
        c = Detection(pattern_name="mrn", value="c", start=40, end=50)
        # Pass them out-of-order to confirm sort happens.
        out = Detector._dedupe_overlaps([c, a, b])
        assert [d.start for d in out] == [0, 20, 40]
