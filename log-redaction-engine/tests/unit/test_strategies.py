"""Unit tests for the C3 redaction strategies.

Coverage layout:

* :class:`TestMask`     — fixed-width asterisk fill across various lengths.
* :class:`TestPartial`  — per-pattern outputs, including the three
  spec-verified exact strings (SSN, credit card, default fallback).
* :class:`TestHash`     — determinism, salt sensitivity, output shape.
* :class:`TestTokenize` — strategy <-> store integration (dedup contract).
* :class:`TestRegistry` — name lookup, missing-key error, ``in`` operator,
  and the runtime-checkable ``Strategy`` Protocol.
* :class:`TestSalt`     — :func:`load_salt` happy + failure paths. These
  tests mutate ``REDACTION_HASH_SALT`` via monkeypatch and clear the
  Settings LRU cache between runs; an autouse fixture restores the
  conftest-injected default after each test so subsequent files aren't
  poisoned.

The :class:`Detection` instances used here are constructed by hand —
we don't need :func:`match_all` to test the strategies in isolation.
"""
from __future__ import annotations

import hashlib
import re

import pytest

from src.detection.patterns import Detection
from src.redaction.salt import load_salt
from src.redaction.strategies import (
    HashStrategy,
    MaskStrategy,
    PartialStrategy,
    Strategy,
    StrategyRegistry,
    TokenizeStrategy,
)
from src.redaction.token_store import TokenStore
from src.settings import get_settings


# ---------------------------------------------------------------------------
# Detection factories — small helpers so test bodies aren't full of
# repetitive keyword args.
# ---------------------------------------------------------------------------

def _det(pattern_name: str, value: str = "x") -> Detection:
    """Build a Detection with the given pattern_name and value.

    ``start``/``end`` are not used by any strategy; we fill them with
    plausible placeholders so the dataclass invariants hold.
    """
    return Detection(
        pattern_name=pattern_name,
        value=value,
        start=0,
        end=len(value),
        confidence=1.0,
        source="regex",
    )


# ---------------------------------------------------------------------------
# TestMask — uniform asterisks regardless of pattern
# ---------------------------------------------------------------------------

class TestMask:
    """``MaskStrategy.redact`` returns ``"*" * len(value)``."""

    def test_spec_verification_ssn_eleven_asterisks(self) -> None:
        # SPEC VERIFICATION (exact): mask("123-45-6789") == "***********"
        # 11 chars total: 9 digits + 2 hyphens, all replaced with "*".
        result = MaskStrategy().redact("123-45-6789", _det("ssn", "123-45-6789"))
        assert result == "***********"
        assert len(result) == 11

    def test_empty_string_returns_empty(self) -> None:
        # Edge case: a zero-length value yields a zero-length mask.
        assert MaskStrategy().redact("", _det("ssn", "")) == ""

    def test_two_char_value(self) -> None:
        assert MaskStrategy().redact("hi", _det("person", "hi")) == "**"

    def test_short_value(self) -> None:
        assert MaskStrategy().redact("abc", _det("person", "abc")) == "***"

    def test_long_value_preserves_length(self) -> None:
        # 256-char input → 256-char output; length-preserving is the
        # mask invariant.
        long = "x" * 256
        assert MaskStrategy().redact(long, _det("person", long)) == "*" * 256

    def test_mask_ignores_detection_pattern(self) -> None:
        # Same input across multiple pattern_names → same output. Mask
        # never branches on the detection type.
        for pattern in ("ssn", "credit_card", "email", "person", "org"):
            assert (
                MaskStrategy().redact("abcd", _det(pattern, "abcd")) == "****"
            )

    def test_mask_has_name_attribute(self) -> None:
        # The class-level ``name`` is part of the public contract.
        assert MaskStrategy.name == "mask"
        assert MaskStrategy().name == "mask"


# ---------------------------------------------------------------------------
# TestPartial — pattern-aware partial redaction
# ---------------------------------------------------------------------------

class TestPartial:
    """Every documented pattern, plus the default fallback for NER hits."""

    # -- spec-verified exact mappings (must not regress) ------------------

    def test_spec_ssn_keeps_last_four_digits(self) -> None:
        # SPEC VERIFICATION (exact): partial(ssn, "123-45-6789") == "***-**-6789"
        result = PartialStrategy().redact(
            "123-45-6789", _det("ssn", "123-45-6789")
        )
        assert result == "***-**-6789"

    def test_spec_credit_card_dashed_keeps_last_four(self) -> None:
        # SPEC VERIFICATION (exact): partial(cc, "4111-1111-1111-1111")
        #     == "****-****-****-1111"
        result = PartialStrategy().redact(
            "4111-1111-1111-1111",
            _det("credit_card", "4111-1111-1111-1111"),
        )
        assert result == "****-****-****-1111"

    # -- credit card separators -----------------------------------------

    def test_credit_card_space_separated(self) -> None:
        # Spaces are preserved verbatim by the digit walker.
        result = PartialStrategy().redact(
            "4111 1111 1111 1111",
            _det("credit_card", "4111 1111 1111 1111"),
        )
        assert result == "**** **** **** 1111"

    # -- US phone --------------------------------------------------------

    def test_phone_parens_dash_format(self) -> None:
        # "(415) 555-1234" has 10 digits → last 4 kept, first 6 masked,
        # parens / space / hyphen preserved.
        result = PartialStrategy().redact(
            "(415) 555-1234", _det("us_phone", "(415) 555-1234")
        )
        assert result == "(***) ***-1234"

    def test_phone_plus_one_country_code(self) -> None:
        # "+1 415-555-1234" has 11 digits → last 4 kept, first 7 masked,
        # the "+" and " " and "-" separators preserved verbatim.
        result = PartialStrategy().redact(
            "+1 415-555-1234", _det("us_phone", "+1 415-555-1234")
        )
        assert result == "+* ***-***-1234"

    # -- email -----------------------------------------------------------

    def test_email_keeps_first_local_char(self) -> None:
        # "alice@example.com" → "a***@example.com"
        result = PartialStrategy().redact(
            "alice@example.com", _det("email", "alice@example.com")
        )
        assert result == "a***@example.com"

    def test_email_short_local_part(self) -> None:
        # Single-char local-part still gets the "first + ***" treatment.
        result = PartialStrategy().redact("a@b.io", _det("email", "a@b.io"))
        assert result == "a***@b.io"

    def test_email_with_no_at_falls_back_to_default(self) -> None:
        # Defensive: the email regex won't produce a value without "@",
        # but the strategy must not blow up if one slips through. It
        # falls through to the generic first+last fallback.
        result = PartialStrategy().redact(
            "noatsign", _det("email", "noatsign")
        )
        # len > 2 → first + middle stars + last
        assert result == "n******n"

    # -- MRN --------------------------------------------------------------

    def test_mrn_keeps_last_three_digits(self) -> None:
        # "MRN-123456" has 6 digits → keep last 3, mask first 3, preserve
        # the "MRN-" literal prefix.
        result = PartialStrategy().redact(
            "MRN-123456", _det("mrn", "MRN-123456")
        )
        assert result == "MRN-***456"

    # -- default fallback (NER) ------------------------------------------

    def test_default_fallback_first_and_last_kept(self) -> None:
        # "Alice" (5 chars) → first + 3 stars + last = "A***e".
        result = PartialStrategy().redact("Alice", _det("person", "Alice"))
        assert result == "A***e"

    def test_default_fallback_short_value_all_masked(self) -> None:
        # len(value) <= 2 → return all stars; no room for first+last.
        # "Bo" is 2 chars → "**".
        result = PartialStrategy().redact("Bo", _det("person", "Bo"))
        assert result == "**"

    def test_default_fallback_single_char(self) -> None:
        # Single-char NER hit (degenerate) → one star.
        result = PartialStrategy().redact("X", _det("org", "X"))
        assert result == "*"

    def test_unknown_pattern_uses_default(self) -> None:
        # An unrecognized pattern_name falls through to the default branch.
        result = PartialStrategy().redact(
            "Customer", _det("future_pattern", "Customer")
        )
        # len > 2 → first + middle stars + last
        assert result == "C******r"

    def test_partial_has_name_attribute(self) -> None:
        assert PartialStrategy.name == "partial"
        assert PartialStrategy().name == "partial"


# ---------------------------------------------------------------------------
# TestHash — salted SHA-256 prefix
# ---------------------------------------------------------------------------

class TestHash:
    """Determinism, salt sensitivity, output shape."""

    # A fixed test salt (NOT the conftest one — we want independence from
    # how Settings is configured). 32 bytes = 64 hex chars.
    _SALT_A = bytes.fromhex("aa" * 32)
    _SALT_B = bytes.fromhex("bb" * 32)

    def test_determinism_same_input_same_salt(self) -> None:
        # Two calls with the same input + salt return byte-identical output.
        strat = HashStrategy(self._SALT_A)
        det = _det("ssn", "123-45-6789")
        assert strat.redact("123-45-6789", det) == strat.redact(
            "123-45-6789", det
        )

    def test_different_salt_produces_different_hash(self) -> None:
        # Salt sensitivity: same plaintext + different salt = different
        # hash. This is the whole point of having a per-deployment salt.
        det = _det("ssn", "123-45-6789")
        a = HashStrategy(self._SALT_A).redact("123-45-6789", det)
        b = HashStrategy(self._SALT_B).redact("123-45-6789", det)
        assert a != b

    def test_output_is_16_lowercase_hex(self) -> None:
        # The truncated SHA-256 prefix is 16 chars of lowercase hex.
        result = HashStrategy(self._SALT_A).redact(
            "alice@example.com", _det("email", "alice@example.com")
        )
        assert re.match(r"^[0-9a-f]{16}$", result), result
        assert len(result) == 16

    def test_matches_manual_sha256_prefix(self) -> None:
        # Re-compute the expected digest by hand and compare; this proves
        # the implementation is using the documented salt-prefix layout
        # (``sha256(salt || value).hexdigest()[:16]``) rather than HMAC or
        # any other variant.
        plaintext = "alice@example.com"
        expected = hashlib.sha256(
            self._SALT_A + plaintext.encode("utf-8")
        ).hexdigest()[:16]
        actual = HashStrategy(self._SALT_A).redact(
            plaintext, _det("email", plaintext)
        )
        assert actual == expected

    def test_hash_different_plaintexts_differ(self) -> None:
        # Two distinct plaintexts hash to different prefixes (collision
        # probability at 16-hex output is ~2^-32 per pair — astronomically
        # safe for these test values).
        strat = HashStrategy(self._SALT_A)
        a = strat.redact("alice@example.com", _det("email", "alice@example.com"))
        b = strat.redact("bob@example.com", _det("email", "bob@example.com"))
        assert a != b

    def test_hash_has_name_attribute(self) -> None:
        assert HashStrategy.name == "hash"
        assert HashStrategy(self._SALT_A).name == "hash"


# ---------------------------------------------------------------------------
# TestTokenize — strategy + store integration
# ---------------------------------------------------------------------------

class TestTokenize:
    """The strategy is a thin shim around the store; we test that contract."""

    def test_same_input_same_token(self) -> None:
        # Deterministic dedup: ``store.tokenize(x)`` always returns the same
        # token for the same input, so the strategy does too.
        store = TokenStore()
        strat = TokenizeStrategy(store)
        det = _det("ssn", "123-45-6789")
        t1 = strat.redact("123-45-6789", det)
        t2 = strat.redact("123-45-6789", det)
        assert t1 == t2
        # And the token can be reversed via the admin role.
        assert store.detokenize(t1, role="admin") == "123-45-6789"

    def test_different_inputs_different_tokens(self) -> None:
        # Two distinct plaintexts get distinct tokens.
        store = TokenStore()
        strat = TokenizeStrategy(store)
        t1 = strat.redact("alice", _det("person", "alice"))
        t2 = strat.redact("bob", _det("person", "bob"))
        assert t1 != t2

    def test_tokenize_has_name_attribute(self) -> None:
        store = TokenStore()
        assert TokenizeStrategy.name == "tokenize"
        assert TokenizeStrategy(store).name == "tokenize"


# ---------------------------------------------------------------------------
# TestRegistry — name lookup, missing-key, ``in`` operator, Protocol check
# ---------------------------------------------------------------------------

class TestRegistry:
    """Registry round-trips the four built-ins; missing names raise KeyError."""

    @staticmethod
    def _make() -> StrategyRegistry:
        # 32-byte salt + empty store — both cheap to construct.
        return StrategyRegistry(
            salt=bytes.fromhex("cc" * 32), token_store=TokenStore()
        )

    def test_get_mask_returns_mask_strategy(self) -> None:
        assert isinstance(self._make().get("mask"), MaskStrategy)

    def test_get_partial_returns_partial_strategy(self) -> None:
        assert isinstance(self._make().get("partial"), PartialStrategy)

    def test_get_hash_returns_hash_strategy(self) -> None:
        assert isinstance(self._make().get("hash"), HashStrategy)

    def test_get_tokenize_returns_tokenize_strategy(self) -> None:
        assert isinstance(self._make().get("tokenize"), TokenizeStrategy)

    def test_unknown_name_raises_keyerror_with_available_list(self) -> None:
        # The error message must list available strategies so operators
        # can spot a typo immediately.
        registry = self._make()
        with pytest.raises(KeyError) as exc_info:
            registry.get("totally-unknown")
        msg = str(exc_info.value)
        assert "totally-unknown" in msg
        # All four built-ins should show up in the "available" listing.
        for name in ("mask", "partial", "hash", "tokenize"):
            assert name in msg

    def test_contains_operator_true_for_known(self) -> None:
        # Sugar for "is this a known strategy name?"; used by the config
        # validator in C4.
        registry = self._make()
        assert "hash" in registry

    def test_contains_operator_false_for_unknown(self) -> None:
        registry = self._make()
        assert "foo" not in registry

    def test_strategy_protocol_runtime_checkable(self) -> None:
        # The ``@runtime_checkable`` decorator on the Protocol lets us
        # use isinstance() for duck-type assertions. Used by the
        # registry's type annotation and by anyone wanting to verify a
        # user-supplied strategy meets the contract.
        assert isinstance(MaskStrategy(), Strategy)
        assert isinstance(PartialStrategy(), Strategy)
        assert isinstance(HashStrategy(bytes.fromhex("dd" * 32)), Strategy)
        assert isinstance(TokenizeStrategy(TokenStore()), Strategy)


# ---------------------------------------------------------------------------
# TestSalt — load_salt() happy + failure paths
# ---------------------------------------------------------------------------

# CRITICAL: tests in this class mutate ``REDACTION_HASH_SALT`` and clear
# the Settings cache. An autouse fixture restores the conftest default
# (and clears the cache one more time) after each test so subsequent test
# files don't observe a poisoned environment.

_CONFTEST_SALT = "deadbeef" * 8  # 64 hex chars — matches conftest.py


@pytest.fixture(autouse=True)
def _restore_salt_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Restore the conftest salt + clear the Settings cache after each test.

    Without this, a TestSalt test that monkeypatches a bad salt would
    leave Settings in a broken state for any subsequent test that calls
    ``get_settings()`` indirectly (e.g., via salt.load_salt()).
    """
    # Set BEFORE the test runs too — so even the happy-path "load default"
    # test sees the canonical value rather than whatever the previous file
    # left behind.
    monkeypatch.setenv("REDACTION_HASH_SALT", _CONFTEST_SALT)
    get_settings.cache_clear()
    yield
    # Monkeypatch undoes the env change automatically; we just need to
    # bust the cache so the next caller re-reads the (restored) env.
    get_settings.cache_clear()


class TestSalt:
    """:func:`load_salt` returns 32 bytes on the happy path; raises otherwise."""

    def test_loads_default_salt_from_conftest(self) -> None:
        # The conftest sets REDACTION_HASH_SALT to "deadbeef" * 8 (64 hex
        # chars) → 32 bytes after decode.
        salt = load_salt()
        assert isinstance(salt, bytes)
        assert len(salt) == 32

    def test_wrong_length_raises_runtimeerror(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # 20-hex-char salt → 10 bytes after decode → mismatch.
        monkeypatch.setenv("REDACTION_HASH_SALT", "ab" * 10)
        get_settings.cache_clear()
        with pytest.raises(RuntimeError) as exc_info:
            load_salt()
        # The message mentions the byte count so operators know to extend
        # the salt rather than guess at the format.
        assert "32 bytes" in str(exc_info.value)

    def test_non_hex_raises_runtimeerror(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # "zz" is not a valid hex pair; bytes.fromhex raises ValueError →
        # load_salt re-raises as RuntimeError with the truncated prefix.
        monkeypatch.setenv("REDACTION_HASH_SALT", "zz" * 32)
        get_settings.cache_clear()
        with pytest.raises(RuntimeError) as exc_info:
            load_salt()
        msg = str(exc_info.value)
        assert "hex" in msg
        # The truncated prefix should NOT leak the full 64-char string.
        # The implementation echoes only the first 8 chars.
        assert "zzzzzzzz" in msg

    def test_load_salt_value_consistent_across_calls(self) -> None:
        # Two calls with the same env return byte-identical salts (this
        # follows from Settings caching, but worth pinning explicitly).
        s1 = load_salt()
        s2 = load_salt()
        assert s1 == s2
