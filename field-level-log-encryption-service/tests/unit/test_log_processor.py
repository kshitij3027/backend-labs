"""Unit tests for the C5 log processor pipeline.

Coverage targets:

* :class:`TestEncryptDecryptRoundTrip` — golden e-commerce + support-ticket
  fixtures exercise the happy path: detection picks the right leaves, the
  active key is stamped, and a decrypt re-produces the original values
  (modulo string-coercion of non-string types).
* :class:`TestParallelEncryptor` — the threshold gate: serial vs
  parallel branches fire predictably, and order is preserved across
  both paths.
* :class:`TestLogProcessor` — edge cases: empty log, deeply-nested PII,
  record_id precedence, missing-envelope errors.

All tests use the zero-byte test KEK injected by :mod:`tests.conftest`.
"""
from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from src.crypto import AESGCMEncryptor, EncryptedField, EnvKeyProvider
from src.detection import Detector
from src.keystore import KeyNotFoundError, KeyStore
from src.processor import LogProcessor, ParallelEncryptor, ProcessorError
from src.processor.parallel import _EncItem


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


_FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures"


@pytest.fixture
def ecommerce_log() -> dict:
    """Fresh deep copy of the e-commerce fixture per-test.

    We re-read from disk (rather than caching at module scope) so that
    a test which mutates the dict can't poison the next test.
    """
    return json.loads((_FIXTURE_DIR / "ecommerce_log.json").read_text())


@pytest.fixture
def support_ticket_log() -> dict:
    """Fresh deep copy of the support-ticket fixture per-test."""
    return json.loads((_FIXTURE_DIR / "support_ticket_log.json").read_text())


@pytest.fixture
def keystore() -> KeyStore:
    """Bootstrapped keystore with one active DEK.

    Constructed per-test so encrypt-time `key_id` is deterministic
    within the test but does not leak across tests.
    """
    store = KeyStore(EnvKeyProvider())
    store.create_initial_active()
    return store


@pytest.fixture
def parallel() -> ParallelEncryptor:
    """Default-size parallel encryptor matching the production settings.

    Counters reset to zero per test (fresh instance). We close the
    pool at teardown via finalizer so the thread pool doesn't leak
    across the test session.
    """
    enc = ParallelEncryptor(
        thread_pool_size=4,
        threshold_fields=4,
        threshold_bytes=4096,
    )
    yield enc
    enc.close()


@pytest.fixture
def processor(keystore: KeyStore, parallel: ParallelEncryptor) -> LogProcessor:
    """Wire the detector + keystore + parallel encryptor into a LogProcessor."""
    return LogProcessor(
        detector=Detector(),
        keystore=keystore,
        parallel=parallel,
    )


# ---------------------------------------------------------------------------
# TestEncryptDecryptRoundTrip — golden fixtures
# ---------------------------------------------------------------------------


class TestEncryptDecryptRoundTrip:
    """Verify the encrypt/decrypt pair against realistic log fixtures."""

    # ---- e-commerce ----------------------------------------------------

    def test_ecommerce_sensitive_fields_are_encrypted(
        self, processor: LogProcessor, ecommerce_log: dict
    ) -> None:
        # The three detected leaves must become EncryptedField dicts
        # (algorithm/encrypted_value/key_id all present).
        out = processor.encrypt(ecommerce_log)
        for path in ("customer_email", "phone"):
            assert isinstance(out[path], dict), path
            assert out[path]["algorithm"] == "AES-256-GCM"
            assert out[path]["encrypted_value"]
            assert out[path]["key_id"]

        # postal_code is nested under shipping/.
        assert out["shipping"]["postal_code"]["algorithm"] == "AES-256-GCM"

    def test_ecommerce_operational_fields_unchanged(
        self, processor: LogProcessor, ecommerce_log: dict
    ) -> None:
        # Operational fields must pass through untouched — this is the
        # whole reason we do FIELD-level rather than message-level
        # encryption.
        out = processor.encrypt(ecommerce_log)
        assert out["order_id"] == "ORD-2026-05-16-001"
        assert out["amount"] == 129.99  # NUMBER, not encrypted
        assert isinstance(out["amount"], float)
        assert out["timestamp"] == "2026-05-16T10:30:00Z"
        assert out["currency"] == "USD"
        assert out["shipping"]["city"] == "Seattle"

    def test_ecommerce_processing_envelope_lists_encrypted_paths(
        self, processor: LogProcessor, ecommerce_log: dict
    ) -> None:
        # `_processing.encrypted_fields` is the authoritative manifest of
        # which paths were transformed. Compare as a set so any
        # incidental re-ordering doesn't break the assertion.
        out = processor.encrypt(ecommerce_log)
        assert set(out["_processing"]["encrypted_fields"]) == {
            "customer_email",
            "phone",
            "shipping.postal_code",
        }

    def test_ecommerce_processing_envelope_carries_active_key_id(
        self,
        processor: LogProcessor,
        keystore: KeyStore,
        ecommerce_log: dict,
    ) -> None:
        # The stamped `key_id` must equal the keystore's CURRENT active
        # key (proves we resolved active inside encrypt() and didn't
        # pick up some stale reference).
        out = processor.encrypt(ecommerce_log)
        assert out["_processing"]["key_id"] == keystore.get_active().key_id

    def test_ecommerce_decrypt_round_trip(
        self, processor: LogProcessor, ecommerce_log: dict
    ) -> None:
        # Encrypt → decrypt → strip _processing. The decrypted dict
        # must equal the original on every plaintext field; numeric
        # fields come back as strings (documented behaviour).
        encrypted = processor.encrypt(ecommerce_log)
        decrypted = processor.decrypt(encrypted)

        assert decrypted["customer_email"] == "alice@example.com"
        assert decrypted["phone"] == "555-867-5309"
        assert decrypted["shipping"]["postal_code"] == "98101"
        # _processing must be stripped from the decrypted output.
        assert "_processing" not in decrypted
        # Operational fields preserved exactly (same types as input).
        assert decrypted["order_id"] == "ORD-2026-05-16-001"
        assert decrypted["amount"] == 129.99
        assert decrypted["shipping"]["city"] == "Seattle"

    def test_ecommerce_input_dict_not_mutated(
        self, processor: LogProcessor, ecommerce_log: dict
    ) -> None:
        # Defensive copy: the caller's dict must remain pristine even
        # after a successful encrypt. Snapshot before, compare after.
        before = json.dumps(ecommerce_log, sort_keys=True)
        _ = processor.encrypt(ecommerce_log)
        after = json.dumps(ecommerce_log, sort_keys=True)
        assert before == after

    # ---- support ticket -----------------------------------------------

    def test_support_ticket_sensitive_fields_encrypted(
        self, processor: LogProcessor, support_ticket_log: dict
    ) -> None:
        # `user_email` and `customer_ssn` hit field-name; `agent` hits
        # via value regex (the email pattern). All three must end up
        # as EncryptedField dicts.
        out = processor.encrypt(support_ticket_log)
        for path in ("user_email", "customer_ssn", "agent"):
            assert isinstance(out[path], dict), path
            assert out[path]["algorithm"] == "AES-256-GCM"

    def test_support_ticket_operational_fields_unchanged(
        self, processor: LogProcessor, support_ticket_log: dict
    ) -> None:
        # ticket_id, priority, and created_at must remain plaintext —
        # those drive triage and routing.
        out = processor.encrypt(support_ticket_log)
        assert out["ticket_id"] == "TKT-9001"
        assert out["priority"] == "P2"
        assert out["created_at"] == "2026-05-16T12:00:00Z"

    def test_support_ticket_decrypt_round_trip(
        self, processor: LogProcessor, support_ticket_log: dict
    ) -> None:
        # Full encrypt → decrypt → verify all values, including the
        # regex-detected `agent` field.
        encrypted = processor.encrypt(support_ticket_log)
        decrypted = processor.decrypt(encrypted)

        assert decrypted["user_email"] == "bob@example.com"
        assert decrypted["customer_ssn"] == "123-45-6789"
        assert decrypted["agent"] == "alice@example.com"
        assert decrypted["ticket_id"] == "TKT-9001"
        assert decrypted["priority"] == "P2"
        assert decrypted["created_at"] == "2026-05-16T12:00:00Z"
        assert "_processing" not in decrypted

    # ---- crypto-level invariants --------------------------------------

    def test_two_encrypts_produce_different_ciphertexts(
        self, processor: LogProcessor, ecommerce_log: dict
    ) -> None:
        # Random 12-byte nonce per encrypt means same plaintext +
        # same key → DIFFERENT ciphertext. This protects against
        # equality-leak attacks where an observer sees that two
        # records had the same email.
        out_a = processor.encrypt(ecommerce_log)
        out_b = processor.encrypt(ecommerce_log)
        assert (
            out_a["customer_email"]["encrypted_value"]
            != out_b["customer_email"]["encrypted_value"]
        )
        assert out_a["customer_email"]["iv"] != out_b["customer_email"]["iv"]

    def test_decrypt_with_unknown_key_raises_keynotfound(
        self,
        keystore: KeyStore,
        parallel: ParallelEncryptor,
        ecommerce_log: dict,
    ) -> None:
        # Encrypt with one keystore, then attempt to decrypt with a
        # FRESH keystore that has never seen that key_id. The lookup
        # must surface `KeyNotFoundError` so callers can return 404
        # (rather than an opaque crypto failure).
        proc_a = LogProcessor(
            detector=Detector(), keystore=keystore, parallel=parallel
        )
        encrypted = proc_a.encrypt(ecommerce_log)

        # New, empty keystore — bootstraps a totally unrelated active key.
        other_store = KeyStore(EnvKeyProvider())
        other_store.create_initial_active()
        proc_b = LogProcessor(
            detector=Detector(), keystore=other_store, parallel=parallel
        )
        with pytest.raises(KeyNotFoundError):
            proc_b.decrypt(encrypted)


# ---------------------------------------------------------------------------
# TestParallelEncryptor — branch selection + ordering
# ---------------------------------------------------------------------------


def _fake_encryptor(key_id: str = "test-key") -> AESGCMEncryptor:
    """Build a deterministic encryptor so tests can decode ciphertext back."""
    return AESGCMEncryptor(dek=b"\x33" * 32, key_id=key_id)


def _make_items(n: int, *, payload_size: int = 10) -> list[_EncItem]:
    """Build ``n`` items whose plaintexts are predictable + ordered.

    We use the index as a left-zero-padded prefix so each plaintext is
    unique AND we can recover the original ordering from a decrypted
    output — that's how `test_parallel_preserves_input_order` confirms
    `pool.map` didn't rearrange the batch.
    """
    items: list[_EncItem] = []
    for i in range(n):
        prefix = f"item-{i:04d}-"
        # Pad with 'x' to reach the desired payload size.
        payload = (prefix + "x" * max(0, payload_size - len(prefix))).encode("utf-8")
        items.append(
            _EncItem(field_path=f"path.{i}", plaintext=payload, field_type="test")
        )
    return items


class TestParallelEncryptor:
    """Threshold-gated dispatcher selects the right branch."""

    def test_serial_branch_for_below_field_threshold(self) -> None:
        # 1 item, fields_threshold=2 → serial regardless of bytes.
        # Counters: 1 serial, 0 parallel.
        enc = ParallelEncryptor(
            thread_pool_size=4, threshold_fields=2, threshold_bytes=1
        )
        try:
            aes = _fake_encryptor()
            items = _make_items(1)
            results = enc.encrypt_many(
                items,
                lambda it: aes.encrypt(
                    it.plaintext,
                    record_id="r",
                    field_path=it.field_path,
                    field_type=it.field_type,
                ),
            )
            assert len(results) == 1
            assert enc.serial_calls == 1
            assert enc.parallel_calls == 0
            assert enc.is_parallel_pool_active is False
        finally:
            enc.close()

    def test_parallel_branch_for_above_thresholds(self) -> None:
        # 3 items × 10B = 30B with thresholds 2 / 1 → both exceeded →
        # parallel branch fires.
        enc = ParallelEncryptor(
            thread_pool_size=4, threshold_fields=2, threshold_bytes=1
        )
        try:
            aes = _fake_encryptor()
            items = _make_items(3, payload_size=10)
            _ = enc.encrypt_many(
                items,
                lambda it: aes.encrypt(
                    it.plaintext,
                    record_id="r",
                    field_path=it.field_path,
                    field_type=it.field_type,
                ),
            )
            assert enc.serial_calls == 0
            assert enc.parallel_calls == 1
            assert enc.is_parallel_pool_active is True
        finally:
            enc.close()

    def test_serial_branch_when_only_bytes_threshold_unmet(self) -> None:
        # 3 items of 10B each = 30B; bytes_threshold=1024 → bytes
        # condition fails → serial path (even though field count is
        # over the field threshold).
        enc = ParallelEncryptor(
            thread_pool_size=4, threshold_fields=2, threshold_bytes=1024
        )
        try:
            aes = _fake_encryptor()
            items = _make_items(3, payload_size=10)
            _ = enc.encrypt_many(
                items,
                lambda it: aes.encrypt(
                    it.plaintext,
                    record_id="r",
                    field_path=it.field_path,
                    field_type=it.field_type,
                ),
            )
            assert enc.serial_calls == 1
            assert enc.parallel_calls == 0
        finally:
            enc.close()

    def test_parallel_preserves_input_order(self) -> None:
        # 5 items with predictable plaintexts; decrypt the results in
        # order to confirm pool.map matched output[i] ↔ items[i].
        enc = ParallelEncryptor(
            thread_pool_size=4, threshold_fields=2, threshold_bytes=1
        )
        try:
            aes = _fake_encryptor("ord-key")
            items = _make_items(5, payload_size=30)
            results = enc.encrypt_many(
                items,
                lambda it: aes.encrypt(
                    it.plaintext,
                    record_id="ord-rec",
                    field_path=it.field_path,
                    field_type=it.field_type,
                ),
            )
            assert enc.parallel_calls == 1
            assert len(results) == 5
            # Decrypt each result with the AAD of the matching input
            # index. A mis-ordered batch would fail the AAD check
            # (InvalidTag), so this is a strong order-preservation
            # assertion, not just a "string-equal" one.
            for original, ef in zip(items, results):
                plaintext = aes.decrypt(
                    ef,
                    record_id="ord-rec",
                    field_path=original.field_path,
                )
                assert plaintext == original.plaintext
        finally:
            enc.close()

    def test_empty_batch_returns_empty_list(self) -> None:
        # Zero items is a valid input — should run the serial branch
        # (cheaper than dispatch) and return an empty list.
        enc = ParallelEncryptor(
            thread_pool_size=4, threshold_fields=2, threshold_bytes=1
        )
        try:
            results = enc.encrypt_many([], lambda it: None)  # type: ignore[arg-type]
            assert results == []
            assert enc.serial_calls == 1
        finally:
            enc.close()


# ---------------------------------------------------------------------------
# TestLogProcessor — edge cases
# ---------------------------------------------------------------------------


class TestLogProcessor:
    """Non-fixture edge cases that the round-trip class doesn't cover."""

    def test_log_with_no_sensitive_fields(
        self, processor: LogProcessor
    ) -> None:
        # All-operational log: encrypted_fields must be empty and the
        # rest of the dict must pass through verbatim. _processing is
        # still attached so consumers know the log was processed.
        log = {
            "order_id": "X1",
            "status": "shipped",
            "count": 7,
            "currency": "USD",
        }
        out = processor.encrypt(log)
        assert out["_processing"]["encrypted_fields"] == []
        assert out["order_id"] == "X1"
        assert out["status"] == "shipped"
        assert out["count"] == 7
        assert out["currency"] == "USD"

    def test_deeply_nested_pii_is_detected(
        self, processor: LogProcessor
    ) -> None:
        # Three-level nesting: the detector must walk all the way down.
        # We also confirm the encrypted leaf appears at the right
        # deep path (not flattened to a top-level key).
        log = {
            "request": {
                "user": {
                    "profile": {
                        "email": "deep@example.com",
                        "country": "US",
                    }
                }
            }
        }
        out = processor.encrypt(log)
        assert (
            out["request"]["user"]["profile"]["email"]["algorithm"]
            == "AES-256-GCM"
        )
        # Sibling at the same depth stays plaintext.
        assert out["request"]["user"]["profile"]["country"] == "US"
        # Manifest carries the full dotted path.
        assert "request.user.profile.email" in out["_processing"]["encrypted_fields"]

    def test_explicit_record_id_is_honored_on_encrypt(
        self, processor: LogProcessor, ecommerce_log: dict
    ) -> None:
        # Passing record_id explicitly must override auto-generation
        # and appear verbatim in the _processing envelope. (Decrypt
        # uses it to reconstruct the AAD; mismatching it would
        # InvalidTag.)
        out = processor.encrypt(ecommerce_log, record_id="custom-record-42")
        assert out["_processing"]["record_id"] == "custom-record-42"

    def test_auto_generated_record_id_is_hex_uuid(
        self, processor: LogProcessor, ecommerce_log: dict
    ) -> None:
        # No explicit id → uuid4().hex (32 lowercase hex chars).
        out = processor.encrypt(ecommerce_log)
        rec_id = out["_processing"]["record_id"]
        assert isinstance(rec_id, str)
        assert len(rec_id) == 32
        # All chars are hex.
        int(rec_id, 16)

    def test_decrypt_explicit_record_id_overrides_envelope(
        self, processor: LogProcessor, ecommerce_log: dict
    ) -> None:
        # Encrypt under id "X"; modify the envelope to claim "Y" but
        # pass the correct "X" explicitly — explicit must win and the
        # decrypt must succeed (AAD matches).
        out = processor.encrypt(ecommerce_log, record_id="correct-id")
        out["_processing"]["record_id"] = "wrong-id-in-envelope"
        decrypted = processor.decrypt(out, record_id="correct-id")
        assert decrypted["customer_email"] == "alice@example.com"

    def test_decrypt_uses_envelope_record_id_when_arg_missing(
        self, processor: LogProcessor, ecommerce_log: dict
    ) -> None:
        # No explicit id on decrypt → fall back to envelope. The
        # round-trip must succeed because the envelope id is what
        # was used at encrypt time.
        out = processor.encrypt(ecommerce_log)
        decrypted = processor.decrypt(out)
        assert decrypted["customer_email"] == "alice@example.com"

    def test_decrypt_without_envelope_and_no_arg_raises(
        self, processor: LogProcessor, ecommerce_log: dict
    ) -> None:
        # Drop the _processing envelope; with no explicit record_id
        # there's no way to reconstruct AAD → ProcessorError.
        out = processor.encrypt(ecommerce_log)
        del out["_processing"]
        with pytest.raises(ProcessorError):
            processor.decrypt(out)

    def test_decrypt_with_explicit_record_id_no_envelope_succeeds(
        self, processor: LogProcessor, ecommerce_log: dict
    ) -> None:
        # Envelope-stripped log + explicit id MUST still decrypt —
        # this is the path callers will use when they carry the
        # record id out-of-band (e.g. HTTP request id).
        out = processor.encrypt(ecommerce_log, record_id="oob-id")
        del out["_processing"]
        decrypted = processor.decrypt(out, record_id="oob-id")
        assert decrypted["customer_email"] == "alice@example.com"

    def test_log_processor_does_not_touch_lists(
        self, processor: LogProcessor
    ) -> None:
        # Detector v1 treats lists as opaque scalars; the processor
        # inherits that behaviour. A list of strings that LOOK like
        # emails must survive encrypt unchanged.
        log = {
            "tags": ["customer@example.com", "vip"],
            "order_id": "X1",
        }
        out = processor.encrypt(log)
        assert out["tags"] == ["customer@example.com", "vip"]
        assert out["_processing"]["encrypted_fields"] == []

    def test_encrypted_field_dict_is_pydantic_parseable(
        self, processor: LogProcessor, ecommerce_log: dict
    ) -> None:
        # The dict written into the log must be `EncryptedField`-shaped
        # — i.e. EncryptedField.model_validate must accept it without
        # raising. This guards against schema drift.
        out = processor.encrypt(ecommerce_log)
        ef = EncryptedField.model_validate(out["customer_email"])
        # base64 round-trips on both binary fields.
        assert base64.b64decode(ef.encrypted_value)
        assert len(base64.b64decode(ef.iv)) == 12  # 96-bit nonce


# ---------------------------------------------------------------------------
# TestLogProcessorWithAuditAndStats — C6 wiring
# ---------------------------------------------------------------------------


class TestLogProcessorWithAuditAndStats:
    """C6: when an :class:`AuditLogger` and :class:`StatsCounters` are
    supplied, the processor emits per-field audit events and bumps the
    standard counters. With both defaulted to ``None`` (the C5 contract,
    tested above) the processor behaves identically to C5 — those tests
    still pass unmodified."""

    def _build_processor(
        self, keystore: KeyStore, parallel: ParallelEncryptor
    ) -> tuple[LogProcessor, StatsCounters, AuditLogger, RingBuffer]:
        """Build a processor wired with fresh audit + stats.

        Returns the live audit logger and ring buffer so individual
        tests can introspect what the processor recorded. The buffer
        is kept small (size 100) to keep snapshots bounded under any
        future test that runs many encrypts.
        """
        from src.audit import AuditEvent, AuditLogger, RingBuffer
        from src.stats import StatsCounters

        stats = StatsCounters()
        buf: RingBuffer[AuditEvent] = RingBuffer(maxlen=100)
        audit = AuditLogger(buf)
        proc = LogProcessor(
            detector=Detector(),
            keystore=keystore,
            parallel=parallel,
            audit_logger=audit,
            stats=stats,
        )
        return proc, stats, audit, buf

    def test_encrypt_updates_stats_counters(
        self,
        keystore: KeyStore,
        parallel: ParallelEncryptor,
        ecommerce_log: dict,
    ) -> None:
        # e-commerce fixture has 3 sensitive leaves:
        # customer_email, phone, shipping.postal_code. After one
        # successful encrypt: fields_detected == fields_encrypted == 3
        # and logs_processed == 1.
        proc, stats, _, _ = self._build_processor(keystore, parallel)
        _ = proc.encrypt(ecommerce_log)

        snap = stats.snapshot()
        assert snap["logs_processed"] == 1
        assert snap["fields_detected"] == 3
        assert snap["fields_encrypted"] == 3
        # No errors on the happy path.
        assert snap["errors"] == 0

    def test_encrypt_emits_one_audit_event_per_field(
        self,
        keystore: KeyStore,
        parallel: ParallelEncryptor,
        ecommerce_log: dict,
    ) -> None:
        # One success "encrypt" audit event per detected field. Each
        # carries the active key_id and the same record_id (so a future
        # decrypt-trail join works).
        proc, _, audit, _ = self._build_processor(keystore, parallel)
        out = proc.encrypt(ecommerce_log)
        record_id = out["_processing"]["record_id"]
        active_key_id = keystore.get_active().key_id

        events = audit.recent()
        # Filter to encrypt success events; should be exactly the 3
        # detected fields.
        enc_events = [
            e
            for e in events
            if e.event_type == "encrypt" and e.outcome == "success"
        ]
        assert len(enc_events) == 3

        observed_paths = sorted(e.field_path for e in enc_events)
        assert observed_paths == [
            "customer_email",
            "phone",
            "shipping.postal_code",
        ]
        # All events share the same record_id and key_id.
        for e in enc_events:
            assert e.record_id == record_id
            assert e.key_id == active_key_id
            # byte_count must be populated (str-coerced plaintext length).
            assert e.byte_count is not None and e.byte_count > 0

    def test_decrypt_updates_stats_and_audit(
        self,
        keystore: KeyStore,
        parallel: ParallelEncryptor,
        ecommerce_log: dict,
    ) -> None:
        # Round trip: after encrypt + decrypt, fields_decrypted ==
        # fields_encrypted, and the audit log contains matching
        # decrypt events alongside the encrypt events.
        proc, stats, audit, _ = self._build_processor(keystore, parallel)
        encrypted = proc.encrypt(ecommerce_log)
        _ = proc.decrypt(encrypted)

        snap = stats.snapshot()
        assert snap["fields_encrypted"] == 3
        assert snap["fields_decrypted"] == 3
        assert snap["errors"] == 0

        events = audit.recent()
        dec_events = [
            e
            for e in events
            if e.event_type == "decrypt" and e.outcome == "success"
        ]
        assert len(dec_events) == 3
        observed_paths = sorted(e.field_path for e in dec_events)
        assert observed_paths == [
            "customer_email",
            "phone",
            "shipping.postal_code",
        ]

    def test_decrypt_against_destroyed_key_records_failure(
        self,
        keystore: KeyStore,
        parallel: ParallelEncryptor,
        ecommerce_log: dict,
    ) -> None:
        # Crypto-shred path: encrypt → rotate (so the encrypting key is
        # now retired) → destroy_key on the retired key → decrypt the
        # original ciphertext. Expected:
        #   1. errors counter >= 1
        #   2. audit has a failure event with failure_reason set
        #   3. the exception propagates to the caller
        from src.keystore.store import KeyDestroyedError

        proc, stats, audit, _ = self._build_processor(keystore, parallel)
        encrypted = proc.encrypt(ecommerce_log)
        original_key_id = encrypted["_processing"]["key_id"]

        # Rotate to put the original key into "retired", then destroy it.
        # destroy_key refuses to act on an active key, hence the
        # rotation step.
        keystore.rotate()
        keystore.destroy_key(original_key_id)

        # Decrypt should now blow up — the key is shredded.
        with pytest.raises(KeyDestroyedError):
            proc.decrypt(encrypted)

        # Errors counter incremented.
        assert stats.snapshot()["errors"] >= 1

        # Audit log carries a single failure event with the reason set.
        events = audit.recent()
        failures = [
            e
            for e in events
            if e.event_type == "decrypt" and e.outcome == "failure"
        ]
        assert len(failures) == 1
        assert failures[0].failure_reason is not None
        assert failures[0].failure_reason != ""
        # The record_id propagates from the input log so operators can
        # join the failure event back to the source.
        assert failures[0].record_id == encrypted["_processing"]["record_id"]

    def test_encrypt_failure_records_audit_event_and_propagates(
        self,
        parallel: ParallelEncryptor,
        ecommerce_log: dict,
    ) -> None:
        # Force encrypt failure by giving the processor a keystore that
        # has no active key — get_active() raises KeyNotFoundError
        # under the hood. Expected:
        #   - exception propagates
        #   - errors counter incremented
        #   - a single failure audit event with failure_reason set
        from src.crypto import EnvKeyProvider
        from src.keystore import KeyNotFoundError, KeyStore

        empty_store = KeyStore(EnvKeyProvider())  # never bootstrapped
        proc, stats, audit, _ = self._build_processor(empty_store, parallel)

        with pytest.raises(KeyNotFoundError):
            proc.encrypt(ecommerce_log)

        assert stats.snapshot()["errors"] >= 1
        events = audit.recent()
        failures = [
            e
            for e in events
            if e.event_type == "encrypt" and e.outcome == "failure"
        ]
        assert len(failures) == 1
        assert failures[0].failure_reason is not None
        # No successful encrypt was completed → fields_encrypted stays at 0.
        assert stats.snapshot()["fields_encrypted"] == 0
        # fields_detected was incremented (detection ran before the
        # keystore lookup failed) — confirms the partial-progress
        # counter contract.
        assert stats.snapshot()["fields_detected"] == 3


# ---------------------------------------------------------------------------
# TestLogProcessorWithCache — C9 wiring
# ---------------------------------------------------------------------------


class TestLogProcessorWithCache:
    """C9: when a :class:`CacheProvider` is injected, the processor
    bumps per-key-id usage counters on every encrypt/decrypt.

    With ``cache=None`` (the existing default), behaviour is identical
    to C5/C6 — the 26+5 prior processor tests still pass unmodified,
    which is the load-bearing property of the optional-arg design.
    """

    def test_encrypt_increments_per_key_encrypt_counter(
        self,
        keystore: KeyStore,
        parallel: ParallelEncryptor,
        ecommerce_log: dict,
    ) -> None:
        # One encrypt call → counter bumped exactly once per call
        # (NOT per field). The key is ``key_usage:<key_id>:encrypt``
        # — same shape used by ``GET /v1/keys``.
        from src.cache import InMemoryCache

        cache = InMemoryCache()
        proc = LogProcessor(
            detector=Detector(),
            keystore=keystore,
            parallel=parallel,
            cache=cache,
        )

        active_key_id = keystore.get_active().key_id
        # Sanity: counter starts at 0.
        assert cache.get_counter(f"key_usage:{active_key_id}:encrypt") == 0

        _ = proc.encrypt(ecommerce_log)

        # After one encrypt: counter == 1.
        assert cache.get_counter(f"key_usage:{active_key_id}:encrypt") == 1

        # A second encrypt bumps to 2 — confirms idempotency of the
        # one-bump-per-call contract.
        _ = proc.encrypt(ecommerce_log)
        assert cache.get_counter(f"key_usage:{active_key_id}:encrypt") == 2

    def test_decrypt_increments_per_key_decrypt_counter(
        self,
        keystore: KeyStore,
        parallel: ParallelEncryptor,
        ecommerce_log: dict,
    ) -> None:
        # One decrypt call with N encrypted fields → counter bumps N
        # times (per-field, not per-call — see the LogProcessor
        # docstring for the rationale). The e-commerce fixture has 3
        # sensitive leaves so the counter ends at 3.
        from src.cache import InMemoryCache

        cache = InMemoryCache()
        proc = LogProcessor(
            detector=Detector(),
            keystore=keystore,
            parallel=parallel,
            cache=cache,
        )

        active_key_id = keystore.get_active().key_id
        # Encrypt first so we have something to decrypt.
        encrypted = proc.encrypt(ecommerce_log)

        # Reset the decrypt counter expectation: zero before decrypt.
        # (encrypt counter is 1 at this point but that's a separate
        # namespace.)
        assert cache.get_counter(f"key_usage:{active_key_id}:decrypt") == 0

        _ = proc.decrypt(encrypted)

        # 3 fields decrypted → counter at 3.
        assert cache.get_counter(f"key_usage:{active_key_id}:decrypt") == 3

    def test_no_cache_preserves_pre_c9_behavior(
        self,
        keystore: KeyStore,
        parallel: ParallelEncryptor,
        ecommerce_log: dict,
    ) -> None:
        # The original ``cache=None`` (default) path must remain a
        # no-op. This is the load-bearing property that lets the
        # pre-C9 tests pass unmodified: constructing a processor
        # without a cache still works, and encrypt/decrypt succeed
        # without any cache interaction.
        proc = LogProcessor(
            detector=Detector(),
            keystore=keystore,
            parallel=parallel,
        )
        # Both directions complete without raising; the result is
        # round-trip-equal on the encrypted fields.
        encrypted = proc.encrypt(ecommerce_log)
        decrypted = proc.decrypt(encrypted)
        assert decrypted["customer_email"] == "alice@example.com"
