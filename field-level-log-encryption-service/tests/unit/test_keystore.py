"""Unit tests for the C4 keystore + rotation policy.

Coverage targets:

* :class:`TestKeyStore`         — lifecycle (active/retired/destroyed),
  rotation atomicity, round-trip after rotation, crypto-shred
  semantics, metadata-only ``list_keys``, and thread-safe rotation.
* :class:`TestRotationManager`  — fake-clock-driven rotation policy,
  including the empty-store bootstrap edge case.

All tests use the zero-byte test KEK injected by :mod:`tests.conftest`.
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest

from src.crypto import EnvKeyProvider
from src.keystore import (
    KeyDestroyedError,
    KeyNotFoundError,
    KeyStore,
    KeyStoreError,
    RotationManager,
)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def provider() -> EnvKeyProvider:
    """Fresh KEK provider using the conftest-injected test key.

    Constructed per-test so any monkeypatching done in one test
    doesn't bleed into the next.
    """
    return EnvKeyProvider()


@pytest.fixture
def store(provider: EnvKeyProvider) -> KeyStore:
    """Empty keystore — caller is responsible for `create_initial_active`.

    The fixture deliberately does NOT bootstrap an active key. Tests
    that need one create it explicitly so the bootstrap path is
    exercised in the assertions.
    """
    return KeyStore(provider)


# ---------------------------------------------------------------------------
# TestKeyStore — lifecycle + rotation + shred
# ---------------------------------------------------------------------------


class TestKeyStore:
    """Behavior of :class:`KeyStore` across the full DEK lifecycle."""

    # ---- bootstrap -----------------------------------------------------

    def test_create_initial_active_returns_active_record(
        self, store: KeyStore
    ) -> None:
        # The bootstrap call must produce an `active` record with a
        # non-empty id — that id will be stamped into every
        # EncryptedField we mint until the first rotation.
        record = store.create_initial_active()
        assert record.status == "active"
        assert record.key_id  # non-empty string

    def test_get_active_returns_same_record_after_bootstrap(
        self, store: KeyStore
    ) -> None:
        # After bootstrap, `get_active()` must return the SAME record
        # (same key_id) — proves the active-pointer is properly wired.
        created = store.create_initial_active()
        assert store.get_active().key_id == created.key_id

    def test_get_for_decrypt_returns_record_by_id(self, store: KeyStore) -> None:
        # Decrypt path looks up by id; for the active key it must
        # return the same record `get_active()` would.
        created = store.create_initial_active()
        looked_up = store.get_for_decrypt(created.key_id)
        assert looked_up.key_id == created.key_id
        assert looked_up.status == "active"

    def test_get_for_decrypt_unknown_id_raises(self, store: KeyStore) -> None:
        # Asking for a key the store has never seen must raise the
        # specific exception — generic Exception would force callers
        # to string-match the message to distinguish failure modes.
        store.create_initial_active()
        with pytest.raises(KeyNotFoundError):
            store.get_for_decrypt("key-does-not-exist-xyz")

    def test_get_active_on_empty_store_raises(self, store: KeyStore) -> None:
        # No bootstrap → no active key → loud failure. This is the
        # signal C7's startup hook checks to know it must bootstrap.
        with pytest.raises(KeyNotFoundError, match="no active key"):
            store.get_active()

    # ---- crypto round-trip via the record's encryptor ------------------

    def test_encrypt_decrypt_round_trip_via_active_encryptor(
        self, store: KeyStore
    ) -> None:
        # The whole point of the keystore is to hand out an encryptor
        # bound to the right DEK version. Round-tripping through it
        # is the canonical "yes, the wiring works" assertion.
        record = store.create_initial_active()
        assert record.encryptor is not None
        plaintext = b"customer@example.com"
        ef = record.encryptor.encrypt(
            plaintext,
            record_id="r1",
            field_path="customer_email",
            field_type="email",
        )
        assert ef.key_id == record.key_id
        decrypted = record.encryptor.decrypt(
            ef, record_id="r1", field_path="customer_email"
        )
        assert decrypted == plaintext

    # ---- rotation ------------------------------------------------------

    def test_rotate_retires_old_and_activates_new(self, store: KeyStore) -> None:
        # After rotate(): the original record is retired (with a
        # non-None retired_at) and a NEW record is active. Both must
        # remain accessible via get_for_decrypt — retired keys are
        # still needed to read old ciphertext.
        original = store.create_initial_active()
        new_active = store.rotate()

        # IDs must differ — `_mint_key_id` includes uuid disambiguator
        # and the rotate path always generates a fresh DEK.
        assert new_active.key_id != original.key_id

        # Old record is now retired.
        retired = store.get_for_decrypt(original.key_id)
        assert retired.status == "retired"
        assert retired.retired_at is not None
        assert retired.retired_at.utcoffset() is not None  # tz-aware

        # New record is active and findable via both lookup paths.
        assert store.get_active().key_id == new_active.key_id
        assert store.get_for_decrypt(new_active.key_id).status == "active"

    def test_retired_key_can_still_decrypt_old_ciphertext(
        self, store: KeyStore
    ) -> None:
        # This is THE property that justifies keeping retired keys
        # around: ciphertext encrypted under k1 must still decrypt
        # after rotation has moved k1 from active → retired.
        k1 = store.create_initial_active()
        assert k1.encryptor is not None
        ef = k1.encryptor.encrypt(
            b"sensitive-payload",
            record_id="r1",
            field_path="user.email",
            field_type="email",
        )

        # Rotate — k1 is now retired, k2 is active.
        store.rotate()

        # Decrypt via the retired record's still-attached encryptor.
        retired = store.get_for_decrypt(k1.key_id)
        assert retired.status == "retired"
        assert retired.encryptor is not None
        assert (
            retired.encryptor.decrypt(
                ef, record_id="r1", field_path="user.email"
            )
            == b"sensitive-payload"
        )

    def test_rotate_with_no_active_still_mints_new_active(
        self, store: KeyStore
    ) -> None:
        # Edge case: calling rotate() on an empty store should still
        # produce a fresh active key (rotation as "start a new
        # generation" is sensible from any starting state).
        new_active = store.rotate()
        assert new_active.status == "active"
        assert store.get_active().key_id == new_active.key_id

    # ---- destruction ---------------------------------------------------

    def test_destroy_active_key_is_rejected(self, store: KeyStore) -> None:
        # Destroying the only key still doing useful work would orphan
        # every subsequent encrypt attempt — must fail loudly so a
        # confused operator doesn't lose data.
        active = store.create_initial_active()
        with pytest.raises(KeyStoreError, match="cannot destroy active key"):
            store.destroy_key(active.key_id)

    def test_destroy_retired_key_zeroes_encryptor(self, store: KeyStore) -> None:
        # The crypto-shred contract: status flips to "destroyed" and
        # the encryptor reference is dropped so the DEK is GC-eligible.
        original = store.create_initial_active()
        store.rotate()  # original is now retired
        store.destroy_key(original.key_id)

        # We can't directly fetch via get_for_decrypt because that
        # raises on destroyed — but list_keys still shows it, so we
        # inspect the underlying record by reaching through the
        # internal map (an API caller cannot do this).
        # pylint: disable=protected-access
        rec = store._records[original.key_id]
        assert rec.status == "destroyed"
        assert rec.destroyed_at is not None
        assert rec.encryptor is None  # crypto-shred dropped the reference

    def test_get_for_decrypt_destroyed_raises_distinct_error(
        self, store: KeyStore
    ) -> None:
        # A destroyed key is different from a missing key — surface
        # that distinction so an audit/forensics workflow can tell
        # "we threw it away on purpose" from "we never had it".
        original = store.create_initial_active()
        store.rotate()
        store.destroy_key(original.key_id)
        with pytest.raises(KeyDestroyedError, match="crypto-shredded"):
            store.get_for_decrypt(original.key_id)

    def test_destroy_already_destroyed_is_idempotent(self, store: KeyStore) -> None:
        # Re-destroying a destroyed key must NOT raise — the
        # invariant is already satisfied, and idempotent destroy
        # makes scripts/operators safer.
        original = store.create_initial_active()
        store.rotate()
        store.destroy_key(original.key_id)
        # No exception on second call:
        store.destroy_key(original.key_id)
        # pylint: disable=protected-access
        assert store._records[original.key_id].status == "destroyed"

    def test_destroy_unknown_key_raises_not_found(self, store: KeyStore) -> None:
        # Distinct from "destroyed" — the key was never in the store.
        store.create_initial_active()
        with pytest.raises(KeyNotFoundError):
            store.destroy_key("key-never-existed")

    # ---- list_keys metadata-only contract ------------------------------

    def test_list_keys_returns_metadata_only_no_dek_or_encryptor(
        self, store: KeyStore
    ) -> None:
        # The most security-critical assertion in this file: the
        # /v1/keys endpoint will return list_keys() verbatim, so it
        # MUST NOT contain any DEK bytes or any object holding them.
        record = store.create_initial_active()
        assert record.encryptor is not None
        dek_bytes_via_internal = record.wrapped_dek  # the wrapped blob

        listing = store.list_keys()
        assert len(listing) == 1
        entry = listing[0]

        # Required metadata is present:
        assert entry["key_id"] == record.key_id
        assert entry["status"] == "active"
        assert entry["kek_id"] == "env-v1"
        assert "created_at" in entry
        assert entry["retired_at"] is None
        assert entry["destroyed_at"] is None

        # Forbidden material is absent (no DEK in any form, no
        # encryptor reference, no wrapped blob).
        for value in entry.values():
            # Direct identity check on the wrapped blob — if the
            # store ever leaked it, this catches it.
            assert value is not dek_bytes_via_internal
        # No bytes-typed values anywhere in the metadata payload.
        # `encryptor`, `dek`, and `wrapped_dek` are the dangerous
        # field names; assert none of them appear.
        assert "dek" not in entry
        assert "wrapped_dek" not in entry
        assert "encryptor" not in entry
        # Belt and braces: scan all values for bytes leakage.
        assert not any(isinstance(v, (bytes, bytearray)) for v in entry.values())

    def test_list_keys_ordered_by_created_at(self, store: KeyStore) -> None:
        # Stable ordering matters for the dashboard — operators
        # expect the oldest-first / newest-last layout.
        first = store.create_initial_active()
        store.rotate()
        store.rotate()

        listing = store.list_keys()
        assert len(listing) == 3
        assert listing[0]["key_id"] == first.key_id
        # Verify created_at is monotonically non-decreasing.
        timestamps = [entry["created_at"] for entry in listing]
        assert timestamps == sorted(timestamps)

    # ---- thread safety -------------------------------------------------

    def test_concurrent_rotate_leaves_exactly_one_active(
        self, store: KeyStore
    ) -> None:
        # Spawn N threads that all rotate at once and synchronize on a
        # Barrier so they hit the lock concurrently. The invariant
        # we test: regardless of interleaving, the final state has
        # EXACTLY one active record (and N retired ones from the N
        # threads that won the lock after the original active key
        # was rotated out).
        store.create_initial_active()
        n_threads = 10
        barrier = threading.Barrier(n_threads)

        def worker() -> None:
            # All threads block here until everyone is ready, then
            # race for the keystore lock simultaneously.
            barrier.wait()
            store.rotate()

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly one active record after N concurrent rotations.
        # pylint: disable=protected-access
        active_records = [
            r for r in store._records.values() if r.status == "active"
        ]
        assert len(active_records) == 1
        # The retired set is the original + (N - 1) intermediate
        # active records that got retired by subsequent rotations.
        retired_records = [
            r for r in store._records.values() if r.status == "retired"
        ]
        assert len(retired_records) == n_threads
        # Total record count = 1 (original active before rotate) +
        # N (one new active per rotate) = N + 1.
        assert len(store._records) == n_threads + 1


# ---------------------------------------------------------------------------
# TestRotationManager — clock-driven rotation policy
# ---------------------------------------------------------------------------


class TestRotationManager:
    """Behavior of :class:`RotationManager` with an injected clock."""

    def test_maybe_rotate_before_interval_returns_false(
        self, store: KeyStore
    ) -> None:
        # Active key just created; only a moment has passed; interval
        # is 30 days. Manager must NOT rotate.
        store.create_initial_active()
        active = store.get_active()

        # Clock returns a time ten minutes after creation — well
        # short of the 30-day interval.
        def fake_now() -> datetime:
            return active.created_at + timedelta(minutes=10)

        manager = RotationManager(store, interval_days=30, now_fn=fake_now)
        assert manager.maybe_rotate() is False
        # Active key id is unchanged.
        assert store.get_active().key_id == active.key_id

    def test_maybe_rotate_after_interval_returns_true_and_rotates(
        self, store: KeyStore
    ) -> None:
        # Clock returns a time well past the interval — manager must
        # rotate and the new active key must differ from the old one.
        store.create_initial_active()
        before = store.get_active()

        def fake_now() -> datetime:
            return before.created_at + timedelta(days=31)

        manager = RotationManager(store, interval_days=30, now_fn=fake_now)
        assert manager.maybe_rotate() is True

        after = store.get_active()
        assert after.key_id != before.key_id
        # And the old key was retired (not destroyed).
        retired = store.get_for_decrypt(before.key_id)
        assert retired.status == "retired"

    def test_maybe_rotate_resets_clock_after_rotation(
        self, store: KeyStore
    ) -> None:
        # After a rotation, the NEW active key's created_at becomes
        # the rotation clock. A subsequent maybe_rotate() within the
        # interval window should return False — proving the manager
        # reads the keystore's timestamps instead of caching its own.
        store.create_initial_active()
        original = store.get_active()

        # First call: time advanced past interval → rotation fires.
        def now_after_interval() -> datetime:
            return original.created_at + timedelta(days=31)

        m1 = RotationManager(store, interval_days=30, now_fn=now_after_interval)
        assert m1.maybe_rotate() is True
        new_active = store.get_active()
        assert new_active.key_id != original.key_id

        # Second call: clock advanced just a little past the rotation
        # moment — well inside the next interval window. No rotation.
        def now_just_after_rotation() -> datetime:
            return new_active.created_at + timedelta(minutes=5)

        m2 = RotationManager(
            store, interval_days=30, now_fn=now_just_after_rotation
        )
        assert m2.maybe_rotate() is False
        assert store.get_active().key_id == new_active.key_id

    def test_maybe_rotate_on_empty_store_returns_false_no_exception(
        self, store: KeyStore
    ) -> None:
        # Startup may schedule maybe_rotate() before the bootstrap
        # hook runs. The manager must absorb the "no active key yet"
        # case silently — startup will create the initial key, and
        # the next call will see it.
        manager = RotationManager(
            store,
            interval_days=30,
            now_fn=lambda: datetime.now(timezone.utc),
        )
        # No exception, returns False.
        assert manager.maybe_rotate() is False
        # And the store remains empty.
        # pylint: disable=protected-access
        assert store._records == {}

    def test_maybe_rotate_exactly_at_interval_boundary_rotates(
        self, store: KeyStore
    ) -> None:
        # The decision rule is `created_at + interval <= now` (≤ not <)
        # — i.e., exactly-at-the-interval triggers rotation. Verifies
        # we don't accidentally use strict `<` and skip the boundary.
        store.create_initial_active()
        active = store.get_active()

        def fake_now() -> datetime:
            return active.created_at + timedelta(days=30)

        manager = RotationManager(store, interval_days=30, now_fn=fake_now)
        assert manager.maybe_rotate() is True
        assert store.get_active().key_id != active.key_id
