"""Versioned in-memory DEK store with explicit lifecycle.

A :class:`KeyStore` holds an ordered set of :class:`KeyRecord` instances —
one per DEK *version* — and routes encrypt/decrypt traffic to the right one:

* New encrypts go through :meth:`KeyStore.get_active` — there is exactly
  one ``status="active"`` record at any time (rotation enforces this under
  a lock).
* Decrypts look up by ``key_id`` via :meth:`KeyStore.get_for_decrypt`,
  which transparently serves *retired* records. Retired keys are still
  perfectly usable for decryption — only ``destroyed`` records are
  rejected (with :class:`KeyDestroyedError`).

Lifecycle
---------
::

    active  ──rotate()──►  retired  ──destroy_key()──►  destroyed
       │
       └── cannot be destroyed directly (must retire first)

Each :class:`KeyRecord` also carries its wrapped DEK (opaque blob from the
:class:`~src.crypto.key_provider.KeyProvider`) so the keystore could in
principle be rehydrated from durable storage in a future commit. For C4
the records are pure in-process state — durability is out of scope.

Crypto-shredding
----------------
:meth:`KeyStore.destroy_key` drops the encryptor reference, which in turn
drops the only strong reference to the in-memory DEK bytes. Python doesn't
let us reliably zero immutable ``bytes`` (the buffer may already have been
interned or copied by the cryptography library), so this is a *best-effort*
shred — the GC reclaims the memory whenever it next runs. Real crypto-
shredding in a hostile environment would use ``ctypes.memset`` against a
mutable ``bytearray`` buffer and pin it via ``mlock``; we do not go that
far in v1 because the spec's threat model is "log leakage", not "hot RAM
extraction". See the docstring on :meth:`destroy_key` for details.

Thread safety
-------------
All public state mutations are guarded by ``self._lock``. The keystore
must remain safe to call from the parallel encrypt path (C5) and the
rotation background task (C7) concurrently.
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from src.crypto.aesgcm import AESGCMEncryptor, generate_dek
from src.crypto.key_provider import KeyProvider


# ---------------------------------------------------------------------------
# Types & errors
# ---------------------------------------------------------------------------

KeyStatus = Literal["active", "retired", "destroyed"]
"""Lifecycle state of a DEK version.

* ``"active"``   — exactly one at a time; new encrypts use this DEK.
* ``"retired"``  — kept for decryption of old ciphertext; not used for
  new encrypts.
* ``"destroyed"`` — crypto-shredded; the DEK material is gone and any
  ciphertext still referencing this ``key_id`` is unrecoverable.
"""


class KeyStoreError(Exception):
    """Base exception for keystore-level failures.

    Subclasses signal specific failure modes; generic invariant
    violations (e.g., trying to destroy an active key) raise this
    class directly.
    """


class KeyNotFoundError(KeyStoreError):
    """Raised when a lookup misses or no active key has been minted yet.

    Two distinct failure modes share this exception by design — both
    are "no usable record under this name", and the caller's recovery
    path is identical in either case (return 404 from the API, or
    bootstrap an initial active key on startup).
    """


class KeyDestroyedError(KeyStoreError):
    """Raised when a caller asks for a key that has been crypto-shredded.

    Distinct from :class:`KeyNotFoundError`: the record EXISTS in the
    store (so we can answer ``list_keys`` accurately) but its DEK has
    been irretrievably zeroed. Decrypting any ciphertext that names
    this key is structurally impossible — surfacing that fact loudly
    helps operators recognize crypto-shred outcomes rather than
    silently 404'ing.
    """


# ---------------------------------------------------------------------------
# KeyRecord — one DEK version
# ---------------------------------------------------------------------------


@dataclass
class KeyRecord:
    """One DEK version + its lifecycle metadata.

    Attributes
    ----------
    key_id : str
        Stable identifier minted by the keystore (``key-<utc>-<hex>``).
        Stamped into every :class:`~src.crypto.schema.EncryptedField`
        produced with this DEK.
    wrapped_dek : bytes
        Opaque blob returned by
        :meth:`~src.crypto.key_provider.KeyProvider.wrap_dek`. Stored
        so the record could in principle survive a process restart by
        re-unwrapping with the KEK — not used at hot path time.
    status : KeyStatus
        Lifecycle state. See :data:`KeyStatus`.
    created_at : datetime
        UTC instant the record was minted.
    retired_at : datetime | None
        UTC instant the record transitioned ``active → retired``.
        Always set on retire; ``None`` for keys that have never been
        rotated out.
    destroyed_at : datetime | None
        UTC instant the record transitioned ``retired → destroyed``.
    encryptor : AESGCMEncryptor | None
        The cached encryptor that holds the in-memory DEK. ``None``
        ONLY after :meth:`KeyStore.destroy_key`, which drops the
        reference as part of the crypto-shred.

    Notes
    -----
    The dataclass uses ``field(default=None)`` for the optional bits
    so callers can construct via ``_mint_active`` without having to
    specify retirement/destruction fields up-front.
    """

    key_id: str
    wrapped_dek: bytes
    status: KeyStatus
    created_at: datetime
    retired_at: datetime | None = None
    destroyed_at: datetime | None = None
    # field() with default=None so destroy_key() can null the encryptor
    # without forcing every construction site to pass `encryptor=...`.
    encryptor: AESGCMEncryptor | None = field(default=None)


# ---------------------------------------------------------------------------
# KeyStore — the versioned container
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    """Thin wrapper so tests can monkeypatch the wall clock if needed."""
    return datetime.now(timezone.utc)


class KeyStore:
    """In-memory store of versioned DEKs with rotation/lifecycle support.

    Composition
    -----------
    The keystore does NOT generate KEK material — it composes a
    :class:`~src.crypto.key_provider.KeyProvider` (typically
    :class:`~src.crypto.key_provider.EnvKeyProvider` in v1) to wrap
    fresh DEKs and an :class:`~src.crypto.aesgcm.AESGCMEncryptor` per
    record so the AES key schedule is amortized.

    Lazy bootstrap
    --------------
    Construction does NOT auto-create an active key — callers must
    invoke :meth:`create_initial_active` exactly once at startup
    (C7's FastAPI lifespan does this). Test code that wants to skip
    that ceremony calls :meth:`create_initial_active` directly. The
    rationale for lazy init: process startup wiring (logging, config,
    KEK availability) should fail loudly *before* we generate any DEK
    material — surprising side effects in ``__init__`` make that
    harder.

    Thread safety
    -------------
    All public state mutations (``create_initial_active``, ``rotate``,
    ``destroy_key``) and reads that walk the record map
    (``get_active``, ``get_for_decrypt``, ``list_keys``) take
    ``self._lock``. The lock is intentionally coarse — keystore
    operations are nanosecond-scale and never blocking on I/O — so
    we pay nothing for the simplicity.
    """

    def __init__(self, provider: KeyProvider) -> None:
        self._provider = provider
        # dict preserves insertion order in 3.7+, but we don't rely on
        # that for "newest active" lookup — we sort by created_at so
        # the contract holds even if a future caller reorders inserts.
        self._records: dict[str, KeyRecord] = {}
        self._lock = threading.Lock()

    # -- bootstrap & rotation -------------------------------------------

    def create_initial_active(self) -> KeyRecord:
        """Mint the first active DEK.

        Idempotent? **No.** Calling this twice produces two active
        records; the second one becomes the new "newest active" and
        the first is left dangling (still status=active but
        unreachable via :meth:`get_active`). Callers should treat
        this as a startup-only operation; rotation after that goes
        through :meth:`rotate`.

        Returns
        -------
        KeyRecord
            The newly created active record. Already inserted into
            the store before return.
        """
        with self._lock:
            return self._mint_active_locked()

    def rotate(self) -> KeyRecord:
        """Retire the current active key and mint a fresh one.

        Atomic under ``self._lock`` so concurrent ``rotate()`` calls
        from multiple threads collapse to a deterministic sequence:
        each grabs the lock in turn, sees the most recent active
        record, retires it, and mints a new one. The end state after
        N concurrent rotations is exactly one active record (the
        most recently minted) and N retired records.

        Returns
        -------
        KeyRecord
            The newly minted active record.

        Notes
        -----
        If there is no current active key (e.g., the caller forgot
        :meth:`create_initial_active`), we still mint a new active —
        rotation as a "begin using a fresh DEK" operation is sensible
        even from an empty state. The C7 wiring will normally call
        ``create_initial_active`` on startup so this branch isn't
        exercised in production.
        """
        with self._lock:
            current = self._find_active_locked()
            if current is not None:
                current.status = "retired"
                current.retired_at = _utcnow()
            return self._mint_active_locked()

    # -- read paths -----------------------------------------------------

    def get_active(self) -> KeyRecord:
        """Return the current active :class:`KeyRecord`.

        Raises
        ------
        KeyNotFoundError
            If no active key exists yet — typically meaning startup
            forgot to call :meth:`create_initial_active`.
        """
        with self._lock:
            active = self._find_active_locked()
            if active is None:
                raise KeyNotFoundError("no active key")
            return active

    def get_for_decrypt(self, key_id: str) -> KeyRecord:
        """Look up a record by id for decryption purposes.

        Decrypt traffic is served by *any* non-destroyed record —
        retired keys remain perfectly readable, which is the whole
        point of keeping them around.

        Raises
        ------
        KeyNotFoundError
            If no record with this id exists.
        KeyDestroyedError
            If the record exists but has been crypto-shredded.
        """
        with self._lock:
            record = self._records.get(key_id)
            if record is None:
                raise KeyNotFoundError(f"unknown key_id {key_id!r}")
            if record.status == "destroyed":
                raise KeyDestroyedError(
                    f"key_id {key_id!r} has been destroyed (crypto-shredded)"
                )
            return record

    # -- destruction ----------------------------------------------------

    def destroy_key(self, key_id: str) -> None:
        """Crypto-shred a retired key.

        Requirements
        ------------
        The target must be ``retired`` first. Destroying an ``active``
        key is rejected with :class:`KeyStoreError` — the caller would
        be discarding the only DEK able to encrypt new fields, which
        is almost certainly a bug. Re-destroying an already-destroyed
        key is a no-op (idempotent).

        Shredding strategy
        ------------------
        We drop the strong reference to the :class:`AESGCMEncryptor`
        (which holds the only reference to the in-memory DEK bytes).
        Python cannot reliably zero an immutable ``bytes`` buffer —
        ``cryptography`` may have copied the key into its OpenSSL
        context, and CPython's small-object allocator may keep the
        original buffer alive briefly — so this is a best-effort
        shred. Once GC runs, both the encryptor and any associated
        OpenSSL state are freed. The ``wrapped_dek`` is left in
        place so audit/forensics still has the proof-of-existence,
        but the DEK cannot be recovered without the KEK (and a real
        KEK rotation in a future deployment would invalidate
        ``wrapped_dek`` too).

        For a hostile threat model, a future implementation would
        use ``bytearray`` + ``ctypes.memset`` + ``mlock``. The
        spec's threat model is leaked log files, not memory-resident
        attackers, so v1 stays simple.

        Raises
        ------
        KeyNotFoundError
            If no record with this id exists.
        KeyStoreError
            If the record is currently ``active``.
        """
        with self._lock:
            record = self._records.get(key_id)
            if record is None:
                raise KeyNotFoundError(f"unknown key_id {key_id!r}")
            if record.status == "destroyed":
                # Idempotent: a second destroy on the same key is a
                # silent success. The crypto-shred has already taken
                # effect; there's nothing left to do.
                return
            if record.status == "active":
                raise KeyStoreError(
                    f"cannot destroy active key {key_id!r}; retire it first"
                )
            # Status must be "retired" at this point.
            record.status = "destroyed"
            record.destroyed_at = _utcnow()
            # Drop the encryptor reference — the GC will reclaim both
            # the AESGCMEncryptor and the DEK bytes it holds.
            record.encryptor = None

    # -- inventory ------------------------------------------------------

    def list_keys(self) -> list[dict[str, Any]]:
        """Return metadata for every record in created-at order.

        Crucially, the returned dicts contain **no DEK bytes and no
        encryptor reference** — only id, status, timestamps, and the
        provider's KEK id. Safe to return verbatim from the
        ``GET /v1/keys`` HTTP endpoint without leaking key material.
        """
        with self._lock:
            # Snapshot under the lock so concurrent rotations can't
            # interleave a half-updated record into our list.
            snapshot = list(self._records.values())

        # Stable order by created_at so the dashboard / API response
        # is deterministic across calls.
        snapshot.sort(key=lambda r: r.created_at)

        kek_id = self._provider.kek_id()
        return [
            {
                "key_id": r.key_id,
                "status": r.status,
                "created_at": r.created_at,
                "retired_at": r.retired_at,
                "destroyed_at": r.destroyed_at,
                "kek_id": kek_id,
            }
            for r in snapshot
        ]

    # -- helpers --------------------------------------------------------

    def _mint_key_id(self) -> str:
        """Build a fresh ``key-<utc>-<hex>`` identifier.

        Uses ``uuid4().hex[:6]`` for the disambiguator: 24 bits of
        entropy is plenty for human-readable id uniqueness within a
        single-second bucket, and trimming to 6 hex chars keeps the
        id short enough for log lines.
        """
        return f"key-{_utcnow():%Y-%m-%dT%H-%M-%S}-{uuid4().hex[:6]}"

    def _mint_active_locked(self) -> KeyRecord:
        """Generate a fresh active record. **Caller must hold the lock.**

        Extracted from :meth:`create_initial_active` and :meth:`rotate`
        so both paths use exactly the same DEK-generation sequence
        (no risk of one path diverging from the other).
        """
        dek = generate_dek()
        wrapped = self._provider.wrap_dek(dek)
        key_id = self._mint_key_id()
        encryptor = AESGCMEncryptor(dek, key_id)
        record = KeyRecord(
            key_id=key_id,
            wrapped_dek=wrapped,
            status="active",
            created_at=_utcnow(),
            encryptor=encryptor,
        )
        self._records[key_id] = record
        return record

    def _find_active_locked(self) -> KeyRecord | None:
        """Return the newest active record, or ``None`` if none exist.

        Caller must hold ``self._lock``. We scan all records and pick
        the most recently created active one — robust against the
        edge case where ``create_initial_active`` was called twice
        and there are two active records (we still return a
        deterministic answer).
        """
        actives = [r for r in self._records.values() if r.status == "active"]
        if not actives:
            return None
        return max(actives, key=lambda r: r.created_at)
