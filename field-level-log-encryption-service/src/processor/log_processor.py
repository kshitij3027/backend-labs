"""End-to-end log encrypt/decrypt pipeline.

The :class:`LogProcessor` is the heart of the service: take a log dict,
find every sensitive leaf via the C2 :class:`~src.detection.detector.Detector`,
encrypt only those leaves with the C4 keystore's *active* DEK, splice the
results back into a deep-cloned log, and stamp a small ``_processing``
envelope so the symmetric :meth:`decrypt` path can reverse the operation.

Algorithmic invariants
----------------------
* The original log dict is **never mutated**. We always work on a
  :func:`copy.deepcopy`.
* Lists are opaque scalars (matches Detector v1 behaviour). They survive
  encrypt unchanged.
* Non-string scalars are coerced to their string form via ``str(value)``
  before encryption; on decrypt we return the UTF-8-decoded plaintext as
  a string (we never try to re-coerce back to ``int``/``float``). The
  decrypted log therefore has string leaves where the original had
  numbers — this is a deliberate choice: round-tripping arbitrary value
  types through ciphertext is unsafe (truncation, locale, NaN). String
  is the canonical wire form.
* All fields in one batch share the *active* DEK at the moment
  :meth:`encrypt` is called. If rotation fires mid-flight, the next
  call uses the new active key — old ciphertext still decrypts because
  the keystore keeps retired records.

Audit / stats integration
-------------------------
This commit (C5) deliberately keeps the constructor minimal — no audit
logger, no stats counter dependencies. C6 will add those without
breaking callers (the additions will be keyword-only with sensible
defaults). Keeping C5 stripped down makes the pipeline easier to unit
test in isolation.
"""
from __future__ import annotations

import copy
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from src.crypto.schema import EncryptedField
from src.detection.detector import Detector
from src.keystore.store import KeyStore

from .parallel import ParallelEncryptor, _EncItem

logger = logging.getLogger(__name__)


# Sentinel key used in the encrypted-log envelope. Keep in one place so a
# future rename only touches one constant.
_PROCESSING_KEY: str = "_processing"
_ALGO_AES_GCM: str = "AES-256-GCM"


class ProcessorError(Exception):
    """Raised for processor-level failures that aren't crypto errors.

    Examples:

    * Missing ``record_id`` on decrypt (no explicit arg, no
      ``_processing`` envelope).
    * Malformed encrypted-field record in the input log.

    Crypto failures (wrong key, tampered tag) propagate as
    ``cryptography.exceptions.InvalidTag``; keystore lookup failures
    propagate as :class:`~src.keystore.store.KeyNotFoundError` /
    :class:`~src.keystore.store.KeyDestroyedError`.
    """


class LogProcessor:
    """Run a log dict through detection + selective encryption.

    Composition
    -----------
    The processor composes three injected services:

    * :class:`Detector` — finds sensitive leaves.
    * :class:`KeyStore` — resolves the active DEK on encrypt and the
      matching DEK by ``key_id`` on decrypt.
    * :class:`ParallelEncryptor` — threshold-gated dispatcher.

    Future commits (C6) will extend the constructor with an
    ``audit_logger`` and a ``stats`` counter; both will be keyword-only
    so existing call sites continue to compile.

    Parameters
    ----------
    detector : Detector
        Already-constructed PII detector.
    keystore : KeyStore
        Already-bootstrapped keystore (at least one active key).
    parallel : ParallelEncryptor
        Pool wrapper. Owns its own ``ThreadPoolExecutor``; caller is
        responsible for ``parallel.close()`` at shutdown.
    """

    def __init__(
        self,
        detector: Detector,
        keystore: KeyStore,
        parallel: ParallelEncryptor,
    ) -> None:
        self._detector = detector
        self._keystore = keystore
        self._parallel = parallel

    # -- public API ------------------------------------------------------

    def encrypt(self, log: dict, *, record_id: str | None = None) -> dict:
        """Detect sensitive leaves, encrypt them, and stamp metadata.

        The input ``log`` is treated as read-only; we deep-copy before
        mutating. The returned dict contains the same shape with each
        detected leaf replaced by an :class:`EncryptedField` dump
        (``model_dump(mode="json")``), and a top-level ``_processing``
        envelope listing every encrypted path plus the active key id.

        Parameters
        ----------
        log : dict
            JSON-decoded log entry. Nested dicts are walked
            recursively; lists are skipped (see module docstring).
        record_id : str | None
            Stable correlator for AAD binding (see
            :class:`~src.crypto.aesgcm.AESGCMEncryptor`). Auto-generated
            from ``uuid4().hex`` if omitted. The same value must be
            available at decrypt time (carried in ``_processing``).

        Returns
        -------
        dict
            New dict; the original ``log`` is unchanged.
        """
        record_id = record_id or uuid4().hex

        # 1) Detection. The detector walks the original log read-only.
        detections = self._detector.detect(log)

        # 2) Build encrypt items. We extract each value via its dotted
        #    path BEFORE deep-copying — the original is read-only anyway,
        #    and we want the items list to reflect the input not the
        #    work-in-progress clone.
        items: list[_EncItem] = []
        for d in detections:
            raw_value = self._get_at_path(log, d.field_path)
            # Match detector v1: serialize everything to UTF-8 bytes via
            # `str(value)`. None becomes "None" which is intentional —
            # field-name hits can fire on a None value too.
            plaintext_bytes = str(raw_value).encode("utf-8")
            items.append(
                _EncItem(
                    field_path=d.field_path,
                    plaintext=plaintext_bytes,
                    field_type=d.field_type,
                )
            )

        # 3) Resolve the active key ONCE. All items in this batch share
        #    it — if rotation fires mid-process, this still produces a
        #    coherent batch under one DEK.
        active = self._keystore.get_active()
        active_encryptor = active.encryptor
        if active_encryptor is None:
            # Defensive: get_active() returns active records whose
            # encryptor is set; only destroy_key() nulls it. If we ever
            # see a null encryptor on an active record something is
            # very wrong upstream.
            raise ProcessorError(
                f"active key {active.key_id!r} has no encryptor (destroyed?)"
            )

        # Closure over the captured encryptor so the worker function
        # doesn't have to take it as an argument (matches the
        # ParallelEncryptor.encrypt_fn signature).
        def encrypt_one(item: _EncItem) -> EncryptedField:
            return active_encryptor.encrypt(
                item.plaintext,
                record_id=record_id,
                field_path=item.field_path,
                field_type=item.field_type,
            )

        # 4) Run the batch (threshold inside ParallelEncryptor decides
        #    serial vs parallel). Output order matches `items` order.
        encrypted_fields = self._parallel.encrypt_many(items, encrypt_one)

        # 5) Build the output. Deep-copy first so we never mutate the input.
        out = copy.deepcopy(log)

        # Splice each EncryptedField into its detected leaf path.
        for d, ef in zip(detections, encrypted_fields):
            self._set_at_path(out, d.field_path, ef.model_dump(mode="json"))

        # 6) Stamp the metadata envelope. Use a sorted-by-path list so
        #    consumers (and tests) get deterministic ordering even if
        #    the detector ever changes its sort key.
        out[_PROCESSING_KEY] = {
            "record_id": record_id,
            "key_id": active.key_id,
            "encrypted_fields": sorted(d.field_path for d in detections),
            "encrypted_at": datetime.now(timezone.utc).isoformat(),
        }
        return out

    def decrypt(self, log: dict, *, record_id: str | None = None) -> dict:
        """Reverse :meth:`encrypt` and return a plaintext log dict.

        Walks the dict recursively. Whenever a leaf is itself a dict
        carrying ``algorithm == "AES-256-GCM"``, we parse it as an
        :class:`EncryptedField`, look up its ``key_id`` in the keystore
        (retired keys are accepted), decrypt with the recovered AAD
        triple, and replace the leaf with the UTF-8 decoded string.

        Parameters
        ----------
        log : dict
            Log produced by :meth:`encrypt` (or anything structurally
            identical, e.g. coming back from an HTTP round-trip).
        record_id : str | None
            Optional explicit AAD record-id. If omitted we read
            ``log["_processing"]["record_id"]``; if that's missing too
            we raise :class:`ProcessorError`. The explicit argument
            wins if both are present — useful for upstream callers
            that already know the canonical id from their request
            context.

        Returns
        -------
        dict
            New dict; the input ``log`` is unchanged. The
            ``_processing`` envelope is stripped from the top level.
        """
        # Resolve the record_id BEFORE we strip the envelope from the
        # clone, so the precedence (explicit arg > envelope) is
        # observable in error messages.
        envelope = log.get(_PROCESSING_KEY) if isinstance(log, dict) else None
        if record_id is None:
            if envelope is None or "record_id" not in envelope:
                raise ProcessorError(
                    "missing record_id: pass it explicitly or include "
                    "_processing.record_id in the input log"
                )
            record_id = envelope["record_id"]

        out = copy.deepcopy(log)
        # Recursive walk + in-place replacement.
        self._walk_for_decrypt(out, parent_path="", record_id=record_id)

        # Drop the envelope from the top-level — callers expect a
        # round-trip-clean log dict.
        out.pop(_PROCESSING_KEY, None)
        return out

    # -- internal: recursive walkers -------------------------------------

    def _walk_for_decrypt(
        self,
        node: Any,
        *,
        parent_path: str,
        record_id: str,
    ) -> None:
        """Recursively decrypt every encrypted-field record in ``node``.

        Mutates the dict in place. Recognizes an encrypted record by
        the marker ``algorithm == "AES-256-GCM"`` on a dict-typed
        leaf — that's the same shape :meth:`encrypt` writes via
        :meth:`EncryptedField.model_dump`. Anything else is left alone.
        """
        if not isinstance(node, dict):
            return

        for key, value in list(node.items()):
            # Skip our own metadata envelope; it stays in place until
            # the top-level pop in decrypt(). Walking into it would
            # produce nothing useful (no AES-256-GCM markers there).
            if key == _PROCESSING_KEY and parent_path == "":
                continue

            path = f"{parent_path}.{key}" if parent_path else key

            if isinstance(value, dict):
                # Encrypted-field marker? If so, decrypt and replace.
                if value.get("algorithm") == _ALGO_AES_GCM:
                    plaintext = self._decrypt_one(
                        value, path=path, record_id=record_id
                    )
                    node[key] = plaintext
                else:
                    # Regular nested dict — recurse.
                    self._walk_for_decrypt(
                        value, parent_path=path, record_id=record_id
                    )
            # Lists / scalars: nothing to do (matches encrypt's contract).

    def _decrypt_one(
        self,
        record_dict: dict,
        *,
        path: str,
        record_id: str,
    ) -> str:
        """Decrypt one ``EncryptedField``-shaped dict and return the string.

        Pydantic-parses the dict for type safety — a tampered payload
        with extra keys or a wrong algorithm literal will fail at
        validation rather than getting all the way to OpenSSL.
        """
        # `model_validate` raises pydantic.ValidationError on bad input;
        # we let that bubble up — the caller-facing route will turn it
        # into a 422.
        ef = EncryptedField.model_validate(record_dict)

        # Route through the keystore so retired keys still decrypt.
        # `get_for_decrypt` raises KeyNotFoundError / KeyDestroyedError
        # which we let bubble up — both are meaningful operator-facing
        # signals.
        key_record = self._keystore.get_for_decrypt(ef.key_id)
        encryptor = key_record.encryptor
        if encryptor is None:
            # Should not happen: get_for_decrypt rejects destroyed keys.
            # Guard anyway so a future bug doesn't silently pass None
            # through to AESGCM.
            raise ProcessorError(
                f"key {ef.key_id!r} has no encryptor (destroyed?)"
            )

        plaintext_bytes = encryptor.decrypt(
            ef, record_id=record_id, field_path=path
        )
        # On encrypt we serialized via str(value).encode("utf-8"); on
        # decrypt we return the string form. Numbers come back as
        # strings (see module docstring).
        return plaintext_bytes.decode("utf-8")

    # -- internal: dotted-path helpers -----------------------------------

    @staticmethod
    def _get_at_path(log: dict, dotted_path: str) -> Any:
        """Extract a value from ``log`` via a dotted path.

        ``"shipping.postal_code"`` resolves to ``log["shipping"]["postal_code"]``.
        Returns ``None`` if any intermediate key is missing — the
        detector should never emit a path that points at a missing
        leaf, so this is purely defensive.
        """
        node: Any = log
        for part in dotted_path.split("."):
            if not isinstance(node, dict):
                return None
            node = node.get(part)
        return node

    @staticmethod
    def _set_at_path(log: dict, dotted_path: str, value: Any) -> None:
        """Set ``log[a][b][c] = value`` for ``dotted_path = "a.b.c"``.

        Assumes all intermediate dicts already exist (they do, because
        we deep-copied from the input which contained them). Raises
        :class:`ProcessorError` if an intermediate is missing — that
        would indicate the input log shape changed between detection
        and splicing, which shouldn't happen on the same call.
        """
        parts = dotted_path.split(".")
        node: Any = log
        for part in parts[:-1]:
            if not isinstance(node, dict) or part not in node:
                raise ProcessorError(
                    f"cannot set at path {dotted_path!r}: missing intermediate {part!r}"
                )
            node = node[part]
        if not isinstance(node, dict):
            raise ProcessorError(
                f"cannot set at path {dotted_path!r}: leaf parent is not a dict"
            )
        node[parts[-1]] = value
