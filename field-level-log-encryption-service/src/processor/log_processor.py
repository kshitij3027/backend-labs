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

Audit / stats integration (C6)
------------------------------
The constructor accepts optional ``audit_logger`` and ``stats`` arguments;
when supplied the processor emits one :class:`~src.audit.AuditEvent` per
encrypted / decrypted field and increments the corresponding
:class:`~src.stats.StatsCounters` keys. **Both arguments default to
``None``** so the C5 contract is preserved: callers that don't care about
audit/stats see exactly the C5 behaviour. This was the load-bearing
property for the original 26 C5 tests, all of which still pass without
modification.

The audit events follow the schema sealed in
:mod:`src.audit.audit_logger` — no plaintext, ciphertext, nonce, or DEK
material is ever recorded. Failure events carry only ``str(exc)`` of
the upstream exception as ``failure_reason``.
"""
from __future__ import annotations

import copy
import logging
import time
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from src.audit import AuditLogger
from src.crypto.schema import EncryptedField
from src.detection.detector import Detector
from src.keystore.store import KeyStore
from src.stats import StatsCounters

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

    Audit + stats are optional collaborators added in C6. Both default
    to ``None`` so the original C5 constructor signature still compiles
    and behaves identically when not supplied.

    Parameters
    ----------
    detector : Detector
        Already-constructed PII detector.
    keystore : KeyStore
        Already-bootstrapped keystore (at least one active key).
    parallel : ParallelEncryptor
        Pool wrapper. Owns its own ``ThreadPoolExecutor``; caller is
        responsible for ``parallel.close()`` at shutdown.
    audit_logger : AuditLogger | None
        Optional sink for per-field audit events. ``None`` (default)
        disables auditing — the encrypt/decrypt path skips the
        record() calls entirely so there's no perf cost when the
        feature is off.
    stats : StatsCounters | None
        Optional counter store. ``None`` (default) disables stats
        bumping. Same rationale as ``audit_logger`` — zero overhead
        when omitted.
    """

    def __init__(
        self,
        detector: Detector,
        keystore: KeyStore,
        parallel: ParallelEncryptor,
        audit_logger: AuditLogger | None = None,
        stats: StatsCounters | None = None,
    ) -> None:
        self._detector = detector
        self._keystore = keystore
        self._parallel = parallel
        # Both audit + stats are optional. Holding the references as
        # attributes (rather than booleans) lets us keep the truthy
        # checks in the hot path concise and the call sites readable.
        self._audit = audit_logger
        self._stats = stats

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

        # Audit / stats are optional collaborators (C6). Capture a start
        # timestamp only when we have something to record against — saves
        # the syscall otherwise.
        observe = self._audit is not None or self._stats is not None
        t0 = time.perf_counter_ns() if observe else 0

        try:
            # 1) Detection. The detector walks the original log read-only.
            detections = self._detector.detect(log)

            # Stats: every detected leaf counts, regardless of whether the
            # encrypt below ends up running (it always does, but the
            # contract is "detected" vs "encrypted").
            if self._stats is not None:
                self._stats.incr("fields_detected", len(detections))

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

            # 7) Audit + stats: every successful encrypt has been
            #    completed by this point. We do this AFTER the splice so
            #    a failure in step 5 doesn't produce orphan audit
            #    "success" events.
            if self._stats is not None:
                # fields_encrypted matches the per-leaf count; logs_processed
                # is per-call (one log in, one log out).
                self._stats.incr("fields_encrypted", len(detections))
                self._stats.incr("logs_processed")

            if self._audit is not None:
                # One audit event per encrypted field. duration_us is the
                # WHOLE-batch wall time divided across events — caller
                # cares more about per-call latency than per-field, and
                # AES-GCM per field is too short to measure individually
                # without dwarfing the work itself.
                duration_us = (time.perf_counter_ns() - t0) // 1000
                for d, item in zip(detections, items):
                    self._audit.record(
                        event_type="encrypt",
                        outcome="success",
                        record_id=record_id,
                        key_id=active.key_id,
                        field_path=d.field_path,
                        field_type=d.field_type,
                        byte_count=len(item.plaintext),
                        duration_us=duration_us,
                    )

            return out

        except Exception as exc:
            # Failure path: bump the errors counter and record a single
            # failure audit event before re-raising. We do NOT emit
            # per-field failure events because the failure is at the
            # batch level (key lookup, encrypt step) — there's no
            # meaningful per-field outcome to record.
            if self._stats is not None:
                self._stats.incr("errors")
            if self._audit is not None:
                self._audit.record(
                    event_type="encrypt",
                    outcome="failure",
                    record_id=record_id,
                    failure_reason=str(exc),
                    duration_us=(time.perf_counter_ns() - t0) // 1000
                    if observe
                    else None,
                )
            raise

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

        # Audit / stats prelude — same pattern as encrypt(). We collect
        # per-field success events into a list and only flush them once
        # the entire walk completes successfully; a mid-walk failure
        # produces a single batch-level failure event instead.
        observe = self._audit is not None or self._stats is not None
        t0 = time.perf_counter_ns() if observe else 0
        decrypted_events: list[dict[str, Any]] = []

        try:
            out = copy.deepcopy(log)
            # Recursive walk + in-place replacement. _walk_for_decrypt
            # appends one dict per field it actually decrypts into
            # decrypted_events — fields that aren't encrypted records
            # are skipped silently (we do NOT want to audit every leaf
            # in the log, only the encrypted ones).
            self._walk_for_decrypt(
                out,
                parent_path="",
                record_id=record_id,
                audit_sink=decrypted_events if observe else None,
            )

            # Drop the envelope from the top-level — callers expect a
            # round-trip-clean log dict.
            out.pop(_PROCESSING_KEY, None)

            # Stats: each field that actually decrypted (i.e. each entry
            # in decrypted_events) counts as one fields_decrypted.
            if self._stats is not None:
                self._stats.incr("fields_decrypted", len(decrypted_events))

            # Audit: emit per-field success events. Duration is whole-
            # batch divided across events (same rationale as encrypt).
            if self._audit is not None:
                duration_us = (time.perf_counter_ns() - t0) // 1000
                for ev in decrypted_events:
                    self._audit.record(
                        event_type="decrypt",
                        outcome="success",
                        record_id=record_id,
                        key_id=ev["key_id"],
                        field_path=ev["field_path"],
                        field_type=ev["field_type"],
                        byte_count=ev["byte_count"],
                        duration_us=duration_us,
                    )

            return out

        except Exception as exc:
            # Failure path: a single batch-level failure event. We don't
            # emit partial success events for fields that decrypted
            # before the failure — that would conflict with the
            # transactional "either the whole log decrypted or none of
            # it did" guarantee.
            if self._stats is not None:
                self._stats.incr("errors")
            if self._audit is not None:
                self._audit.record(
                    event_type="decrypt",
                    outcome="failure",
                    record_id=record_id,
                    failure_reason=str(exc),
                    duration_us=(time.perf_counter_ns() - t0) // 1000
                    if observe
                    else None,
                )
            raise

    # -- internal: recursive walkers -------------------------------------

    def _walk_for_decrypt(
        self,
        node: Any,
        *,
        parent_path: str,
        record_id: str,
        audit_sink: list[dict[str, Any]] | None = None,
    ) -> None:
        """Recursively decrypt every encrypted-field record in ``node``.

        Mutates the dict in place. Recognizes an encrypted record by
        the marker ``algorithm == "AES-256-GCM"`` on a dict-typed
        leaf — that's the same shape :meth:`encrypt` writes via
        :meth:`EncryptedField.model_dump`. Anything else is left alone.

        When ``audit_sink`` is provided, every successfully decrypted
        field appends a small descriptor (``key_id``, ``field_path``,
        ``field_type``, ``byte_count``) so the caller can build audit
        events without re-walking the tree. Fields that aren't
        encrypted-field records do NOT append to the sink — we only
        audit operations we actually performed.
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
                    if audit_sink is not None:
                        # Record only the metadata we already had on
                        # the input record — no plaintext, no ciphertext.
                        audit_sink.append(
                            {
                                "key_id": value.get("key_id"),
                                "field_path": path,
                                "field_type": value.get("field_type"),
                                "byte_count": len(plaintext.encode("utf-8")),
                            }
                        )
                else:
                    # Regular nested dict — recurse.
                    self._walk_for_decrypt(
                        value,
                        parent_path=path,
                        record_id=record_id,
                        audit_sink=audit_sink,
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
