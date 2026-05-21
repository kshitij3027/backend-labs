"""End-to-end redaction pipeline: detect → choose strategy → apply → metadata.

The :class:`RedactionProcessor` is the single entry point the HTTP layer (C7)
will call per request. It composes the three previously-built subsystems:

* :class:`~src.detection.detector.Detector` (C2) for finding PII spans.
* :class:`~src.redaction.strategies.StrategyRegistry` (C3) for transforming
  matched values according to the configured strategy.
* :class:`~src.config.manager.ConfigurationManager` (C4) for retrieving the
  active policy as a frozen snapshot.

Two collaborators are intentionally **optional**: ``audit_logger`` and
``stats``. C5 ships without those subsystems wired (C6 adds them) so they
default to ``None``; when ``None``, the corresponding side effects are
skipped entirely. This preserves the property that the C5 test suite can
exercise the full pipeline without C6 fixtures.

Right-to-left splicing
----------------------
``redact_entry`` walks each configured field's detections in **descending
start order** so that splicing the redacted value back into the source
text never invalidates the offsets of earlier (left-of) detections. The
metadata list is then reversed at the end so callers observe the natural
left-to-right ordering they expect.

Immutability of caller input
----------------------------
The caller's ``entry`` dict is **never mutated**. We work on a shallow
copy throughout — that's sufficient because the only values we touch are
the top-level string fields named in ``config.fields_to_redact``; nested
structures (lists, dicts) pass through unchanged.

No plaintext in metadata
------------------------
:class:`RedactionMetadata` carries only the pattern name, the strategy
applied, and the byte offsets where the match was found. We do NOT echo
the original value, the redacted value, the confidence, or the source
into the output — those fields would either leak plaintext (the whole
point of redaction is to suppress it) or duplicate information the
caller can derive from the redacted message itself.
"""
from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict

from src.config.manager import ConfigurationManager
from src.detection.detector import Detector
from src.detection.patterns import Detection
from src.redaction.strategies import StrategyRegistry
from src.settings import get_settings


# ---------------------------------------------------------------------------
# Public output models
# ---------------------------------------------------------------------------

class RedactionMetadata(BaseModel):
    """One per-redaction record: which pattern fired, which strategy ran, where.

    Frozen + ``extra="forbid"`` so callers cannot accidentally tack on a
    plaintext field downstream and re-introduce the leak we just removed.
    No confidence / source / value fields — those are leakage vectors that
    don't earn their keep in the output.

    Attributes
    ----------
    pattern : str
        The :attr:`Detection.pattern_name` of the hit (``"ssn"``,
        ``"credit_card"``, etc.).
    strategy : str
        The strategy name that was applied (``"mask"``, ``"partial"``,
        ``"hash"``, ``"tokenize"``).
    start : int
        Byte offset into the **original** message where the redaction
        started. Note: because redacted values may differ in length from
        the original (``hash`` / ``tokenize`` shrink/grow), these offsets
        do NOT necessarily correspond to positions in the redacted text.
        They describe the original input — which is what auditors care
        about ("at byte 14 we found an SSN").
    end : int
        Exclusive end offset, paired with ``start``.
    """

    pattern: str
    strategy: str
    start: int
    end: int

    # ``frozen=True`` + ``extra="forbid"``: keeps any downstream code from
    # mutating the record or sneaking plaintext in via an extra key.
    model_config = ConfigDict(extra="forbid", frozen=True)


class RedactedEntry(BaseModel):
    """The processor's per-entry output.

    Mirrors the shape of a typical log entry (``message`` / ``timestamp``
    / ``level``) plus a ``redactions`` list describing what fired. The
    model uses ``extra="allow"`` so callers passing arbitrary additional
    fields (request_id, trace_id, custom tags, etc.) get them back
    verbatim — the processor does not whitelist a fixed shape.

    Attributes
    ----------
    message : str
        Possibly-redacted log message. Defaults to empty if the caller
        didn't provide one (e.g., when redacting only ``user_data`` or
        ``details``).
    timestamp : str
        ISO-8601 timestamp, passed through unchanged from the input.
    level : str
        Log level (``"INFO"`` / ``"ERROR"`` / etc.), passed through.
    redactions : list[RedactionMetadata]
        One entry per applied redaction, in left-to-right order matching
        the original input text positions.
    """

    message: str
    timestamp: str
    level: str
    redactions: list[RedactionMetadata] = []

    # ``extra="allow"``: arbitrary extra fields the caller passed (e.g.,
    # ``trace_id``, ``service``, nested ``user_data``) round-trip through
    # the processor unchanged. This is the property the HTTP layer relies
    # on to preserve caller-provided context fields.
    model_config = ConfigDict(extra="allow")


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------

class RedactionProcessor:
    """Detect-then-redact pipeline; entry point for the HTTP layer.

    Construction is dependency-injection only: the processor never builds
    its own detector / registry / config — they're handed in by the
    bootstrap (C7 will wire them at FastAPI startup). That makes the
    processor trivial to unit-test by passing minimal stubs.

    Parameters
    ----------
    detector : Detector
        Already-constructed detection orchestrator.
    strategy_registry : StrategyRegistry
        Already-constructed name → :class:`~src.redaction.strategies.Strategy`
        map.
    config_manager : ConfigurationManager
        Holds the current :class:`~src.config.models.RedactionConfig`.
        We call :meth:`ConfigurationManager.get` once per
        :meth:`redact_entry` so a mid-batch hot-reload doesn't tear the
        policy seen by one entry.
    audit_logger : object | None
        Optional audit sink with a ``.record(...)`` method matching the
        C6 contract. ``None`` (default) disables per-redaction auditing.
        Even when supplied, audit events are only emitted if the active
        config's ``audit_all_redactions`` flag is true.
    stats : object | None
        Optional stats facade with ``counters.incr(name)``,
        ``throughput.record()``, and ``latency.record(latency_ms)``.
        ``None`` (default) disables all stats updates.
    batch_parallel_threshold : int | None
        Minimum batch size at which :meth:`redact_batch` switches from
        the serial path to a :class:`ThreadPoolExecutor`-backed fan-out.
        ``None`` (default) reads :attr:`Settings.BATCH_PARALLEL_THRESHOLD`
        so production wiring picks up the env-configured value without
        any extra plumbing. Tests pass explicit ints (often very low,
        e.g. 10) to exercise the parallel branch on small fixtures.
    thread_pool_size : int | None
        Worker count for the per-call :class:`ThreadPoolExecutor`.
        ``None`` (default) reads :attr:`Settings.THREAD_POOL_SIZE`. The
        pool is built and shut down inside each :meth:`redact_batch`
        invocation that takes the parallel branch — see that method's
        docstring for the rationale.
    """

    def __init__(
        self,
        detector: Detector,
        strategy_registry: StrategyRegistry,
        config_manager: ConfigurationManager,
        audit_logger: Optional[Any] = None,
        stats: Optional[Any] = None,
        batch_parallel_threshold: int | None = None,
        thread_pool_size: int | None = None,
    ) -> None:
        # All five collaborators stored as private attributes; the hot
        # path reads them per call. We keep them named after their roles
        # rather than ``_d``/``_s``/``_c`` so a future maintainer doesn't
        # have to consult __init__ to read redact_entry().
        self._detector = detector
        self._strategy_registry = strategy_registry
        self._config_manager = config_manager
        self._audit_logger = audit_logger
        self._stats = stats

        # Resolve parallel-batch knobs from Settings when the caller
        # didn't override. We read once at construction so a settings
        # mutation mid-lifetime doesn't affect an existing processor —
        # consistent with how the rest of the engine treats config (a
        # snapshot is captured per redact_entry call, but the pool
        # tuning is process-lifetime).
        if batch_parallel_threshold is None or thread_pool_size is None:
            _settings = get_settings()
            if batch_parallel_threshold is None:
                batch_parallel_threshold = _settings.BATCH_PARALLEL_THRESHOLD
            if thread_pool_size is None:
                thread_pool_size = _settings.THREAD_POOL_SIZE
        self._batch_parallel_threshold = batch_parallel_threshold
        self._thread_pool_size = thread_pool_size

    # -- public API ------------------------------------------------------

    def redact_entry(self, entry: dict) -> RedactedEntry:
        """Detect + redact every configured field in ``entry``.

        Algorithm:

        1. Snapshot the config (atomic via ``manager.get()``).
        2. Shallow-copy ``entry`` so we never mutate the caller's dict.
        3. For each ``field_name`` in ``config.fields_to_redact`` that
           is present in the entry AND carries a ``str`` value:
           a. Run the detector against the field's text.
           b. Sort the detections by ``start`` **descending** so we can
              splice right-to-left without invalidating earlier offsets.
           c. For each detection, look up the configured rule; skip if
              no rule exists for the pattern, or the detection's
              confidence is below the rule's ``confidence_min``.
           d. Apply the strategy and splice the redacted value back into
              the field text. Append a :class:`RedactionMetadata` entry.
           e. Optionally fire audit + stats side effects.
        4. Reverse the metadata list so it's in left-to-right order.
        5. Construct the :class:`RedactedEntry`, defaulting any missing
           ``message`` / ``timestamp`` / ``level`` to empty strings, and
           passing through any extra keys via ``extra="allow"``.

        Latency note
        ------------
        ``time.monotonic_ns()`` is captured BEFORE config retrieval and
        recorded AFTER the splice/audit loop. The recorded value is
        therefore "wall time including the policy snapshot" — which is
        what an operator graphing per-request latency cares about.
        """
        t0 = time.monotonic_ns()

        # 1) Snapshot the policy. Holding our own reference for the rest
        # of the call means a mid-flight reload can't tear our view.
        config = self._config_manager.get()

        # 2) Shallow copy. The processor only touches top-level string
        # fields named in fields_to_redact; nested values are passed
        # through unchanged. ``dict(entry)`` is sufficient.
        out_entry: dict[str, Any] = dict(entry)

        # Accumulator for metadata across all fields in this entry.
        redactions: list[RedactionMetadata] = []

        # 3) Walk each configured field. We honor the on-disk ordering of
        # ``fields_to_redact`` so the metadata list is deterministic.
        for field_name in config.fields_to_redact:
            # Skip missing or non-string fields silently — the field may
            # exist but carry a list / dict (in which case the detection
            # layer wouldn't know what to do with it).
            text = entry.get(field_name)
            if not isinstance(text, str):
                continue

            detections = self._detector.detect(text)
            if not detections:
                continue

            # Right-to-left ordering keeps every earlier detection's
            # (start, end) offsets valid as we splice. If we walked
            # left-to-right a length-changing strategy (hash/tokenize)
            # would invalidate every subsequent offset and require
            # re-counting after each splice.
            detections_sorted = sorted(detections, key=lambda d: d.start, reverse=True)

            for detection in detections_sorted:
                # Look up the rule by pattern. Missing rule => the
                # operator hasn't configured a strategy for this
                # pattern; we leave the match in the text and don't
                # record metadata for it. This is the documented
                # "configurable redaction" behavior — operators turn
                # patterns on by adding rules, off by omitting them.
                rule = config.rules.get(detection.pattern_name)
                if rule is None:
                    continue

                # Confidence gate. Regex hits arrive at 1.0 so they pass
                # any threshold <= 1.0; NER hits at 0.85 are skipped if
                # the operator raised the bar (e.g. confidence_min=0.95).
                if detection.confidence < rule.confidence_min:
                    continue

                # Apply the strategy. The registry is the canonical
                # source for strategy instances — calling ``get`` here
                # rather than caching means a future "swap strategy at
                # runtime" feature works without touching this code.
                strategy = self._strategy_registry.get(rule.strategy)
                redacted_value = strategy.redact(detection.value, detection)

                # Splice into the field text. Because we sorted
                # right-to-left, ``detection.start`` and ``detection.end``
                # still index the correct positions even after earlier
                # (right-side) splices.
                text = text[: detection.start] + redacted_value + text[detection.end :]

                # Append metadata BEFORE we reverse; we'll flip the list
                # to left-to-right after all fields are processed.
                redactions.append(
                    RedactionMetadata(
                        pattern=detection.pattern_name,
                        strategy=rule.strategy,
                        start=detection.start,
                        end=detection.end,
                    )
                )

                # ---- Audit side effect (C6) ------------------------
                # We fire per-redaction only when:
                #   * an audit_logger was injected, AND
                #   * the active config has ``audit_all_redactions`` set.
                # Both gates exist so an operator can disable per-record
                # auditing at runtime without restarting (toggle via
                # config reload).
                if self._audit_logger is not None and config.audit_all_redactions:
                    self._audit_logger.record(
                        event_type="redaction",
                        outcome="success",
                        pattern_name=detection.pattern_name,
                        strategy=rule.strategy,
                        # ``list(...)`` because rule.compliance_tags is a
                        # pydantic list; passing it directly to most
                        # serializers would still work but copying here
                        # ensures the audit sink can't mutate the rule.
                        compliance_tags=list(rule.compliance_tags),
                    )

                # ---- Stats side effect (C6) -----------------------
                # Per-pattern counter. ``self._stats.counters.incr`` is
                # the canonical entry point; we don't gate on the
                # config flag because counters are aggregated metrics,
                # not per-record audit.
                if self._stats is not None:
                    self._stats.counters.incr(detection.pattern_name)

            # Write the (possibly modified) text back. We do this even
            # when ``detections`` was non-empty but no rule fired —
            # ``text`` would be unchanged in that case, so the write is
            # idempotent.
            out_entry[field_name] = text

        # 4) Reverse so the metadata appears left-to-right in the output.
        # We accumulated right-to-left because that's the splice order.
        redactions.reverse()

        # 5) Latency + throughput. Computed in ms (float) — operators
        # typically graph p50/p95 in milliseconds and this saves the
        # consumer a division.
        latency_ms = (time.monotonic_ns() - t0) / 1_000_000.0
        if self._stats is not None:
            self._stats.throughput.record()
            self._stats.latency.record(latency_ms)

        # 6) Build the result. ``setdefault`` ensures the three "shaped"
        # fields always exist with at least an empty-string value; any
        # extra keys the caller passed flow through via ``extra="allow"``.
        payload = dict(out_entry)
        payload.setdefault("message", "")
        payload.setdefault("timestamp", "")
        payload.setdefault("level", "")
        # Overwrite any caller-passed ``redactions`` — that field is
        # owned by the processor's output, not the input.
        payload["redactions"] = redactions

        return RedactedEntry(**payload)

    def redact_batch(self, entries: list[dict]) -> list[RedactedEntry]:
        """Batch redaction with a threshold-gated parallel path.

        Dispatch:

        * **Serial** — when ``len(entries) < self._batch_parallel_threshold``.
          A trivial list comprehension over :meth:`redact_entry`, identical
          in semantics to a loop of single-entry calls. Cheaper than the
          parallel path for small batches because no thread-pool setup
          cost is paid.
        * **Parallel** — when ``len(entries) >= self._batch_parallel_threshold``.
          We fan out across a fresh :class:`ThreadPoolExecutor` sized at
          ``self._thread_pool_size``. ``ex.map`` preserves input order,
          so the contract ("one output per input, same order") matches
          the serial branch exactly.

        Pool lifecycle
        --------------
        The :class:`ThreadPoolExecutor` is built and shut down per call
        (via the ``with`` context manager). That's acceptable for v1:
        the per-entry work — regex scan, optional NER, strategy splice,
        audit/stats fan-out — dominates the dispatch cost. A future
        optimization could hoist the pool to ``self._pool`` to amortize
        the thread-creation overhead across calls; the public contract
        of :meth:`redact_batch` would not change.

        Thread safety
        -------------
        :meth:`redact_entry` only reads from this processor instance and
        from the per-call config snapshot it pulls from
        :class:`ConfigurationManager`. It does NOT mutate any shared
        state besides the optional ``audit_logger`` / ``stats`` sinks,
        which are expected to be internally thread-safe (the C6
        :class:`~src.audit.audit_logger.AuditLogger` ring buffer and the
        :class:`~src.stats.stats.Stats` counters both serialize their
        writes). That makes the parallel path safe without any extra
        locking here.
        """
        # No special-case for empty input: the serial path's
        # comprehension already returns ``[]`` for an empty list, and
        # the parallel branch's threshold check naturally falls through
        # to it (0 < threshold for any sensible threshold).
        if len(entries) < self._batch_parallel_threshold:
            return [self.redact_entry(e) for e in entries]

        # Parallel branch: ``ThreadPoolExecutor.map`` preserves input
        # order, so the returned list aligns 1:1 with ``entries``.
        # ``with`` ensures the pool is shut down even if a worker
        # raises — the exception propagates up to the caller naturally.
        with ThreadPoolExecutor(max_workers=self._thread_pool_size) as ex:
            results = list(ex.map(self.redact_entry, entries))
        return results

    def detect_entry(self, entry: dict) -> list[Detection]:
        """Detect PII in every configured field of ``entry`` WITHOUT redacting.

        Used by the future ``POST /v1/detect`` endpoint (C7) so callers
        can preview what would be redacted before committing to a redact
        call. No splicing, no metadata, no stats / counters — just the
        raw deduplicated detections.

        Audit policy
        ------------
        We optionally emit ONE ``detect`` audit event per call (not per
        detection) so the trail records who probed the engine without
        flooding the audit sink with per-hit records. The latter would
        be appropriate for ``redact`` (where every hit is also a state
        change) but not here.
        """
        config = self._config_manager.get()

        # Accumulator across all configured fields; same iteration order
        # as the redaction path so callers can correlate by field name.
        all_detections: list[Detection] = []
        for field_name in config.fields_to_redact:
            text = entry.get(field_name)
            if not isinstance(text, str):
                continue
            # extend (not append) because detect() returns a list per call.
            all_detections.extend(self._detector.detect(text))

        # Single audit event per call — see method docstring for the
        # "low-volume on the detect path" rationale.
        if self._audit_logger is not None:
            self._audit_logger.record(
                event_type="detect",
                outcome="success",
            )

        return all_detections
