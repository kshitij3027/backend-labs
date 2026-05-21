"""HTTP route handlers for the log redaction engine (C7).

Routes
------
* ``POST /api/redact``  — batch redaction; returns processed entries.
* ``POST /v1/detect``   — dry-run detection; NO redaction applied, never
  echoes plaintext. Used by ops to preview what would be redacted before
  flipping the switch.
* ``GET  /api/stats``   — operational snapshot (throughput, latency,
  pattern hit counts).
* ``GET  /api/config``  — current redaction configuration (the dict the
  manager is serving as the active policy).
* ``POST /api/config``  — atomic config hot-reload. Validation failure
  returns 422 with the structured error list; the old config remains
  active until a valid one is posted.

Composition via ``app.state``
-----------------------------
Every collaborator (processor, config_manager, audit_logger, ring_buffer,
stats) is constructed once at startup in :func:`src.main.lifespan` and
stashed onto ``app.state``. The route functions read them off the
incoming ``Request`` so the routes themselves are stateless and trivial
to unit-test by injecting a stub app.

Plaintext leakage policy
------------------------
The ``/v1/detect`` route NEVER returns the raw matched substring. The
helper :func:`_value_preview` produces a masked preview of the value
(first 2 chars + ``***`` + last 2 chars for length ≥ 5; full-asterisk
mask otherwise) which is what flows over the wire. Even an authorized
operator probing the engine via this endpoint sees only the masked form.
"""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Path, Request, status
from pydantic import ValidationError

from src.compliance.reports import ComplianceReport, ComplianceRuleSet, generate_report
from src.config.models import RedactionConfig

from .metrics import DETECTIONS_TOTAL, REDACTIONS_TOTAL
from .models import (
    DetectionItem,
    DetectRequest,
    DetectResponse,
    RedactRequest,
    RedactResponse,
    StatsResponse,
)

logger = logging.getLogger(__name__)

# Single module-level router; mounted at :func:`src.main.app.include_router`.
# Routes attach via decorators so the wiring stays declarative and the test
# layer can ``app.include_router`` into a stripped-down app without
# import-order issues.
router = APIRouter()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _value_preview(value: str) -> str:
    """Return a masked preview of ``value`` suitable for the wire.

    Rule
    ----
    * ``len(value) >= 5`` → ``value[:2] + "***" + value[-2:]``
      (e.g. ``"123-45-6789"`` → ``"12***89"``).
    * Otherwise            → ``"*" * len(value)`` (full mask for short
      values where the prefix + suffix would themselves reveal too much).

    The 5-char floor is intentional: a 4-char preview like ``"a***b"``
    would expose 40% of a 5-char string, which is more leakage than the
    "always show 2+2" rule allows. Anything below the floor gets fully
    masked.

    Used by the ``/v1/detect`` route to project each detection without
    echoing the raw plaintext. The rule lives here (not on
    :class:`DetectionItem`) because the model is wire-only — it should
    never see the raw value at all, and the mask must be applied before
    the value crosses into the model.
    """
    if len(value) >= 5:
        # Prefix + middle marker + suffix. Keep the marker as a literal
        # ``***`` (3 stars) so the visual shape is uniform across
        # different value lengths.
        return value[:2] + "***" + value[-2:]
    # Below the threshold — fully mask. ``len(value)`` keeps the visual
    # length stable so the consumer can see "something was there" without
    # learning anything about the content.
    return "*" * len(value)


# ---------------------------------------------------------------------------
# /api/redact — batch redaction
# ---------------------------------------------------------------------------


@router.post("/api/redact", response_model=RedactResponse, tags=["redact"])
async def redact_endpoint(req: RedactRequest, request: Request) -> RedactResponse:
    """Redact a batch of log entries through the :class:`RedactionProcessor`.

    Returns one processed entry per input entry, preserving caller-
    supplied extra fields via the processor's ``extra="allow"`` policy.

    Side effects
    ------------
    * Prometheus counter ``redactions_total{pattern,strategy}`` is bumped
      per applied redaction (one increment per :class:`RedactionMetadata`
      in the result list).
    * The processor itself emits per-redaction audit events and stats
      updates when its ``audit_logger`` + ``stats`` collaborators are
      wired (which they are in the production lifespan).
    """
    # The processor singleton is built once at startup in src.main.lifespan
    # and stashed onto app.state. Reaching in here (vs taking it as a
    # FastAPI Depends parameter) keeps the route signature minimal and
    # matches the reference field-encryption-service shape.
    processor = request.app.state.processor

    # ``e.model_dump()`` preserves any caller-supplied extra fields
    # because LogEntry.model_config = ConfigDict(extra="allow"). The
    # processor consumes a plain dict, not a pydantic model.
    results = processor.redact_batch([e.model_dump() for e in req.log_entries])

    # Prometheus accounting. We bump per redaction (not per entry) so a
    # batch with two SSN hits in one message correctly counts as two
    # rather than one. The (pattern, strategy) label tuple matches the
    # counter's declared label set in src.api.metrics.
    for r in results:
        for meta in r.redactions:
            REDACTIONS_TOTAL.labels(
                pattern=meta.pattern,
                strategy=meta.strategy,
            ).inc()

    # ``r.model_dump()`` projects the RedactedEntry back to a plain dict
    # so the response is JSON-serializable straight through FastAPI's
    # pydantic encoder.
    return RedactResponse(processed_entries=[r.model_dump() for r in results])


# ---------------------------------------------------------------------------
# /v1/detect — dry-run detection (no redaction)
# ---------------------------------------------------------------------------


@router.post("/v1/detect", response_model=DetectResponse, tags=["detect"])
async def detect_endpoint(
    req: DetectRequest, request: Request
) -> DetectResponse:
    """Return every detection without applying any redaction.

    Use case: operators / customer support need to confirm what the
    engine WOULD touch before flipping on a new preset. The endpoint
    never echoes the matched plaintext — :func:`_value_preview` masks
    every value before it crosses into the response model.

    Audit policy
    ------------
    Emits EXACTLY ONE audit event per request (``event_type="detect"``,
    ``outcome="success"``) — not per detection. The detect path is
    expected to be high-traffic (it's the cheap dry-run); per-hit
    audits would balloon the audit channel without adding signal.
    """
    # Resolve collaborators directly off app.state. We use ``detector`` +
    # ``config_manager`` instead of ``processor.detect_entry()`` because the
    # processor's detect_entry would emit one audit event PER entry — the
    # spec requires exactly one audit event per request, not per entry.
    detector = request.app.state.detector
    config_manager = request.app.state.config_manager
    audit_logger = getattr(request.app.state, "audit_logger", None)

    # Fast path: an empty batch short-circuits before we touch the
    # processor or the audit channel. Matches the redact endpoint's
    # "empty input → empty output" behavior so callers see consistent
    # semantics across the two endpoints.
    if not req.log_entries:
        return DetectResponse(detections=[])

    # Snapshot the active config ONCE per request so a mid-flight
    # reload can't tear the policy seen across entries. RedactionConfig
    # is frozen so the reference stays valid for the rest of the call.
    config = config_manager.get()

    items: list[DetectionItem] = []
    for idx, entry in enumerate(req.log_entries):
        entry_dict = entry.model_dump()
        # Walk every configured field; gather all detections per entry.
        # Mirrors the loop in processor.detect_entry() but without the
        # processor's per-call audit event so we can emit exactly one at
        # the route level (matches the docstring's audit policy).
        for field_name in config.fields_to_redact:
            text = entry_dict.get(field_name)
            if not isinstance(text, str):
                continue
            for d in detector.detect(text):
                items.append(
                    DetectionItem(
                        entry_index=idx,
                        pattern=d.pattern_name,
                        # ``_value_preview`` is the load-bearing masker; never
                        # pass d.value directly into a model that will be
                        # serialized to a response.
                        value_preview=_value_preview(d.value),
                        start=d.start,
                        end=d.end,
                        confidence=d.confidence,
                    )
                )

    # Prometheus accounting. ``detections_total{pattern}`` only carries
    # the pattern label because no strategy is applied on this path.
    for item in items:
        DETECTIONS_TOTAL.labels(pattern=item.pattern).inc()

    # Exactly one audit event per request. Recording the OUTCOME (not
    # the detection volume) keeps the audit channel low-volume on what
    # is expected to be a high-traffic dry-run endpoint.
    if audit_logger is not None:
        audit_logger.record(event_type="detect", outcome="success")

    return DetectResponse(detections=items)


# ---------------------------------------------------------------------------
# /api/stats — operational snapshot
# ---------------------------------------------------------------------------


@router.get("/api/stats", response_model=StatsResponse, tags=["stats"])
async def get_stats(request: Request) -> StatsResponse:
    """Return the live throughput / latency / pattern-hit snapshot.

    All four sub-components (throughput, latency, counters) are
    independently thread-safe; the snapshot is consistent in the sense
    that each sub-component returns its own atomic view, but across
    sub-components there's no global lock — under heavy concurrent
    traffic the numbers may reflect slightly-different instants. That
    skew is in the microseconds and irrelevant for the dashboard's 1s
    polling interval.
    """
    stats = request.app.state.stats

    # Per-pattern counter snapshot. The dict is a copy (PatternCounters
    # materializes a fresh dict inside its lock) so callers can serialize
    # it without worrying about concurrent mutation.
    pattern_hits = stats.counters.snapshot()

    # Latency histogram snapshot — gives us mean + p50/p95/p99. We
    # surface mean + p95 here (the operationally interesting pair); the
    # dashboard pulls p50 + p99 from the same dict in a later commit.
    lat = stats.latency.snapshot()

    return StatsResponse(
        # ``total_count`` is the sum across every retained bucket — it
        # reflects total ops since startup (modulo bucket eviction over
        # the configured window). The "logs_processed" naming matches
        # the public API's spec wording.
        logs_processed=stats.throughput.total_count(),
        ops_per_second=stats.throughput.ops_per_second(),
        avg_latency_ms=lat["mean_ms"],
        p95_latency_ms=lat["p95_ms"],
        pattern_hits=pattern_hits,
    )


# ---------------------------------------------------------------------------
# /api/config — get / post
# ---------------------------------------------------------------------------


@router.get("/api/config", tags=["config"])
async def get_config(request: Request) -> dict:
    """Return the currently-active redaction configuration as a dict.

    ``RedactionConfig.model_dump()`` produces a plain dict that round-
    trips through FastAPI's JSON encoder. Callers that want a full
    re-import can POST the same dict back to ``/api/config`` — the
    JSON representation is symmetric.
    """
    # Hit the manager (not a cached copy) so a recent reload is
    # reflected immediately. ``.get()`` is fast (a single lock-guarded
    # attribute read) so there is no need to memoize at the route level.
    return request.app.state.config_manager.get().model_dump()


@router.post("/api/config", tags=["config"])
async def post_config(request: Request) -> dict:
    """Atomically hot-reload the redaction configuration from a JSON body.

    Body shape
    ----------
    The full :class:`~src.config.models.RedactionConfig` document as JSON
    — the same shape ``/api/config`` returns.

    Error mapping
    -------------
    * Validation failure (unknown pattern_name, extra field, missing
      ``rules``, etc.) → ``422 Unprocessable Entity`` with the structured
      pydantic error list as ``detail``. The OLD config remains active.
    * JSON syntax error → ``422`` with a single-element error list.
    * Anything else (filesystem failure on backup, etc.) propagates as 500.

    Side effects
    ------------
    On success: emits one ``config_reload`` audit event when an audit
    logger is wired on app.state.
    """
    body = await request.body()

    try:
        # ``reload_from_json`` does validate-then-swap atomically:
        # validation runs OUTSIDE the lock, and only on success does the
        # rebind happen under the lock. If validation fails, ``_config``
        # is never touched and the old policy stays active — this is the
        # rollback guarantee tested in test_api_config.
        new_config: RedactionConfig = request.app.state.config_manager.reload_from_json(
            body.decode("utf-8")
        )
    except ValidationError as exc:
        # 422 with the structured error list. Pydantic's ``.errors()``
        # returns a list of dicts suitable for FastAPI's ``detail`` slot.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=exc.errors(),
        ) from exc
    except json.JSONDecodeError as exc:
        # Malformed JSON body (not a pydantic validation issue). Surface
        # as 422 so the client uniformly handles "bad body" the same way.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{"type": "json_invalid", "msg": str(exc)}],
        ) from exc
    except UnicodeDecodeError as exc:
        # Body bytes that aren't valid UTF-8. Same 422 treatment — the
        # client sent a structurally-broken request.
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=[{"type": "encoding_invalid", "msg": str(exc)}],
        ) from exc

    # Success: emit one audit event so the trail records WHO changed the
    # config (the actor field defaults to "system" in the audit event;
    # the request-side identity is wired up in a future RBAC commit).
    audit_logger = getattr(request.app.state, "audit_logger", None)
    if audit_logger is not None:
        audit_logger.record(event_type="config_reload", outcome="success")

    # Echo the new config back — the same shape GET /api/config would
    # return. Clients can use this to confirm the swap took effect
    # without a follow-up GET.
    return new_config.model_dump()


# ---------------------------------------------------------------------------
# /api/compliance/{rule_set} — per-regime redaction report (C8)
# ---------------------------------------------------------------------------


@router.get(
    "/api/compliance/{rule_set}",
    response_model=ComplianceReport,
    tags=["compliance"],
)
async def compliance_endpoint(
    rule_set: ComplianceRuleSet = Path(
        ..., description="Compliance regime: GDPR, HIPAA, or PCI_DSS"
    ),
    request: Request = None,
) -> ComplianceReport:
    """Return a redaction summary for the given compliance regime.

    The path parameter ``rule_set`` is constrained to the closed
    :data:`~src.compliance.reports.ComplianceRuleSet` Literal so
    FastAPI surfaces a 422 for any value outside ``{"GDPR", "HIPAA",
    "PCI_DSS"}`` without entering this handler.

    The aggregation walks the audit ring buffer (singleton built at
    startup, available on ``app.state.ring_buffer``) and returns the
    immutable :class:`ComplianceReport` directly — FastAPI's pydantic
    encoder handles the JSON projection.

    Performance
    -----------
    Designed to handle 100 k events in under 30 s (single O(n) pass
    over the filtered list). The reported ``report_generation_time_ms``
    on the response lets ops verify the budget is being met under
    real traffic.
    """
    # Pull the singleton off app.state — built once at startup in
    # src.main.lifespan and shared across every request. Matches the
    # access pattern used by /api/stats and /api/redact for consistency.
    ring_buffer = request.app.state.ring_buffer
    report = generate_report(ring_buffer=ring_buffer, rule_set=rule_set)
    return report
