"""Metric-based detector: statistical correlation across per-second series.

Where the other detectors link individual events, this one links METRICS: it
reads the aggregator's per-second rings (never raw events for statistics)
over the last :data:`WINDOW_N` completed seconds and hunts relationships with
the method matched to the data shape (per the plan's research):

- **Pearson/Spearman** over the curated :data:`PARAMETRIC_PAIRS` — whichever
  of linear/monotone fits better (smaller p) is kept per pair;
- **time-lagged cross-correlation** on the three target incident pairs
  (:data:`TLCC_PAIRS`) for lead-lag propagation ("the db pool saturates, web
  errors follow seconds later"), with the winning lag reported in details;
- **Jaccard overlap** of per-source error-presence binaries
  (:data:`JACCARD_PAIRS`) — do these two services error in the same seconds?
- **normalized mutual information** on the targets (:data:`MI_PAIRS`) for
  nonlinear dependence Pearson is blind to.

False-positive control: every Pearson/Spearman/TLCC p-value collected in a
cycle goes through ONE Benjamini-Hochberg pass at ``settings.fdr_q`` —
re-scanning many pairs every 2 s is a textbook multiple-testing setup, and BH
bounds the expected false-discovery fraction among emissions (see
:mod:`src.engine.significance`; the two methods on a shared target pair are
positively dependent, where BH remains valid). Survivors must also clear
|r| >= :data:`MIN_STRENGTH_PARAMETRIC`. Jaccard/MI carry no p-value and use
their own strength floors plus sample-support gates instead.

Scoring: strength = |r| (or J, or NMI), clamped [0, 1]. Confidence for the
BH-tested methods = ``(1 - p_adj) * min(1, n / 30)`` — significance discounted
by sample support; Jaccard = ``min(1, union / 15)``; MI = ``0.5 + 0.5 *
min(1, (n - 10) / 50)``.

Metric findings are series-level, so event refs are representative rather
than causal: each series maps to its owning source and anchors to that
source's LATEST window event (any level), or to a synthetic
``metric:{series}`` ref stamped at the cycle clock when the window holds none
— refs always exist and never carry a future timestamp. Everything is guarded:
an unknown series name or a short/NaN/empty ring skips that pair quietly, so
the first minute after a cold start simply emits nothing.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np

from src.config import Settings
from src.engine.base import (
    DedupeCache,
    DetectionContext,
    clamp01,
    new_correlation_id,
    pair_key,
)
from src.engine.significance import (
    benjamini_hochberg,
    bh_adjusted,
    jaccard,
    lagged_xcorr,
    mutual_information,
    pearson_or_spearman,
)
from src.models import Correlation, CorrelationType, EventRef, LogEvent, SourceType
from src.parsers import SERVICE_BY_SOURCE

logger = logging.getLogger(__name__)

#: Hard per-cycle emission bound (mirrors the cascade/user detectors' caps).
MAX_EMISSIONS_PER_TICK = 20

#: Samples read per series: the last 60 completed seconds (well within the
#: aggregator's 119-completed-second capacity).
WINDOW_N = 60

#: Emission floors per method family — strengths below these are noise-level.
MIN_STRENGTH_PARAMETRIC = 0.4
MIN_STRENGTH_MI = 0.35
MIN_STRENGTH_JACCARD = 0.3

#: Jaccard needs at least this many union seconds before overlap means much.
_MIN_JACCARD_UNION = 5

#: Confidence saturation points: n/30 samples for BH-tested methods, union/15
#: seconds for Jaccard, (n-10)/50 extra samples for MI.
_N_SATURATION = 30.0
_UNION_SATURATION = 15.0
_MI_BASE_N = 10.0
_MI_EXTRA_SATURATION = 50.0

#: The three incident relationships the generator's scenarios manufacture
#: (DB_POOL_SATURATION / PAYMENT_SLOWDOWN / INVENTORY_TIMEOUTS) — always
#: tested first, and by every applicable method.
TARGET_PAIRS: tuple[tuple[str, str], ...] = (
    ("web.error_rate", "db.pool_utilization"),
    ("payment.latency_ms_avg", "user.abandonment_count"),
    ("inventory.timeout_count", "checkout.failure_count"),
)

#: Curated Pearson/Spearman pairs: the 3 targets first, then the plausible
#: cross-tier relationships worth watching — load fan-out down the stack,
#: latency coupling, and error co-movement into checkout failures.
PARAMETRIC_PAIRS: tuple[tuple[str, str], ...] = TARGET_PAIRS + (
    ("web.request_count", "api.request_count"),
    ("web.request_count", "db.query_count"),
    ("api.request_count", "payment.txn_count"),
    ("web.latency_ms_avg", "api.latency_ms_avg"),
    ("api.latency_ms_avg", "payment.latency_ms_avg"),
    ("db.pool_utilization", "api.latency_ms_avg"),
    ("web.error_5xx_count", "db.error_count"),
    ("api.error_count", "inventory.timeout_count"),
    ("payment.error_count", "user.abandonment_count"),
    ("web.error_rate", "checkout.failure_count"),
    ("payment.latency_ms_avg", "checkout.failure_count"),
    ("db.error_count", "checkout.failure_count"),
    ("inventory.latency_ms_avg", "checkout.failure_count"),
)

#: Lead-lag (TLCC) and mutual-information passes run on the targets only.
TLCC_PAIRS: tuple[tuple[str, str], ...] = TARGET_PAIRS
MI_PAIRS: tuple[tuple[str, str], ...] = TARGET_PAIRS

#: Error-presence Jaccard pairs as (source_a, source_b) SourceType values —
#: upstream infrastructure against the tier it fails into.
JACCARD_PAIRS: tuple[tuple[str, str], ...] = (
    ("database", "web"),
    ("inventory", "api_service"),
    ("payment", "web"),
)

#: Series-name prefix -> owning source. Checkout failures surface in the API
#: tier's log stream and cart abandonment in the web tier's, hence their maps.
_PREFIX_SOURCE: dict[str, SourceType] = {
    "web": SourceType.WEB,
    "db": SourceType.DATABASE,
    "api": SourceType.API_SERVICE,
    "payment": SourceType.PAYMENT,
    "inventory": SourceType.INVENTORY,
    "checkout": SourceType.API_SERVICE,
    "user": SourceType.WEB,
}


@dataclass
class _Candidate:
    """One BH-pool entry: a tested series pair plus its statistic and p-value."""

    metric_a: str
    metric_b: str
    method: str
    r: float
    p: float
    n: int
    lag: int | None = None


class MetricDetector:
    """Series-pair correlation with one BH-FDR pass per cycle (module docstring)."""

    name = "metric_based"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._dedupe = DedupeCache()

    def detect(self, ctx: DetectionContext) -> list[Correlation]:
        """Run the parametric/TLCC, Jaccard, and MI phases over the aggregator."""
        if ctx.aggregator is None:
            return []
        latest = _latest_by_source(ctx.window_events)
        found: list[Correlation] = []
        self._parametric_phase(ctx, latest, found)
        self._jaccard_phase(ctx, latest, found)
        self._mi_phase(ctx, latest, found)
        return found

    # --- Phase 1: Pearson/Spearman + TLCC candidates through one BH pass ----------
    def _parametric_phase(
        self,
        ctx: DetectionContext,
        latest: dict[SourceType, LogEvent],
        found: list[Correlation],
    ) -> None:
        min_n = int(self.settings.min_samples)
        candidates: list[_Candidate] = []
        for metric_a, metric_b in PARAMETRIC_PAIRS:
            try:
                series = ctx.aggregator.aligned((metric_a, metric_b), WINDOW_N)
                result = pearson_or_spearman(
                    series[metric_a], series[metric_b], min_n=min_n
                )
            except Exception:  # noqa: BLE001 — a bad series must not kill the cycle
                logger.debug(
                    "parametric pair (%s, %s) skipped", metric_a, metric_b, exc_info=True
                )
                continue
            if result is None:
                continue  # too few finite pairs / zero variance — nothing testable
            method, r, p, n = result
            candidates.append(_Candidate(metric_a, metric_b, method, r, p, n))
        for metric_a, metric_b in TLCC_PAIRS:
            try:
                series = ctx.aggregator.aligned((metric_a, metric_b), WINDOW_N)
                a_vals, b_vals = series[metric_a], series[metric_b]
                lag, r, p = lagged_xcorr(a_vals, b_vals)
                n = _finite_pairs_at_lag(a_vals, b_vals, lag)
            except Exception:  # noqa: BLE001 — a bad series must not kill the cycle
                logger.debug(
                    "TLCC pair (%s, %s) skipped", metric_a, metric_b, exc_info=True
                )
                continue
            if not np.isfinite(r) or n < min_n:
                continue  # no lag was computable at all
            candidates.append(
                _Candidate(metric_a, metric_b, "lagged_xcorr", r, p, n, lag=lag)
            )
        if not candidates:
            return

        # ONE Benjamini-Hochberg pass over ALL p-values collected this cycle —
        # the multiple-testing false-positive filter (see module docstring).
        pvals = np.array([cand.p for cand in candidates])
        keep = benjamini_hochberg(pvals, q=float(self.settings.fdr_q))
        adjusted = bh_adjusted(pvals)
        ttl = float(self.settings.dedup_ttl_seconds)
        for i, cand in enumerate(candidates):
            if len(found) >= MAX_EMISSIONS_PER_TICK:
                return
            if not keep[i] or abs(cand.r) < MIN_STRENGTH_PARAMETRIC:
                continue  # not significant after FDR, or too weak to matter
            key = pair_key(self.name, cand.metric_a, cand.metric_b, cand.method)
            if not self._dedupe.seen(key, ctx.now, ttl):
                continue  # this pair+method already emitted within the TTL
            p_adj = float(adjusted[i])
            details = {
                "method": cand.method,
                "metric_a": cand.metric_a,
                "metric_b": cand.metric_b,
                "r": round(float(cand.r), 4),
                "p": round(float(cand.p), 6),
                "p_adj": round(p_adj, 6),
                "n": int(cand.n),
                "window_seconds": WINDOW_N,
            }
            if cand.lag is not None:
                details["lag_seconds"] = int(cand.lag)
            found.append(
                self._emit(
                    ctx,
                    latest,
                    cand.metric_a,
                    cand.metric_b,
                    strength=clamp01(abs(cand.r)),
                    confidence=clamp01(
                        (1.0 - p_adj) * min(1.0, cand.n / _N_SATURATION)
                    ),
                    details=details,
                )
            )

    # --- Phase 2: error-presence Jaccard overlap (no p-value) ---------------------
    def _jaccard_phase(
        self,
        ctx: DetectionContext,
        latest: dict[SourceType, LogEvent],
        found: list[Correlation],
    ) -> None:
        ttl = float(self.settings.dedup_ttl_seconds)
        for source_a, source_b in JACCARD_PAIRS:
            if len(found) >= MAX_EMISSIONS_PER_TICK:
                return
            try:
                overlap, union_n = jaccard(
                    ctx.aggregator.error_presence(source_a, WINDOW_N),
                    ctx.aggregator.error_presence(source_b, WINDOW_N),
                )
            except Exception:  # noqa: BLE001 — a bad source must not kill the cycle
                logger.debug(
                    "jaccard pair (%s, %s) skipped", source_a, source_b, exc_info=True
                )
                continue
            if overlap < MIN_STRENGTH_JACCARD or union_n < _MIN_JACCARD_UNION:
                continue  # weak overlap, or too few error seconds to trust it
            metric_a = f"error_presence.{source_a}"
            metric_b = f"error_presence.{source_b}"
            if not self._dedupe.seen(
                pair_key(self.name, metric_a, metric_b, "jaccard"), ctx.now, ttl
            ):
                continue  # this pair already emitted within the TTL
            found.append(
                self._emit(
                    ctx,
                    latest,
                    metric_a,
                    metric_b,
                    strength=clamp01(overlap),
                    confidence=clamp01(min(1.0, union_n / _UNION_SATURATION)),
                    details={
                        "method": "jaccard",
                        "metric_a": metric_a,
                        "metric_b": metric_b,
                        "j": round(float(overlap), 4),
                        "n": int(union_n),
                        "window_seconds": WINDOW_N,
                    },
                )
            )

    # --- Phase 3: normalized mutual information on the target pairs ---------------
    def _mi_phase(
        self,
        ctx: DetectionContext,
        latest: dict[SourceType, LogEvent],
        found: list[Correlation],
    ) -> None:
        min_n = int(self.settings.min_samples)
        ttl = float(self.settings.dedup_ttl_seconds)
        for metric_a, metric_b in MI_PAIRS:
            if len(found) >= MAX_EMISSIONS_PER_TICK:
                return
            try:
                series = ctx.aggregator.aligned((metric_a, metric_b), WINDOW_N)
                a_vals, b_vals = series[metric_a], series[metric_b]
                nmi = mutual_information(a_vals, b_vals)
                n = int(np.count_nonzero(np.isfinite(a_vals) & np.isfinite(b_vals)))
            except Exception:  # noqa: BLE001 — a bad series must not kill the cycle
                logger.debug(
                    "MI pair (%s, %s) skipped", metric_a, metric_b, exc_info=True
                )
                continue
            if nmi < MIN_STRENGTH_MI or n < min_n:
                continue  # weak dependence, or too few paired samples
            if not self._dedupe.seen(
                pair_key(self.name, metric_a, metric_b, "mutual_information"),
                ctx.now,
                ttl,
            ):
                continue  # this pair already emitted within the TTL
            found.append(
                self._emit(
                    ctx,
                    latest,
                    metric_a,
                    metric_b,
                    strength=clamp01(nmi),
                    confidence=clamp01(
                        0.5 + 0.5 * min(1.0, (n - _MI_BASE_N) / _MI_EXTRA_SATURATION)
                    ),
                    details={
                        "method": "mutual_information",
                        "metric_a": metric_a,
                        "metric_b": metric_b,
                        "nmi": round(float(nmi), 4),
                        "n": n,
                        "window_seconds": WINDOW_N,
                    },
                )
            )

    # --- Emission ------------------------------------------------------------------
    def _emit(
        self,
        ctx: DetectionContext,
        latest: dict[SourceType, LogEvent],
        metric_a: str,
        metric_b: str,
        *,
        strength: float,
        confidence: float,
        details: dict,
    ) -> Correlation:
        """Build the Correlation, anchoring each ref to its series' source."""
        return Correlation(
            id=new_correlation_id(),
            detected_at=ctx.now,
            correlation_type=CorrelationType.METRIC,
            event_a=_ref_for_series(metric_a, ctx, latest),
            event_b=_ref_for_series(metric_b, ctx, latest),
            strength=strength,
            confidence=confidence,
            details=details,
        )


def _ref_for_series(
    series_name: str, ctx: DetectionContext, latest: dict[SourceType, LogEvent]
) -> EventRef:
    """An event ref for a series: its source's newest window event, else synthetic.

    Metric findings are series-level, so the ref is representative rather than
    causal — but it must always exist and carry a sane (never future)
    timestamp. When the window holds no event of the mapped source, a
    synthetic ``metric:{series}`` ref stamped at the cycle clock stands in.
    """
    source = _series_source(series_name)
    event = latest.get(source)
    if event is not None:
        return EventRef.from_event(event)
    return EventRef(
        id=f"metric:{series_name}",
        source=source,
        service=SERVICE_BY_SOURCE[source],
        message=f"metric series {series_name}",
        timestamp=ctx.now,
        correlation_id=None,
    )


def _series_source(series_name: str) -> SourceType:
    """Map a series name to its owning source (see :data:`_PREFIX_SOURCE`)."""
    prefix, _, rest = series_name.partition(".")
    if prefix == "error_presence":
        return SourceType(rest)
    return _PREFIX_SOURCE[prefix]


def _latest_by_source(events: list[LogEvent]) -> dict[SourceType, LogEvent]:
    """The newest window event per source (any level), for emission refs."""
    latest: dict[SourceType, LogEvent] = {}
    for event in events:
        held = latest.get(event.source)
        if held is None or event.timestamp >= held.timestamp:
            latest[event.source] = event
    return latest


def _finite_pairs_at_lag(a: np.ndarray, b: np.ndarray, lag: int) -> int:
    """Finite sample pairs backing a TLCC statistic at ``lag`` — its honest n."""
    if lag > 0:
        x, y = a[:-lag], b[lag:]
    elif lag < 0:
        x, y = a[-lag:], b[:lag]
    else:
        x, y = a, b
    return int(np.count_nonzero(np.isfinite(x) & np.isfinite(y)))
