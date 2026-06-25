"""Batch temporal pattern mining (Feature Area A).

This module mines the warm-up corpus for *recurring temporal patterns* — structure that is
visible only when logs are bucketed by time-of-day (hour 0-23) and day-of-week (Mon=0 ..
Sun=6). The synthetic corpus (:func:`src.log_generator.generate_logs`) deliberately embeds
several such patterns (a nightly 02:00 error spike, a Tuesday-morning auth burst, business-
hours performance degradation, a Friday-evening payment peak, ...); the job of
:func:`mine_temporal_patterns` is to *rediscover* them from the data alone, with no prior
knowledge of how they were planted.

Approach (kept deliberately simple and explainable):

* Parse every log to ``(timestamp, level, service, status)`` and derive ``hour`` / ``weekday``.
* Establish two baselines from the whole corpus: the overall **error rate** (share of logs at
  level ERROR/CRITICAL) and the overall **per-hour volume** (so a single hour can be compared
  against the typical hour).
* Detect four kinds of pattern, each emitted as a uniform dict
  (``pattern_id``/``kind``/``description``/``window``/``metric``/``count``/``services``):

  - ``hourly_error_spike``    — an hour whose error rate runs materially above baseline.
  - ``weekday_service_burst`` — a (weekday, hour, service) cell where one service's volume is
    anomalously concentrated (e.g. auth on Tuesday mornings, payment on Friday evenings).
  - ``business_hours_perf``   — elevated high-latency / performance-family share during
    business hours (Mon-Fri 09:00-17:00) versus the overnight baseline.
  - ``hour_volume_peak``      — the busiest hours of the day by volume.

* Results are sorted by a severity ``metric`` (descending) and capped at ``max_patterns``.

**Time-based weighting.** The ``metric`` for the error/perf kinds is the *ratio to baseline*
(e.g. an hour at 2.3x the baseline error rate), which is itself a per-hour-normalized,
volume-aware weighting: an hour is only flagged when its rate stands out *relative to the
typical hour*, and small-sample hours are filtered by a minimum count so a single noisy log
cannot manufacture a spike. This keeps the signal robust without a heavier time-decay model.

Everything is defensive: empty / tiny / malformed inputs return whatever can be found (often
an empty list) and never raise.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import TYPE_CHECKING, Any

from src.preprocessing import parse_log

if TYPE_CHECKING:  # pragma: no cover - typing only
    from src.schemas import LogEntry

# Levels treated as "errors" for error-rate baselines / spike detection.
_ERROR_LEVELS: frozenset[str] = frozenset({"ERROR", "CRITICAL"})

# A response time at/above this (ms) is treated as a "high-latency" / performance-family log
# for the business-hours degradation signal. Matches the generator's perf latency band
# (~800-5000ms) so the planted degradation is what gets measured.
_HIGH_LATENCY_MS: float = 800.0

# Detection thresholds (tuned against the seeded corpus; conservative so noise is not flagged).
_ERROR_SPIKE_RATIO: float = 1.4  # hour error rate must be >= this x the overall rate
_ERROR_SPIKE_MIN_COUNT: int = 10  # ... and the hour must carry enough logs to be meaningful
_BURST_RATIO: float = 1.8  # a service's share in a cell vs its overall share
_BURST_MIN_COUNT: int = 6  # ... with at least this many logs in the cell
_PERF_RATIO: float = 1.5  # business-hours high-latency share vs overnight share
_TOP_HOURS: int = 3  # how many busiest hours to emit as volume peaks

# Business-hours window (weekdays only) and overnight baseline window for the perf signal.
_BIZ_HOURS = range(9, 17)
_NIGHT_HOURS = range(0, 6)

_WEEKDAY_NAMES = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")


def _weekday_name(weekday: int) -> str:
    """Return the 3-letter name for a 0=Mon..6=Sun weekday index (safe for out-of-range)."""
    if 0 <= weekday < len(_WEEKDAY_NAMES):
        return _WEEKDAY_NAMES[weekday]
    return f"wd{weekday}"


def _parsed_with_time(logs: "list[LogEntry | dict]") -> list[dict[str, Any]]:
    """Parse logs and keep only those with a usable ``datetime`` timestamp.

    Each surviving record is augmented with ``hour`` (0-23) and ``weekday`` (0=Mon..6=Sun)
    so the downstream detectors can bucket without re-deriving. Records whose timestamp is
    missing / unparseable are dropped (they carry no temporal signal).
    """
    out: list[dict[str, Any]] = []
    for log in logs or []:
        parsed = parse_log(log)
        ts = parsed.get("timestamp")
        if not isinstance(ts, datetime):
            continue
        parsed["hour"] = ts.hour
        parsed["weekday"] = ts.weekday()
        out.append(parsed)
    return out


def hour_histogram(logs: "list[LogEntry | dict]") -> dict[int, dict[str, float]]:
    """Bucket logs by hour-of-day, returning per-hour volume + error counts/rate.

    Every hour ``0..23`` is present in the result (zero-filled) so callers — the dashboard or
    tests — can index any hour without a ``KeyError``.

    Args:
        logs: A list of :class:`~src.schemas.LogEntry` or parsed/plain dicts.

    Returns:
        ``{hour: {"count": int, "error_count": int, "error_rate": float}}`` for ``hour`` in
        ``0..23``. ``error_rate`` is ``error_count / count`` (``0.0`` for an empty hour).
    """
    counts = {h: 0 for h in range(24)}
    errors = {h: 0 for h in range(24)}
    for parsed in _parsed_with_time(logs):
        h = parsed["hour"]
        counts[h] += 1
        if parsed.get("level") in _ERROR_LEVELS:
            errors[h] += 1
    return {
        h: {
            "count": counts[h],
            "error_count": errors[h],
            "error_rate": (errors[h] / counts[h]) if counts[h] else 0.0,
        }
        for h in range(24)
    }


def _detect_hourly_error_spikes(
    parsed: list[dict[str, Any]], base_error_rate: float
) -> list[dict[str, Any]]:
    """Flag hours whose error rate runs >= ``_ERROR_SPIKE_RATIO`` x the overall rate.

    Rediscovers the planted nightly 02:00 error spike (and any other elevated hour). Hours
    with fewer than ``_ERROR_SPIKE_MIN_COUNT`` logs are skipped so a tiny noisy hour cannot
    masquerade as a spike. The emitted ``metric`` is the ratio-to-baseline (a per-hour-volume-
    normalized weighting), which doubles as the sort key.
    """
    if base_error_rate <= 0:
        return []

    by_hour_count: dict[int, int] = defaultdict(int)
    by_hour_err: dict[int, int] = defaultdict(int)
    by_hour_services: dict[int, set[str]] = defaultdict(set)
    for p in parsed:
        h = p["hour"]
        by_hour_count[h] += 1
        if p.get("level") in _ERROR_LEVELS:
            by_hour_err[h] += 1
            if p.get("service"):
                by_hour_services[h].add(p["service"])

    patterns: list[dict[str, Any]] = []
    for h in range(24):
        count = by_hour_count.get(h, 0)
        if count < _ERROR_SPIKE_MIN_COUNT:
            continue
        rate = by_hour_err.get(h, 0) / count
        ratio = rate / base_error_rate
        if ratio < _ERROR_SPIKE_RATIO:
            continue
        patterns.append(
            {
                "pattern_id": f"temporal-error-h{h:02d}",
                "kind": "hourly_error_spike",
                "description": (
                    f"Elevated error rate at {h:02d}:00 "
                    f"({ratio:.1f}x baseline, {rate:.0%} of logs are errors)"
                ),
                "window": f"{h:02d}:00-{h:02d}:59 daily",
                "metric": round(ratio, 3),
                "count": count,
                "services": sorted(by_hour_services.get(h, set())),
            }
        )
    return patterns


def _detect_weekday_service_bursts(parsed: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flag (weekday, hour, service) cells where one service is anomalously concentrated.

    For each service we compare its share of traffic inside a (weekday, hour) cell against its
    overall share of the corpus; a cell whose local share is >= ``_BURST_RATIO`` x the global
    share (and that carries >= ``_BURST_MIN_COUNT`` logs) is a burst. This rediscovers the
    Tuesday-morning auth brute-force burst and the Friday-evening payment peak without naming
    either service explicitly. Bursts are merged across adjacent hours of the same
    (weekday, service) into a single contiguous-window pattern so a multi-hour burst reads as
    one finding rather than three.
    """
    total = len(parsed)
    if total == 0:
        return []

    svc_total: dict[str, int] = defaultdict(int)
    cell_count: dict[tuple[int, int, str], int] = defaultdict(int)
    cell_total: dict[tuple[int, int], int] = defaultdict(int)
    for p in parsed:
        s = p.get("service") or ""
        if not s:
            continue
        wd, h = p["weekday"], p["hour"]
        svc_total[s] += 1
        cell_count[(wd, h, s)] += 1
        cell_total[(wd, h)] += 1

    # Collect qualifying (weekday, hour, service) cells keyed by (weekday, service) so adjacent
    # hours can be merged into one window.
    hits: dict[tuple[int, str], list[tuple[int, int, float]]] = defaultdict(list)
    for (wd, h, s), c in cell_count.items():
        if c < _BURST_MIN_COUNT:
            continue
        local_total = cell_total[(wd, h)]
        if local_total == 0:
            continue
        local_share = c / local_total
        global_share = svc_total[s] / total
        if global_share <= 0:
            continue
        ratio = local_share / global_share
        if ratio < _BURST_RATIO:
            continue
        hits[(wd, s)].append((h, c, ratio))

    patterns: list[dict[str, Any]] = []
    for (wd, s), cells in hits.items():
        cells.sort()  # by hour
        # Merge consecutive hours into contiguous runs.
        runs: list[list[tuple[int, int, float]]] = []
        for cell in cells:
            if runs and cell[0] == runs[-1][-1][0] + 1:
                runs[-1].append(cell)
            else:
                runs.append([cell])
        for run in runs:
            hours = [c[0] for c in run]
            count = sum(c[1] for c in run)
            peak_ratio = max(c[2] for c in run)
            h_lo, h_hi = hours[0], hours[-1]
            wd_name = _weekday_name(wd)
            window = (
                f"{wd_name} {h_lo:02d}:00-{h_hi:02d}:59"
                if h_lo != h_hi
                else f"{wd_name} {h_lo:02d}:00"
            )
            patterns.append(
                {
                    "pattern_id": f"temporal-burst-{wd}-{h_lo:02d}-{s}",
                    "kind": "weekday_service_burst",
                    "description": (
                        f"{s} burst {window} ({peak_ratio:.1f}x its usual share, "
                        f"{count} logs)"
                    ),
                    "window": window,
                    "metric": round(peak_ratio, 3),
                    "count": count,
                    "services": [s],
                }
            )
    return patterns


def _detect_business_hours_perf(parsed: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Flag elevated high-latency share during business hours vs the overnight baseline.

    Compares the share of high-latency / performance-family logs (``response_time_ms`` >=
    ``_HIGH_LATENCY_MS``) inside weekday business hours (09:00-17:00) against the overnight
    window (00:00-06:00). A ratio >= ``_PERF_RATIO`` rediscovers the planted business-hours
    performance degradation. Emits at most one pattern.
    """
    biz_hi = biz_tot = night_hi = night_tot = 0
    biz_services: set[str] = set()
    for p in parsed:
        rt = p.get("response_time_ms")
        is_hi = isinstance(rt, (int, float)) and rt >= _HIGH_LATENCY_MS
        wd, h = p["weekday"], p["hour"]
        if wd < 5 and h in _BIZ_HOURS:
            biz_tot += 1
            if is_hi:
                biz_hi += 1
                if p.get("service"):
                    biz_services.add(p["service"])
        elif h in _NIGHT_HOURS:
            night_tot += 1
            if is_hi:
                night_hi += 1

    if biz_tot == 0:
        return []
    biz_share = biz_hi / biz_tot
    night_share = (night_hi / night_tot) if night_tot else 0.0
    # Ratio vs a small floor so a (rare) zero overnight share still yields a finite, sortable
    # metric rather than dividing by zero.
    ratio = biz_share / max(night_share, 0.01)
    if ratio < _PERF_RATIO or biz_hi == 0:
        return []
    return [
        {
            "pattern_id": "temporal-bizhours-perf",
            "kind": "business_hours_perf",
            "description": (
                f"Performance degradation in business hours: {biz_share:.0%} high-latency "
                f"logs Mon-Fri 09:00-17:00 vs {night_share:.0%} overnight ({ratio:.1f}x)"
            ),
            "window": "Mon-Fri 09:00-17:00",
            "metric": round(ratio, 3),
            "count": biz_hi,
            "services": sorted(biz_services),
        }
    ]


def _detect_hour_volume_peaks(
    parsed: list[dict[str, Any]], top_n: int = _TOP_HOURS
) -> list[dict[str, Any]]:
    """Emit the ``top_n`` busiest hours of the day by volume as ``hour_volume_peak`` patterns.

    The ``metric`` is the hour's volume relative to the mean hourly volume (so a peak at 1.6x
    the typical hour reads consistently with the ratio-based metrics of the other kinds).
    """
    by_hour: dict[int, int] = defaultdict(int)
    by_hour_services: dict[int, set[str]] = defaultdict(set)
    for p in parsed:
        h = p["hour"]
        by_hour[h] += 1
        if p.get("service"):
            by_hour_services[h].add(p["service"])
    if not by_hour:
        return []

    active_hours = [h for h, c in by_hour.items() if c > 0]
    mean_vol = sum(by_hour.values()) / max(len(active_hours), 1)
    ordered = sorted(active_hours, key=lambda h: (-by_hour[h], h))[:top_n]

    patterns: list[dict[str, Any]] = []
    for h in ordered:
        count = by_hour[h]
        ratio = count / mean_vol if mean_vol else 1.0
        patterns.append(
            {
                "pattern_id": f"temporal-volume-h{h:02d}",
                "kind": "hour_volume_peak",
                "description": (
                    f"Traffic peak at {h:02d}:00 ({count} logs, {ratio:.1f}x the typical hour)"
                ),
                "window": f"{h:02d}:00-{h:02d}:59 daily",
                "metric": round(ratio, 3),
                "count": count,
                "services": sorted(by_hour_services.get(h, set())),
            }
        )
    return patterns


def mine_temporal_patterns(
    logs: "list[LogEntry | dict]", max_patterns: int = 12
) -> list[dict[str, Any]]:
    """Mine a batch of logs for recurring temporal patterns.

    Runs the four detectors (hourly error spikes, weekday service bursts, business-hours
    performance degradation, hourly volume peaks) over the corpus, concatenates their findings,
    sorts by severity (``metric`` descending, ``count`` as a tiebreaker), and caps the result
    at ``max_patterns``. On a representative corpus this rediscovers the >= 5 distinct temporal
    patterns the generator plants.

    Each pattern is a dict with keys:

    * ``pattern_id`` — stable identifier for the finding.
    * ``kind`` — one of ``hourly_error_spike`` / ``weekday_service_burst`` /
      ``business_hours_perf`` / ``hour_volume_peak``.
    * ``description`` — human-readable summary (e.g. "Elevated error rate at 02:00 (2.3x ...)").
    * ``window`` — the time window the pattern occupies (e.g. ``"02:00-02:59 daily"``).
    * ``metric`` — a severity score (ratio-to-baseline / ratio-to-typical-hour) used for ranking.
    * ``count`` — number of logs underpinning the pattern.
    * ``services`` — the services involved (sorted; possibly empty).

    Args:
        logs: A list of :class:`~src.schemas.LogEntry` or parsed/plain dicts. May be empty or
            tiny — the function returns whatever it can find and never raises.
        max_patterns: Maximum number of patterns to return (highest-severity first).

    Returns:
        A list of pattern dicts (length ``0..max_patterns``), most severe first.
    """
    parsed = _parsed_with_time(logs)
    if not parsed:
        return []

    total = len(parsed)
    error_count = sum(1 for p in parsed if p.get("level") in _ERROR_LEVELS)
    base_error_rate = error_count / total if total else 0.0

    patterns: list[dict[str, Any]] = []
    patterns.extend(_detect_hourly_error_spikes(parsed, base_error_rate))
    patterns.extend(_detect_weekday_service_bursts(parsed))
    patterns.extend(_detect_business_hours_perf(parsed))
    patterns.extend(_detect_hour_volume_peaks(parsed))

    # Severity-rank: higher metric first, then larger supporting count, then a stable id so the
    # ordering is fully deterministic for a given corpus.
    patterns.sort(key=_severity_key)

    return _diverse_cap(patterns, max_patterns)


def _severity_key(pattern: dict[str, Any]) -> tuple[float, int, str]:
    """Sort key: severity ``metric`` desc, then supporting ``count`` desc, then stable id."""
    return (
        -float(pattern.get("metric", 0.0)),
        -int(pattern.get("count", 0)),
        pattern["pattern_id"],
    )


def _diverse_cap(
    patterns: list[dict[str, Any]], max_patterns: int
) -> list[dict[str, Any]]:
    """Cap to ``max_patterns`` while guaranteeing each *kind* keeps its strongest finding.

    A naive top-N truncation lets one prolific kind (e.g. many ``weekday_service_burst`` cells)
    crowd out rarer-but-important kinds such as the single ``hourly_error_spike`` at 02:00. To
    keep the result representative of the *distinct* temporal patterns, we first reserve the
    highest-severity pattern of each kind, then fill the remaining slots by global severity. The
    final list is re-sorted by severity so callers still see most-severe-first.
    """
    if len(patterns) <= max_patterns:
        return patterns

    selected: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    # 1) Reserve the top pattern of each kind (patterns is already severity-sorted, so the first
    #    occurrence of a kind is its strongest), in order of strength.
    seen_kinds: set[str] = set()
    for p in patterns:
        if len(selected) >= max_patterns:
            break
        if p["kind"] not in seen_kinds:
            seen_kinds.add(p["kind"])
            selected.append(p)
            seen_ids.add(p["pattern_id"])

    # 2) Fill any remaining slots with the next-strongest patterns overall.
    for p in patterns:
        if len(selected) >= max_patterns:
            break
        if p["pattern_id"] not in seen_ids:
            selected.append(p)
            seen_ids.add(p["pattern_id"])

    selected.sort(key=_severity_key)
    return selected


__all__ = ["mine_temporal_patterns", "hour_histogram"]
