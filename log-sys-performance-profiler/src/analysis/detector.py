from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field
from statistics import median
from typing import Literal

from src.analysis.statistics import baseline_window, percentile, rolling_zscore
from src.metrics.ring_buffer import RingBuffer
from src.metrics.sample import MetricSample, StageName
from src.settings import Settings

BottleneckType = Literal["serial", "resource", "contention", "architectural"]
Severity = Literal["low", "medium", "high"]

STAGES: list[StageName] = ["parse", "validate", "transform", "write"]
ADJACENT_PAIRS: list[tuple[StageName, StageName]] = [
    ("parse", "validate"),
    ("validate", "transform"),
    ("transform", "write"),
]


@dataclass(slots=True, frozen=True)
class Bottleneck:
    id: str
    type: BottleneckType
    stage: str
    severity: Severity
    evidence_window: tuple[float, float]
    z_score: float
    started_at: float
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["evidence_window"] = list(d["evidence_window"])
        return d


def _severity(z: float) -> Severity:
    if z >= 4.5:
        return "high"
    if z >= 3.0:
        return "medium"
    return "low"


class BottleneckDetector:
    """Evaluates the ring buffer over a sliding window and emits Bottleneck
    records when statistically significant patterns appear. Severity escalates
    to 'high' if the same (type, stage) fires across two consecutive evaluations.
    """

    def __init__(
        self,
        buffer: RingBuffer,
        settings: Settings,
        queue_maxsize_lookup=None,
    ) -> None:
        self._buffer = buffer
        self._settings = settings
        self._history: dict[tuple[str, str], int] = {}
        self._queue_max_fn = queue_maxsize_lookup or (lambda _stage: settings.queue_maxsize)

    def evaluate(self, throughput_lps: float | None = None) -> list[Bottleneck]:
        window = self._settings.detection_window_sec
        z_threshold = self._settings.bottleneck_z_threshold
        samples = self._buffer.snapshot(window_sec=window)
        if len(samples) < 5:
            return []

        now = time.time()
        ev_start = now - window
        bottlenecks: list[Bottleneck] = []

        # ---- 1. Serial: one stage's p95 dominates ----
        per_stage_latencies: dict[StageName, list[float]] = {s: [] for s in STAGES}
        for sm in samples:
            per_stage_latencies.setdefault(sm.stage, []).append(sm.latency_ms)
        p95s = {st: percentile(lats, 95) for st, lats in per_stage_latencies.items() if lats}
        if p95s:
            slowest_stage = max(p95s, key=lambda k: p95s[k])
            slowest_p95 = p95s[slowest_stage]
            others = [v for k, v in p95s.items() if k != slowest_stage]
            other_med = median(others) if others else 0.0
            if other_med > 0 and slowest_p95 >= 1.5 * other_med:
                baseline = baseline_window(
                    self._buffer.snapshot(window_sec=60.0),
                    now,
                    lookback_sec=60.0,
                    detection_window_sec=window,
                )
                baseline_latencies = [s.latency_ms for s in baseline if s.stage == slowest_stage]
                z = rolling_zscore(baseline_latencies, slowest_p95)
                if z >= z_threshold:
                    bottlenecks.append(self._emit(
                        "serial", slowest_stage, z, ev_start, now,
                        {"p95_ms": slowest_p95, "other_median_p95_ms": other_med},
                    ))

        # ---- 2. Resource: sustained high CPU or growing mem ----
        for stage, lats in per_stage_latencies.items():
            stage_samples = [s for s in samples if s.stage == stage]
            if not stage_samples:
                continue
            high_cpu_share = sum(1 for s in stage_samples if s.cpu_pct >= 85.0) / len(stage_samples)
            if high_cpu_share >= 0.8:
                z = max(2.0, high_cpu_share * 5.0)  # synthetic z gated by share
                if z >= z_threshold:
                    mean_cpu = sum(s.cpu_pct for s in stage_samples) / len(stage_samples)
                    bottlenecks.append(self._emit(
                        "resource", stage, z, ev_start, now,
                        {"mean_cpu_pct": mean_cpu, "high_cpu_share": high_cpu_share},
                    ))
                    continue
            # mem growth slope
            if len(stage_samples) >= 2:
                first = stage_samples[0]
                last = stage_samples[-1]
                dt = max(last.ts - first.ts, 1e-9)
                mem_slope_mb_s = (last.mem_mb - first.mem_mb) / dt
                if mem_slope_mb_s >= 10.0:
                    z = max(2.0, mem_slope_mb_s / 5.0)
                    if z >= z_threshold:
                        bottlenecks.append(self._emit(
                            "resource", stage, z, ev_start, now,
                            {"mem_slope_mb_s": mem_slope_mb_s},
                        ))

        # ---- 3. Contention: adjacent-queue saturation or starvation ----
        for a, b in ADJACENT_PAIRS:
            b_samples = [s for s in samples if s.stage == b]
            if len(b_samples) < 5:
                continue
            qmax = self._queue_max_fn(b)
            full_share = sum(1 for s in b_samples if s.queue_depth >= qmax) / len(b_samples)
            empty_share = sum(1 for s in b_samples if s.queue_depth == 0) / len(b_samples)
            pair_stage = f"{a}->{b}"
            if full_share >= 0.6:
                z = max(2.0, full_share * 4.5)
                if z >= z_threshold:
                    bottlenecks.append(self._emit(
                        "contention", pair_stage, z, ev_start, now,
                        {"kind": "back_pressure", "full_share": full_share},
                    ))
            elif empty_share >= 0.6:
                z = max(2.0, empty_share * 4.5)
                if z >= z_threshold:
                    bottlenecks.append(self._emit(
                        "contention", pair_stage, z, ev_start, now,
                        {"kind": "starvation", "empty_share": empty_share},
                    ))

        # ---- 4. Architectural: low throughput with no other class firing ----
        if throughput_lps is not None and not bottlenecks:
            theoretical = max(1.0, float(self._settings.theoretical_max_lps))
            if throughput_lps < 0.3 * theoretical:
                ratio = throughput_lps / theoretical
                z = max(2.0, (1.0 - ratio) * 4.0)
                if z >= z_threshold:
                    bottlenecks.append(self._emit(
                        "architectural", "all", z, ev_start, now,
                        {"throughput_lps": throughput_lps, "theoretical_max_lps": theoretical},
                    ))

        # ---- Severity escalation on persistence ----
        current_keys = {(b.type, b.stage) for b in bottlenecks}
        escalated: list[Bottleneck] = []
        for b in bottlenecks:
            key = (b.type, b.stage)
            self._history[key] = self._history.get(key, 0) + 1
            if self._history[key] >= 2 and b.severity != "high":
                escalated.append(Bottleneck(
                    id=b.id, type=b.type, stage=b.stage, severity="high",
                    evidence_window=b.evidence_window, z_score=b.z_score,
                    started_at=b.started_at, details=b.details,
                ))
            else:
                escalated.append(b)
        # decay history for keys not in current
        for k in list(self._history.keys()):
            if k not in current_keys:
                del self._history[k]
        return escalated

    def _emit(
        self,
        type_: BottleneckType,
        stage: str,
        z: float,
        ev_start: float,
        ev_end: float,
        details: dict,
    ) -> Bottleneck:
        return Bottleneck(
            id=uuid.uuid4().hex,
            type=type_,
            stage=stage,
            severity=_severity(z),
            evidence_window=(ev_start, ev_end),
            z_score=z,
            started_at=ev_start,
            details=details,
        )
