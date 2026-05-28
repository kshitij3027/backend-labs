from __future__ import annotations

from dataclasses import asdict, dataclass

from src.analysis.detector import Bottleneck, BottleneckType


@dataclass(slots=True, frozen=True)
class Recommendation:
    bottleneck_id: str
    suggestion: str
    expected_impact: str
    applies_to_stage: str
    optimization_name: str | None

    def to_dict(self) -> dict:
        return asdict(self)


# Rule template: (suggestion, expected_impact, applies_to_stage, optimization_name)
RULES: dict[tuple[BottleneckType, str], tuple[str, str, str, str | None]] = {
    ("serial", "parse"): (
        "Replace regex-based parser with finite-state-machine parser",
        ">=40% reduction in p95 parse latency",
        "parse",
        "fsm_parser",
    ),
    ("serial", "validate"): (
        "Hoist schema compilation out of the per-record path; precompile validators once",
        ">=25% reduction in p95 validate latency",
        "validate",
        "precompiled_validator",
    ),
    ("resource", "write"): (
        "Batch writes - coalesce small records into larger chunks",
        ">=50% throughput uplift, lower write CPU%",
        "write",
        "batch_writer",
    ),
    ("resource", "transform"): (
        "Use object pooling for hot per-record allocations to reduce GC pressure",
        ">=20% reduction in peak RSS, lower CPU",
        "transform",
        "object_pool",
    ),
    ("contention", "validate->transform"): (
        "Add async I/O between validate and transform to overlap waits with CPU work",
        "queue depth normalizes, +30% throughput",
        "transform",
        "async_io_variant",
    ),
    ("contention", "transform->write"): (
        "Batch writes downstream OR increase write worker concurrency",
        "back-pressure clears within 10s",
        "write",
        "batch_writer",
    ),
    ("architectural", "all"): (
        "Increase worker concurrency per stage AND adopt object pooling for hot allocations",
        ">=2x throughput at same CPU budget",
        "all",
        "object_pool",
    ),
    ("large_file_read", "parse"): (
        "Use mmap for large file reads - avoid per-line read() syscalls",
        ">=2x parse throughput on large files",
        "parse",
        "mmap_reader",
    ),
}


_SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2}


class RecommendationEngine:
    def recommend(self, bottlenecks: list[Bottleneck]) -> list[Recommendation]:
        # Sort by severity desc so high-severity recs land first
        sorted_b = sorted(bottlenecks, key=lambda b: _SEVERITY_RANK.get(b.severity, 99))
        seen_opt_names: set[str] = set()
        out: list[Recommendation] = []
        for b in sorted_b:
            key = (b.type, b.stage)
            if key not in RULES:
                continue
            sug, impact, stage, opt = RULES[key]
            if opt is not None and opt in seen_opt_names:
                continue
            if opt is not None:
                seen_opt_names.add(opt)
            out.append(Recommendation(
                bottleneck_id=b.id,
                suggestion=sug,
                expected_impact=impact,
                applies_to_stage=stage,
                optimization_name=opt,
            ))
        return out
