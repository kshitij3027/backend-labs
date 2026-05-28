from __future__ import annotations

from src.loadgen.runner import RunSummary
from src.store.run_store import RunStore


def _summary(run_id: str = "r1", started_at: float = 1.0) -> RunSummary:
    return RunSummary(
        run_id=run_id,
        started_at=started_at,
        finished_at=started_at + 1.0,
        baseline_or_optimized="baseline",
        optimization_name=None,
        workload_seed=42,
        log_count=100,
        concurrency=4,
        throughput_lps=100.0,
        p50_ms=1.0,
        p95_ms=2.0,
        p99_ms=3.0,
        peak_cpu=10.0,
        peak_mem_mb=50.0,
    )


def test_put_and_get() -> None:
    store = RunStore()
    s = _summary(run_id="abc", started_at=10.0)
    store.put(s)
    got = store.get("abc")
    assert got is not None
    assert got.run_id == "abc"
    assert got.started_at == 10.0


def test_list_ordering() -> None:
    store = RunStore()
    store.put(_summary(run_id="mid", started_at=20.0))
    store.put(_summary(run_id="oldest", started_at=10.0))
    store.put(_summary(run_id="newest", started_at=30.0))
    listed = store.list()
    assert [r.run_id for r in listed] == ["newest", "mid", "oldest"]


def test_list_limit() -> None:
    store = RunStore()
    for i in range(10):
        store.put(_summary(run_id=f"r{i}", started_at=float(i)))
    listed = store.list(limit=3)
    assert len(listed) == 3
    # Most recent first
    assert listed[0].run_id == "r9"
    assert listed[1].run_id == "r8"
    assert listed[2].run_id == "r7"


def test_clear() -> None:
    store = RunStore()
    store.put(_summary(run_id="a", started_at=1.0))
    store.put(_summary(run_id="b", started_at=2.0))
    assert len(store) == 2
    store.clear()
    assert len(store) == 0
    assert store.list() == []


def test_get_missing() -> None:
    store = RunStore()
    assert store.get("nope") is None
