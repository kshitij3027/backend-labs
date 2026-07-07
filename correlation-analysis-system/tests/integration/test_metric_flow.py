"""Integration: a scripted DB-pool-saturation incident through the REAL
MetricAggregator + CorrelationEngine yields a significant metric_based
web<->db correlation.

No Redis required — the engine runs store-less. The trace is 90 hand-built
seconds (no generator, no randomness): every second carries 5 web requests
and 3 db pool reports; during seconds 30..60 the pool saturates (20/20 in
use) while 40% of web requests turn 500 — exactly the co-movement the metric
detector must catch in ``web.error_rate`` vs ``db.pool_utilization`` and pass
through its Benjamini-Hochberg significance gate.
"""

from src.aggregation import MetricAggregator
from src.config import Settings
from src.engine import CorrelationEngine
from src.models import CorrelationType, LogEvent, SourceType

T0 = 1_000_000.0
INCIDENT_SECONDS = range(30, 61)  # inclusive 30..60
POOL_SIZE = 20.0


def _web_event(sec: int, i: int, status: int, latency_ms: float) -> LogEvent:
    return LogEvent(
        id=f"web-{sec}-{i}",
        timestamp=T0 + sec + 0.05 + 0.15 * i,
        source=SourceType.WEB,
        service="nginx",
        level="ERROR" if status >= 500 else "INFO",
        message=f"GET /checkout -> {status}",
        error_code=f"HTTP_{status}" if status >= 500 else None,
        metrics={"status": float(status), "latency_ms": latency_ms},
    )


def _db_event(sec: int, i: int, pool_in_use: int) -> LogEvent:
    return LogEvent(
        id=f"db-{sec}-{i}",
        timestamp=T0 + sec + 0.1 + 0.2 * i,
        source=SourceType.DATABASE,
        service="postgresql",
        level="INFO",
        message=f"connection pool {pool_in_use}/{int(POOL_SIZE)}",
        metrics={"pool_in_use": float(pool_in_use), "pool_size": POOL_SIZE},
    )


def test_pool_saturation_trace_emits_significant_web_db_correlation():
    aggregator = MetricAggregator()
    last_web = last_db = None
    for sec in range(90):
        incident = sec in INCIDENT_SECONDS
        pool_in_use = 20 if incident else 3 + sec % 3  # saturated vs 3..5 of 20
        for i in range(5):
            status = 500 if incident and i < 2 else 200  # 2/5 = 40% during incident
            last_web = _web_event(sec, i, status, 320.0 if incident else 48.0 + i)
            aggregator.add_event(last_web)
        for i in range(3):
            last_db = _db_event(sec, i, pool_in_use)
            aggregator.add_event(last_db)
        aggregator.roll(T0 + sec + 1)  # complete this second

    engine = CorrelationEngine(Settings(_env_file=None), aggregator, store=None)
    now = T0 + 90.0
    found = engine.detect([], window_events=[last_web, last_db], now=now)

    # The scripted trace only excites the metric detector (no shared journey
    # ids, no user ids, no new events, no fresh ERROR pair in the ref window).
    assert found
    assert all(corr.correlation_type is CorrelationType.METRIC for corr in found)

    target = {"web.error_rate", "db.pool_utilization"}
    hits = [
        corr
        for corr in found
        if {corr.details["metric_a"], corr.details["metric_b"]} == target
        and "p_adj" in corr.details
    ]
    assert hits
    corr = hits[0]
    assert corr.details["p_adj"] < 0.05
    assert corr.strength >= 0.4
    assert corr.details["n"] >= Settings(_env_file=None).min_samples

    # Refs anchor to the freshest window event of each series' source, with
    # sane (never future) timestamps.
    assert corr.event_a.source is SourceType.WEB
    assert corr.event_a.id == last_web.id
    assert corr.event_b.source is SourceType.DATABASE
    assert corr.event_b.id == last_db.id
    assert corr.event_a.timestamp <= now
    assert corr.event_b.timestamp <= now

    assert engine.stats()["types"]["metric_based"] >= 1
