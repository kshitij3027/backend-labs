"""Unit tests for the CascadeDetector (spec area: error cascade correlation).

Error chains are hand-built with explicit timestamps: ERROR/FATAL events are
gap-clustered (consecutive gap <= cascade_window_seconds = 10 s) and a cluster
spanning >= 2 distinct sources emits one root->leaf correlation. Scores follow
the exact formulas: strength = 0.5 * (1 - dt/10) + 0.5 * min(1, distinct/3);
confidence = 0.4 + 0.3 (shared correlation_id) + 0.2 (shared user_id)
+ 0.1 (known root-cause direction). Clusters only emit while the LEAF (the
latest cross-source error — the emitted event_b) is at most FRESHNESS_SECONDS
old; stale-leaf clusters are skipped without being marked seen.
"""

import pytest

from src.config import Settings
from src.engine.base import DetectionContext
from src.engine.cascade import CascadeDetector
from src.models import (
    DB_POOL_EXHAUSTED,
    DB_QUERY_ERROR,
    HTTP_500,
    HTTP_502,
    CorrelationType,
    LogEvent,
    SourceType,
)


def mk_event(source: SourceType, ts: float, **kw) -> LogEvent:
    """A hand-built LogEvent with sensible defaults for the non-essential fields."""
    return LogEvent(
        id=kw.pop("id", f"{source.value}-{ts}"),
        timestamp=ts,
        source=source,
        service=kw.pop("service", source.value),
        level=kw.pop("level", "INFO"),
        message=kw.pop("message", f"{source.value} event at {ts}"),
        **kw,
    )


def mk_error(source: SourceType, ts: float, code: str, **kw) -> LogEvent:
    """An ERROR-level event carrying an error code (level overridable, e.g. FATAL)."""
    return mk_event(source, ts, level=kw.pop("level", "ERROR"), error_code=code, **kw)


def make_ctx(window, now) -> DetectionContext:
    """Cascade detection only reads window_events; new_events stays empty."""
    return DetectionContext(
        now=now, new_events=[], window_events=list(window), aggregator=None
    )


def make_detector() -> CascadeDetector:
    return CascadeDetector(Settings(_env_file=None))


def test_db_to_web_cascade_with_shared_ids_scores_exactly():
    detector = make_detector()
    # The raw postgres line is FATAL — the detector must chain it regardless.
    root = mk_error(
        SourceType.DATABASE, 1000.0, DB_POOL_EXHAUSTED,
        level="FATAL", correlation_id="corr_x", user_id="user_9",
    )
    leaf = mk_error(
        SourceType.WEB, 1003.0, HTTP_500, correlation_id="corr_x", user_id="user_9"
    )

    found = detector.detect(make_ctx([root, leaf], now=1004.0))

    assert len(found) == 1
    corr = found[0]
    assert corr.correlation_type is CorrelationType.CASCADE
    assert corr.detected_at == 1004.0
    # The upstream root is always event_a, the downstream leaf event_b.
    assert corr.event_a.source is SourceType.DATABASE
    assert corr.event_b.source is SourceType.WEB
    # dt = 3 s over a 10 s window, 2 distinct sources.
    assert corr.strength == pytest.approx(0.5 * (1 - 3 / 10) + 0.5 * (2 / 3), abs=1e-6)
    # 0.4 base + 0.3 shared correlation_id + 0.2 shared user_id + 0.1 known
    # db -> web root-cause direction = 1.0.
    assert corr.confidence == pytest.approx(1.0)
    assert corr.details["chain_length"] == 2
    assert corr.details["distinct_services"] == 2
    assert corr.details["span_seconds"] == pytest.approx(3.0)
    assert corr.details["root_error"] == DB_POOL_EXHAUSTED
    chain = corr.details["chain"]
    assert [hop["source"] for hop in chain] == ["database", "web"]
    assert chain[0]["error_code"] == DB_POOL_EXHAUSTED
    assert chain[0]["ts"] == 1000.0
    assert chain[1]["error_code"] == HTTP_500


def test_direction_only_cascade_scores_base_plus_direction_bonus():
    detector = make_detector()
    events = [
        # Different journeys, no user ids: only the 0.1 direction bonus applies.
        mk_error(SourceType.DATABASE, 1000.0, DB_POOL_EXHAUSTED, correlation_id="corr_a"),
        mk_error(SourceType.WEB, 1003.0, HTTP_500, correlation_id="corr_b"),
    ]

    found = detector.detect(make_ctx(events, now=1004.0))

    assert len(found) == 1
    assert found[0].confidence == pytest.approx(0.5)  # 0.4 base + 0.1 direction


def test_errors_fifteen_seconds_apart_split_into_singleton_clusters():
    detector = make_detector()
    events = [
        mk_error(SourceType.DATABASE, 1000.0, DB_POOL_EXHAUSTED),
        mk_error(SourceType.WEB, 1015.0, HTTP_500),  # gap 15 s > 10 s window
    ]
    assert detector.detect(make_ctx(events, now=1016.0)) == []


def test_same_source_only_cluster_never_emits():
    detector = make_detector()
    events = [
        mk_error(SourceType.DATABASE, 1000.0, DB_POOL_EXHAUSTED),
        mk_error(SourceType.DATABASE, 1002.0, DB_QUERY_ERROR),
    ]
    assert detector.detect(make_ctx(events, now=1004.0)) == []


def test_three_service_chain_outscores_two_service_chain():
    three = make_detector().detect(
        make_ctx(
            [
                mk_error(SourceType.DATABASE, 1000.0, DB_POOL_EXHAUSTED),
                mk_error(SourceType.API_SERVICE, 1001.5, HTTP_502),
                mk_error(SourceType.WEB, 1003.0, HTTP_500),
            ],
            now=1004.0,
        )
    )
    two = make_detector().detect(
        make_ctx(
            [
                mk_error(SourceType.DATABASE, 1000.0, DB_POOL_EXHAUSTED),
                mk_error(SourceType.WEB, 1003.0, HTTP_500),
            ],
            now=1004.0,
        )
    )

    assert len(three) == 1 and len(two) == 1
    # Same 3 s root->leaf span; breadth term 3/3 = 1.0 vs 2/3.
    assert three[0].strength == pytest.approx(0.5 * (1 - 3 / 10) + 0.5 * 1.0, abs=1e-6)
    assert two[0].strength == pytest.approx(0.5 * (1 - 3 / 10) + 0.5 * (2 / 3), abs=1e-6)
    assert three[0].strength > two[0].strength
    # Leaf = the LATEST cross-source error, not the middle hop.
    assert three[0].event_b.source is SourceType.WEB
    assert three[0].details["chain_length"] == 3


def test_same_cluster_deduped_within_ttl():
    detector = make_detector()  # dedup_ttl_seconds = 30
    events = [
        mk_error(SourceType.DATABASE, 1000.0, DB_POOL_EXHAUSTED),
        mk_error(SourceType.WEB, 1003.0, HTTP_500),
    ]
    assert len(detector.detect(make_ctx(events, now=1004.0))) == 1
    # Next cycle, the cluster is still in the window — but it already emitted.
    assert detector.detect(make_ctx(events, now=1006.0)) == []


def test_info_and_warn_events_never_form_cascades():
    detector = make_detector()
    events = [
        mk_event(SourceType.DATABASE, 1000.0, correlation_id="corr_x"),  # INFO
        mk_event(SourceType.WEB, 1003.0, level="WARN", correlation_id="corr_x"),
    ]
    assert detector.detect(make_ctx(events, now=1004.0)) == []


def test_stale_cluster_not_emitted():
    # Freshness guard: a valid 2-source cluster whose newest error (here also
    # the cross-source leaf) is 10 s old — still inside the 2*window recency
    # cutoff of 20 s — must not emit: detected_at would trail the underlying
    # events by ~10 s, breaching the 5 s detection-latency contract.
    detector = make_detector()
    events = [
        mk_error(SourceType.DATABASE, 1000.0, DB_POOL_EXHAUSTED),
        mk_error(SourceType.WEB, 1003.0, HTTP_500),
    ]
    assert detector.detect(make_ctx(events, now=1013.0)) == []
    # The stale skip must NOT mark the cluster seen: the very same cluster,
    # evaluated while fresh (now = newest error + 2 s), emits normally.
    assert len(detector.detect(make_ctx(events, now=1005.0))) == 1


def test_fresh_chain_with_stale_cross_source_leaf_not_emitted():
    # A db-dominated storm keeps the CLUSTER fresh (newest db error 1 s old)
    # while the leaf — the latest cross-source error, the emitted event_b —
    # is already 6 s stale. Emitting would stamp detected_at ~6 s past the
    # leaf, so the guard must anchor on the leaf and stay quiet.
    detector = make_detector()
    events = [
        mk_error(SourceType.DATABASE, 1000.0, DB_POOL_EXHAUSTED),
        mk_error(SourceType.WEB, 1001.0, HTTP_500),
        mk_error(SourceType.DATABASE, 1006.0, DB_QUERY_ERROR),
    ]
    assert detector.detect(make_ctx(events, now=1007.0)) == []
    # A fresh cross-source error arrives: propagation is observable again, the
    # skip above left no dedupe mark (same root/leaf-pair key), and event_b
    # anchors to the fresh leaf.
    events.append(mk_error(SourceType.WEB, 1007.5, HTTP_500))
    found = detector.detect(make_ctx(events, now=1008.0))
    assert len(found) == 1
    assert found[0].event_b.timestamp == 1007.5


def test_stale_errors_beyond_double_window_are_ignored():
    detector = make_detector()
    events = [
        mk_error(SourceType.DATABASE, 1000.0, DB_POOL_EXHAUSTED),
        mk_error(SourceType.WEB, 1003.0, HTTP_500),
    ]
    # now=1030 -> recency cutoff 1030 - 2*10 = 1010: both errors are history.
    assert detector.detect(make_ctx(events, now=1030.0)) == []
