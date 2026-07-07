"""Unit tests for CorrelationEngine initialization (spec area: engine initialization).

A freshly built engine must register exactly the C4+C5 detectors, honor
injected settings, and expose empty-but-well-shaped read models: the
spec-verbatim zeroed stats payload, empty recent/timeline lists, and an
all-zero 5x5 matrix.
"""

from src.aggregation import MetricAggregator
from src.config import Settings
from src.engine import CorrelationEngine
from src.models import SourceType


def make_engine(**overrides) -> CorrelationEngine:
    """An engine over real Settings + a real aggregator, with no Redis store."""
    settings = Settings(_env_file=None, **overrides)
    return CorrelationEngine(settings, MetricAggregator(), store=None)


def test_registers_expected_detectors():
    engine = make_engine()
    assert {det.name for det in engine.detectors} == {
        "temporal",
        "session_based",
        "error_cascade",
        "user_based",
    }


def test_empty_stats_payload_is_exact():
    # SPEC shape: exactly these four keys, zero-valued, before any detection.
    assert make_engine().stats() == {
        "total": 0,
        "types": {},
        "avg_strength": 0.0,
        "recent_count": 0,
    }


def test_recent_is_empty():
    assert make_engine().recent() == []


def test_timeline_is_empty():
    assert make_engine().timeline() == []


def test_matrix_has_five_sources_and_zero_cells():
    matrix = make_engine().matrix()
    assert matrix["sources"] == [source.value for source in SourceType]
    assert matrix["sources"] == ["web", "database", "api_service", "payment", "inventory"]
    assert len(matrix["cells"]) == 5
    assert all(len(row) == 5 for row in matrix["cells"])
    assert all(cell == 0.0 for row in matrix["cells"] for cell in row)


def test_settings_override_reaches_temporal_detector():
    engine = make_engine(window_seconds=10)
    temporal = next(det for det in engine.detectors if det.name == "temporal")
    assert temporal.settings.window_seconds == 10
