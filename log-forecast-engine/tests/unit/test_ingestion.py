"""Unit tests for ingestion schema validation (C2).

These are DB-free: they assert that the Pydantic v2 contracts reject bad input
(NaN / inf / blank name / empty batch) and accept good input, including the
optional-timestamp behaviour. The DB-touching ``ingest_metrics`` round-trip is
covered in ``tests/integration/test_ingestion_api.py``.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.schemas import (
    MetricIngest,
    MetricIngestRequest,
    MetricPoint,
)


# --------------------------------------------------------------------------- #
# Value finiteness
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_metric_ingest_rejects_non_finite(bad: float) -> None:
    with pytest.raises(ValidationError):
        MetricIngest(metric_name="response_time", value=bad)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_metric_point_rejects_non_finite(bad: float) -> None:
    with pytest.raises(ValidationError):
        MetricPoint(
            metric_name="response_time",
            timestamp=datetime.now(timezone.utc),
            value=bad,
        )


def test_metric_ingest_accepts_finite() -> None:
    m = MetricIngest(metric_name="response_time", value=99.5)
    assert m.value == 99.5


# --------------------------------------------------------------------------- #
# Name validation
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("bad_name", ["", "   ", "\t"])
def test_blank_name_rejected(bad_name: str) -> None:
    with pytest.raises(ValidationError):
        MetricIngest(metric_name=bad_name, value=1.0)


def test_name_is_stripped() -> None:
    m = MetricIngest(metric_name="  response_time  ", value=1.0)
    assert m.metric_name == "response_time"


def test_name_too_long_rejected() -> None:
    with pytest.raises(ValidationError):
        MetricIngest(metric_name="x" * 65, value=1.0)


# --------------------------------------------------------------------------- #
# Optional timestamp + tz coercion
# --------------------------------------------------------------------------- #
def test_timestamp_optional_on_ingest() -> None:
    m = MetricIngest(metric_name="throughput", value=10.0)
    assert m.timestamp is None


def test_naive_timestamp_coerced_to_utc() -> None:
    m = MetricIngest(
        metric_name="throughput",
        timestamp=datetime(2026, 1, 1, 12, 0),  # naive
        value=10.0,
    )
    assert m.timestamp is not None
    assert m.timestamp.tzinfo is not None
    assert m.timestamp == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def test_metric_point_requires_timestamp() -> None:
    with pytest.raises(ValidationError):
        MetricPoint(metric_name="throughput", value=10.0)  # type: ignore[call-arg]


# --------------------------------------------------------------------------- #
# Request envelope
# --------------------------------------------------------------------------- #
def test_request_requires_non_empty_points() -> None:
    with pytest.raises(ValidationError):
        MetricIngestRequest(points=[])


def test_request_parses_points() -> None:
    req = MetricIngestRequest(
        points=[
            {"metric_name": "response_time", "value": 1.0},
            {"metric_name": "error_rate", "value": 0.1},
        ]
    )
    assert len(req.points) == 2
    assert req.points[0].metric_name == "response_time"
