"""Unit tests for the domain models in ``src.models``.

Covers enum values/lookups and the Pydantic request models' defaults and
validation rules (required fields, min-length, Literal op constraints).
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.models import (
    Aggregation,
    Filter,
    Format,
    IngestEntry,
    IngestRequest,
    LogEntry,
    QueryClass,
    QueryRequest,
    Tier,
)


def test_format_enum_values() -> None:
    assert Format.ROW.value == "row"
    assert Format.COLUMNAR.value == "columnar"
    assert Format.HYBRID.value == "hybrid"
    # Value-based lookup returns the canonical member.
    assert Format("columnar") is Format.COLUMNAR


def test_query_class_enum_values() -> None:
    assert QueryClass.ANALYTICAL.value == "analytical"
    assert QueryClass.FULL_RECORD.value == "full_record"
    assert QueryClass.MIXED.value == "mixed"


def test_tier_enum_values() -> None:
    assert Tier.HOT.value == "hot"
    assert Tier.WARM.value == "warm"
    assert Tier.COLD.value == "cold"


def test_log_entry_defaults() -> None:
    entry = LogEntry()
    assert entry.ts is None
    assert entry.fields == {}


def test_log_entry_round_trip() -> None:
    entry = LogEntry(ts=1.0, fields={"a": 1})
    assert entry.ts == 1.0
    assert entry.fields == {"a": 1}


def test_ingest_entry_requires_fields() -> None:
    with pytest.raises(ValidationError):
        IngestEntry()  # type: ignore[call-arg]


def test_ingest_entry_accepts_fields() -> None:
    entry = IngestEntry(fields={"msg": "hello"})
    assert entry.ts is None
    assert entry.fields == {"msg": "hello"}


def test_ingest_request_requires_at_least_one_entry() -> None:
    with pytest.raises(ValidationError):
        IngestRequest(entries=[])


def test_ingest_request_tenant_default() -> None:
    req = IngestRequest(entries=[IngestEntry(fields={"a": 1})])
    assert req.tenant == "default"
    assert len(req.entries) == 1


def test_filter_accepts_valid_op() -> None:
    flt = Filter(column="status", op="eq", value=200)
    assert flt.op == "eq"
    assert flt.column == "status"
    assert flt.value == 200


def test_filter_rejects_invalid_op() -> None:
    with pytest.raises(ValidationError):
        Filter(column="status", op="like", value="2%")  # type: ignore[arg-type]


def test_aggregation_allows_none_column_for_count() -> None:
    agg = Aggregation(op="count")
    assert agg.op == "count"
    assert agg.column is None


def test_aggregation_rejects_invalid_op() -> None:
    with pytest.raises(ValidationError):
        Aggregation(op="median", column="latency")  # type: ignore[arg-type]


def test_query_request_defaults() -> None:
    req = QueryRequest()
    assert req.tenant == "default"
    assert req.columns is None
    assert req.filters == []
    assert req.aggregations == []
    assert req.group_by == []
    assert req.limit is None
