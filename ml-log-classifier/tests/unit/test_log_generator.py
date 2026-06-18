"""Unit tests for :mod:`src.log_generator` (Commit 2).

These exercise the deterministic, template-driven synthetic log generator that
produces the labeled training corpus: exact count at the default size, the flat
5-key record schema, full per-class label coverage, byte-level determinism, the
realistic noise embedded in ``raw_log`` (timestamps/IPs/UUIDs/latency/id tokens),
JSONL round-tripping, ISO-8601 timestamp parseability, the canonical
DB-timeout-as-SYSTEM example, ``ValueError`` on negative counts, and the CLI.

All tests are hermetic: every file write targets pytest's ``tmp_path``, and the
generator depends only on a fixed base time + a seeded RNG (never ``now()``), so
nothing leaks between tests or depends on the wall clock.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path

import pytest

from src.log_generator import (
    CATEGORIES,
    SERVICES,
    SEVERITIES,
    generate_logs,
    main,
    read_jsonl,
    summarize,
    write_jsonl,
)

# ---------------------------------------------------------------------------
# Regexes for the noise tokens embedded in raw_log (matched across all 1000).
# ---------------------------------------------------------------------------

#: ISO-8601 with a millisecond ``Z`` suffix, e.g. ``2026-06-21T15:32:10.123Z``.
_ISO_MS_Z_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z")
#: Private-range IPv4 the generator emits, e.g. ``10.12.200.7``.
_IPV4_10_RE = re.compile(r"\b10\.\d{1,3}\.\d{1,3}\.\d{1,3}\b")
#: RFC-4122 v4 UUID (version nibble 4, variant nibble in [89ab]).
_UUID4_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b"
)
#: A latency token like ``123ms``.
_LATENCY_RE = re.compile(r"\b\d+ms\b")
#: A request/connection id token like ``req_id=...`` or ``conn_id=...``.
_REQ_CONN_ID_RE = re.compile(r"\b(?:req_id|conn_id)=")


@pytest.fixture(scope="module")
def logs() -> list[dict]:
    """A single default 1000-record corpus reused by the read-only assertions."""
    return generate_logs(1000, seed=42)


def test_default_count_is_exactly_1000(logs: list[dict]) -> None:
    """At the default size the floors fit, so the corpus is *exactly* 1000 long."""
    assert len(logs) == 1000


def test_records_have_exact_schema_and_valid_labels(logs: list[dict]) -> None:
    """Every record has exactly the 5 schema keys with in-taxonomy labels, and the
    ``service`` word plus ``[SEVERITY]`` token are embedded in ``raw_log``."""
    expected_keys = {"raw_log", "service", "severity", "category", "timestamp"}
    for rec in logs:
        assert set(rec) == expected_keys
        assert rec["service"] in SERVICES
        assert rec["severity"] in SEVERITIES
        assert rec["category"] in CATEGORIES
        assert isinstance(rec["raw_log"], str) and rec["raw_log"]
        # raw_log carries the labels as text for preprocessing to normalize.
        assert rec["service"] in rec["raw_log"]
        assert f"[{rec['severity']}]" in rec["raw_log"]


def test_full_label_coverage(logs: list[dict]) -> None:
    """``summarize`` reports a non-zero count for every service/severity/category."""
    summary = summarize(logs)
    assert summary["total"] == 1000
    assert set(summary["by_service"]) == set(SERVICES)
    assert set(summary["by_severity"]) == set(SEVERITIES)
    assert set(summary["by_category"]) == set(CATEGORIES)
    assert all(n > 0 for n in summary["by_service"].values())
    assert all(n > 0 for n in summary["by_severity"].values())
    assert all(n > 0 for n in summary["by_category"].values())


def test_determinism_same_seed_identical(logs: list[dict]) -> None:
    """Same ``(count, seed)`` yields an identical record list (byte-for-byte)."""
    again = generate_logs(1000, seed=42)
    assert again == logs
    # Stronger: identical once serialized, proving no hidden non-determinism.
    assert json.dumps(again) == json.dumps(logs)


def test_determinism_different_seed_differs(logs: list[dict]) -> None:
    """A different seed produces a different corpus."""
    other = generate_logs(1000, seed=7)
    assert other != logs


def test_noise_tokens_present_across_corpus(logs: list[dict]) -> None:
    """The realistic noise tokens each appear at least once across all raw_logs."""
    blob = "\n".join(rec["raw_log"] for rec in logs)
    assert _ISO_MS_Z_RE.search(blob), "no ISO-ms-Z timestamp found"
    assert _IPV4_10_RE.search(blob), "no 10.x.x.x IPv4 found"
    assert _UUID4_RE.search(blob), "no UUIDv4 found"
    assert _LATENCY_RE.search(blob), "no NNNms latency token found"
    assert _REQ_CONN_ID_RE.search(blob), "no req_id=/conn_id= token found"


def test_write_then_read_jsonl_round_trip(
    logs: list[dict], tmp_path: Path
) -> None:
    """``write_jsonl`` followed by ``read_jsonl`` reproduces the list exactly."""
    out = tmp_path / "logs.jsonl"
    write_jsonl(logs, str(out))
    assert out.exists()
    # One JSON object per line, no blank trailing record.
    lines = out.read_text(encoding="utf-8").splitlines()
    assert len(lines) == len(logs)
    assert read_jsonl(str(out)) == logs


def test_timestamp_parses_as_iso8601(logs: list[dict]) -> None:
    """Each record's ``timestamp`` is a valid ISO-8601 string."""
    for rec in logs:
        parsed = datetime.fromisoformat(rec["timestamp"])
        assert isinstance(parsed, datetime)


def test_canonical_db_timeout_is_labeled_system(logs: list[dict]) -> None:
    """The canonical DB connection/timeout failure is represented as SYSTEM.

    At least one (service=database, severity=ERROR, category=SYSTEM) record's
    ``raw_log`` mentions both a connection failure and a timeout.
    """
    matches = [
        rec
        for rec in logs
        if rec["service"] == "database"
        and rec["severity"] == "ERROR"
        and rec["category"] == "SYSTEM"
    ]
    assert matches, "no (database, ERROR, SYSTEM) record generated"
    assert any(
        "connection failed" in rec["raw_log"].lower()
        and "timeout" in rec["raw_log"].lower()
        for rec in matches
    ), "canonical DB-timeout SYSTEM message not found"


def test_negative_count_raises_value_error() -> None:
    """A negative ``count`` is rejected with ``ValueError``."""
    with pytest.raises(ValueError):
        generate_logs(-1)


def test_small_count_floors_win_and_cover_classes() -> None:
    """For small counts the per-class floors win, so length >= count and every
    class is still represented (the trainability guarantee)."""
    logs = generate_logs(10, seed=42)
    assert len(logs) >= 10
    summary = summarize(logs)
    assert all(n > 0 for n in summary["by_severity"].values())
    assert all(n > 0 for n in summary["by_category"].values())


def test_main_cli_writes_nonempty_jsonl(tmp_path: Path) -> None:
    """``main`` returns 0 and writes a non-empty, valid JSONL file."""
    out = tmp_path / "out.jsonl"
    rc = main(["--count", "20", "--out", str(out), "--seed", "42"])
    assert rc == 0
    assert out.exists()
    lines = out.read_text(encoding="utf-8").splitlines()
    assert lines, "CLI produced an empty file"
    # Every line is a valid JSON record with the full schema.
    for line in lines:
        rec = json.loads(line)
        assert set(rec) == {
            "raw_log",
            "service",
            "severity",
            "category",
            "timestamp",
        }
