"""Unit tests for the deterministic synthetic log generator (C4).

Covers: shape/validity, reproducibility (determinism), family/level/service
coverage, the embedded temporal signal (non-uniform error distribution with an
elevated 02:00 hour), corpus writing, and the committed ``data/sample.jsonl``.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

import pytest

from src.log_generator import (
    FAMILIES,
    SERVICES,
    generate_logs,
    generate_pattern_batch,
    write_corpus,
)
from src.schemas import LogEntry

# Repo root = two levels up from this test file (tests/unit/ -> repo root).
REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE_PATH = REPO_ROOT / "data" / "sample.jsonl"


def _serialize(logs: list[LogEntry]) -> list[dict]:
    return [entry.model_dump(mode="json") for entry in logs]


# --------------------------------------------------------------------------- #
# Shape & validity
# --------------------------------------------------------------------------- #


def test_generate_logs_returns_exact_count_of_valid_entries() -> None:
    logs = generate_logs(100, seed=7)
    assert len(logs) == 100
    assert all(isinstance(entry, LogEntry) for entry in logs)


def test_generate_logs_zero_returns_empty() -> None:
    assert generate_logs(0) == []


def test_generate_logs_sorted_by_timestamp() -> None:
    logs = generate_logs(200, seed=3)
    timestamps = [entry.timestamp for entry in logs]
    assert timestamps == sorted(timestamps)


# --------------------------------------------------------------------------- #
# Determinism
# --------------------------------------------------------------------------- #


def test_same_seed_produces_identical_output() -> None:
    first = generate_logs(150, seed=7)
    second = generate_logs(150, seed=7)
    assert _serialize(first) == _serialize(second)


def test_different_seed_produces_different_output() -> None:
    a = generate_logs(150, seed=7)
    b = generate_logs(150, seed=8)
    assert _serialize(a) != _serialize(b)


def test_default_window_is_fixed_and_deterministic() -> None:
    # No start/now passed => anchored to the fixed reference, so repeated calls
    # (even across "runs") are byte-for-byte identical.
    assert _serialize(generate_logs(50)) == _serialize(generate_logs(50))


# --------------------------------------------------------------------------- #
# Coverage: services, levels, families/keywords, optional fields
# --------------------------------------------------------------------------- #


def test_multiple_services_and_levels_appear() -> None:
    logs = generate_logs(800, seed=42)
    services = {entry.service for entry in logs}
    levels = {entry.level for entry in logs}
    # All generated services come from the public SERVICES list.
    assert services.issubset(set(SERVICES))
    assert len(services) >= 4
    assert len(levels) >= 3
    # The marquee anomaly level shows up somewhere.
    assert "CRITICAL" in levels


def test_security_performance_error_messages_all_appear() -> None:
    logs = generate_logs(800, seed=42)
    blob = " ".join(entry.message for entry in logs).lower()
    # security
    assert "failed login" in blob
    assert "brute force" in blob
    # performance
    assert "slow query" in blob
    assert "timeout" in blob
    # error
    assert "exception" in blob
    assert "disk full" in blob


def test_some_logs_have_response_time_and_status_code() -> None:
    logs = generate_logs(300, seed=11)
    assert any(entry.response_time_ms is not None for entry in logs)
    assert any(entry.status_code is not None for entry in logs)
    # Performance family should push some latencies into the elevated band.
    assert any((entry.response_time_ms or 0) >= 800 for entry in logs)
    # Error/security families should produce some 5xx/4xx statuses.
    assert any((entry.status_code or 0) >= 500 for entry in logs)


# --------------------------------------------------------------------------- #
# generate_pattern_batch
# --------------------------------------------------------------------------- #


def test_pattern_batch_security_is_all_security() -> None:
    logs = generate_pattern_batch("security", 60, seed=5)
    assert len(logs) == 60
    assert all(isinstance(entry, LogEntry) for entry in logs)
    # Every record is auth-service security traffic.
    assert all(entry.service == "auth" for entry in logs)
    blob = " ".join(entry.message for entry in logs).lower()
    assert any(kw in blob for kw in ("failed login", "brute force", "unauthorized", "token"))


def test_pattern_batch_performance_elevates_latency() -> None:
    logs = generate_pattern_batch("performance", 60, seed=5)
    assert all((entry.response_time_ms or 0) >= 800 for entry in logs)


def test_pattern_batch_is_deterministic() -> None:
    a = generate_pattern_batch("error", 40, seed=9)
    b = generate_pattern_batch("error", 40, seed=9)
    assert _serialize(a) == _serialize(b)


def test_pattern_batch_rejects_unknown_family() -> None:
    with pytest.raises(ValueError):
        generate_pattern_batch("bogus", 10)
    # Sanity: the four documented families are all accepted.
    assert set(FAMILIES) == {"security", "performance", "error", "normal"}
    for family in FAMILIES:
        assert len(generate_pattern_batch(family, 3)) == 3


# --------------------------------------------------------------------------- #
# Temporal signal
# --------------------------------------------------------------------------- #


def test_error_distribution_is_temporally_non_uniform() -> None:
    """ERROR/CRITICAL logs are NOT spread uniformly across hour-of-day.

    Specifically the nightly-batch window (02:00) carries an elevated share of
    error/critical logs relative to the overall baseline — i.e. a temporal
    pattern is detectable. A larger sample is used so the statistic is stable.
    """
    logs = generate_logs(4000, seed=42)

    totals: dict[int, int] = defaultdict(int)
    errors: dict[int, int] = defaultdict(int)
    for entry in logs:
        hour = entry.timestamp.hour
        totals[hour] += 1
        if entry.level in ("ERROR", "CRITICAL"):
            errors[hour] += 1

    overall_share = sum(errors.values()) / len(logs)
    assert 0.0 < overall_share < 1.0  # there are some, but not all, errors

    # The 02:00 hour must be populated and its error share clearly elevated.
    assert totals[2] > 0
    hour02_share = errors[2] / totals[2]
    assert hour02_share > overall_share * 1.4

    # Distribution across hours is not flat: per-hour error shares vary.
    per_hour_shares = [errors[h] / totals[h] for h in range(24) if totals[h] > 0]
    spread = max(per_hour_shares) - min(per_hour_shares)
    assert spread > 0.1


def test_business_hours_have_more_performance_logs_than_overnight() -> None:
    """Performance issues concentrate in weekday business hours (temporal pattern 3)."""
    logs = generate_logs(4000, seed=42)
    perf_keywords = ("slow query", "high latency", "timeout", "pool exhausted", "gc pause")

    def is_perf(entry: LogEntry) -> bool:
        msg = entry.message.lower()
        return any(kw in msg for kw in perf_keywords)

    biz, biz_total, night, night_total = 0, 0, 0, 0
    for entry in logs:
        weekday = entry.timestamp.weekday()
        hour = entry.timestamp.hour
        if weekday < 5 and 9 <= hour < 17:
            biz_total += 1
            biz += int(is_perf(entry))
        elif hour < 6:
            night_total += 1
            night += int(is_perf(entry))

    assert biz_total > 0 and night_total > 0
    biz_share = biz / biz_total
    night_share = night / night_total
    assert biz_share > night_share


# --------------------------------------------------------------------------- #
# write_corpus
# --------------------------------------------------------------------------- #


def test_write_corpus_writes_parseable_jsonl(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "corpus.jsonl"
    count = write_corpus(str(target), n=120, seed=42)
    assert count == 120
    assert target.exists()

    lines = target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 120
    # Every line parses back into a LogEntry.
    parsed = [LogEntry(**json.loads(line)) for line in lines]
    assert len(parsed) == 120
    assert all(isinstance(entry, LogEntry) for entry in parsed)


def test_write_corpus_is_deterministic(tmp_path: Path) -> None:
    a = tmp_path / "a.jsonl"
    b = tmp_path / "b.jsonl"
    write_corpus(str(a), n=80, seed=42)
    write_corpus(str(b), n=80, seed=42)
    assert a.read_text(encoding="utf-8") == b.read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Committed sample corpus
# --------------------------------------------------------------------------- #


def test_committed_sample_corpus_is_valid() -> None:
    assert SAMPLE_PATH.exists(), f"missing committed corpus at {SAMPLE_PATH}"
    lines = SAMPLE_PATH.read_text(encoding="utf-8").splitlines()
    assert len(lines) >= 500

    # First and last lines parse as LogEntry.
    first = LogEntry(**json.loads(lines[0]))
    last = LogEntry(**json.loads(lines[-1]))
    assert isinstance(first, LogEntry)
    assert isinstance(last, LogEntry)
    # Corpus is time-ordered.
    assert first.timestamp <= last.timestamp


def test_committed_sample_corpus_has_pattern_variety() -> None:
    lines = SAMPLE_PATH.read_text(encoding="utf-8").splitlines()
    entries = [LogEntry(**json.loads(line)) for line in lines]
    services = Counter(entry.service for entry in entries)
    levels = Counter(entry.level for entry in entries)
    assert len(services) >= 4
    assert len(levels) >= 3
