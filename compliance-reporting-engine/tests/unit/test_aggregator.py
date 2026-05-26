"""Unit tests for :mod:`src.reporting.aggregator`.

The aggregator is the seam between persistence and the rest of the
pipeline, so the tests exercise:

  * the canonical payload shape (every consumer downstream — exporters,
    signer, API responses — relies on the same five top-level keys),
  * the framework-not-registered guard,
  * round-tripping through the real seeder + repository (so any drift
    between event_type allowlists in the seeder and category mappings
    in the rules class would show up immediately).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.frameworks.sox import SOXRules
from src.logs.repository import insert_log_events
from src.logs.seeder import generate_synthetic_logs
from src.reporting.aggregator import build_report_payload


async def _seed_window(session_factory, count: int, frameworks: list[str]) -> tuple[datetime, datetime]:
    """Generate + insert ``count`` events over a fixed 7-day window, return bounds."""
    period_end = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    period_start = period_end - timedelta(days=7)
    events = generate_synthetic_logs(
        count,
        frameworks=frameworks,
        seed=42,
        period_start=period_start,
        period_end=period_end,
    )
    async with session_factory() as session:
        await insert_log_events(session, events)
        await session.commit()
    return period_start, period_end


async def test_aggregator_payload_shape(session_factory) -> None:
    """The returned dict carries the canonical five top-level keys + nested shapes."""
    period_start, period_end = await _seed_window(
        session_factory, count=30, frameworks=["SOX", "HIPAA"]
    )

    async with session_factory() as session:
        payload = await build_report_payload(
            session, "SOX", period_start, period_end
        )

    # --- Top-level keys ---
    assert set(payload.keys()) == {"framework", "period", "summary", "findings", "data"}

    # --- framework ---
    assert payload["framework"] == "SOX"

    # --- period: dict with ISO-8601 start/end ---
    assert isinstance(payload["period"], dict)
    assert payload["period"]["start"] == period_start.isoformat()
    assert payload["period"]["end"] == period_end.isoformat()

    # --- summary: keys are exactly SOX's declared categories, values are ints ---
    assert isinstance(payload["summary"], dict)
    assert set(payload["summary"].keys()) == set(SOXRules.categories)
    for value in payload["summary"].values():
        assert isinstance(value, int)
        assert value >= 0

    # --- findings: list of strings (may be empty for a tiny seeded window) ---
    assert isinstance(payload["findings"], list)
    for finding in payload["findings"]:
        assert isinstance(finding, str)

    # --- data.events: list of dicts with the LogEvent.to_dict() shape ---
    assert isinstance(payload["data"], dict)
    assert "events" in payload["data"]
    events = payload["data"]["events"]
    assert isinstance(events, list)
    # The seeder might have tagged everything HIPAA-only, so we don't
    # assert events is non-empty — but every event we DO have must be a
    # SOX-tagged dict with the canonical to_dict() keys.
    expected_event_keys = {
        "id", "timestamp", "framework_tags", "event_type", "actor",
        "resource", "action", "outcome", "sensitivity", "payload",
    }
    for event in events:
        assert isinstance(event, dict)
        assert expected_event_keys.issubset(event.keys())
        # Every event we see in a SOX report must carry the SOX tag.
        assert "SOX" in event["framework_tags"]


async def test_aggregator_summary_counts_match_classified_events(session_factory) -> None:
    """Summary counts equal what SOXRules.classify says about the seeded events."""
    period_start, period_end = await _seed_window(
        session_factory, count=200, frameworks=["SOX"]
    )

    # Fetch the same events through the repository (the aggregator
    # uses) and recompute the summary the same way the aggregator does —
    # we're checking the aggregator passes the right list to summarize().
    from src.logs.repository import query_logs_for_framework_in_window

    async with session_factory() as session:
        events = await query_logs_for_framework_in_window(
            session, "SOX", period_start, period_end
        )
        payload = await build_report_payload(
            session, "SOX", period_start, period_end
        )

    expected_summary = SOXRules.summarize(events)
    assert payload["summary"] == expected_summary
    # And the total events matches what the repository returned.
    assert len(payload["data"]["events"]) == len(events)


async def test_aggregator_unknown_framework_raises(session_factory) -> None:
    """A framework code that isn't in FRAMEWORK_REGISTRY raises ValueError immediately."""
    period_end = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    period_start = period_end - timedelta(days=7)

    async with session_factory() as session:
        with pytest.raises(ValueError) as exc_info:
            await build_report_payload(
                session, "NOPE", period_start, period_end
            )
    assert "NOPE" in str(exc_info.value)


async def test_aggregator_empty_window_yields_empty_data(session_factory) -> None:
    """A window with no logged events still returns the canonical shape (zero counts)."""
    # No seeding — the table is empty.
    period_end = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    period_start = period_end - timedelta(days=7)

    async with session_factory() as session:
        payload = await build_report_payload(
            session, "SOX", period_start, period_end
        )

    assert payload["framework"] == "SOX"
    assert payload["data"]["events"] == []
    # Summary is zero-filled for every category.
    assert payload["summary"] == {cat: 0 for cat in SOXRules.categories}
    # No events -> no findings rules fire.
    assert payload["findings"] == []


async def test_aggregator_pci_dss_passes_period_end_to_findings(session_factory) -> None:
    """PCI-DSS's widened findings signature is exercised via the period_end kwarg path."""
    # Seed a tiny window with PCI-DSS-tagged events so the
    # "key rotation overdue" rule fires (no key_rotation events at all
    # in the window -> finding emitted).
    period_end = datetime(2026, 5, 25, 12, 0, 0, tzinfo=timezone.utc)
    period_start = period_end - timedelta(days=7)

    events = generate_synthetic_logs(
        50,
        frameworks=["PCI_DSS"],
        seed=99,
        period_start=period_start,
        period_end=period_end,
    )
    async with session_factory() as session:
        await insert_log_events(session, events)
        await session.commit()

    async with session_factory() as session:
        payload = await build_report_payload(
            session, "PCI_DSS", period_start, period_end
        )

    # findings is a list, period_end-aware rule didn't blow up.
    assert isinstance(payload["findings"], list)
