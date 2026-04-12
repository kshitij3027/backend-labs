"""Tests for the Markov chain event simulator."""
from __future__ import annotations

import asyncio
import math

import pytest

from src.simulator.generator import (
    ALL_PROFILES,
    CASUAL_BROWSER,
    ACTIVE_SHOPPER,
    POWER_USER,
    BOUNCER,
    EventSimulator,
)
from src.models import Event


class TestGenerateEvent:
    """Test single event generation."""

    def test_generate_event_valid(self):
        """Generated Event has all required fields with correct types."""
        sim = EventSimulator(num_users=5, events_per_second=10.0)
        event = sim.generate_event()

        assert isinstance(event, Event)
        assert event.user_id is not None and event.user_id.startswith("sim_user_")
        assert event.event_type is not None and len(event.event_type) > 0
        assert event.timestamp is not None
        assert event.device_type in ("desktop", "mobile", "tablet")
        assert event.page_url is not None and event.page_url.startswith("/")
        assert isinstance(event.metadata, dict)
        assert event.metadata.get("simulator") is True


class TestMarkovTransitions:
    """Verify transition probability matrices are well-formed."""

    @pytest.mark.parametrize("profile", ALL_PROFILES, ids=lambda p: p["name"])
    def test_markov_transitions_sum_to_one(self, profile):
        """Each state's transition probabilities must sum to ~1.0."""
        for state, transitions in profile["transitions"].items():
            total = sum(transitions.values())
            assert math.isclose(total, 1.0, abs_tol=1e-9), (
                f"Profile '{profile['name']}', state '{state}': "
                f"transitions sum to {total}, expected 1.0"
            )


class TestProfileEventGeneration:
    """Verify events come from the correct profile transitions."""

    @pytest.mark.parametrize(
        "profile",
        [CASUAL_BROWSER, ACTIVE_SHOPPER, POWER_USER, BOUNCER],
        ids=lambda p: p["name"],
    )
    def test_all_profiles_generate_events(self, profile):
        """Events generated for a profile use only event types from that profile's transitions."""
        sim = EventSimulator(num_users=1, events_per_second=10.0)
        # Force the single user to use the specified profile
        user_id = list(sim._user_states.keys())[0]
        sim._user_states[user_id]["profile"] = profile
        sim._user_states[user_id]["current_event"] = profile["start_event"]

        valid_event_types = set()
        for transitions in profile["transitions"].values():
            valid_event_types.update(transitions.keys())

        for _ in range(50):
            event = sim.generate_event(user_id)
            assert event.event_type in valid_event_types, (
                f"Profile '{profile['name']}': unexpected event_type "
                f"'{event.event_type}' not in {valid_event_types}"
            )


class TestDeviceVariation:
    """Verify the simulator produces multiple device types."""

    def test_device_variation(self):
        """Over 100 events, at least 2 different device types should appear."""
        sim = EventSimulator(num_users=20, events_per_second=10.0)
        devices = set()
        for _ in range(100):
            event = sim.generate_event()
            devices.add(event.device_type)
        assert len(devices) >= 2, (
            f"Expected at least 2 device types, got {devices}"
        )


class TestSimulatorRate:
    """Verify the simulator produces events at approximately the target rate."""

    @pytest.mark.asyncio
    async def test_rate_approximately_correct(self):
        """Run simulator for 2s at 10 events/sec; expect 10-30 events (wide CI tolerance)."""
        sim = EventSimulator(num_users=5, events_per_second=10.0)
        stop_event = asyncio.Event()
        collected: list[Event] = []

        async def sink(event: Event) -> None:
            collected.append(event)

        task = asyncio.create_task(sim.run(sink, stop_event))

        await asyncio.sleep(2.0)
        stop_event.set()
        await task

        count = len(collected)
        assert 10 <= count <= 30, (
            f"Expected 10-30 events in 2s at 10/sec, got {count}"
        )
