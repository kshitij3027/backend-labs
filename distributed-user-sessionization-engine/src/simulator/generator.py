"""Background event simulator with Markov chain user behavior profiles."""
from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Callable, Awaitable

from src.models import Event

logger = logging.getLogger(__name__)

# Page URL pools
PAGES = [
    "/home", "/products", "/products/widget", "/products/gadget",
    "/search", "/search?q=deals", "/cart", "/checkout",
    "/account", "/about", "/help", "/blog",
]

DEVICES = ["desktop", "mobile", "tablet"]

# ── User Profiles (Markov Transition Matrices) ──
# Each profile maps: event_type -> {next_event_type: probability}

CASUAL_BROWSER = {
    "name": "casual_browser",
    "transitions": {
        "page_view": {"page_view": 0.50, "click": 0.30, "search": 0.10, "logout": 0.10},
        "click": {"page_view": 0.60, "click": 0.20, "search": 0.10, "logout": 0.10},
        "search": {"page_view": 0.50, "click": 0.20, "search": 0.20, "logout": 0.10},
        "logout": {"page_view": 1.0},  # restart
    },
    "start_event": "page_view",
    "device_weights": {"desktop": 0.3, "mobile": 0.5, "tablet": 0.2},
    "avg_session_events": 5,
}

ACTIVE_SHOPPER = {
    "name": "active_shopper",
    "transitions": {
        "page_view": {"page_view": 0.20, "click": 0.20, "search": 0.30, "add_to_cart": 0.20, "logout": 0.10},
        "click": {"page_view": 0.30, "search": 0.20, "add_to_cart": 0.30, "click": 0.10, "logout": 0.10},
        "search": {"page_view": 0.30, "click": 0.30, "search": 0.10, "add_to_cart": 0.20, "logout": 0.10},
        "add_to_cart": {"page_view": 0.20, "click": 0.10, "search": 0.10, "purchase": 0.40, "add_to_cart": 0.10, "logout": 0.10},
        "purchase": {"page_view": 0.30, "logout": 0.70},
        "logout": {"page_view": 1.0},
    },
    "start_event": "page_view",
    "device_weights": {"desktop": 0.5, "mobile": 0.3, "tablet": 0.2},
    "avg_session_events": 12,
}

POWER_USER = {
    "name": "power_user",
    "transitions": {
        "page_view": {"page_view": 0.15, "click": 0.30, "search": 0.30, "add_to_cart": 0.15, "logout": 0.10},
        "click": {"page_view": 0.20, "click": 0.25, "search": 0.25, "add_to_cart": 0.20, "logout": 0.10},
        "search": {"page_view": 0.15, "click": 0.25, "search": 0.20, "add_to_cart": 0.25, "purchase": 0.10, "logout": 0.05},
        "add_to_cart": {"page_view": 0.10, "click": 0.15, "search": 0.15, "add_to_cart": 0.10, "purchase": 0.40, "logout": 0.10},
        "purchase": {"page_view": 0.40, "search": 0.30, "logout": 0.30},
        "logout": {"page_view": 1.0},
    },
    "start_event": "page_view",
    "device_weights": {"desktop": 0.7, "mobile": 0.2, "tablet": 0.1},
    "avg_session_events": 20,
}

BOUNCER = {
    "name": "bouncer",
    "transitions": {
        "page_view": {"page_view": 0.30, "click": 0.20, "logout": 0.50},
        "click": {"page_view": 0.30, "logout": 0.70},
        "logout": {"page_view": 1.0},
    },
    "start_event": "page_view",
    "device_weights": {"desktop": 0.2, "mobile": 0.6, "tablet": 0.2},
    "avg_session_events": 2,
}

ALL_PROFILES = [CASUAL_BROWSER, ACTIVE_SHOPPER, POWER_USER, BOUNCER]
PROFILE_WEIGHTS = [0.35, 0.30, 0.20, 0.15]  # casual, shopper, power, bouncer


def _weighted_choice(choices: dict[str, float]) -> str:
    """Pick a random key weighted by values."""
    items = list(choices.items())
    keys = [k for k, _ in items]
    weights = [w for _, w in items]
    return random.choices(keys, weights=weights, k=1)[0]


class EventSimulator:
    """Generates realistic user behavior events using Markov chains."""

    def __init__(self, num_users: int, events_per_second: float):
        self._num_users = num_users
        self._events_per_second = events_per_second
        self._user_states: dict[str, dict] = {}  # user_id -> {profile, current_event, device, event_count}
        self._initialize_users()

    def _initialize_users(self) -> None:
        for i in range(self._num_users):
            user_id = f"sim_user_{i:04d}"
            profile = random.choices(ALL_PROFILES, PROFILE_WEIGHTS, k=1)[0]
            device = _weighted_choice(profile["device_weights"])
            self._user_states[user_id] = {
                "profile": profile,
                "current_event": profile["start_event"],
                "device": device,
                "event_count": 0,
            }

    def generate_event(self, user_id: str | None = None) -> Event:
        """Generate the next event for a user (or random user if not specified)."""
        if user_id is None:
            user_id = random.choice(list(self._user_states.keys()))

        state = self._user_states[user_id]
        profile = state["profile"]
        current = state["current_event"]

        # Get next event type from Markov chain
        transitions = profile["transitions"].get(current, {"page_view": 1.0})
        next_event = _weighted_choice(transitions)

        # If logout, might switch device on "restart"
        if next_event == "logout":
            state["event_count"] = 0
            # Possibly switch device after logout
            if random.random() < 0.3:
                state["device"] = _weighted_choice(profile["device_weights"])

        page = random.choice(PAGES)
        event = Event(
            user_id=user_id,
            event_type=next_event,
            timestamp=datetime.now(timezone.utc),
            device_type=state["device"],
            page_url=page,
            metadata={"url": page, "simulator": True},
        )

        state["current_event"] = next_event
        state["event_count"] += 1
        return event

    async def run(
        self,
        sink: Callable[[Event], Awaitable[None]],
        stop_event: asyncio.Event,
    ) -> None:
        """Generate events at the configured rate until stop_event is set."""
        interval = 1.0 / max(self._events_per_second, 0.1)
        total_generated = 0
        logger.info(
            "Simulator started: %d users, %.1f events/sec",
            self._num_users, self._events_per_second,
        )
        while not stop_event.is_set():
            try:
                event = self.generate_event()
                await sink(event)
                total_generated += 1
                # Use exponential inter-arrival time for realism
                wait = random.expovariate(self._events_per_second)
                await asyncio.wait_for(stop_event.wait(), timeout=wait)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Simulator error")
                await asyncio.sleep(0.1)
        logger.info("Simulator stopped after %d events", total_generated)
