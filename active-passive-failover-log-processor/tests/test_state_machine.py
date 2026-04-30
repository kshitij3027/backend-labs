"""Tests for src/state_machine.py — every transition + every callback path."""

from __future__ import annotations

import asyncio

import pytest

from src.models import NodeState
from src.state_machine import (
    InvalidTransitionError,
    NodeStateMachine,
)


# Materialised once per test run for parametrisation. We rebuild the
# (from, to) pairs from the class-level _ALLOWED so new entries get
# coverage automatically.
_ALL_VALID: list[tuple[NodeState, NodeState]] = sorted(
    (
        (from_state, to_state)
        for from_state, allowed in NodeStateMachine._ALLOWED.items()
        for to_state in allowed
    ),
    key=lambda pair: (pair[0].value, pair[1].value),
)


def _all_invalid() -> list[tuple[NodeState, NodeState]]:
    """Every (from, to) pair NOT in the allowed table — every disallowed transition."""
    pairs: list[tuple[NodeState, NodeState]] = []
    for from_state in NodeState:
        for to_state in NodeState:
            allowed = NodeStateMachine._ALLOWED.get(from_state, set())
            if to_state not in allowed:
                pairs.append((from_state, to_state))
    return pairs


# --- starting state --------------------------------------------------------


async def test_initial_state_is_what_caller_passes() -> None:
    """The ctor accepts whatever the caller hands it — no implicit reset."""
    for state in NodeState:
        sm = NodeStateMachine(initial_state=state, node_id="node-test")
        assert sm.state is state


async def test_node_id_is_exposed() -> None:
    sm = NodeStateMachine(initial_state=NodeState.INACTIVE, node_id="node-7")
    assert sm.node_id == "node-7"


# --- happy paths -----------------------------------------------------------


@pytest.mark.parametrize("from_state,to_state", _ALL_VALID)
async def test_every_allowed_transition_succeeds(
    from_state: NodeState, to_state: NodeState
) -> None:
    sm = NodeStateMachine(initial_state=from_state, node_id="node-test")
    assert sm.can_transition_to(to_state) is True
    await sm.transition_to(to_state, reason="happy-path")
    assert sm.state is to_state


# --- role property ---------------------------------------------------------


async def test_role_is_primary_only_when_state_is_primary() -> None:
    for state in NodeState:
        sm = NodeStateMachine(initial_state=state, node_id="node-test")
        if state is NodeState.PRIMARY:
            assert sm.role == "primary"
        else:
            assert sm.role == "standby", f"state {state} should map to standby"


async def test_role_flips_with_state() -> None:
    sm = NodeStateMachine(initial_state=NodeState.INACTIVE, node_id="node-test")
    assert sm.role == "standby"
    await sm.transition_to(NodeState.PRIMARY)
    assert sm.role == "primary"
    await sm.transition_to(NodeState.STANDBY)
    assert sm.role == "standby"


# --- invalid transitions ---------------------------------------------------


@pytest.mark.parametrize("from_state,to_state", _all_invalid())
async def test_every_disallowed_transition_raises(
    from_state: NodeState, to_state: NodeState
) -> None:
    sm = NodeStateMachine(initial_state=from_state, node_id="node-test")
    assert sm.can_transition_to(to_state) is False
    with pytest.raises(InvalidTransitionError):
        await sm.transition_to(to_state)
    assert sm.state is from_state  # state unchanged on rejection


async def test_invalid_transition_message_mentions_states_and_reason() -> None:
    """The error message must include from-state, to-state, and the supplied reason
    so an operator can debug a wedged node from a single log line."""
    sm = NodeStateMachine(initial_state=NodeState.INACTIVE, node_id="node-test")
    with pytest.raises(InvalidTransitionError) as exc_info:
        await sm.transition_to(NodeState.ELECTION, reason="bogus-call")
    msg = str(exc_info.value)
    assert "INACTIVE" in msg
    assert "ELECTION" in msg
    assert "bogus-call" in msg


async def test_failed_is_terminal() -> None:
    """FAILED has no outgoing transitions — every target is rejected."""
    for target in NodeState:
        sm = NodeStateMachine(initial_state=NodeState.FAILED, node_id="node-test")
        with pytest.raises(InvalidTransitionError):
            await sm.transition_to(target)


# --- can_transition_to is pure --------------------------------------------


async def test_can_transition_to_does_not_mutate() -> None:
    sm = NodeStateMachine(initial_state=NodeState.INACTIVE, node_id="node-test")
    sm.can_transition_to(NodeState.PRIMARY)  # allowed
    sm.can_transition_to(NodeState.ELECTION)  # disallowed
    assert sm.state is NodeState.INACTIVE


# --- callbacks: sync + async ----------------------------------------------


async def test_async_callback_fires_after_successful_transition() -> None:
    sm = NodeStateMachine(initial_state=NodeState.INACTIVE, node_id="node-test")
    fired = asyncio.Event()
    captured: list[tuple[NodeState, NodeState, str]] = []

    async def cb(old: NodeState, new: NodeState, reason: str) -> None:
        captured.append((old, new, reason))
        fired.set()

    sm.on_transition(cb)
    await sm.transition_to(NodeState.STANDBY, reason="boot")
    await asyncio.wait_for(fired.wait(), timeout=1.0)
    assert captured == [(NodeState.INACTIVE, NodeState.STANDBY, "boot")]


async def test_sync_callback_fires_after_successful_transition() -> None:
    sm = NodeStateMachine(initial_state=NodeState.INACTIVE, node_id="node-test")
    captured: list[tuple[NodeState, NodeState, str]] = []

    def cb(old: NodeState, new: NodeState, reason: str) -> None:
        captured.append((old, new, reason))

    sm.on_transition(cb)
    await sm.transition_to(NodeState.PRIMARY, reason="manual-bootstrap")
    assert captured == [(NodeState.INACTIVE, NodeState.PRIMARY, "manual-bootstrap")]


async def test_mixed_sync_and_async_callbacks_both_fire_in_order() -> None:
    sm = NodeStateMachine(initial_state=NodeState.INACTIVE, node_id="node-test")
    order: list[str] = []

    def sync_cb(old: NodeState, new: NodeState, reason: str) -> None:
        order.append("sync")

    async def async_cb(old: NodeState, new: NodeState, reason: str) -> None:
        order.append("async")

    sm.on_transition(sync_cb)
    sm.on_transition(async_cb)
    await sm.transition_to(NodeState.STANDBY)
    assert order == ["sync", "async"]


# --- callbacks: failure isolation -----------------------------------------


async def test_callback_exception_does_not_abort_other_callbacks() -> None:
    sm = NodeStateMachine(initial_state=NodeState.INACTIVE, node_id="node-test")
    saw: list[str] = []

    def bad(old: NodeState, new: NodeState, reason: str) -> None:
        saw.append("bad-called")
        raise RuntimeError("boom")

    async def bad_async(old: NodeState, new: NodeState, reason: str) -> None:
        saw.append("bad-async-called")
        raise RuntimeError("boom-async")

    def good(old: NodeState, new: NodeState, reason: str) -> None:
        saw.append("good-called")

    sm.on_transition(bad)
    sm.on_transition(bad_async)
    sm.on_transition(good)

    # Should NOT raise, despite two callbacks blowing up.
    await sm.transition_to(NodeState.STANDBY)

    assert sm.state is NodeState.STANDBY
    assert saw == ["bad-called", "bad-async-called", "good-called"]


async def test_callback_not_invoked_for_invalid_transition() -> None:
    sm = NodeStateMachine(initial_state=NodeState.INACTIVE, node_id="node-test")
    fired: list[None] = []

    async def cb(old: NodeState, new: NodeState, reason: str) -> None:
        fired.append(None)

    sm.on_transition(cb)
    with pytest.raises(InvalidTransitionError):
        await sm.transition_to(NodeState.ELECTION)
    assert fired == []


# --- concurrency -----------------------------------------------------------


async def test_concurrent_transitions_are_serialised() -> None:
    """Two coroutines racing to transition: the asyncio.Lock guarantees
    they run sequentially. We pick a scenario where the first transition
    moves the state into FAILED (terminal), so the second transition's
    legality check ALWAYS sees FAILED and rejects — no flakiness from
    scheduling order.

    To make ordering deterministic we give the "B" call a tiny head-start
    pause; under Python's asyncio, t1 is created first and the cooperative
    scheduler picks it up first, so t1 acquires the lock first.
    """
    sm = NodeStateMachine(initial_state=NodeState.PRIMARY, node_id="node-test")

    results: list[str] = []

    async def call_a() -> None:
        try:
            await sm.transition_to(NodeState.FAILED, reason="A")
            results.append("a-ok")
        except InvalidTransitionError:
            results.append("a-rejected")

    async def call_b() -> None:
        # Tiny await so call_a is guaranteed to grab the lock first.
        await asyncio.sleep(0.005)
        try:
            await sm.transition_to(NodeState.STANDBY, reason="B")
            results.append("b-ok")
        except InvalidTransitionError:
            results.append("b-rejected")

    await asyncio.gather(call_a(), call_b())

    # call_a wins, transitions PRIMARY -> FAILED.
    # call_b then tries FAILED -> STANDBY which is REJECTED (FAILED is terminal).
    assert results == ["a-ok", "b-rejected"]
    assert sm.state is NodeState.FAILED


async def test_concurrent_transitions_observe_coherent_state() -> None:
    """Even with a yielding async callback, no second transition can
    observe a half-applied state — the lock serialises the entire
    (mutate-then-fire-callbacks) critical section.
    """
    sm = NodeStateMachine(initial_state=NodeState.INACTIVE, node_id="node-test")
    seen: list[tuple[NodeState, NodeState]] = []

    async def cb(old: NodeState, new: NodeState, reason: str) -> None:
        # Yield mid-callback so the scheduler MAY try to interleave.
        await asyncio.sleep(0)
        seen.append((old, new))

    sm.on_transition(cb)

    async def t1() -> None:
        await sm.transition_to(NodeState.STANDBY, reason="t1")

    async def t2() -> None:
        # Slight delay so t1's transition lands first.
        await asyncio.sleep(0.005)
        await sm.transition_to(NodeState.ELECTION, reason="t2")

    await asyncio.gather(t1(), t2())

    assert sm.state is NodeState.ELECTION
    # Both callback invocations must observe coherent (old, new) pairs.
    assert seen == [
        (NodeState.INACTIVE, NodeState.STANDBY),
        (NodeState.STANDBY, NodeState.ELECTION),
    ]


