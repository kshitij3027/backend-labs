"""Unit tests for :mod:`src.reporting.state_machine`.

The state machine is a tiny module but it's the single chokepoint that
keeps the ``Report.state`` column honest, so we want explicit coverage
of:

  * every legal edge in :data:`ALLOWED_TRANSITIONS` (forward path +
    short-circuit-to-FAILED edges),
  * a handful of illegal moves (terminal-to-anything, skip-ahead,
    backward), and
  * the string-input ergonomic path (callers most often pass
    ``Report.state`` directly off the ORM row).
"""
from __future__ import annotations

import pytest

from src.reporting.state_machine import (
    ALLOWED_TRANSITIONS,
    ReportState,
    assert_transition,
)


def test_legal_transitions() -> None:
    """Every (from, to) edge in ALLOWED_TRANSITIONS is accepted by the guard."""
    # Sanity check that the adjacency map has at least one legal edge
    # outbound from every non-terminal state (otherwise the for-loop
    # below would silently pass with zero assertions).
    non_terminal = {
        ReportState.PENDING,
        ReportState.AGGREGATING,
        ReportState.EXPORTING,
        ReportState.SIGNING,
    }
    for state in non_terminal:
        assert ALLOWED_TRANSITIONS[state], (
            f"Non-terminal state {state.value} has no outbound transitions"
        )

    for from_state, allowed in ALLOWED_TRANSITIONS.items():
        for to_state in allowed:
            # Should NOT raise.
            assert_transition(from_state, to_state)


def test_illegal_transitions_raise() -> None:
    """A handful of explicit illegal moves raise ValueError."""
    illegal_cases = [
        # Terminal -> anything is illegal.
        (ReportState.COMPLETED, ReportState.PENDING),
        (ReportState.COMPLETED, ReportState.AGGREGATING),
        (ReportState.FAILED, ReportState.AGGREGATING),
        (ReportState.FAILED, ReportState.COMPLETED),
        # Skip-ahead is illegal (must go through every phase).
        (ReportState.PENDING, ReportState.COMPLETED),
        (ReportState.PENDING, ReportState.EXPORTING),
        (ReportState.AGGREGATING, ReportState.SIGNING),
        # Backward is illegal.
        (ReportState.EXPORTING, ReportState.PENDING),
        (ReportState.SIGNING, ReportState.AGGREGATING),
    ]
    for from_state, to_state in illegal_cases:
        with pytest.raises(ValueError) as exc_info:
            assert_transition(from_state, to_state)
        # The error message names both states so debugging logs are
        # immediately actionable.
        assert from_state.value in str(exc_info.value)
        assert to_state.value in str(exc_info.value)


def test_string_inputs_are_accepted() -> None:
    """``assert_transition`` coerces string args (matches ``Report.state``'s storage type)."""
    # Legal — should not raise.
    assert_transition("PENDING", "AGGREGATING")
    assert_transition("AGGREGATING", "EXPORTING")
    assert_transition("SIGNING", "COMPLETED")
    assert_transition("EXPORTING", "FAILED")

    # Mixed enum + string also OK (one of each direction).
    assert_transition(ReportState.PENDING, "AGGREGATING")
    assert_transition("SIGNING", ReportState.COMPLETED)


def test_string_input_illegal_transition_raises() -> None:
    """String-form illegal moves still raise (not just enum-form)."""
    with pytest.raises(ValueError):
        assert_transition("COMPLETED", "PENDING")
    with pytest.raises(ValueError):
        assert_transition("PENDING", "COMPLETED")


def test_unknown_string_raises_value_error() -> None:
    """Bogus state strings raise ValueError (via ``ReportState(value)`` coercion)."""
    with pytest.raises(ValueError):
        assert_transition("NOT_A_REAL_STATE", "PENDING")
    with pytest.raises(ValueError):
        assert_transition("PENDING", "NOT_A_REAL_STATE")


def test_every_non_terminal_state_can_reach_failed() -> None:
    """The any-state->FAILED escape edge exists for every non-terminal state."""
    for state in (
        ReportState.PENDING,
        ReportState.AGGREGATING,
        ReportState.EXPORTING,
        ReportState.SIGNING,
    ):
        assert ReportState.FAILED in ALLOWED_TRANSITIONS[state]
        # And the guard agrees.
        assert_transition(state, ReportState.FAILED)


def test_terminal_states_have_empty_allowlist() -> None:
    """COMPLETED and FAILED are terminal — no outbound edges."""
    assert ALLOWED_TRANSITIONS[ReportState.COMPLETED] == set()
    assert ALLOWED_TRANSITIONS[ReportState.FAILED] == set()
