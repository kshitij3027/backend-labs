import pytest
from src.erasure.state_machine import (
    InvalidTransitionError, assert_transition, is_terminal,
)
from src.persistence.models import RequestState


def test_pending_to_discovering_allowed():
    assert_transition(RequestState.PENDING, RequestState.DISCOVERING)


def test_discovering_to_executing_allowed():
    assert_transition(RequestState.DISCOVERING, RequestState.EXECUTING)


def test_executing_to_verifying_allowed():
    assert_transition(RequestState.EXECUTING, RequestState.VERIFYING)


def test_verifying_to_completed_allowed():
    assert_transition(RequestState.VERIFYING, RequestState.COMPLETED)


def test_any_state_can_go_to_failed():
    for s in (RequestState.PENDING, RequestState.DISCOVERING, RequestState.EXECUTING, RequestState.VERIFYING):
        assert_transition(s, RequestState.FAILED)


def test_terminal_states_have_no_transitions():
    for s in (RequestState.COMPLETED, RequestState.FAILED):
        for target in RequestState:
            with pytest.raises(InvalidTransitionError):
                assert_transition(s, target)


def test_skipping_a_state_raises():
    with pytest.raises(InvalidTransitionError):
        assert_transition(RequestState.PENDING, RequestState.EXECUTING)
    with pytest.raises(InvalidTransitionError):
        assert_transition(RequestState.DISCOVERING, RequestState.VERIFYING)


def test_executing_can_skip_verifying_when_disabled():
    # The coordinator with verification_enabled=False jumps EXECUTING → COMPLETED.
    assert_transition(RequestState.EXECUTING, RequestState.COMPLETED)


def test_is_terminal():
    assert is_terminal(RequestState.COMPLETED) is True
    assert is_terminal(RequestState.FAILED) is True
    assert is_terminal(RequestState.PENDING) is False
