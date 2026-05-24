"""Erasure request state machine — allowed transitions + guard."""
from __future__ import annotations

from src.persistence.models import RequestState


# Allowed forward transitions. Any state can also transition to FAILED.
_ALLOWED: dict[RequestState, set[RequestState]] = {
    RequestState.PENDING: {RequestState.DISCOVERING, RequestState.FAILED},
    RequestState.DISCOVERING: {RequestState.EXECUTING, RequestState.FAILED},
    RequestState.EXECUTING: {RequestState.VERIFYING, RequestState.FAILED, RequestState.COMPLETED},
    # If verification is disabled, EXECUTING can jump directly to COMPLETED.
    RequestState.VERIFYING: {RequestState.COMPLETED, RequestState.FAILED},
    RequestState.COMPLETED: set(),  # terminal
    RequestState.FAILED: set(),     # terminal
}


class InvalidTransitionError(RuntimeError):
    pass


def is_terminal(state: RequestState) -> bool:
    return state in (RequestState.COMPLETED, RequestState.FAILED)


def assert_transition(current: RequestState, target: RequestState) -> None:
    if target not in _ALLOWED.get(current, set()):
        raise InvalidTransitionError(
            f"illegal transition {current.value} -> {target.value}"
        )
