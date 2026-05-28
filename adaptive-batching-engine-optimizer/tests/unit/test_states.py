"""Unit tests for src.states — the optimizer's operating-state machine."""

from __future__ import annotations

from src.models import OptimizerState
from src.states import StateMachine


def _advance_to_optimizing(sm: StateMachine) -> None:
    """Drive a fresh machine from LEARNING to OPTIMIZING via enough samples."""
    for _ in range(sm.learning_samples):
        sm.update(breach=False, recovery_ready=False, stable=False)
    assert sm.state is OptimizerState.OPTIMIZING


# --- initial state -----------------------------------------------------------


def test_initial_state_is_learning() -> None:
    assert StateMachine(learning_samples=3).state is OptimizerState.LEARNING


def test_uses_settings_default_learning_samples_when_none() -> None:
    """Default learning_samples is 5 per settings."""
    sm = StateMachine()
    assert sm.learning_samples == 5


# --- LEARNING -> OPTIMIZING --------------------------------------------------


def test_learning_advances_to_optimizing_after_exactly_n_samples() -> None:
    sm = StateMachine(learning_samples=3)

    assert sm.update(breach=False, recovery_ready=False, stable=False) is OptimizerState.LEARNING
    assert sm.state is OptimizerState.LEARNING  # 1 sample
    assert sm.update(breach=False, recovery_ready=False, stable=False) is OptimizerState.LEARNING
    assert sm.state is OptimizerState.LEARNING  # 2 samples, still learning

    # The 3rd sample reaches learning_samples and flips to OPTIMIZING.
    assert (
        sm.update(breach=False, recovery_ready=False, stable=False)
        is OptimizerState.OPTIMIZING
    )
    assert sm.state is OptimizerState.OPTIMIZING


# --- OPTIMIZING <-> STABLE ---------------------------------------------------


def test_optimizing_settles_to_stable_when_stable_true() -> None:
    sm = StateMachine(learning_samples=3)
    _advance_to_optimizing(sm)

    assert (
        sm.update(breach=False, recovery_ready=False, stable=True) is OptimizerState.STABLE
    )
    assert sm.state is OptimizerState.STABLE


def test_optimizing_stays_optimizing_when_not_stable() -> None:
    sm = StateMachine(learning_samples=3)
    _advance_to_optimizing(sm)

    assert (
        sm.update(breach=False, recovery_ready=False, stable=False)
        is OptimizerState.OPTIMIZING
    )


def test_stable_drifts_back_to_optimizing_when_not_stable() -> None:
    sm = StateMachine(learning_samples=3)
    _advance_to_optimizing(sm)
    sm.update(breach=False, recovery_ready=False, stable=True)
    assert sm.state is OptimizerState.STABLE

    # Drift: stable signal drops -> back to OPTIMIZING.
    assert (
        sm.update(breach=False, recovery_ready=False, stable=False)
        is OptimizerState.OPTIMIZING
    )
    assert sm.state is OptimizerState.OPTIMIZING


def test_stable_stays_stable_while_stable_true() -> None:
    sm = StateMachine(learning_samples=3)
    _advance_to_optimizing(sm)
    sm.update(breach=False, recovery_ready=False, stable=True)
    assert (
        sm.update(breach=False, recovery_ready=False, stable=True) is OptimizerState.STABLE
    )


# --- any -> EMERGENCY on breach ----------------------------------------------


def test_breach_forces_emergency_from_learning() -> None:
    sm = StateMachine(learning_samples=3)
    assert sm.state is OptimizerState.LEARNING

    assert (
        sm.update(breach=True, recovery_ready=False, stable=False)
        is OptimizerState.EMERGENCY
    )
    assert sm.state is OptimizerState.EMERGENCY


def test_breach_forces_emergency_from_optimizing() -> None:
    sm = StateMachine(learning_samples=3)
    _advance_to_optimizing(sm)

    assert (
        sm.update(breach=True, recovery_ready=False, stable=False)
        is OptimizerState.EMERGENCY
    )
    assert sm.state is OptimizerState.EMERGENCY


def test_breach_forces_emergency_from_stable() -> None:
    sm = StateMachine(learning_samples=3)
    _advance_to_optimizing(sm)
    sm.update(breach=False, recovery_ready=False, stable=True)
    assert sm.state is OptimizerState.STABLE

    assert (
        sm.update(breach=True, recovery_ready=False, stable=False)
        is OptimizerState.EMERGENCY
    )
    assert sm.state is OptimizerState.EMERGENCY


# --- EMERGENCY -> OPTIMIZING on recovery -------------------------------------


def test_emergency_stays_emergency_while_not_recovery_ready() -> None:
    sm = StateMachine(learning_samples=3)
    sm.update(breach=True, recovery_ready=False, stable=False)
    assert sm.state is OptimizerState.EMERGENCY

    assert (
        sm.update(breach=False, recovery_ready=False, stable=False)
        is OptimizerState.EMERGENCY
    )
    # Even multiple non-ready ticks keep it pinned.
    assert (
        sm.update(breach=False, recovery_ready=False, stable=True)
        is OptimizerState.EMERGENCY
    )


def test_emergency_advances_to_optimizing_when_recovery_ready() -> None:
    sm = StateMachine(learning_samples=3)
    sm.update(breach=True, recovery_ready=False, stable=False)
    assert sm.state is OptimizerState.EMERGENCY

    assert (
        sm.update(breach=False, recovery_ready=True, stable=False)
        is OptimizerState.OPTIMIZING
    )
    assert sm.state is OptimizerState.OPTIMIZING


def test_emergency_resumes_in_optimizing_not_learning() -> None:
    """Recovery resumes at OPTIMIZING rather than re-running LEARNING."""
    sm = StateMachine(learning_samples=3)
    sm.update(breach=True, recovery_ready=False, stable=False)
    sm.update(breach=False, recovery_ready=True, stable=False)
    assert sm.state is OptimizerState.OPTIMIZING
    # A single non-stable optimizing tick stays OPTIMIZING (no learning replay).
    assert (
        sm.update(breach=False, recovery_ready=False, stable=False)
        is OptimizerState.OPTIMIZING
    )


# --- breach precedence -------------------------------------------------------


def test_breach_takes_priority_over_recovery_ready_and_stable() -> None:
    # From STABLE, breach wins even when stable is also true.
    sm = StateMachine(learning_samples=3)
    _advance_to_optimizing(sm)
    sm.update(breach=False, recovery_ready=False, stable=True)
    assert sm.state is OptimizerState.STABLE
    assert (
        sm.update(breach=True, recovery_ready=True, stable=True) is OptimizerState.EMERGENCY
    )

    # From EMERGENCY, breach wins even when recovery_ready is true.
    sm2 = StateMachine(learning_samples=3)
    sm2.update(breach=True, recovery_ready=False, stable=False)
    assert sm2.state is OptimizerState.EMERGENCY
    assert (
        sm2.update(breach=True, recovery_ready=True, stable=True)
        is OptimizerState.EMERGENCY
    )


# --- reset() -----------------------------------------------------------------


def test_reset_returns_to_learning_with_samples_cleared() -> None:
    sm = StateMachine(learning_samples=3)
    _advance_to_optimizing(sm)
    sm.update(breach=False, recovery_ready=False, stable=True)
    assert sm.state is OptimizerState.STABLE

    sm.reset()
    assert sm.state is OptimizerState.LEARNING

    # The sample counter is cleared: it needs the full learning_samples again.
    sm.update(breach=False, recovery_ready=False, stable=False)
    sm.update(breach=False, recovery_ready=False, stable=False)
    assert sm.state is OptimizerState.LEARNING  # only 2 samples since reset
    assert (
        sm.update(breach=False, recovery_ready=False, stable=False)
        is OptimizerState.OPTIMIZING
    )
