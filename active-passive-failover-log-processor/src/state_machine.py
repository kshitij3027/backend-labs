"""Node state machine for the active-passive failover cluster.

The cluster runs a 5-state model and rejects every transition that isn't
in the table below. Each successful transition fires every registered
callback; a callback that raises is logged but does NOT block the other
callbacks or roll back the state change.

Valid transitions (anything else raises ``InvalidTransitionError``):

* ``INACTIVE -> STANDBY``  — boot as a passive node.
* ``INACTIVE -> PRIMARY``  — boot as the configured initial primary.
* ``INACTIVE -> FAILED``   — terminal error during startup.
* ``STANDBY  -> ELECTION`` — primary heartbeat went stale.
* ``STANDBY  -> PRIMARY``  — direct promotion (single-node bootstrap path).
* ``STANDBY  -> FAILED``   — terminal error from STANDBY.
* ``ELECTION -> PRIMARY``  — won the leader-lock race.
* ``ELECTION -> STANDBY``  — lost the race; rejoin as standby.
* ``ELECTION -> FAILED``   — election timed out / terminal error.
* ``PRIMARY  -> STANDBY``  — self-demote on lock loss / manual failover.
* ``PRIMARY  -> FAILED``   — terminal error from PRIMARY.

``FAILED`` is terminal — there are no outgoing transitions from it.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Awaitable, Callable, Union

from src.models import NodeState

logger = logging.getLogger(__name__)


# Type alias: callbacks may be sync OR async, returning either None or an
# Awaitable[None] (as Python typing exposes via Union).
TransitionCallback = Callable[
    [NodeState, NodeState, str],
    Union[Awaitable[None], None],
]


class InvalidTransitionError(RuntimeError):
    """Raised when ``transition_to()`` is called for a (from, to) pair not in the table."""


class NodeStateMachine:
    """Atomic, validated state machine guarded by an ``asyncio.Lock``.

    Callbacks registered via :py:meth:`on_transition` fire in registration
    order on every successful transition. They run inside the critical
    section so the observed ordering matches the real state changes; this
    means callbacks must be cheap (await microtasks only).

    The constructor does NOT enforce that ``initial_state`` be ``INACTIVE``
    — callers may bootstrap the machine into any state. The transition
    table still applies on every subsequent call.
    """

    # Allowed (from -> {to_states}) — class-level so it's the single source
    # of truth and tests can introspect it directly.
    _ALLOWED: dict[NodeState, set[NodeState]] = {
        NodeState.INACTIVE: {
            NodeState.STANDBY,
            NodeState.PRIMARY,
            NodeState.FAILED,
        },
        NodeState.STANDBY: {
            NodeState.ELECTION,
            NodeState.PRIMARY,  # single-node bootstrap path
            NodeState.FAILED,
        },
        NodeState.ELECTION: {
            NodeState.PRIMARY,
            NodeState.STANDBY,
            NodeState.FAILED,
        },
        NodeState.PRIMARY: {
            NodeState.STANDBY,
            NodeState.FAILED,
        },
        # FAILED is terminal — no outgoing transitions.
        NodeState.FAILED: set(),
    }

    def __init__(self, initial_state: NodeState, node_id: str) -> None:
        self._state: NodeState = initial_state
        self._node_id: str = node_id
        self._lock: asyncio.Lock = asyncio.Lock()
        self._callbacks: list[TransitionCallback] = []

    # --- public read-only accessors ----------------------------------------

    @property
    def state(self) -> NodeState:
        """Current state. Reading is lock-free — Python attribute reads are atomic."""
        return self._state

    @property
    def role(self) -> str:
        """Externally-visible role.

        Returns ``"primary"`` iff the machine is in ``PRIMARY``; every
        other state (including ``FAILED``) reports as ``"standby"`` so
        ``/role`` consumers see a stable two-value field.
        """
        return "primary" if self._state is NodeState.PRIMARY else "standby"

    @property
    def node_id(self) -> str:
        """Node identifier passed to the constructor."""
        return self._node_id

    def can_transition_to(self, new_state: NodeState) -> bool:
        """Pure check — does the transition table allow ``current -> new_state``?

        Does not mutate state and does not acquire the lock. Useful for
        tests and dashboards that want to display "can demote" / "can
        promote" without paying the lock cost.
        """
        return new_state in self._ALLOWED.get(self._state, set())

    # --- callback registration ---------------------------------------------

    def on_transition(self, callback: TransitionCallback) -> None:
        """Register a callback fired after every successful transition.

        Callbacks are called as ``cb(old_state, new_state, reason)``.
        They may be sync or async; async callbacks are awaited, sync
        callbacks are invoked directly. A callback that raises is logged
        via ``logger.exception`` and swallowed — it does not block any
        other callback and does not roll back the state change.
        """
        self._callbacks.append(callback)

    # --- mutation ----------------------------------------------------------

    async def transition_to(
        self,
        new_state: NodeState,
        *,
        reason: str = "",
    ) -> None:
        """Atomically move to ``new_state`` if the transition is allowed.

        On disallowed transitions, raises :class:`InvalidTransitionError`
        with a message containing the current state, the attempted target
        state, and the supplied ``reason`` (if any).

        On success, every registered callback is fired in registration
        order. Callbacks run inside the lock so observers see the same
        total order as state mutations.
        """
        async with self._lock:
            old_state = self._state
            allowed = self._ALLOWED.get(old_state, set())
            if new_state not in allowed:
                msg = (
                    f"invalid transition: {old_state.value} -> {new_state.value}"
                    f" (node={self._node_id}, reason={reason!r})"
                )
                raise InvalidTransitionError(msg)

            logger.info(
                "node %s state transition: %s -> %s (reason=%s)",
                self._node_id,
                old_state.value,
                new_state.value,
                reason or "<none>",
            )
            self._state = new_state

            for cb in self._callbacks:
                try:
                    result = cb(old_state, new_state, reason)
                    if inspect.isawaitable(result):
                        await result
                except Exception:
                    logger.exception(
                        "transition callback failed (node=%s, %s -> %s)",
                        self._node_id,
                        old_state.value,
                        new_state.value,
                    )
