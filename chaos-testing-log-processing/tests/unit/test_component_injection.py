"""Unit tests for the C9 component-failure injection primitives.

Exercises ``src/injection/component.py`` with a mocked ``DockerClient``.
We assert that ``apply_component_action`` routes to the right Docker
lifecycle primitive for each action (pause/kill/restart), and that
``rollback`` undoes what was actually applied. The action state dict
returned by ``apply_component_action`` is the bookkeeping that the
rollback closure captures.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.injection.component import (
    ComponentInjectionError,
    VALID_ACTIONS,
    apply_component_action,
    rollback,
)


# --------------------------------------------------------------------------- #
# apply_component_action
# --------------------------------------------------------------------------- #


class TestApplyComponentAction:
    """Routing across pause / kill / restart + invalid-action surface."""

    def test_pause_calls_client_pause_and_returns_state(self) -> None:
        client = MagicMock(name="docker_client")

        state = apply_component_action(client, "log-consumer", action="pause")

        client.pause.assert_called_once_with("log-consumer")
        assert state == {"action": "pause"}

    def test_kill_calls_client_kill_with_sigkill_and_returns_state(self) -> None:
        client = MagicMock(name="docker_client")

        state = apply_component_action(client, "log-consumer", action="kill")

        client.kill.assert_called_once_with("log-consumer", signal="SIGKILL")
        assert state == {"action": "kill"}

    def test_restart_calls_client_restart_with_timeout_and_returns_state(
        self,
    ) -> None:
        client = MagicMock(name="docker_client")

        state = apply_component_action(client, "log-consumer", action="restart")

        client.restart.assert_called_once_with("log-consumer", timeout=5)
        assert state == {"action": "restart"}

    def test_default_action_is_pause(self) -> None:
        """No ``action`` kwarg -> defaults to pause."""
        client = MagicMock(name="docker_client")

        state = apply_component_action(client, "log-consumer")

        client.pause.assert_called_once_with("log-consumer")
        assert state == {"action": "pause"}
        # Sanity: nothing else got called.
        client.kill.assert_not_called()
        client.restart.assert_not_called()

    def test_unknown_action_raises_component_injection_error(self) -> None:
        client = MagicMock(name="docker_client")

        with pytest.raises(ComponentInjectionError) as excinfo:
            apply_component_action(client, "log-consumer", action="nuke")

        # Error message must name the invalid input and reveal the valid set.
        msg = str(excinfo.value)
        assert "nuke" in msg
        for allowed in VALID_ACTIONS:
            assert allowed in msg
        # Defense in depth: nothing on the client should have been touched.
        client.pause.assert_not_called()
        client.kill.assert_not_called()
        client.restart.assert_not_called()


# --------------------------------------------------------------------------- #
# rollback
# --------------------------------------------------------------------------- #


class TestRollback:
    """The rollback closure undoes whichever lifecycle action ran."""

    def test_rollback_pause_calls_unpause(self) -> None:
        client = MagicMock(name="docker_client")

        rollback(client, "log-consumer", {"action": "pause"})

        client.unpause.assert_called_once_with("log-consumer")
        # Nothing else.
        client.pause.assert_not_called()
        client.kill.assert_not_called()
        client.restart.assert_not_called()

    def test_rollback_kill_gets_target_and_starts_container(self) -> None:
        """For kill, rollback asks docker for the killed container and starts it."""
        client = MagicMock(name="docker_client")
        container = MagicMock(name="container_obj")
        client.get_target.return_value = container

        rollback(client, "log-consumer", {"action": "kill"})

        client.get_target.assert_called_once_with("log-consumer")
        container.start.assert_called_once()

    def test_rollback_restart_is_noop(self) -> None:
        """Restart is self-recovering -> rollback invokes no client methods."""
        client = MagicMock(name="docker_client")

        rollback(client, "log-consumer", {"action": "restart"})

        client.unpause.assert_not_called()
        client.pause.assert_not_called()
        client.kill.assert_not_called()
        client.restart.assert_not_called()
        client.get_target.assert_not_called()

    def test_rollback_none_state_is_noop(self) -> None:
        """``rollback(state=None)`` must not raise and must not touch the client."""
        client = MagicMock(name="docker_client")

        # Must not raise.
        rollback(client, "log-consumer", None)

        client.unpause.assert_not_called()
        client.pause.assert_not_called()
        client.kill.assert_not_called()
        client.restart.assert_not_called()
        client.get_target.assert_not_called()

    def test_rollback_swallows_unpause_exception(self) -> None:
        """Best-effort: ``client.unpause`` raising MUST NOT propagate."""
        client = MagicMock(name="docker_client")
        client.unpause.side_effect = RuntimeError("daemon gone")

        # Must not raise.
        rollback(client, "log-consumer", {"action": "pause"})

        client.unpause.assert_called_once_with("log-consumer")
