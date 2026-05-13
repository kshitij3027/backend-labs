"""Component failure injection via Docker lifecycle primitives.

Three supported actions (selected via ``scenario.parameters["action"]``):

* ``pause``  — :class:`DockerClient` ``pause`` / ``unpause`` (default).
* ``kill``   — SIGKILL the container; rollback ``docker start`` as best-effort.
* ``restart``— ``docker restart`` (graceful stop + start); no rollback needed.

The default action is ``pause`` because it is fully reversible and does
not lose container state.
"""

from __future__ import annotations

import logging

from ..docker_client.client import DockerClient

logger = logging.getLogger(__name__)

VALID_ACTIONS = {"pause", "kill", "restart"}


class ComponentInjectionError(Exception):
    """Raised on an unsupported component-failure action."""


def apply_component_action(
    client: DockerClient, container: str, action: str = "pause"
) -> dict:
    """Apply the requested action; return a state dict for rollback."""
    if action not in VALID_ACTIONS:
        raise ComponentInjectionError(
            f"unsupported action '{action}' (allowed: {sorted(VALID_ACTIONS)})"
        )
    if action == "pause":
        client.pause(container)
        logger.info("paused %s", container)
        return {"action": "pause"}
    if action == "kill":
        client.kill(container, signal="SIGKILL")
        logger.info("killed %s with SIGKILL", container)
        return {"action": "kill"}
    if action == "restart":
        client.restart(container, timeout=5)
        logger.info("restarted %s", container)
        return {"action": "restart"}
    raise ComponentInjectionError(f"unreachable: {action}")


def rollback(client: DockerClient, container: str, state: dict | None) -> None:
    """Undo a component action where possible (best-effort)."""
    action = (state or {}).get("action")
    try:
        if action == "pause":
            client.unpause(container)
            logger.info("unpaused %s", container)
            return
        if action == "kill":
            # Best-effort: ask docker to start the killed container.
            container_obj = client.get_target(container)
            try:
                container_obj.start()
                logger.info("restarted killed container %s", container)
            except Exception:
                logger.warning("could not restart killed container %s; manual recovery needed", container)
            return
        if action == "restart":
            # Restart is self-recovering — nothing to do here.
            logger.debug("no rollback needed for restart on %s", container)
            return
        logger.warning("rollback called with unknown action=%r on %s", action, container)
    except Exception:
        logger.exception("component rollback failed for %s (action=%r)", container, action)
