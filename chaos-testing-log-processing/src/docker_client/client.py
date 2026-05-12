"""Thin wrapper around the docker SDK with an allowlist enforced.

Every method that touches a container goes through ``_resolve_target`` which
validates the container both carries the ``chaos.target=true`` label AND
its name appears in the safety allowlist. Operations on any other container
raise :class:`NotAllowlistedError` and never touch Docker.

The wrapper exposes safe primitives consumed by the per-type injection
modules (network/resource/component). Each mutating call emits a single
INFO-level audit log line so that an operator can reconstruct exactly
which containers were touched and when. Read-only calls
(``list_chaos_targets``, ``get_target``) are not audited.

Failure-mode contract:
    - Operating on a name not in the configured allowlist raises
      :class:`NotAllowlistedError`. We fail-closed on this — the call never
      reaches the Docker daemon.
    - Operating on a name in the allowlist but where the container is
      missing in Docker raises :class:`TargetNotFoundError`.
    - Operating on a container that exists but lacks the
      ``chaos.target=true`` label (e.g. someone renamed an unrelated
      container into the allowlist) also raises :class:`NotAllowlistedError`
      — this is the defense-in-depth second check.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

import docker
from docker.errors import APIError, NotFound
from docker.models.containers import Container


logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Exceptions
# --------------------------------------------------------------------------- #


class DockerClientError(Exception):
    """Base class for all errors raised by :class:`DockerClient`."""


class NotAllowlistedError(DockerClientError):
    """Raised when an operation references a container outside the allowlist.

    Also raised, defensively, when the container exists in Docker but is
    missing the ``chaos.target=true`` label.
    """


class TargetNotFoundError(DockerClientError):
    """Raised when a container in the allowlist is not present in Docker."""


# --------------------------------------------------------------------------- #
# Client
# --------------------------------------------------------------------------- #


class DockerClient:
    """Allowlisted facade over the Docker Python SDK.

    Parameters:
        allowlist: Iterable of container names that the framework is
            permitted to operate on. Anything else raises
            :class:`NotAllowlistedError` and never reaches the daemon.
        socket_path: Filesystem path to the Docker unix socket. Ignored
            when ``client`` is supplied directly.
        client: Optional pre-constructed ``docker.DockerClient`` instance,
            primarily for dependency injection in tests.
    """

    def __init__(
        self,
        allowlist: Iterable[str],
        socket_path: str = "/var/run/docker.sock",
        client: docker.DockerClient | None = None,
    ) -> None:
        self._allowlist: frozenset[str] = frozenset(allowlist)
        if client is not None:
            self._client = client
        else:
            self._client = docker.DockerClient(base_url=f"unix://{socket_path}")

    # ------------------------------------------------------------------ #
    # Allowlist
    # ------------------------------------------------------------------ #

    @property
    def allowlist(self) -> frozenset[str]:
        """The configured allowlist, as an immutable frozenset."""
        return self._allowlist

    # ------------------------------------------------------------------ #
    # Read-only listing
    # ------------------------------------------------------------------ #

    def list_chaos_targets(self) -> list[Container]:
        """Return containers that are eligible chaos targets.

        Eligibility = ``chaos.target=true`` label AND name in the
        configured allowlist. The Docker-side label filter is the cheap
        pre-filter; the allowlist intersect is the authoritative check.
        """
        containers: list[Container] = self._client.containers.list(
            filters={"label": "chaos.target=true"}
        )
        eligible = [c for c in containers if c.name in self._allowlist]
        return eligible

    # ------------------------------------------------------------------ #
    # Allowlisted target resolution
    # ------------------------------------------------------------------ #

    def get_target(self, name: str) -> Container:
        """Resolve a container by name with allowlist + label enforcement.

        Raises:
            NotAllowlistedError: ``name`` is not in the configured
                allowlist, OR the container exists but is missing the
                ``chaos.target=true`` label.
            TargetNotFoundError: ``name`` is in the allowlist but no such
                container exists in Docker.
        """
        if name not in self._allowlist:
            raise NotAllowlistedError(
                f"container {name!r} is not in the chaos allowlist"
            )
        try:
            container = self._client.containers.get(name)
        except NotFound as exc:
            raise TargetNotFoundError(
                f"container {name!r} is in the allowlist but not present in Docker"
            ) from exc

        # Defense in depth: verify the chaos.target label is set, in case
        # someone renamed an unrelated container into the allowlist.
        labels = getattr(container, "labels", None) or {}
        if labels.get("chaos.target") != "true":
            raise NotAllowlistedError(
                f"container {name!r} exists but is missing chaos.target=true label"
            )
        return container

    # ------------------------------------------------------------------ #
    # Mutating primitives — each goes through ``get_target`` for safety
    # ------------------------------------------------------------------ #

    def exec(
        self,
        name: str,
        cmd: list[str] | str,
        user: str | None = None,
        privileged: bool = False,
    ) -> tuple[int, bytes]:
        """Execute ``cmd`` inside the container and return ``(exit_code, output)``.

        ``output`` is combined stdout+stderr as bytes (``demux=False``).
        Raises :class:`NotAllowlistedError` / :class:`TargetNotFoundError`
        from the underlying :meth:`get_target` check.
        """
        container = self.get_target(name)
        logger.info(
            "docker.exec container=%s user=%s privileged=%s cmd=%r",
            name,
            user,
            privileged,
            cmd,
        )
        exit_code, output = container.exec_run(
            cmd,
            user=user or "",
            privileged=privileged,
            demux=False,
        )
        return exit_code, output

    def pause(self, name: str) -> None:
        """Pause the container (SIGSTOP all processes)."""
        container = self.get_target(name)
        logger.info("docker.pause container=%s", name)
        container.pause()

    def unpause(self, name: str) -> None:
        """Unpause the container (SIGCONT all processes)."""
        container = self.get_target(name)
        logger.info("docker.unpause container=%s", name)
        container.unpause()

    def kill(self, name: str, signal: str = "SIGKILL") -> None:
        """Send ``signal`` to the container's main process."""
        container = self.get_target(name)
        logger.info("docker.kill container=%s signal=%s", name, signal)
        container.kill(signal=signal)

    def restart(self, name: str, timeout: int = 5) -> None:
        """Restart the container with a graceful-stop timeout."""
        container = self.get_target(name)
        logger.info("docker.restart container=%s timeout=%ss", name, timeout)
        container.restart(timeout=timeout)

    # ------------------------------------------------------------------ #
    # Network surgery
    # ------------------------------------------------------------------ #

    def disconnect_network(self, name: str, network: str) -> dict:
        """Disconnect a container from a named network.

        Returns a record of the saved aliases/IPv4 so a later
        :meth:`connect_network` can fully restore the original wiring.

        Procedure:
            1. Resolve the container through the allowlist.
            2. Resolve the network object.
            3. Read current aliases + IPAM IPv4 address out of
               ``container.attrs["NetworkSettings"]["Networks"][network]``.
            4. Force-disconnect the container from the network.

        Returns:
            ``{"aliases": [...], "ipv4": "..." or None}``
        """
        container = self.get_target(name)
        net = self._client.networks.get(network)

        # Best-effort metadata capture for restore.
        networks_meta = (
            container.attrs.get("NetworkSettings", {}).get("Networks", {})
        )
        net_meta = networks_meta.get(network, {}) or {}
        aliases = list(net_meta.get("Aliases") or [])
        ipam_cfg = net_meta.get("IPAMConfig") or {}
        ipv4 = ipam_cfg.get("IPv4Address") if isinstance(ipam_cfg, dict) else None

        logger.info(
            "docker.network.disconnect container=%s network=%s aliases=%s ipv4=%s",
            name,
            network,
            aliases,
            ipv4,
        )
        net.disconnect(container, force=True)
        return {"aliases": aliases, "ipv4": ipv4}

    def connect_network(
        self,
        name: str,
        network: str,
        aliases: list[str] | None = None,
        ipv4: str | None = None,
    ) -> None:
        """Reconnect a container to a network, restoring aliases / IPv4."""
        container = self.get_target(name)
        net = self._client.networks.get(network)
        logger.info(
            "docker.network.connect container=%s network=%s aliases=%s ipv4=%s",
            name,
            network,
            aliases,
            ipv4,
        )
        net.connect(
            container,
            aliases=aliases or None,
            ipv4_address=ipv4 or None,
        )

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        """Best-effort close of the underlying Docker client."""
        try:
            self._client.close()
        except (APIError, OSError, AttributeError) as exc:
            logger.warning("docker.close swallowed error: %s", exc)
