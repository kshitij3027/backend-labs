"""tc-netem based network fault primitives.

The framework manipulates the target container's egress netem qdisc on its
``eth0`` interface. Both latency and packet-loss share a single root qdisc
on the device — that means a second injection on the same target replaces
the first (we use ``tc qdisc change`` after ``add`` reports "File exists").

Rollback removes the root qdisc and is idempotent: a stale call after the
qdisc was already removed (e.g., target restart) is a no-op."""

from __future__ import annotations

import logging

from ..docker_client.client import DockerClient

logger = logging.getLogger(__name__)


class NetworkInjectionError(Exception):
    """Raised when tc returns a non-zero exit code we can't classify as benign."""


def _decode(output: bytes) -> str:
    return output.decode("utf-8", errors="replace").strip()


def _run_tc(client: DockerClient, container: str, args: list[str]) -> tuple[int, str]:
    """Run a single tc command inside the target. Returns (exit_code, stderr_decoded)."""
    exit_code, output = client.exec(container, ["tc", *args], user="root")
    return exit_code, _decode(output)


def _exists_already(msg: str) -> bool:
    return "File exists" in msg or "RTNETLINK answers: File exists" in msg


def _no_qdisc(msg: str) -> bool:
    # When deleting a qdisc that isn't there:
    return any(
        s in msg
        for s in (
            "RTNETLINK answers: No such file or directory",
            "Cannot delete qdisc with handle of zero",
            "Cannot find device",
        )
    )


def inject_latency(
    client: DockerClient, container: str, latency_ms: int, jitter_ms: int = 0
) -> None:
    """Apply a netem ``delay`` qdisc. Replaces an existing root qdisc."""
    if latency_ms <= 0:
        raise NetworkInjectionError(f"latency_ms must be > 0 (got {latency_ms})")

    netem_args = ["netem", "delay", f"{latency_ms}ms"]
    if jitter_ms > 0:
        netem_args.append(f"{jitter_ms}ms")

    code, msg = _run_tc(
        client, container, ["qdisc", "add", "dev", "eth0", "root", *netem_args]
    )
    if code != 0 and _exists_already(msg):
        code, msg = _run_tc(
            client,
            container,
            ["qdisc", "change", "dev", "eth0", "root", *netem_args],
        )
    if code != 0:
        raise NetworkInjectionError(
            f"tc netem delay failed on {container}: {msg}"
        )
    logger.info(
        "tc netem delay %dms (+/- %dms) applied to %s",
        latency_ms,
        jitter_ms,
        container,
    )


def inject_packet_loss(
    client: DockerClient, container: str, loss_pct: float
) -> None:
    """Apply a netem ``loss`` qdisc. Replaces an existing root qdisc."""
    if not 0 < loss_pct <= 100:
        raise NetworkInjectionError(
            f"loss_pct must be in (0, 100] (got {loss_pct})"
        )

    netem_args = ["netem", "loss", f"{loss_pct}%"]
    code, msg = _run_tc(
        client, container, ["qdisc", "add", "dev", "eth0", "root", *netem_args]
    )
    if code != 0 and _exists_already(msg):
        code, msg = _run_tc(
            client,
            container,
            ["qdisc", "change", "dev", "eth0", "root", *netem_args],
        )
    if code != 0:
        raise NetworkInjectionError(
            f"tc netem loss failed on {container}: {msg}"
        )
    logger.info("tc netem loss %.1f%% applied to %s", loss_pct, container)


def rollback(client: DockerClient, container: str) -> None:
    """Idempotent removal of the root qdisc on eth0."""
    code, msg = _run_tc(client, container, ["qdisc", "del", "dev", "eth0", "root"])
    if code == 0:
        logger.info("tc qdisc del cleared %s", container)
        return
    if _no_qdisc(msg):
        logger.debug("tc qdisc already absent on %s (%s)", container, msg)
        return
    # Best-effort: log loudly, but DO NOT raise — rollback must be tolerant.
    logger.warning(
        "tc qdisc del returned non-zero on %s: %s", container, msg
    )


# ---------- Network partition (docker network disconnect/connect) ----------


def inject_partition(
    client: DockerClient, container: str, network: str
) -> dict:
    """Disconnect a target from a docker network.

    Returns a state dict so :func:`rollback_partition` can restore the
    original aliases/ipv4 when the experiment ends.
    """
    state = client.disconnect_network(container, network)
    logger.info(
        "partitioned %s from network %s (aliases=%s ipv4=%s)",
        container,
        network,
        state.get("aliases"),
        state.get("ipv4"),
    )
    return state


def rollback_partition(
    client: DockerClient,
    container: str,
    network: str,
    state: dict | None,
) -> None:
    """Reconnect a target to a docker network, restoring its previous aliases."""
    aliases = (state or {}).get("aliases") or None
    ipv4 = (state or {}).get("ipv4") or None
    try:
        client.connect_network(container, network, aliases=aliases, ipv4=ipv4)
        logger.info("rejoined %s to %s (aliases=%s)", container, network, aliases)
    except Exception:
        # Best-effort; rollback must not raise into the engine.
        logger.exception("rejoin failed for %s on %s", container, network)
