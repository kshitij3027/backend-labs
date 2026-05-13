"""stress-ng based resource pressure injection.

Both CPU and memory pressure run as a single backgrounded ``stress-ng``
process inside the target container. We let stress-ng manage its own
``--timeout`` so the worker is self-terminating; the rollback handler
sends ``pkill stress-ng`` to kill it early if the engine aborts before
the timeout elapses.

The injection function returns no state — rollback only needs the
container name. Rollback is idempotent (pkill on a missing process is a
no-op).
"""

from __future__ import annotations

import logging

from ..docker_client.client import DockerClient

logger = logging.getLogger(__name__)


class ResourceInjectionError(Exception):
    """Raised when stress-ng cannot be launched."""


def _decode(output: bytes) -> str:
    return output.decode("utf-8", errors="replace").strip()


def inject_cpu_pressure(
    client: DockerClient,
    container: str,
    cores: int = 1,
    load_pct: int = 100,
    duration_s: int = 30,
) -> None:
    """Spawn a stress-ng CPU worker inside the target.

    ``stress-ng --cpu {cores} --cpu-load {load_pct} --timeout {duration_s}s``.
    The process is detached (`sh -c "... &"`) so this call returns quickly;
    rollback uses pkill to terminate it early.
    """
    if cores < 1:
        raise ResourceInjectionError(f"cores must be >= 1 (got {cores})")
    if not 1 <= load_pct <= 100:
        raise ResourceInjectionError(f"load_pct must be in [1, 100] (got {load_pct})")
    if duration_s < 1:
        raise ResourceInjectionError(f"duration_s must be >= 1 (got {duration_s})")

    cmd = [
        "sh", "-c",
        f"nohup stress-ng --cpu {cores} --cpu-load {load_pct} "
        f"--timeout {duration_s}s > /tmp/stress-ng.log 2>&1 & echo $!",
    ]
    code, output = client.exec(container, cmd, user="root")
    if code != 0:
        raise ResourceInjectionError(
            f"stress-ng CPU launch failed on {container}: {_decode(output)}"
        )
    logger.info(
        "stress-ng CPU launched on %s: cores=%d load=%d%% duration=%ds (pid=%s)",
        container, cores, load_pct, duration_s, _decode(output),
    )


def inject_memory_pressure(
    client: DockerClient,
    container: str,
    bytes_per_worker: str = "256M",
    workers: int = 1,
    duration_s: int = 30,
) -> None:
    """Spawn a stress-ng memory worker (VM) inside the target."""
    if workers < 1:
        raise ResourceInjectionError(f"workers must be >= 1 (got {workers})")
    if duration_s < 1:
        raise ResourceInjectionError(f"duration_s must be >= 1 (got {duration_s})")

    cmd = [
        "sh", "-c",
        f"nohup stress-ng --vm {workers} --vm-bytes {bytes_per_worker} "
        f"--timeout {duration_s}s > /tmp/stress-ng.log 2>&1 & echo $!",
    ]
    code, output = client.exec(container, cmd, user="root")
    if code != 0:
        raise ResourceInjectionError(
            f"stress-ng VM launch failed on {container}: {_decode(output)}"
        )
    logger.info(
        "stress-ng VM launched on %s: workers=%d bytes=%s duration=%ds (pid=%s)",
        container, workers, bytes_per_worker, duration_s, _decode(output),
    )


def rollback(client: DockerClient, container: str) -> None:
    """Best-effort pkill of any stress-ng process inside the target.

    ``pkill -f stress-ng`` matches both the parent and worker children.
    Exit code 1 means "no process matched" — that's fine (the timeout
    fired before our rollback)."""
    cmd = ["pkill", "-f", "stress-ng"]
    code, output = client.exec(container, cmd, user="root")
    if code == 0:
        logger.info("stress-ng killed on %s", container)
    elif code == 1:
        logger.debug("no stress-ng process to kill on %s", container)
    else:
        logger.warning(
            "pkill stress-ng returned %d on %s: %s",
            code, container, _decode(output),
        )
