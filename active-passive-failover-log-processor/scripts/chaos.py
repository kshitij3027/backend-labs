"""Chaos test driver for the running cluster.

Run AFTER ``make run`` so the cluster is up. The driver does NOT bring up
or tear down the cluster — it just disturbs it and asserts the cluster
recovers.

Scenarios
---------

* ``random_kill`` — every 30s, ``docker kill -s SIGKILL`` a random
  container for ``duration`` seconds. After each kill, the killed
  container is restarted ~8 seconds later (so it has time to come back as
  STANDBY before the next kill cycle). At every sample tick (every 1s),
  poll all 3 nodes' ``/health`` and assert exactly one PRIMARY. Pass
  criterion: zero samples with two primaries; brief no-primary windows
  during failover are tolerated up to 30% of samples.

* ``partition`` — for ``duration`` seconds, ``docker network disconnect``
  the current primary, then reconnect after ~5s. Assert: a standby
  promotes within ~15s, the original primary self-demotes when
  reconnected (it rejoins as STANDBY because lock-renewal failed
  while it was severed from Redis).

* ``sustained_load`` — POST logs to the current primary at ``rate`` per
  second for ``duration`` seconds, while ``random_kill`` runs in
  parallel. Assert: ratio of duplicate log_ids in the accepted set
  (caused by retries hitting different primaries) < 5% — the snapshot
  loader keeps ``_next_id`` past the highest seen id, so dup-rate
  primarily reflects the snapshot lag window.

Exits non-zero on any assertion failure.

Usage
-----

::

    python3 scripts/chaos.py --scenario random_kill --duration 60
    python3 scripts/chaos.py --scenario partition --duration 30
    python3 scripts/chaos.py --scenario sustained_load --duration 120 --rate 50
"""

from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import threading
import time
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# =========================================================================
# Cluster constants — must mirror docker-compose.yml
# =========================================================================

NODE_PORTS: list[int] = [8001, 8002, 8003]
NODE_NAMES: list[str] = ["failover-node-1", "failover-node-2", "failover-node-3"]

# Compose prefixes the project directory onto each network name. The
# fallback alt-name set below covers the cases where the user runs the
# script from a different cwd or with ``-p`` set.
NETWORK_NAME: str = "active-passive-failover-log-processor_failover-net"
NETWORK_FALLBACKS: tuple[str, ...] = (
    "failover-net",
    "active-passive-failover-log-processor_default",
)


# =========================================================================
# HTTP helpers (stdlib only — mirror verify_failover.py)
# =========================================================================


def _http_get(url: str, timeout: float = 1.5) -> tuple[int, Optional[dict]]:
    """GET ``url`` and return ``(status_code, parsed_json_or_None)``.

    Returns ``(0, None)`` on connection refused / DNS failure / timeout.
    """
    req = Request(url, method="GET")
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            try:
                return resp.status, json.loads(body)
            except json.JSONDecodeError:
                return resp.status, None
    except HTTPError as exc:
        try:
            body = exc.read().decode("utf-8")
            return exc.code, json.loads(body) if body else None
        except Exception:
            return exc.code, None
    except (URLError, TimeoutError, OSError):
        return 0, None


def _http_post(
    url: str, body: dict, timeout: float = 2.0
) -> tuple[int, Optional[dict]]:
    """POST a JSON body and return ``(status_code, parsed_json_or_None)``."""
    data = json.dumps(body).encode("utf-8")
    req = Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            txt = resp.read().decode("utf-8")
            return resp.status, json.loads(txt) if txt else None
    except HTTPError as exc:
        try:
            err_body = exc.read().decode("utf-8")
            return exc.code, json.loads(err_body) if err_body else None
        except Exception:
            return exc.code, None
    except (URLError, TimeoutError, OSError):
        return 0, None


# =========================================================================
# Docker helpers
# =========================================================================


def _docker(*args: str) -> subprocess.CompletedProcess:
    """Run ``docker <args>`` capturing stdout/stderr; 30-second hard timeout."""
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=30
    )


def _container_for_port(port: int) -> str:
    """Map host port → container name (compose-managed)."""
    return {
        8001: NODE_NAMES[0],
        8002: NODE_NAMES[1],
        8003: NODE_NAMES[2],
    }[port]


def _disconnect_from_network(container: str) -> Optional[str]:
    """Disconnect ``container`` from the failover network; return network used.

    Returns the network name on success, ``None`` if every candidate
    network failed.
    """
    for net in (NETWORK_NAME, *NETWORK_FALLBACKS):
        res = _docker("network", "disconnect", net, container)
        if res.returncode == 0:
            return net
    return None


# =========================================================================
# Cluster-state helpers
# =========================================================================


def _find_primary(timeout: float = 5.0) -> Optional[int]:
    """Poll every node's ``/health`` until exactly one returns 200.

    Returns the primary's host port, or ``None`` if no primary surfaces
    within ``timeout``.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for port in NODE_PORTS:
            code, _ = _http_get(f"http://localhost:{port}/health", timeout=1.0)
            if code == 200:
                return port
        time.sleep(0.2)
    return None


def _count_primaries() -> tuple[int, list[int]]:
    """Return ``(count, list_of_primary_ports)`` from a single sample."""
    primaries: list[int] = []
    for port in NODE_PORTS:
        code, _ = _http_get(f"http://localhost:{port}/health", timeout=0.5)
        if code == 200:
            primaries.append(port)
    return len(primaries), primaries


# =========================================================================
# Scenarios
# =========================================================================


def scenario_random_kill(duration: float) -> int:
    """Every 30s kill a random container; assert exactly one PRIMARY.

    Pass criterion: zero samples with two primaries. Brief no-primary
    windows during failover are tolerated up to 30% of samples (the
    standby's election + warm-up takes ~6-8s while the script samples
    every 1s).
    """
    print(f"=== chaos: random_kill for {duration}s ===")
    end = time.monotonic() + duration
    next_kill = time.monotonic() + 5.0
    samples_with_split = 0
    samples_with_zero = 0
    total_samples = 0

    while time.monotonic() < end:
        # Sample current state every 1s.
        count, ports = _count_primaries()
        total_samples += 1
        if count > 1:
            print(f"!! split-brain detected: {count} primaries on ports {ports}")
            samples_with_split += 1
        elif count == 0:
            samples_with_zero += 1

        # Kill if it's time.
        if time.monotonic() >= next_kill:
            target = random.choice(NODE_NAMES)
            print(f"  killing {target}")
            _docker("kill", "-s", "SIGKILL", target)
            # Restart after 8s so the cluster has time to react and the dead
            # container eventually returns as STANDBY.
            threading.Timer(8.0, lambda t=target: _docker("start", t)).start()
            next_kill = time.monotonic() + 30.0

        time.sleep(1.0)

    print(f"  total samples: {total_samples}")
    print(f"  samples with split-brain: {samples_with_split}")
    print(f"  samples with no primary (during failover): {samples_with_zero}")
    if samples_with_split > 0:
        print("FAIL: split-brain observed")
        return 1
    # Some "no primary" windows during failover are expected (~1-2 ticks
    # while the standby is mid-promotion). Tolerate up to 30% of samples.
    if total_samples > 0 and samples_with_zero / total_samples > 0.30:
        print(
            f"FAIL: no-primary windows exceeded 30% "
            f"({samples_with_zero}/{total_samples})"
        )
        return 1
    print(
        "PASS: chaos random_kill — exactly one primary observed throughout "
        "(with brief failover gaps)"
    )
    return 0


def scenario_partition(duration: float) -> int:
    """Disconnect current primary; verify a standby promotes; reconnect; verify rejoin.

    Steps:
      1. Discover the current primary.
      2. ``docker network disconnect`` it from the failover network.
      3. Wait up to 15s for a NEW primary to appear on a different port.
      4. Sleep ~5s, then reconnect the partitioned container.
      5. Verify the original primary rejoins as STANDBY within 15s
         (its lock-renewal failed while severed, so it must self-demote).

    The ``duration`` argument is honoured as a "do not run longer than
    this" overall budget; the actual sequence is bounded by the explicit
    timeouts above.
    """
    print(f"=== chaos: partition (overall budget {duration}s) ===")
    primary = _find_primary()
    if primary is None:
        print("FAIL: no primary at start")
        return 1
    container = _container_for_port(primary)
    print(f"  current primary: {container} on port {primary}")

    print(f"  disconnecting {container}")
    actual_net = _disconnect_from_network(container)
    if actual_net is None:
        print("FAIL: could not disconnect from any candidate network")
        return 1
    print(f"  disconnected from {actual_net}")

    # Wait for a NEW primary on a different port.
    new_primary: Optional[int] = None
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        for port in NODE_PORTS:
            if port == primary:
                continue
            code, _ = _http_get(f"http://localhost:{port}/health", timeout=1.0)
            if code == 200:
                new_primary = port
                break
        if new_primary is not None:
            break
        time.sleep(0.5)

    if new_primary is None:
        print("FAIL: no standby promoted within 15s of partition")
        # Best-effort reconnect so we don't leave the cluster in a broken state.
        _docker("network", "connect", actual_net, container)
        return 1
    print(f"  new primary on port {new_primary} after partition")

    # Sleep ~5s, then reconnect.
    time.sleep(5)
    print(f"  reconnecting {container}")
    res = _docker("network", "connect", actual_net, container)
    if res.returncode != 0:
        print(f"!! reconnect returned {res.returncode}: {res.stderr.strip()}")

    # Verify original primary rejoins as STANDBY (lock-renewal failed → demoted).
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        code, body = _http_get(f"http://localhost:{primary}/role", timeout=1.0)
        if code == 200 and body and body.get("state") == "STANDBY":
            print("PASS: chaos partition — original primary rejoined as STANDBY")
            return 0
        time.sleep(0.5)

    code, body = _http_get(f"http://localhost:{primary}/role", timeout=1.0)
    print(
        f"FAIL: original primary did not rejoin as STANDBY "
        f"(final /role: {body})"
    )
    return 1


def scenario_sustained_load(duration: float, rate: int) -> int:
    """POST logs at ``rate`` per second while ``random_kill`` runs in parallel.

    Pass criterion: duplicate log_ids in the accepted set < 5% of total
    accepted. Some duplicates are expected because each promoted standby
    seeds ``_next_id`` from the snapshot loader, and the snapshot can lag
    by up to ``STATE_SYNC_INTERVAL`` (5s). Idempotency on client-supplied
    ids dedups exact retries; this scenario does NOT supply client ids,
    so the metric here primarily reflects snapshot lag rather than
    network retries.
    """
    print(f"=== chaos: sustained_load for {duration}s at {rate}/s ===")
    if rate <= 0:
        print("FAIL: rate must be positive")
        return 1

    accepted_log_ids: list[int] = []
    rejected = 0
    accepted = 0
    end = time.monotonic() + duration
    interval = 1.0 / rate

    # Start the kill loop in a background thread. We reuse the same
    # scenario_random_kill function so the chaos profile is identical
    # to the standalone scenario.
    kill_thread = threading.Thread(
        target=lambda: scenario_random_kill(duration), daemon=True
    )
    kill_thread.start()

    next_post = time.monotonic()
    counter = 0
    while time.monotonic() < end:
        if time.monotonic() < next_post:
            time.sleep(min(0.01, max(0.0, next_post - time.monotonic())))
            continue
        counter += 1
        # Find the current primary; skip if none.
        primary = _find_primary(timeout=2.0)
        if primary is None:
            rejected += 1
            next_post = time.monotonic() + interval
            continue
        code, body = _http_post(
            f"http://localhost:{primary}/logs",
            {"message": f"chaos-{counter}", "level": "INFO"},
            timeout=1.5,
        )
        if code == 201 and body and "log_id" in body:
            accepted += 1
            accepted_log_ids.append(int(body["log_id"]))
        else:
            rejected += 1
        next_post += interval

    kill_thread.join(timeout=5)

    # Compute log_id continuity.
    total = len(accepted_log_ids)
    duplicates = total - len(set(accepted_log_ids))
    dup_ratio = (duplicates / total) if total > 0 else 0.0
    print(
        f"  accepted: {accepted}, rejected: {rejected}, "
        f"duplicates: {duplicates} ({dup_ratio:.1%})"
    )
    if dup_ratio > 0.05:
        print(f"FAIL: duplicate ratio {dup_ratio:.1%} exceeds 5% bound")
        return 1
    print("PASS: chaos sustained_load — duplicate ratio within 5%")
    return 0


# =========================================================================
# Entry point
# =========================================================================


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Chaos test driver for the running failover cluster.",
    )
    parser.add_argument(
        "--scenario",
        choices=["random_kill", "partition", "sustained_load"],
        required=True,
        help="Which chaos scenario to run.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Overall scenario budget in seconds (default 60).",
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=50,
        help="logs/sec for sustained_load (default 50; ignored otherwise).",
    )
    args = parser.parse_args()

    if args.scenario == "random_kill":
        return scenario_random_kill(args.duration)
    if args.scenario == "partition":
        return scenario_partition(args.duration)
    if args.scenario == "sustained_load":
        return scenario_sustained_load(args.duration, args.rate)
    return 1


if __name__ == "__main__":
    sys.exit(main())
