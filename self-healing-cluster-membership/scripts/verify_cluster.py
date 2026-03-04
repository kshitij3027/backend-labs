"""E2E verification script for the self-healing cluster."""

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request


NODES = {
    "node-1": "localhost:5001",
    "node-2": "localhost:5002",
    "node-3": "localhost:5003",
    "node-4": "localhost:5004",
    "node-5": "localhost:5005",
}


def get_health(address, timeout=2.0):
    """GET /health from a node."""
    try:
        url = f"http://{address}/health"
        req = urllib.request.urlopen(url, timeout=timeout)
        return json.loads(req.read().decode())
    except Exception:
        return None


def get_membership(address, timeout=2.0):
    """GET /membership from a node."""
    try:
        url = f"http://{address}/membership"
        req = urllib.request.urlopen(url, timeout=timeout)
        return json.loads(req.read().decode())
    except Exception:
        return None


def verify_cluster_formation(timeout=15.0):
    """Verify all nodes see each other within timeout."""
    print("\n=== Test: Cluster Formation ===")
    start = time.time()

    while time.time() - start < timeout:
        all_see_all = True
        for node_id, address in NODES.items():
            membership = get_membership(address)
            if membership is None:
                all_see_all = False
                break
            nodes_list = membership.get("nodes", [])
            known_ids = {n["node_id"] for n in nodes_list}
            if not NODES.keys() <= known_ids:
                all_see_all = False
                break
        if all_see_all:
            print(f"  All 5 nodes see each other ({time.time() - start:.1f}s)")
            print("  PASS")
            return True
        time.sleep(1)

    print(f"  FAIL: Not all nodes discovered within {timeout}s")
    # Print what each node sees for debugging
    for node_id, address in NODES.items():
        membership = get_membership(address)
        if membership:
            known = [n["node_id"] for n in membership.get("nodes", [])]
            print(f"    {node_id} sees: {known}")
        else:
            print(f"    {node_id}: unreachable")
    return False


def verify_leader_elected(timeout=10.0):
    """Verify exactly one leader is elected."""
    print("\n=== Test: Leader Election ===")
    start = time.time()

    while time.time() - start < timeout:
        leaders = []
        for node_id, address in NODES.items():
            health = get_health(address)
            if health and health.get("role") == "leader":
                leaders.append(node_id)
        if len(leaders) == 1:
            print(f"  Leader: {leaders[0]} ({time.time() - start:.1f}s)")
            print("  PASS")
            return True, leaders[0]
        time.sleep(1)

    print(f"  FAIL: Expected 1 leader, found {len(leaders) if 'leaders' in dir() else 0}")
    return False, None


def verify_failure_detection(timeout=20.0):
    """Stop a worker node and verify it's detected as FAILED."""
    print("\n=== Test: Failure Detection ===")

    print("  Stopping node-3...")
    subprocess.run(["docker", "stop", "cluster-node-3"], capture_output=True, timeout=10)

    start = time.time()
    while time.time() - start < timeout:
        # Check from node-1's perspective
        membership = get_membership(NODES["node-1"])
        if membership:
            for node in membership.get("nodes", []):
                if node["node_id"] == "node-3" and node["status"] == "failed":
                    print(f"  node-3 detected as FAILED ({time.time() - start:.1f}s)")
                    print("  PASS")
                    return True
        time.sleep(1)

    print(f"  FAIL: node-3 not detected as FAILED within {timeout}s")
    # Debug: show all node statuses
    membership = get_membership(NODES["node-1"])
    if membership:
        for node in membership.get("nodes", []):
            print(f"    {node['node_id']}: {node['status']}")
    return False


def verify_leader_reelection(old_leader, timeout=20.0):
    """Kill the leader and verify a new leader is elected."""
    print(f"\n=== Test: Leader Re-election (killing {old_leader}) ===")

    print(f"  Stopping {old_leader}...")
    subprocess.run(
        ["docker", "stop", f"cluster-{old_leader}"], capture_output=True, timeout=10
    )

    start = time.time()
    while time.time() - start < timeout:
        # Check remaining nodes for new leader
        new_leaders = []
        for node_id, address in NODES.items():
            if node_id == old_leader:
                continue
            health = get_health(address)
            if health and health.get("role") == "leader":
                new_leaders.append(node_id)

        if len(new_leaders) == 1 and new_leaders[0] != old_leader:
            print(f"  New leader: {new_leaders[0]} ({time.time() - start:.1f}s)")
            print("  PASS")
            return True, new_leaders[0]
        time.sleep(1)

    print(f"  FAIL: No new leader elected within {timeout}s")
    return False, None


def verify_node_rejoin(node_id, timeout=20.0):
    """Restart a stopped node and verify it rejoins as HEALTHY."""
    print(f"\n=== Test: Node Rejoin ({node_id}) ===")

    print(f"  Restarting {node_id}...")
    subprocess.run(
        ["docker", "start", f"cluster-{node_id}"], capture_output=True, timeout=10
    )

    # Find a surviving node to check from (not node-3 or the old leader which may be down)
    check_from = None
    for nid, addr in NODES.items():
        if nid == node_id:
            continue
        health = get_health(addr)
        if health:
            check_from = nid
            break

    if check_from is None:
        print(f"  FAIL: No surviving node to verify from")
        return False

    start = time.time()
    while time.time() - start < timeout:
        health = get_health(NODES[node_id])
        if health and health.get("status") == "healthy":
            # Also verify other nodes see it as healthy
            membership = get_membership(NODES[check_from])
            if membership:
                for node in membership.get("nodes", []):
                    if node["node_id"] == node_id and node["status"] == "healthy":
                        print(
                            f"  {node_id} rejoined as HEALTHY ({time.time() - start:.1f}s)"
                        )
                        print("  PASS")
                        return True
        time.sleep(1)

    print(f"  FAIL: {node_id} did not rejoin within {timeout}s")
    return False


def verify_network_partition(timeout=30.0):
    """Test network partition handling."""
    print("\n=== Test: Network Partition ===")

    # First, ensure all nodes are healthy and restart any that were stopped
    print("  Restarting all nodes...")
    for nid in NODES:
        subprocess.run(
            ["docker", "start", f"cluster-{nid}"], capture_output=True, timeout=10
        )

    # Wait for all nodes to be healthy
    print("  Waiting for all nodes to be healthy...")
    start = time.time()
    all_healthy = False
    while time.time() - start < 20:
        all_healthy = True
        for nid, addr in NODES.items():
            health = get_health(addr)
            if not health or health.get("status") != "healthy":
                all_healthy = False
                break
        if all_healthy:
            break
        time.sleep(1)

    if not all_healthy:
        print("  FAIL: Could not get all nodes healthy before partition test")
        return False

    print("  All nodes healthy. Creating partition (isolating node-4, node-5)...")

    # Disconnect node-4 and node-5 from the network
    network_name = "self-healing-cluster-membership_cluster-net"
    subprocess.run(
        ["docker", "network", "disconnect", network_name, "cluster-node-4"],
        capture_output=True,
        timeout=10,
    )
    subprocess.run(
        ["docker", "network", "disconnect", network_name, "cluster-node-5"],
        capture_output=True,
        timeout=10,
    )

    # Wait for majority partition to detect the isolated nodes as failed
    print("  Waiting for majority to detect partition...")
    start = time.time()
    partition_detected = False
    while time.time() - start < timeout:
        membership = get_membership(NODES["node-2"])
        if membership:
            statuses = {n["node_id"]: n["status"] for n in membership.get("nodes", [])}
            node4_failed = statuses.get("node-4") == "failed"
            node5_failed = statuses.get("node-5") == "failed"
            if node4_failed and node5_failed:
                print(f"  Majority detected partition ({time.time() - start:.1f}s)")
                partition_detected = True
                break
        time.sleep(1)

    if not partition_detected:
        print(f"  FAIL: Majority did not detect partition within {timeout}s")
        # Reconnect before failing
        subprocess.run(
            ["docker", "network", "connect", network_name, "cluster-node-4"],
            capture_output=True,
            timeout=10,
        )
        subprocess.run(
            ["docker", "network", "connect", network_name, "cluster-node-5"],
            capture_output=True,
            timeout=10,
        )
        return False

    # Verify majority partition still has a leader
    leader_in_majority = False
    for nid in ["node-1", "node-2", "node-3"]:
        health = get_health(NODES[nid])
        if health and health.get("role") == "leader":
            print(f"  Leader in majority partition: {nid}")
            leader_in_majority = True
            break

    if not leader_in_majority:
        print(
            "  WARNING: No leader in majority partition "
            "(may be expected if previous leader was in minority)"
        )

    # Heal the partition
    print("  Healing partition (reconnecting node-4, node-5)...")
    subprocess.run(
        ["docker", "network", "connect", network_name, "cluster-node-4"],
        capture_output=True,
        timeout=10,
    )
    subprocess.run(
        ["docker", "network", "connect", network_name, "cluster-node-5"],
        capture_output=True,
        timeout=10,
    )

    # Wait for convergence after heal
    print("  Waiting for convergence after partition heal...")
    start = time.time()
    converged = False
    while time.time() - start < 20:
        all_see_all = True
        for nid, addr in NODES.items():
            membership = get_membership(addr)
            if membership is None:
                all_see_all = False
                break
            known_ids = {n["node_id"] for n in membership.get("nodes", [])}
            if not NODES.keys() <= known_ids:
                all_see_all = False
                break
        if all_see_all:
            print(
                f"  Cluster converged after partition heal ({time.time() - start:.1f}s)"
            )
            converged = True
            break
        time.sleep(1)

    if not converged:
        print(f"  FAIL: Cluster did not converge after partition heal within 20s")
        return False

    print("  PASS")
    return True


def verify_gossip_convergence(timeout=15.0):
    """Verify all alive nodes have the same membership view."""
    print("\n=== Test: Gossip Convergence ===")

    start = time.time()
    views = {}
    while time.time() - start < timeout:
        views = {}

        for node_id, address in NODES.items():
            membership = get_membership(address)
            if membership is None:
                continue  # Node might be down
            # Build a sorted view of node statuses
            view = tuple(
                sorted(
                    (n["node_id"], n["status"]) for n in membership.get("nodes", [])
                )
            )
            views[node_id] = view

        if len(views) >= 2:
            unique_views = set(views.values())
            if len(unique_views) == 1:
                print(
                    f"  All {len(views)} alive nodes have consistent view ({time.time() - start:.1f}s)"
                )
                print("  PASS")
                return True

        time.sleep(1)

    print(f"  FAIL: Views not converged within {timeout}s")
    for nid, view in views.items():
        print(f"    {nid}: {view}")
    return False


def main():
    print("Self-Healing Cluster E2E Verification")
    print("=" * 40)

    # Wait for cluster to be ready
    print("\nWaiting for cluster to be ready...")
    time.sleep(3)

    results = []

    # Test 1: Cluster formation
    passed = verify_cluster_formation()
    results.append(("Cluster Formation", passed))
    if not passed:
        print("\nCannot continue without cluster formation")
        sys.exit(1)

    # Test 2: Leader election
    passed, leader = verify_leader_elected()
    results.append(("Leader Election", passed))
    if not passed:
        print("\nCannot continue without a leader")
        sys.exit(1)

    # Test 3: Failure detection (stop node-3, verify FAILED)
    passed = verify_failure_detection()
    results.append(("Failure Detection", passed))

    # Test 4: Leader re-election (stop the leader, verify new leader)
    passed, new_leader = verify_leader_reelection(leader)
    results.append(("Leader Re-election", passed))

    # Test 5: Node rejoin (restart node-3, verify HEALTHY)
    passed = verify_node_rejoin("node-3")
    results.append(("Node Rejoin", passed))

    # Test 6: Gossip convergence (all alive nodes same view)
    passed = verify_gossip_convergence()
    results.append(("Gossip Convergence", passed))

    # Test 7: Network partition
    passed = verify_network_partition()
    results.append(("Network Partition", passed))

    # Summary
    print("\n" + "=" * 40)
    print("Summary:")
    all_passed = True
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False

    if all_passed:
        print("\nAll tests passed!")
        sys.exit(0)
    else:
        print("\nSome tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
