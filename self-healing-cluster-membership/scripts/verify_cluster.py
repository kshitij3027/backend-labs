"""E2E verification script for the self-healing cluster."""

import json
import sys
import time
import urllib.request
import urllib.error


NODES = {
    "node-1": "node-1:5000",
    "node-2": "node-2:5000",
    "node-3": "node-3:5000",
    "node-4": "node-4:5000",
    "node-5": "node-5:5000",
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
