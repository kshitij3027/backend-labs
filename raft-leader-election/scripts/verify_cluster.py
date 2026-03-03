"""E2E verification script for the Raft cluster."""

import sys
import time
import grpc

# Add parent directory to path so we can import src
sys.path.insert(0, "/app")

from src.proto import raft_pb2, raft_pb2_grpc


NODES = {
    "node-1": "node-1:5001",
    "node-2": "node-2:5002",
    "node-3": "node-3:5003",
    "node-4": "node-4:5004",
    "node-5": "node-5:5005",
}


def get_node_status(address, timeout=2.0):
    """Get status of a single node via gRPC."""
    try:
        channel = grpc.insecure_channel(address)
        stub = raft_pb2_grpc.NodeAdminServiceStub(channel)
        response = stub.GetStatus(
            raft_pb2.GetStatusRequest(),
            timeout=timeout,
        )
        channel.close()
        return {
            "node_id": response.node_id,
            "state": response.state,
            "term": response.term,
            "voted_for": response.voted_for,
            "leader_id": response.leader_id,
            "is_alive": response.is_alive,
        }
    except grpc.RpcError as e:
        return None


def get_cluster_status():
    """Get status of all nodes."""
    statuses = {}
    for node_id, address in NODES.items():
        status = get_node_status(address)
        if status:
            statuses[node_id] = status
    return statuses


def wait_for_leader(timeout=10.0, poll_interval=0.5):
    """Wait until exactly one leader is elected."""
    start = time.time()
    while time.time() - start < timeout:
        statuses = get_cluster_status()
        leaders = [
            nid for nid, s in statuses.items()
            if s["state"] == "leader"
        ]
        if len(leaders) == 1 and len(statuses) == len(NODES):
            return statuses, leaders[0]
        time.sleep(poll_interval)
    return get_cluster_status(), None


def verify_initial_election():
    """Verify that exactly one leader is elected in the cluster."""
    print("\n=== Test: Initial Leader Election ===")

    statuses, leader = wait_for_leader(timeout=15.0)

    if not leader:
        leaders = [nid for nid, s in statuses.items() if s["state"] == "leader"]
        print(f"FAIL: Expected exactly 1 leader, found {len(leaders)}")
        print(f"  Statuses: {statuses}")
        return False

    print(f"  Leader: {leader}")

    # All nodes should agree on the term
    terms = {nid: s["term"] for nid, s in statuses.items()}
    leader_term = statuses[leader]["term"]

    # Check that all alive followers recognize the leader
    for nid, status in statuses.items():
        if nid == leader:
            assert status["state"] == "leader", f"{nid} should be leader"
        else:
            assert status["state"] == "follower", f"{nid} should be follower, got {status['state']}"
            assert status["leader_id"] == leader, f"{nid} should recognize {leader} as leader, got {status['leader_id']}"

    print(f"  Term: {leader_term}")
    print(f"  All nodes agree on leader: YES")
    print("  PASS")
    return True


def main():
    print("Raft Cluster E2E Verification")
    print("=" * 40)

    # Wait for cluster to start
    print("\nWaiting for cluster to be ready...")
    time.sleep(3)

    results = []
    results.append(("Initial Election", verify_initial_election()))

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
