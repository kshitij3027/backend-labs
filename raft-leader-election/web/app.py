"""Flask + SocketIO web dashboard for Raft cluster visualization."""

import time
import threading
import grpc
from flask import Flask, render_template, jsonify, request
from flask_socketio import SocketIO

# Import proto modules - these are compiled at build time
import sys
sys.path.insert(0, "/app")
from src.proto import raft_pb2, raft_pb2_grpc


app = Flask(__name__)
app.config["SECRET_KEY"] = "raft-dashboard-secret"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")


# Node configuration
NODES = {
    "node-1": "node-1:5001",
    "node-2": "node-2:5002",
    "node-3": "node-3:5003",
    "node-4": "node-4:5004",
    "node-5": "node-5:5005",
}


class ClusterManager:
    """Manages gRPC connections to all Raft nodes."""

    def __init__(self, nodes):
        self._nodes = nodes
        self._channels = {}
        self._admin_stubs = {}

    def _get_stub(self, node_id):
        """Get or create admin stub for a node."""
        if node_id not in self._channels:
            address = self._nodes[node_id]
            self._channels[node_id] = grpc.insecure_channel(address)
            self._admin_stubs[node_id] = raft_pb2_grpc.NodeAdminServiceStub(
                self._channels[node_id]
            )
        return self._admin_stubs[node_id]

    def get_node_status(self, node_id):
        """Get status of a single node."""
        try:
            stub = self._get_stub(node_id)
            response = stub.GetStatus(
                raft_pb2.GetStatusRequest(), timeout=1.0
            )
            return {
                "node_id": response.node_id,
                "state": response.state,
                "term": response.term,
                "voted_for": response.voted_for,
                "leader_id": response.leader_id,
                "is_alive": response.is_alive,
            }
        except grpc.RpcError:
            return {
                "node_id": node_id,
                "state": "unreachable",
                "term": 0,
                "voted_for": "",
                "leader_id": "",
                "is_alive": False,
            }

    def get_cluster_status(self):
        """Get status of all nodes."""
        statuses = {}
        for node_id in self._nodes:
            statuses[node_id] = self.get_node_status(node_id)
        return statuses

    def stop_node(self, node_id):
        """Stop a specific node."""
        try:
            stub = self._get_stub(node_id)
            response = stub.StopNode(
                raft_pb2.StopNodeRequest(graceful=True), timeout=2.0
            )
            return response.success
        except grpc.RpcError:
            return False

    def kill_leader(self):
        """Find and kill the current leader."""
        statuses = self.get_cluster_status()
        for node_id, status in statuses.items():
            if status["state"] == "leader" and status["is_alive"]:
                success = self.stop_node(node_id)
                return {
                    "killed": node_id,
                    "success": success,
                    "term": status["term"],
                }
        return {"killed": None, "success": False, "error": "No leader found"}

    def create_partition(self, group_a, group_b):
        """Create a network partition between two groups of nodes.

        Each node in group_a blocks all nodes in group_b and vice versa.
        """
        results = []
        for node_a in group_a:
            for node_b in group_b:
                addr_b = self._nodes[node_b]
                results.append(self._block_peer(node_a, addr_b))
                addr_a = self._nodes[node_a]
                results.append(self._block_peer(node_b, addr_a))
        return all(results)

    def heal_partition(self):
        """Remove all partition blocks from all nodes."""
        results = []
        for node_id in self._nodes:
            blocked = self._get_blocked_peers(node_id)
            for peer in blocked:
                results.append(self._unblock_peer(node_id, peer))
        return all(results)

    def _block_peer(self, node_id, peer_address):
        try:
            stub = self._get_stub(node_id)
            response = stub.BlockPeer(
                raft_pb2.BlockPeerRequest(peer_address=peer_address), timeout=2.0
            )
            return response.success
        except grpc.RpcError:
            return False

    def _unblock_peer(self, node_id, peer_address):
        try:
            stub = self._get_stub(node_id)
            response = stub.UnblockPeer(
                raft_pb2.BlockPeerRequest(peer_address=peer_address), timeout=2.0
            )
            return response.success
        except grpc.RpcError:
            return False

    def _get_blocked_peers(self, node_id):
        try:
            stub = self._get_stub(node_id)
            response = stub.GetBlockedPeers(
                raft_pb2.GetBlockedPeersRequest(), timeout=2.0
            )
            return list(response.blocked_peers)
        except grpc.RpcError:
            return []

    def get_election_log(self, limit=50):
        """Get election events from all nodes."""
        all_events = []
        for node_id in self._nodes:
            try:
                stub = self._get_stub(node_id)
                response = stub.GetElectionLog(
                    raft_pb2.GetElectionLogRequest(limit=limit), timeout=1.0
                )
                for event in response.events:
                    all_events.append({
                        "timestamp": event.timestamp,
                        "event_type": event.event_type,
                        "node_id": event.node_id,
                        "term": event.term,
                        "details": event.details,
                    })
            except grpc.RpcError:
                pass
        # Sort by timestamp
        all_events.sort(key=lambda e: e["timestamp"])
        return all_events[-limit:]


cluster_manager = ClusterManager(NODES)


# Background poller
def background_poller():
    """Poll cluster status every 200ms and push updates via SocketIO."""
    while True:
        try:
            status = cluster_manager.get_cluster_status()
            socketio.emit("cluster_update", status)
        except Exception:
            pass
        socketio.sleep(0.2)


# Routes
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    return jsonify(cluster_manager.get_cluster_status())


@app.route("/api/kill-leader", methods=["POST"])
def api_kill_leader():
    result = cluster_manager.kill_leader()
    return jsonify(result)


@app.route("/api/election-log")
def api_election_log():
    limit = request.args.get("limit", 50, type=int)
    events = cluster_manager.get_election_log(limit=limit)
    return jsonify(events)


@app.route("/api/partition", methods=["POST"])
def api_create_partition():
    data = request.get_json()
    group_a = data.get("group_a", [])
    group_b = data.get("group_b", [])
    if not group_a or not group_b:
        return jsonify({"success": False, "error": "Both groups required"}), 400
    success = cluster_manager.create_partition(group_a, group_b)
    return jsonify({"success": success, "group_a": group_a, "group_b": group_b})


@app.route("/api/heal-partition", methods=["POST"])
def api_heal_partition():
    success = cluster_manager.heal_partition()
    return jsonify({"success": success})


@socketio.on("connect")
def handle_connect():
    status = cluster_manager.get_cluster_status()
    socketio.emit("cluster_update", status)


def main():
    socketio.start_background_task(background_poller)
    socketio.run(app, host="0.0.0.0", port=8080, debug=False)


if __name__ == "__main__":
    main()
