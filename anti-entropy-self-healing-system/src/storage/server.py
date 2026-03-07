import os
from flask import Flask, request, jsonify
from src.config import Config
from src.storage.store import StorageStore

node_id = os.getenv("NODE_ID", "unknown")
store = StorageStore(node_id=node_id)
app = Flask(__name__)


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "healthy", "node_id": store.node_id})


@app.route("/data/<key>", methods=["GET"])
def get_data(key: str):
    entry = store.get(key)
    if entry is None:
        return jsonify({"error": "not found", "key": key}), 404
    return jsonify({
        "key": key,
        "value": entry.value,
        "version": entry.version,
        "timestamp": entry.timestamp,
    })


@app.route("/data/<key>", methods=["PUT"])
def put_data(key: str):
    body = request.get_json(force=True)
    value = body.get("value", "")
    version = body.get("version")
    timestamp = body.get("timestamp")
    entry = store.put(key, value, version=version, timestamp=timestamp)
    return jsonify({
        "key": key,
        "value": entry.value,
        "version": entry.version,
        "timestamp": entry.timestamp,
    }), 200


@app.route("/data", methods=["GET"])
def get_all_data():
    all_entries = store.get_all()
    entries = {}
    for k, e in all_entries.items():
        entries[k] = {
            "value": e.value,
            "version": e.version,
            "timestamp": e.timestamp,
        }
    return jsonify({"entries": entries})


@app.route("/keys", methods=["GET"])
def get_keys():
    return jsonify({"keys": store.keys()})


@app.route("/merkle/root", methods=["GET"])
def merkle_root():
    return jsonify({"root_hash": store.get_merkle_root(), "node_id": store.node_id})


@app.route("/merkle/leaves", methods=["GET"])
def merkle_leaves():
    return jsonify({"leaves": store.get_merkle_leaves(), "node_id": store.node_id})


if __name__ == "__main__":
    app.run(host=Config.NODE_HOST, port=Config.NODE_PORT)
