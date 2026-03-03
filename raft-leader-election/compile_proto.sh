#!/bin/bash
set -e

PROTO_DIR="src/proto"
OUT_DIR="src/proto"

echo "Compiling proto files..."
python -m grpc_tools.protoc \
    -I"$PROTO_DIR" \
    --python_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    "$PROTO_DIR/raft.proto"

# Fix imports in generated files (use relative imports)
sed -i.bak 's/^import raft_pb2/from . import raft_pb2/' "$OUT_DIR/raft_pb2_grpc.py"
rm -f "$OUT_DIR/raft_pb2_grpc.py.bak"

echo "Proto compilation complete."
