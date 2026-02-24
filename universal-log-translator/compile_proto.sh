#!/bin/bash
set -e
echo "Compiling Protocol Buffer definitions..."
mkdir -p src/generated
touch src/generated/__init__.py
if ls proto/*.proto 1>/dev/null 2>&1; then
    python -m grpc_tools.protoc -I./proto --python_out=src/generated proto/*.proto
    echo "Proto compilation complete."
else
    echo "No .proto files found, skipping compilation."
fi
