#!/bin/bash
set -e
echo "Compiling Protocol Buffer definitions..."
mkdir -p src/generated
protoc -I/usr/include -I./proto --python_out=src/generated proto/log_entry.proto
echo "Proto compilation complete."
