#!/bin/sh
set -e
CERT_DIR="${CERT_DIR:-./certs}"
mkdir -p "$CERT_DIR"

# Generate CA key and cert
openssl genrsa -out "$CERT_DIR/ca.key" 2048
openssl req -new -x509 -days 365 -key "$CERT_DIR/ca.key" -out "$CERT_DIR/ca.crt" \
  -subj "/CN=LoadGenCA"

# Generate server key and CSR
openssl genrsa -out "$CERT_DIR/server.key" 2048
openssl req -new -key "$CERT_DIR/server.key" -out "$CERT_DIR/server.csr" \
  -subj "/CN=perf-server"

# Sign with CA, including SAN for localhost and perf-server
cat > "$CERT_DIR/san.cnf" <<SANEOF
[req]
distinguished_name = req_distinguished_name
[req_distinguished_name]
[v3_ext]
subjectAltName = DNS:localhost,DNS:perf-server,IP:127.0.0.1
SANEOF

openssl x509 -req -days 365 -in "$CERT_DIR/server.csr" -CA "$CERT_DIR/ca.crt" \
  -CAkey "$CERT_DIR/ca.key" -CAcreateserial -out "$CERT_DIR/server.crt" \
  -extfile "$CERT_DIR/san.cnf" -extensions v3_ext

# Cleanup CSR and temp files
rm -f "$CERT_DIR/server.csr" "$CERT_DIR/san.cnf" "$CERT_DIR/ca.srl"

echo "Certificates generated in $CERT_DIR"
