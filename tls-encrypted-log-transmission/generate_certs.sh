#!/bin/sh
# Generate self-signed CA and server certificate with SANs.
# Designed for Alpine (sh, not bash) — uses temp files instead of process substitution.

set -e

CERT_DIR="${1:-/app/certs}"
mkdir -p "$CERT_DIR"

# Generate CA key and certificate
openssl genrsa -out "$CERT_DIR/ca.key" 2048 2>/dev/null
openssl req -new -x509 -days 365 -key "$CERT_DIR/ca.key" \
    -out "$CERT_DIR/ca.crt" \
    -subj "/CN=TLS Log CA" 2>/dev/null

# Generate server key and CSR
openssl genrsa -out "$CERT_DIR/server.key" 2048 2>/dev/null
openssl req -new -key "$CERT_DIR/server.key" \
    -out "$CERT_DIR/server.csr" \
    -subj "/CN=tls-server" 2>/dev/null

# Create SAN extension file (temp file for sh compatibility)
SAN_FILE=$(mktemp)
cat > "$SAN_FILE" <<EOF
subjectAltName=DNS:localhost,DNS:tls-server,IP:127.0.0.1
EOF

# Sign server cert with CA
openssl x509 -req -days 365 \
    -in "$CERT_DIR/server.csr" \
    -CA "$CERT_DIR/ca.crt" \
    -CAkey "$CERT_DIR/ca.key" \
    -CAcreateserial \
    -out "$CERT_DIR/server.crt" \
    -extfile "$SAN_FILE" 2>/dev/null

rm -f "$SAN_FILE" "$CERT_DIR/server.csr" "$CERT_DIR/ca.srl"

echo "Certificates generated in $CERT_DIR"
echo "  CA cert:     $CERT_DIR/ca.crt"
echo "  Server cert: $CERT_DIR/server.crt"
echo "  Server key:  $CERT_DIR/server.key"
