"""Standalone smoke-test client — connects to the TCP log server and sends sample messages."""

import json
import socket
import sys


def send_message(sock: socket.socket, level: str, message: str):
    """Send a single NDJSON message and print the server's response."""
    payload = json.dumps({"level": level, "message": message}) + "\n"
    sock.sendall(payload.encode("utf-8"))

    response = b""
    while b"\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            break
        response += chunk

    print(f"  Sent: [{level}] {message}")
    print(f"  Recv: {response.decode('utf-8').strip()}")
    print()


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000

    print(f"Connecting to {host}:{port}...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5.0)
    sock.connect((host, port))
    print(f"Connected!\n")

    try:
        send_message(sock, "DEBUG", "this is a debug message")
        send_message(sock, "INFO", "application started")
        send_message(sock, "WARNING", "memory usage high")
        send_message(sock, "ERROR", "disk full")
        send_message(sock, "CRITICAL", "system crash imminent")
    finally:
        sock.close()
        print("Connection closed.")


if __name__ == "__main__":
    main()
